/* Copyright 2026 - 2026 wzycc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */
// job_registry.cpp — the registry facade.
//
// Durable state lives in JobStore (JobRecord); kernel attribution lives in
// KernelAttribution; the RPC surface lives in inventory_rpc.cpp. This file
// keeps the CollectorScheduler-facing seams: lifecycle callbacks, the
// collection applier, retirement/drain flow and the automatic-selection
// trigger.
#include "core/job_registry.hpp"
#include <spdlog/spdlog.h>
#include <date/date.h>
#include "common/config.hpp"
#include <signal.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <thread>
#include <chrono>
#include <filesystem>
#include <algorithm>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>

#include "core/job_identity.hpp"

#include "core/reentrancy_gate.hpp"
#include "core/inventory_config.hpp"
#include "core/inventory_rpc.hpp"
#include "core/collector_registry.hpp"
#include "common/utils.hpp"
#include "common/common_job.hpp"
#include "common/local_rpc.hpp"
#include "core/group_probe_stats.hpp"
#include <algorithm>

#include <set>
#include <unordered_set>
#include <type_traits>

// Maintenance-side inventory health (source/queue/reconciliation state) is
// published by the heartbeat through this seam. When no provider
// is registered, the status handler serves the explicit "not_configured"
// marker instead of guessing a source state.
#if __has_include("core/inventory_status_provider.hpp")
#include "core/inventory_status_provider.hpp"
#define JOBLENS_HAS_INVENTORY_STATUS_PROVIDER 1
#endif

// Collector names are collector-instance names from
// collectors_config.collectors[].name (what CollectorNames historically meant)
// plus registered collector types. Unknown names are rejected before mutation.
static std::set<std::string> known_collector_names()
{
    std::set<std::string> names;
    for (const auto& name : CollectorRegistry::instance().list()) names.insert(name);
    try {
        const auto collectors = Config::instance().getRawNode("collectors_config")["collectors"];
        if (collectors && collectors.IsSequence()) {
            for (const auto& item : collectors) {
                if (item["name"] && item["name"].IsScalar())
                    names.insert(item["name"].as<std::string>());
            }
        }
    } catch (const std::exception& error) {
        spdlog::warn("JobRegistry: could not read configured collector names: {}", error.what());
    }
    return names;
}

// Loads a rules-mode policy during startup and reports the outcome once. The
// engine is otherwise materialized on the first observed job, which would make
// a missing directory or a broken rule a registration-time surprise.
static void load_rules_at_startup(const std::string& adapter_name,
                                  AutoCollectorPolicy& policy)
{
    if (!policy.uses_rules()) return;   // default/off policies have no engine
    std::string error;
    const std::size_t loaded = policy.load(error);
    if (!error.empty()) {
        spdlog::error("JobRegistry: automatic rules for '{}' could not load: {}",
                      adapter_name, error);
    } else if (loaded == 0) {
        spdlog::warn("JobRegistry: auto_add=rule for '{}' but no rule file matched the "
                     "configured prefix; jobs of this type will get no automatic selection",
                     adapter_name);
    } else {
        spdlog::info("JobRegistry: loaded {} rule(s) for '{}'", loaded, adapter_name);
    }
}

// ---------------------------------------------------------------------------
// Inventory record helpers
// ---------------------------------------------------------------------------

namespace {

int64_t now_us()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// Stable map key for one process generation. The triple is what makes a pid a
// process; comparing bare pids would let a recycled number alias an old
// manager.
std::string process_identity_key(const ProcessIdentity& identity)
{
    return fmt::format("{}:{}:{}", identity.pid, identity.start_ticks, identity.boot_id);
}

JobStore::WriteOp put_record_op(const JobRecord& record) {
    JobStore::WriteOp op;
    op.kind = JobStore::WriteOp::PutRecord;
    op.internal_id = record.job.JobID;
    op.record = record;
    return op;
}

JobStore::WriteOp delete_record_op(uint64_t internal_id) {
    JobStore::WriteOp op;
    op.kind = JobStore::WriteOp::DeleteRecord;
    op.internal_id = internal_id;
    return op;
}

JobStore::WriteOp identity_op(JobStore::WriteOp::Kind kind, uint64_t internal_id,
                              SchedulerKey key) {
    JobStore::WriteOp op;
    op.kind = kind;
    op.internal_id = internal_id;
    op.key = std::move(key);
    return op;
}

// Screen native fields here; adapters screen structured namespaces below.

// The surviving rows of the typed-facts duplicate screen, shared by the
// CREATE dedup and the UPDATE fallback: sub_attr match plus the
// adapter-owned namespace limit (structured facts disagree = different job).
// The namespace semantics live with the adapters; this screen only drives
// the comparison through the registry facade.
std::vector<JobRecord> scheduler_screened_duplicates(
    const JobRegistry& registry, const Job& request, std::vector<JobRecord> rows) {
    const auto request_facts = registry.namespaceFactsOf(request);
    std::vector<JobRecord> out;
    for (auto& row : rows) {
        if (!namespace_facts_compatible(request_facts, registry.namespaceFactsOf(row.job)))
            continue;
        out.push_back(std::move(row));
    }
    return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// RecordTxn — the one durable write path for record state
// ---------------------------------------------------------------------------
// Every record mutation is one atomic JobStore batch plus the memory image
// that batch produces, and the two never separate: the caller builds the image
// it wants, commit() writes both, and a refused write publishes nothing.
// Callers hold mutation_mtx_ across build -> commit, so a decided revision
// cannot move before its batch lands.
class JobRegistry::RecordTxn {
public:
    RecordTxn(JobRegistry& registry, const char* reason, bool durable = true)
        : registry_(registry), reason_(reason), durable_(durable)
    {
    }

    // PutRecord plus its memory image (insert-or-assign).
    RecordTxn& put(JobRecord record)
    {
        cacheKey(record);
        ops_.push_back(put_record_op(record));
        assign_.push_back(std::move(record));
        return *this;
    }

    // PutRecord plus the record's i: entry, published together. The facts may
    // not name a job on the manual-registration path, so the entry is written
    // only when the derived key is complete.
    RecordTxn& putIndexed(JobRecord record)
    {
        const uint64_t internal_id = record.job.JobID;
        std::string index_key;
        std::optional<SchedulerKey> key = registry_.keyOf(record.job);
        if (key) {
            index_key = key->index_key();
            record.index_key = index_key;
        }
        put(std::move(record));
        if (key) {
            index_put_.emplace_back(std::move(index_key), internal_id);
            ops_.push_back(
                identity_op(JobStore::WriteOp::PutIdentity, internal_id, std::move(*key)));
        }
        return *this;
    }

    // PutRecord whose image replaces an existing map entry only: a row that is
    // archived while this batch commits must not be resurrected by publication.
    RecordTxn& replace(JobRecord record)
    {
        cacheKey(record);
        ops_.push_back(put_record_op(record));
        replace_.push_back(std::move(record));
        return *this;
    }

    // DeleteRecord, the record's i: release when the facts name a job, and
    // removal of its memory image.
    RecordTxn& erase(const JobRecord& record)
    {
        const uint64_t internal_id = record.job.JobID;
        ops_.push_back(delete_record_op(internal_id));
        if (const auto key = registry_.keyOf(record.job)) {
            ops_.push_back(
                identity_op(JobStore::WriteOp::DeleteIdentity, internal_id, *key));
        }
        erased_.push_back(internal_id);
        return *this;
    }

    // i: maintenance that carries no record image of its own.
    RecordTxn& indexPut(const SchedulerKey& key, uint64_t internal_id)
    {
        index_put_.emplace_back(key.index_key(), internal_id);
        ops_.push_back(identity_op(JobStore::WriteOp::PutIdentity, internal_id, key));
        return *this;
    }

    RecordTxn& indexRelease(const SchedulerKey& key, uint64_t internal_id)
    {
        index_release_.emplace_back(key.index_key(), internal_id);
        ops_.push_back(identity_op(JobStore::WriteOp::DeleteIdentity, internal_id, key));
        return *this;
    }

    // h: archive row of a deleting batch.
    RecordTxn& history(const JobRecord& record, std::vector<std::string> applied,
                       const std::string& provenance)
    {
        JobStore::WriteOp op;
        op.kind = JobStore::WriteOp::PutHistory;
        op.internal_id = record.job.JobID;
        op.record = record;
        op.applied_collectors = std::move(applied);
        op.history_provenance = provenance;
        ops_.push_back(std::move(op));
        return *this;
    }

    // c: allocator high-water mark riding a create.
    RecordTxn& reserveCounter(uint64_t durable_counter, bool exhausted)
    {
        JobStore::WriteOp op;
        op.kind = JobStore::WriteOp::ReserveCounter;
        op.durable_counter = durable_counter;
        op.counter_exhausted = exhausted;
        ops_.push_back(std::move(op));
        return *this;
    }

    // Names a row whose previous image this batch overwrites (the recovery
    // fence). A create passes none: it published no image to revert.
    RecordTxn& touch(uint64_t internal_id)
    {
        touched_.push_back(internal_id);
        return *this;
    }

    // Commit the batch and publish the images it wrote. False publishes
    // nothing and leaves the store's error in error().
    bool commit()
    {
        if (!registry_.store_->commit(std::move(ops_), durable_, touched_, reason_, error_)) {
            return false;
        }
        publish();
        return true;
    }

    const std::string& error() const { return error_; }

private:
    // The durable i: key is a cache of the derived key: the store needs it to
    // release the entry during recovery, where no adapter is available.
    void cacheKey(JobRecord& record)
    {
        if (!record.index_key.empty()) return;
        if (const auto key = registry_.keyOf(record.job)) record.index_key = key->index_key();
    }

    // One critical section: no reader can observe a half-published batch.
    void publish()
    {
        if (assign_.empty() && replace_.empty() && erased_.empty() && index_put_.empty() &&
            index_release_.empty()) {
            return;
        }
        std::unique_lock lg(registry_.records_mtx_);
        for (auto& record : assign_)
            registry_.records_[record.job.JobID] = std::move(record);
        for (auto& record : replace_) {
            const auto it = registry_.records_.find(record.job.JobID);
            if (it != registry_.records_.end()) it->second = std::move(record);
        }
        for (const auto& entry : index_release_) {
            const auto it = registry_.identity_index_.find(entry.first);
            if (it != registry_.identity_index_.end() && it->second == entry.second)
                registry_.identity_index_.erase(it);
        }
        for (const auto& entry : index_put_)
            registry_.identity_index_[entry.first] = entry.second;
        for (const auto internal_id : erased_)
            registry_.eraseRecordLocked(internal_id);
    }

    JobRegistry& registry_;
    const char* reason_;
    bool durable_;
    std::vector<JobStore::WriteOp> ops_;
    std::vector<uint64_t> touched_;
    std::vector<JobRecord> assign_;
    std::vector<JobRecord> replace_;
    std::vector<uint64_t> erased_;
    std::vector<std::pair<std::string, uint64_t>> index_put_;
    std::vector<std::pair<std::string, uint64_t>> index_release_;
    std::string error_;
};

SubAttrMatcher JobRegistry::duplicateMatcherOf(const Job& job) const {
    if (const auto* adapter = namespaceAdapterOf(job))
        return adapter->duplicateMatcher(job);
    SubAttrMatcher matcher;
    matcher.subtype = job.subtype;
    return matcher;
}

NamespaceFacts JobRegistry::namespaceFactsOf(const Job& job) const {
    const auto* adapter = namespaceAdapterOf(job);
    if (adapter != nullptr) return adapter->namespaceFacts(job);
    return {job.cluster_name, job.clusterTag};
}

NamespaceFacts JobRegistry::selectorFactsOf(const NativeSelector& selector) const {
    const auto* adapter = namespaceAdapterNamed(selector.adapter);
    if (adapter != nullptr) return adapter->selectorNamespaceFacts(selector);
    return {selector.cluster_name, selector.cluster_tag};
}

const ISchedulerAdapter* JobRegistry::adapterNamed(const std::string& name) const {
    return namespaceAdapterNamed(name);
}

bool JobRegistry::adoptNamespaceAliases(Job& job, const std::string& cluster_name,
                                        const std::string& cluster_tag) const {
    if (const auto* adapter = namespaceAdapterOf(job))
        return adapter->adoptNamespaceAliases(job, cluster_name, cluster_tag);
    // No registered adapter answers for this scheduler: the generic business
    // keeps the aliases in the common fields — the same fields the generic
    // comparisons read.
    job.cluster_name = cluster_name;
    job.clusterTag = cluster_tag;
    return true;
}

void JobRegistry::projectRecordFacts(Job& job) const {
    if (const auto* adapter = namespaceAdapterOf(job)) adapter->projectFacts(job);
}

void JobRegistry::normalizeNamespaceFacts(Job& job) const {
    if (const auto* adapter = namespaceAdapterOf(job)) {
        // Ingress and restored facts may carry namespace aliases only in the
        // common fields. Adopt them into empty typed slots (facts win), then
        // render the typed namespace back to the common fields.
        adapter->adoptNamespaceAliases(job, job.cluster_name, job.clusterTag);
        adapter->renderNamespace(job);
    }
}

ISchedulerAdapter* JobRegistry::namespaceAdapterNamed(const std::string& name) const {
    if (name.empty()) return nullptr;
    for (const auto& adapter : adapters_)
        if (adapter->name() == name) return adapter.get();
    std::lock_guard lock(namespace_adapter_mtx_);
    const auto cached = namespace_adapters_.find(name);
    if (cached != namespace_adapters_.end()) return cached->second.get();
    // Built but never armed: the watcher carries no state until a discovery
    // source starts it, so this instance answers facts questions only.
    auto built = SchedulerAdapterRegistry::instance().build(name, InventoryTypeSettings{});
    if (!built) return nullptr;
    auto* const raw = built.get();
    namespace_adapters_[name] = std::move(built);
    return raw;
}

ISchedulerAdapter* JobRegistry::namespaceAdapterOf(const Job& job) const {
    const std::string* wanted = &job.subtype_name;
    std::string enum_fallback;
    if (wanted->empty()) {
        const char* name = adapter_name_for(job.subtype);
        if (name == nullptr) return nullptr;
        enum_fallback = name;
        wanted = &enum_fallback;
    }
    return namespaceAdapterNamed(*wanted);
}

std::optional<SchedulerKey> JobRegistry::keyOf(const Job& job) const {
    // The adapter owning the facts derives the key from them; interpretation
    // goes through the namespace cache, so it does not depend on which
    // discovery types are configured. Without an adapter the generic selector
    // is the best the facts can say — and nullopt is reported, never a guess.
    if (const auto* adapter = namespaceAdapterOf(job)) return adapter->keyOf(job);
    return canonicalize(selector_of(job));
}


// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

JobRegistry::JobRegistry() : JobRegistry(JobRegistryOptions{}) {}

JobRegistry::JobRegistry(const JobRegistryOptions& options) {
    Job fake_job;
    fake_job.JobID = 0;
    set_pids(fake_job, {1});
    fake_job.jobtype = JobType::Sys;
    fake_job.subtype = JobSubType::Common;
    fake_job.sub_attr = CommonJobAttr{
        .auto_update_child = false
    };
    fake_job.CollectorNames = {"Easter egg"}; //实际上这里的添加方式不一样，所以无所谓这里的名字
    sentinel_job_ = fake_job; // 直接持有避免拉起回调

    // Liveness probe: the admission gate and restore validation both need a
    // positive process-identity check, so the production probe is installed
    // before the store restores anything.  A test may substitute it.
    process_probe_ = options.test_process_probe ? options.test_process_probe
                                                : make_proc_liveness_probe();

    // The registry owns the records; the store is the persistence executor
    // (it opens the db and admits the schema). Rows are loaded and validated
    // with the same probe the admission gate uses, so a row cannot be
    // published as verified here and refused there (or worse, vice versa).
    openStore(options);
    restoreExistingRecords();

    // Selection validation is registry-owned: one gate for the RPC surface and
    // the automatic-selection path.
    installCollectorValidator();

    // Attribution owns the kernel map namespace, so it must exist before any
    // discovery admission or collector attachment. The capability gate itself
    // is installed by armInventory below.
    if (options.enable_pid_tracker) init_pid_tracker();

    // Inventory startup, in the order that matters: reset the heartbeat
    // bookkeeping and install the registration sink (both BEFORE the wiring
    // decision, so a Disabled registry exits in the same state as today),
    // then arm the components (adapters, policies, probes, capability gate),
    // then admit the restored owners through that gate, then start the sources
    // (the sink must exist before any poll thread), run the first
    // authoritative pass (blocking: the inventory is settled before the RPC
    // surface opens), publish the status providers and finally start the
    // maintenance worker. Production always arms this stack (a registered job
    // needs retirement/checkpoint upkeep); the purely static form is a
    // test-only escape hatch (test_static_inventory).
    InventoryWiring wiring = InventoryWiring::Disabled;
    if (!options.test_static_inventory) {
        resetHeartbeatState();
        installHeartbeatSink();
        wiring = armInventory(options);
    }

    // Restored active records must be re-seeded into the kernel namespace this
    // process now owns, before any collector can attach to them. This runs
    // after arming: the capability gate decides which adapters may participate,
    // and a record admitted before the gate exists would be seeded
    // unconditionally (capable() defaults to true while no lookup is armed).
    admitRestoredOwners();

    if (!options.test_static_inventory && wiring != InventoryWiring::Disabled) {
        startDiscoverySources(wiring);
        runInitialInventoryPass();
        publishStatusProvider();
        startHeartbeat();
        if (wiring == InventoryWiring::Test)
            spdlog::info("JobRegistry: heartbeat running with {} test source(s)",
                         hb_sources_.size());
        else
            spdlog::info("JobRegistry: inventory heartbeat running with {} discovery type(s)",
                         hb_sources_.size());
    }

    if (options.register_rpc) regRPCHandle();
}

void JobRegistry::openStore(const JobRegistryOptions& options)
{
    JobStore::Options store_options;
    store_options.db_path = options.db_path;
    // Row projection belongs to the adapter that owns the facts. The store
    // keeps parsing the envelope; it never names a scheduler.
    store_options.row_decoder = [this](const nlohmann::json& row) {
        return decodeStoredJob(row);
    };
    store_ = std::make_unique<JobStore>(store_options);
    store_->setRowProvider([this](uint64_t id) -> std::optional<JobRecord> {
        std::shared_lock lg(records_mtx_);
        const auto it = records_.find(id);
        if (it == records_.end()) return std::nullopt;
        return it->second;
    });
    if (store_->ready()) {
        const auto health = store_->health();
        spdlog::info("JobRegistry: store ready (schema {}, migration {})",
                     health.schema_version, health.migration_state);
    } else {
        spdlog::warn("JobRegistry: job db not running");
    }
}

Job JobRegistry::decodeStoredJob(const nlohmann::json& row) const
{
    Job skeleton;
    skeleton.subtype = static_cast<JobSubType>(row.value("subtype", 0));
    skeleton.subtype_name = row.value("subtype_name", "");
    // Legacy rows written before subtype_name moved onto the Job carry the
    // adapter only under "adapter"; use it for decoder selection too.
    if (skeleton.subtype_name.empty())
        skeleton.subtype_name = row.value("adapter", "");
    if (const auto* adapter = namespaceAdapterOf(skeleton))
        return adapter->decodeFacts(row);

    // Open-world / Common rows: the facts channel is JSON, or the generic
    // common attributes. A typed row with no adapter keeps its stored facts
    // as JSON rather than guessing a scheduler schema.
    Job job;
    job.jobtype = static_cast<JobType>(row.value("jobtype", 0));
    job.subtype = skeleton.subtype;
    job.subtype_name = skeleton.subtype_name;
    set_pids(job, row.value("pids", nlohmann::json::array()).get<std::vector<pid_t>>());
    job.NativeJobID = row.value("native_job_id", "");
    job.cluster_name = row.value("cluster_name", "");
    job.clusterTag = row.value("clusterTag", "");
    if (!job.subtype_name.empty()) {
        job.sub_attr = row.contains("sub_attr") && row["sub_attr"].is_object()
                           ? row["sub_attr"]
                           : nlohmann::json::object();
    } else if (job.subtype == JobSubType::Common || job.subtype == JobSubType::Unknown) {
        job.sub_attr = CommonJobAttr{.auto_update_child = true};
    } else {
        job.sub_attr = row.contains("sub_attr") && row["sub_attr"].is_object()
                           ? row["sub_attr"]
                           : nlohmann::json::object();
    }
    return job;
}

void JobRegistry::restoreExistingRecords()
{
    auto rows = store_->loadAll();
    validateRestoredExecutions(rows);
    // Adopt any common namespace aliases into the typed facts before the
    // restored rows become visible.
    for (auto& row : rows) normalizeNamespaceFacts(row.job);
    for (auto& row : rows) publishRecord(std::move(row));
}

void JobRegistry::installCollectorValidator()
{
    const auto known = known_collector_names();
    setCollectorValidator(
        [known](const std::string& name) { return known.count(name) != 0; });
}

void JobRegistry::runInitialInventoryPass()
{
    if (!runPass(true, /*wait=*/true)) {
        spdlog::warn("JobRegistry: the initial inventory pass did not run");
    }
}

JobRegistry& JobRegistry::instance() {
    static JobRegistry reg;
    return reg;
}

// ---------------------------------------------------------------------------
// Inventory heartbeat — the maintenance function of the registry.
// Thread-free policy components (probes, retirement rules, absence counters)
// are driven from one periodic worker; there is no separate service object.
// ---------------------------------------------------------------------------

namespace {
constexpr int kAbsencesBeforeRetirement = 2;
constexpr std::size_t kEndedExecutionCapacity = 4096;
constexpr std::uint32_t kWaitSliceMs = 100;
}  // namespace

std::int64_t JobRegistry::clockNow() const {
    return now_epoch_ms();
}

void JobRegistry::noteHeartbeatError(const std::string& error) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    degraded_ = true;
    hb_last_error_ = error;
    ++hb_error_epoch_;
}

void JobRegistry::notifyExit(pid_t pid) {
    // Exit evidence is coalesced into one authoritative pass request: the
    // cheap-and-bounded rule the queued hint used to provide, without a
    // queue. The heartbeat cooldown still limits forced scans.
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_events_received_;
    }
    (void)pid;
    requestRescan("exit hint");
}

void JobRegistry::requestRescan(const std::string& reason) {
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_rescan_requests_;
        // Wake the maintenance loop instead of waiting for its next 100 ms
        // slice: the first request of a burst is serviced at once, while the
        // forced-scan cooldown keeps a job-start storm from becoming a scan
        // storm.
        hb_wake_ = true;
        hb_cv_.notify_all();
    }
    for (const auto& [name, source] : hb_sources_) {
        try {
            source->requestRescan();
        } catch (const std::exception& error) {
            spdlog::error("JobRegistry: rescan request failed ({}): {}", reason, error.what());
        }
    }
}

bool JobRegistry::rescanRequested() const {
    for (const auto& [name, source] : hb_sources_) {
        try {
            if (source->health().rescan_requested) return true;
        } catch (const std::exception& error) {
            spdlog::debug("JobRegistry: source health failed: {}", error.what());
        }
    }
    return false;
}

UpsertResult JobRegistry::processObservation(const Job& job, std::set<uint64_t>& observed,
                                            std::set<uint64_t>& created) {
    // Discovery entry: this runs for a manager the pass did not track. The
    // result is the acceptance signal: a refused merge must leave the manager
    // unknown so the next pass observes it again (an identity conflict means a
    // different live execution already owns the identity; marking it tracked
    // would both hide the new manager and keep the old record seen forever).
    UpsertResult result;
    const auto key = keyOf(job);
    const ProcessIdentity manager = manager_of(job);
    if (key && manager.complete() && isObsolete(*key, manager)) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_obsolete_hints_;
        spdlog::debug("JobRegistry: discarded hint for the ended execution of '{}'",
                      key->native_job_id);
        result.message = "obsolete execution generation";
        return result;
    }

    // Readiness: the facts name a job AND the execution generation is complete.
    if (!key || !manager.complete()) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_incomplete_observations_;
        spdlog::debug("JobRegistry: incomplete discovery '{}'", job.NativeJobID);
    }

    // Admission is a creation gate. An identity the registry already owns (a
    // manual row whose execution is being discovered, or a record an event
    // registered first) evolves without re-running the policy; capture_all
    // off + Off must not drop the observation that completes its manager.
    if (key && ownRecordByKey(*key).has_value()) {
        result = upsertObservedJob(job);
    } else {
        const auto admission = admissionDecision(job);
        if (!admission) {
            result.fail(RegistrationError::NotAdmitted,
                        "the inventory admission policy refused this observation");
            return result;
        }
        result = upsertObservedJob(job, &*admission);
    }
    if (result.ok) {
        observed.insert(result.internal_id);
        if (result.disposition == UpsertDisposition::Created)
            created.insert(result.internal_id);
        std::lock_guard<std::mutex> lock(hb_mtx_);
        auto& entry = absences_[result.internal_id];
        entry.manager = manager;
        entry.complete_absences = 0;
        entry.last_seen_ms = clockNow();
        entry.last_seen_seq = ++observation_seq_;
        suspect_ids_.erase(result.internal_id);
        return result;
    }

    if (result.error == "incomplete_identity") {
        // An unresolvable identity is not an error of the pass: it stays out
        // of the identity index and is retried on the next observation.
        return result;
    }

    if (result.error == "execution_generation_conflict" ||
        result.error == "identity_quarantined" ||
        result.error == "instance_closing" ||
        result.error == "identity_index_mismatch") {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_upsert_conflicts_;
        hb_last_error_ = result.error + (result.message.empty() ? "" : ": " + result.message);
        spdlog::warn("JobRegistry: discovery for '{}' rejected: {}",
                     job.NativeJobID, result.error);
        return result;
    }

    noteHeartbeatError("inventory upsert failed: " + result.error +
                       (result.message.empty() ? "" : ": " + result.message));
    return result;
}

void JobRegistry::repairMissingInitialDecisions() {
    // Bounded per pass: a large backlog converges over several passes instead
    // of stalling one.
    constexpr std::size_t kMaxPerPass = 128;
    std::size_t repaired = 0;
    for (const auto& record : ownRecords()) {
        if (repaired >= kMaxPerPass) break;
        if (!missing_initial_decision(record)) continue;
        // The snapshot may predate a retirement this pass already applied:
        // re-read the record and decide from the current image, never from a
        // closing or vanished one.
        const auto current = ownRecord(record.job.JobID);
        if (!current || current->state != RecordState::Active) continue;
        const auto decision = autoSelectionFor(current->job, current->job.subtype_name);
        if (!decision) continue;   // policy disarmed since the predicate was read
        const auto outcome = applyAutoPolicy(
            {AutoSelection{current->job.JobID, decision->collectors,
                           decision->policy_revision}});
        if (!outcome.ok) {
            {
                std::lock_guard<std::mutex> lock(hb_mtx_);
                ++hb_decision_failures_;
            }
            spdlog::warn("JobRegistry: initial decision repair for JobID {} was rejected: {}",
                         current->job.JobID, outcome.message);
            continue;
        }
        ++repaired;
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            ++hb_decision_repairs_;
        }
        spdlog::info("JobRegistry: repaired the missing initial decision of JobID {}",
                     current->job.JobID);
    }
}

RefreshResult JobRegistry::probeRecordEvidence(const Job& job) const {
    // No adapter, no scheduler scope: the manager identity (when the record has
    // one) and the recorded member identities are the only evidence. This is
    // the pre-existing member rule, now reported as a RefreshResult so the
    // retirement decision reads one shape.
    RefreshResult result;
    const ProcessIdentity& manager = manager_of(job);
    if (manager.complete()) {
        const LivenessResult liveness =
            process_probe_ ? process_probe_(manager) : LivenessResult{};
        result.manager = liveness.state;
    }
    for (const auto& member : members_of(job)) {
        if (result.any_member_live) break;
        const LivenessResult liveness =
            process_probe_ ? process_probe_(member) : LivenessResult{};
        if (liveness.state == LivenessState::Alive) {
            result.any_member_live = true;
        } else if (liveness.state == LivenessState::Unknown) {
            // An unreadable member might be alive: never death evidence.
            result.member_inconclusive = true;
        }
    }
    return result;
}

void JobRegistry::probeSurvivingMembers(const Job& stored, const Job& refreshed,
                                        RefreshResult& evidence) const {
    if (refreshed.execution.members_authoritative) return;  // a confirmed read wins
    if (!members_of(refreshed).empty()) return;             // the read produced evidence
    for (const auto& member : members_of(stored)) {
        if (evidence.any_member_live) break;
        const LivenessResult liveness =
            process_probe_ ? process_probe_(member) : LivenessResult{};
        if (liveness.state == LivenessState::Alive) {
            evidence.any_member_live = true;
        } else if (liveness.state == LivenessState::Unknown) {
            evidence.member_inconclusive = true;
        }
    }
}

std::optional<JobRegistry::RefreshedEvidence> JobRegistry::applyRefreshedRecord(
    const JobRecord& snapshot, Job refreshed, RefreshResult evidence) {
    RefreshedEvidence outcome;
    outcome.evidence = evidence;
    bool changed = false;
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        // The refresh read ran unlocked: the record may have moved on. Re-read
        // the current image and decide from it, never from the snapshot.
        const auto current = ownRecord(snapshot.job.JobID);
        if (!current || current->state == RecordState::Closing) return std::nullopt;

        // A newer observation committed while this pass was reading: its facts
        // win and its evidence is fresher than this pass' verdict.
        if (current->job.execution.read_order != 0 &&
            refreshed.execution.read_order != 0 &&
            refreshed.execution.read_order < current->job.execution.read_order) {
            return std::nullopt;
        }

        // A refresh never moves identity: the record keeps the facts it was
        // created with. The derived key is only the merge's comparison anchor,
        // so it must resolve; a row whose facts and index cache disagree is
        // left untouched instead of being silently re-indexed.
        const std::optional<SchedulerKey> stored_key = keyOf(current->job);
        if (!stored_key && !current->index_key.empty()) {
            spdlog::warn("JobRegistry: JobID {} carries an index entry its facts no "
                         "longer resolve; refusing to refresh it",
                         current->job.JobID);
            return std::nullopt;
        }
        const SchedulerKey merge_key = stored_key ? *stored_key : SchedulerKey{};

        JobRecord updated;
        const auto result = mergeDiscoveredJob(*current, refreshed, merge_key,
                                               current->job.JobID, now_us(), updated,
                                               MergeKind::Refresh);
        if (!result.ok) {
            {
                std::lock_guard<std::mutex> lock(hb_mtx_);
                ++hb_refresh_failures_;
            }
            spdlog::warn("JobRegistry: refresh of JobID {} was refused: {}",
                         current->job.JobID, result.error);
            return std::nullopt;
        }
        if (result.disposition == UpsertDisposition::Unchanged) {
            // Evidence did not move: no revision, no durable write, no wake.
            // Publish the memory image anyway so the read order and
            // last_seen_us advance exactly like the old repeated-observation
            // path (bookkeeping only).
            outcome.has_member_evidence = !members_of(updated.job).empty();
            {
                std::unique_lock lg(records_mtx_);
                records_[current->job.JobID] = std::move(updated);
            }
            return outcome;
        }

        RecordTxn txn(*this, "refresh inventory evidence", /*durable=*/false);
        txn.touch(current->job.JobID);
        txn.replace(std::move(updated));
        if (!txn.commit()) {
            {
                std::lock_guard<std::mutex> lock(hb_mtx_);
                ++hb_refresh_failures_;
            }
            spdlog::warn("JobRegistry: refresh of JobID {} could not be written: {}",
                         current->job.JobID, txn.error());
            return std::nullopt;
        }
        const auto published = ownRecord(current->job.JobID);
        outcome.has_member_evidence =
            published ? !members_of(published->job).empty() : true;
        changed = true;
    }
    // Evidence changes reach bound consumers through the runtime, which re-reads
    // the record. Outside every registry lock, on an unsynced batch.
    if (changed) wake_runtime();
    return outcome;
}

bool JobRegistry::retireIfConfirmed(const JobRecord& record, const std::string& reason) {
    // Only retire the generation the evidence was gathered for. A late
    // decision for an older execution must never remove newer ownership.
    const auto current = ownRecord(record.job.JobID);
    if (!current) return false;
    if (!(manager_of(current->job) == manager_of(record.job))) {
        spdlog::info("JobRegistry: JobID {} changed execution identity before "
                     "retirement; keeping current ownership", record.job.JobID);
        return false;
    }

    rememberEnded(record);
    if (!retireInstance(record.job.JobID)) {
        noteHeartbeatError("inventory retirement failed for JobID " +
                           std::to_string(record.job.JobID));
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_retirements_;
        closing_executions_.emplace_back(record.job.JobID,
                                           manager_of(record.job));
        absences_.erase(record.job.JobID);
        suspect_ids_.erase(record.job.JobID);
    }
    spdlog::info("JobRegistry: retired JobID {}: {}", record.job.JobID, reason);
    return true;
}

std::optional<JobRegistry::InitialDecision> JobRegistry::admissionDecision(const Job& job) {
    // Blanket registration on: every captured job registers; the
    // automatic selection stays with the post-registration path
    // (apply_auto_policy), so no decision is pre-evaluated here.
    if (inventory_capture_all_) {
        InitialDecision blanket;
        return blanket;
    }

    // Blanket registration off: the type's auto_add tri-state decides. A type
    // without an entry — including subtypes no configured adapter owns —
    // behaves as Off.
    const auto state = auto_add_states_.find(job.subtype_name);
    if (state == auto_add_states_.end() || state->second == AutoAdd::Off) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_dropped_observations_;
        return std::nullopt;
    }

    // Default/Rule: the armed policy carries the answer (the configured list
    // verbatim, or the rule engine's per-job verdict). An armed state without
    // a policy is inconsistent arming; refusing admission keeps the contract
    // (never register without the promised selection) instead of guessing.
    const auto policy = auto_policies_.find(job.subtype_name);
    if (policy == auto_policies_.end() || !policy->second.enabled()) {
        spdlog::error("JobRegistry: auto_add state for '{}' is armed without a policy; "
                      "refusing admission",
                      job.subtype_name);
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_dropped_observations_;
        return std::nullopt;
    }

    std::string revision;
    std::string error;
    const auto decision = policy->second.select(job, revision, error);
    if (!error.empty()) {
        // Engine failure or a refused decision (accepted without collectors
        // and no defaults configured) keeps the old decision: nothing is
        // registered now and the observation is neither a
        // reject nor a drop — the next one re-evaluates with the same
        // evidence plus time.
        spdlog::warn("JobRegistry: admission evaluation failed for '{}' ({}); the "
                     "observation is not registered",
                     job.NativeJobID, error);
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            ++hb_admission_errors_;
        }
        noteHeartbeatError("admission evaluation failed for '" + job.NativeJobID +
                           "': " + error);
        return std::nullopt;
    }
    if (state->second == AutoAdd::Default) {
        // Defaults mode: the (possibly empty) configured list is the initial
        // selection; an empty list registers the job with no collectors.
        InitialDecision default_decision;
        default_decision.initial_collectors = decision.collectors;
        default_decision.policy_revision = revision;
        return default_decision;
    }
    // Rules mode: the rule's own verdict decides — reject = no registration
    // at all when blanket registration is off, unlike the blanket mode where
    // a reject keeps the job inventoried with an empty selection. An accept
    // without collectors has already resolved to the configured defaults
    // inside the policy (or was refused there as an error).
    if (!decision.accepted) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_rejected_observations_;
        return std::nullopt;
    }
    InitialDecision accepted;
    accepted.initial_collectors = decision.collectors;
    accepted.policy_revision = revision;
    return accepted;
}

void JobRegistry::rememberEnded(const JobRecord& record) {
    const auto key = keyOf(record.job);
    if (!key) return;
    std::lock_guard<std::mutex> lock(hb_mtx_);
    ended_executions_[key->key()] = manager_of(record.job);
    while (ended_executions_.size() > kEndedExecutionCapacity) {
        ended_executions_.erase(ended_executions_.begin());
    }
}

bool JobRegistry::isObsolete(const SchedulerKey& key,
                             const ProcessIdentity& generation) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    const auto it = ended_executions_.find(key.key());
    return it != ended_executions_.end() && it->second == generation;
}

void JobRegistry::evaluateRetirements(
    const std::set<uint64_t>& observed_ids,
    const std::map<std::string, bool>& source_complete,
    const std::map<uint64_t, RefreshedEvidence>& refreshed,
    std::uint64_t scan_seq) {
    const auto records = ownRecords();
    const std::int64_t now = clockNow();

    for (const auto& record : records) {
        if (record.job.JobID == 0) continue;
        if (record.state == RecordState::Closing) continue;

        int absences = 0;
        bool seen_this_pass = false;
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            auto& entry = absences_[record.job.JobID];
            const ProcessIdentity manager = manager_of(record.job);
            if (!(entry.manager == manager)) {
                entry.manager = manager;
                entry.complete_absences = 0;
            }
            // A registration accepted on another thread (an exec event) during
            // this pass is not in observed_ids, but its observation sequence is
            // greater than the pass's: counting it absent would mark a job
            // that just registered as suspect and unverified.
            seen_this_pass = observed_ids.count(record.job.JobID) != 0 ||
                             entry.last_seen_seq > scan_seq;
            if (seen_this_pass) {
                entry.complete_absences = 0;
                entry.last_seen_ms = now;
                suspect_ids_.erase(record.job.JobID);
            } else {
                const auto coverage = source_complete.find(record.job.subtype_name);
                const bool absence_allowed =
                    coverage != source_complete.end() && coverage->second;
                if (absence_allowed && entry.complete_absences < kAbsencesBeforeRetirement) {
                    ++entry.complete_absences;
                }
                absences = entry.complete_absences;
                if (absences > 0) suspect_ids_.insert(record.job.JobID);
                else suspect_ids_.erase(record.job.JobID);
            }
        }

        // Absence is liveness evidence, not a lifecycle state: the record
        // stays Open, and an unverified instance is refused kernel admission
        // and never retired. A presence observation restores the verdict; the
        // call is outside hb_mtx_ (records_mtx_ is taken under it, never the
        // other way) and before the seen/no-evidence short-circuits.
        setExecutionVerified(record.job.JobID, absences == 0);
        if (seen_this_pass) continue;

        // The eager refresh this pass already read is the record's evidence;
        // re-probing here would read the same facts twice. A record without a
        // refresh this pass (created now, or one whose refresh lost to newer
        // facts) has no verdict to apply.
        const auto refreshed_it = refreshed.find(record.job.JobID);
        if (refreshed_it == refreshed.end()) continue;
        const RefreshedEvidence& evidence = refreshed_it->second;

        // Records without a validated execution generation (manual
        // registrations, unresolved observations) have no manager to probe.
        // The member list is their only evidence: live or unreadable pids keep
        // the record, an all-dead list retires it, and an empty list is "no
        // evidence yet", never "all dead".
        const ProcessIdentity manager = manager_of(record.job);
        if (!manager.complete()) {
            if (!evidence.has_member_evidence) continue;
            if (evidence.evidence.any_member_live || evidence.evidence.member_inconclusive)
                continue;
            retireIfConfirmed(record,
                              "no live process for a manually registered instance");
            continue;
        }

        // Retire only on positive termination evidence: a dead manager plus a
        // cgroup that was read and is confirmed empty. A readable but
        // non-empty cgroup whose members could not be stat'ed is NOT an empty
        // cgroup, and an unreadable cgroup is no evidence at all.
        if (evidence.evidence.manager == LivenessState::Dead &&
            evidence.evidence.cgroup_empty_confirmed) {
            retireIfConfirmed(
                record, "positive termination evidence (manager ended, execution cgroup empty)");
            continue;
        }

        if (absences < kAbsencesBeforeRetirement) continue;
        // Unknown is not death: an unreadable manager blocks the absence rule
        // exactly like a live one does.
        if (evidence.evidence.manager != LivenessState::Dead) continue;
        if (evidence.evidence.any_member_live || evidence.evidence.member_inconclusive)
            continue;

        retireIfConfirmed(
            record, "two complete source passes without an observation and no live member");
    }

    // Prune heartbeat state for records that no longer exist.
    {
        std::set<uint64_t> live;
        for (const auto& record : records) live.insert(record.job.JobID);
        std::lock_guard<std::mutex> lock(hb_mtx_);
        for (auto it = absences_.begin(); it != absences_.end();) {
            if (live.count(it->first) == 0) it = absences_.erase(it);
            else ++it;
        }
        for (auto it = suspect_ids_.begin(); it != suspect_ids_.end();) {
            if (live.count(*it) == 0) it = suspect_ids_.erase(it);
            else ++it;
        }
    }
}

bool JobRegistry::runPass(bool do_scan, bool wait, bool refresh_facts) {
    // Passes are serialized by pass_mtx_. The worker never waits for a pass
    // (it retries on the next slice), while an on-demand reconcile waits so
    // that returning means "a pass completed".
    std::unique_lock<std::mutex> pass_lock(pass_mtx_, std::defer_lock);
    if (wait) {
        pass_lock.lock();
    } else if (!pass_lock.try_lock()) {
        return false;
    }

    std::uint64_t error_epoch = 0;
    std::uint64_t scan_seq = 0;
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        if (!hb_admitted_ || hb_stopping_) return false;
        // Anything noteHeartbeatError() records from here on belongs to this
        // pass and must survive its own "scan complete" status update. The
        // observation sequence is captured before the scan: a registration
        // accepted during the pass must not be read as an absence.
        error_epoch = hb_error_epoch_;
        scan_seq = observation_seq_;
    }

    const std::int64_t started_ms = clockNow();
    std::set<uint64_t> observed;
    std::set<uint64_t> created;
    std::map<std::string, bool> source_complete;
    bool complete = true;
    std::string error;

    // Manager triple -> record id from the current image, built only when the
    // pass enumerates: a manager this registry already tracks costs no
    // observe() at all.
    std::map<std::string, uint64_t> manager_index;
    if (do_scan) {
        for (const auto& record : ownRecords()) {
            if (record.state == RecordState::Closing) continue;
            const ProcessIdentity& manager = manager_of(record.job);
            if (!manager.complete()) continue;
            manager_index.emplace(process_identity_key(manager), record.job.JobID);
        }

        for (const auto& [name, source] : hb_sources_) {
            // The pass only discovers managers: no job ad, no member read, no
            // fact merge into an existing record.
            ManagerSet managers;
            try {
                managers = source->enumerateManagers();
            } catch (const std::exception& exception) {
                managers.complete = false;
                managers.error =
                    std::string("manager enumeration threw: ") + exception.what();
            }
            if (!managers.complete) {
                const std::string detail =
                    managers.error.empty() ? "incomplete manager pass" : managers.error;
                if (error.empty()) {
                    error = name.empty() ? detail : name + ": " + detail;
                }
            }
            complete = complete && managers.complete;
            // Coverage is keyed by adapter name; a record of one adapter is
            // only absence-eligible when that adapter's source covered its
            // inputs (records with no adapter — manual registrations — never
            // are).
            const auto inserted = source_complete.emplace(name, managers.complete);
            if (!inserted.second)
                inserted.first->second = inserted.first->second && managers.complete;

            for (const auto& manager : managers.managers) {
                if (!manager.complete()) continue;   // a partial entry names nobody
                const auto tracked = manager_index.find(process_identity_key(manager));
                if (tracked != manager_index.end()) {
                    observed.insert(tracked->second);
                    continue;
                }
                std::optional<Job> job;
                try {
                    job = source->observe(manager);
                } catch (const std::exception& exception) {
                    spdlog::warn("JobRegistry: observe() for a '{}' manager failed: {}",
                                 name, exception.what());
                    continue;
                }
                if (!job) continue;   // gone/unreadable: retried, never guessed
                const auto observation = processObservation(*job, observed, created);
                // Only an accepted observation may make this manager
                // "tracked". An identity conflict (a different live execution
                // owns the job) must keep it unknown: mapping it to the old
                // record would hide the new manager and keep the old record
                // permanently "seen", so it could never retire.
                if (!observation.ok) continue;
                uint64_t record_id = 0;
                if (observation.disposition == UpsertDisposition::Created) {
                    record_id = observation.internal_id;
                } else if (const auto key = keyOf(*job)) {
                    if (const auto record = ownRecordByKey(*key);
                        record && manager_of(record->job) == manager) {
                        record_id = record->job.JobID;
                    }
                }
                if (record_id != 0) {
                    manager_index.emplace(process_identity_key(manager), record_id);
                }
            }
        }
    }

    // Eager update: every active record, whatever produced it, goes through the
    // same evidence reader and the same apply. A record created by this pass
    // already carries the observe() image and is skipped. An idle retry tick
    // (refresh_facts=false) re-runs the bookkeeping only: the fact readers are
    // cadence-bound, not 100 ms-tick-bound.
    std::map<uint64_t, RefreshedEvidence> refreshed;
    if (refresh_facts) {
        for (const auto& record : ownRecords()) {
            if (record.job.JobID == 0 || record.state == RecordState::Closing) continue;
            if (created.count(record.job.JobID) != 0) continue;
            Job job = record.job;
            RefreshResult evidence;
            try {
                // Reads run strictly outside every registry lock; the adapter
                // owns the scheduler-specific part, the registry passes the
                // probes.
                if (const auto* adapter = namespaceAdapterOf(job)) {
                    evidence = adapter->refreshFacts(job, process_probe_, cgroup_probe_);
                    probeSurvivingMembers(record.job, job, evidence);
                } else {
                    update_job(job);
                    evidence = probeRecordEvidence(job);
                }
            } catch (const std::exception& exception) {
                {
                    std::lock_guard<std::mutex> lock(hb_mtx_);
                    ++hb_refresh_failures_;
                }
                spdlog::warn("JobRegistry: fact refresh for JobID {} failed: {}",
                             record.job.JobID, exception.what());
                continue;
            }
            if (const auto applied = applyRefreshedRecord(record, std::move(job), evidence))
                refreshed.emplace(record.job.JobID, *applied);
        }
    }

    // Missing initial decisions are a record-driven obligation: repair them on
    // every pass, even when no new observation arrived.
    repairMissingInitialDecisions();

    // Checkpoint: facts and intent are written without a fence on the hot path,
    // so one fsynced batch per pass bounds what a process crash can lose. The
    // pass cadence (reconcile_interval_sec) is the checkpoint cadence on
    // purpose: no extra knob, no extra thread.
    {
        // Keep the row-provider snapshot and its checkpoint write inside the
        // same transaction boundary as record/index mutations.
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        if (!store_->flushDirty("maintenance checkpoint")) {
            spdlog::warn("JobRegistry: the maintenance checkpoint could not be written");
        }
    }

    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_passes_;
        if (do_scan) {
            last_scan_start_ms_ = started_ms;
            last_scan_complete_ = complete;
            if (complete) {
                last_scan_success_ms_ = clockNow();
                // A registration/admission error raised while this pass was
                // running is not a scan failure, but the stack is not healthy
                // either: keep it visible until a later pass observes no new
                // error (a fresh scan error is recorded below).
                if (hb_error_epoch_ == error_epoch) {
                    degraded_ = false;
                    hb_last_error_.clear();
                }
            } else {
                degraded_ = true;
                hb_last_error_ = error.empty() ? "source pass incomplete" : error;
            }
        }
    }

    // A targeted pass has no authoritative source coverage, so it never counts
    // as an absence; only positive termination evidence applies.
    static const std::map<std::string, bool> no_coverage;
    evaluateRetirements(observed, do_scan ? source_complete : no_coverage, refreshed,
                        scan_seq);

    // Closing records are not "done" just because they left the active map.
    // Finalize restored closings and retry a cleanup that failed after its ack;
    // a drain still awaiting its ack is left to the collector runtime.
    reconcileClosingRemovals();

    return true;
}

bool JobRegistry::reconcileInventoryNow() {
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        // A callback running on the heartbeat thread must never wait for a
        // pass it is itself inside of.
        if (hb_worker_id_ != std::thread::id() &&
            hb_worker_id_ == std::this_thread::get_id()) {
            return false;
        }
        if (!hb_admitted_ && !hb_running_) return false;
    }
    return runPass(true, /*wait=*/true);
}

void JobRegistry::heartbeatLoop() {
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        hb_worker_id_ = std::this_thread::get_id();
    }
    const auto interval =
        std::chrono::milliseconds(static_cast<long long>(reconcile_interval_sec_) * 1000);
    const auto slice = std::chrono::milliseconds(kWaitSliceMs);
    auto next_full = std::chrono::steady_clock::now() + interval;

    std::unique_lock<std::mutex> lock(hb_mtx_);
    while (!hb_stopping_) {
        // A slice-bounded wait keeps stop() bounded; a rescan request wakes it
        // at once so the first late-metadata job converges in tens of ms, while
        // the forced-scan cooldown still limits repeats.
        hb_cv_.wait_for(lock, slice, [this]() { return hb_stopping_ || hb_wake_; });
        hb_wake_ = false;
        if (hb_stopping_) break;

        const bool due = std::chrono::steady_clock::now() >= next_full;
        bool rescan = false;
        if (!due) {
            // Discovery events and queue overflows ask for an authoritative
            // pass. Reading source health needs no heartbeat lock.
            lock.unlock();
            rescan = rescanRequested();
            lock.lock();
            if (!rescan || hb_stopping_) continue;
        }

        lock.unlock();
        bool executed = false;
        if (due) {
            executed = runPass(true, false);
            if (executed) next_full = std::chrono::steady_clock::now() + interval;
        } else if (rescan) {
            // Coalesce: one forced pass per second at most, so a job-start
            // storm cannot turn into a /proc scan storm. The cooldown only
            // consumes on a pass that actually ran; a merge with an in-flight
            // pass must not burn the window.
            const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            const bool allowed =
                last_forced_scan_ms_ == 0 || now_ms - last_forced_scan_ms_ >= 1000;
            if (allowed) {
                executed = runPass(true, false);
                if (executed) last_forced_scan_ms_ = now_ms;
            } else {
                // Inside the cooldown window the rescan flag stays set until a
                // pass consumes it, and this loop wakes every 100 ms. Re-read
                // no facts here: that would turn one coalesced request into
                // ~10 full refreshes. Bookkeeping and closing retries only.
                runPass(false, false, /*refresh_facts=*/false);
            }
        }
        lock.lock();
        // A pass already in flight (an explicit reconcile, for example) owns
        // the discovery reads; retry on the next slice (the wait is
        // slice-bounded, so no wake flag is needed).
    }
}

void JobRegistry::startHeartbeat() {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    if (hb_stopping_ || hb_running_) return;
    hb_running_ = true;
    heartbeat_thread_ = std::make_unique<std::thread>([this]() { heartbeatLoop(); });
}

void JobRegistry::installHeartbeatSink() {
    // The single registration entry every discovery path uses: exec events
    // and the sources' own scans hand one discovered Job over on the source's
    // poll thread and get the result back. No worker, no queue, no fsync on
    // this path.
    heartbeat_sink_.on_job = [this](const Job& job) {
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            ++hb_events_received_;
        }
        // An accepted event is positive presence evidence: a pass in flight
        // must not read this record as absent because it was not in that
        // pass' manager set.
        const auto note_seen = [this, &job](uint64_t internal_id) {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            auto& entry = absences_[internal_id];
            entry.manager = manager_of(job);
            entry.complete_absences = 0;
            entry.last_seen_ms = clockNow();
            entry.last_seen_seq = ++observation_seq_;
            suspect_ids_.erase(internal_id);
        };
        // Admission is a creation gate. An identity the registry already owns
        // keeps updating without re-running the policy: a rules-mode Lua
        // evaluation on every exec event would be wasted, and a live record
        // must not stop accepting its own facts because a rule changed.
        bool tracked = false;
        if (const auto key = keyOf(job)) {
            tracked = ownRecordByKey(*key).has_value();
        }
        if (tracked) {
            auto result = upsertObservedJob(job);
            if (result.ok) note_seen(result.internal_id);
            return result;
        }
        // Unknown identity: the same gate as the creation path. A refused
        // observation is not an error of the source, and retry_registration()
        // is false for it, so the watcher never loops rescans against the
        // policy.
        const auto admission = admissionDecision(job);
        if (!admission) {
            UpsertResult refused;
            refused.fail(RegistrationError::NotAdmitted,
                         "the inventory admission policy refused this observation");
            return refused;
        }
        auto result = upsertObservedJob(job, &*admission);
        if (result.ok) note_seen(result.internal_id);
        return result;
    };
}

void JobRegistry::publishStatusProvider() {
    set_inventory_status_provider([this]() { return heartbeatStatusJson(); });
    set_inventory_refresh_provider([this]() { return reconcileInventoryNow(); });
    {
        std::lock_guard<std::mutex> lock(exit_observer_mtx_);
        exit_observer_ = [this](pid_t pid) { notifyExit(pid); };
    }
    std::lock_guard<std::mutex> lock(hb_mtx_);
    status_published_ = true;
}

void JobRegistry::retractStatusProvider() {
    if (status_published_) {
        set_inventory_status_provider({});
        status_published_ = false;
    }
    set_inventory_refresh_provider({});
    {
        std::lock_guard<std::mutex> lock(exit_observer_mtx_);
        exit_observer_ = {};
    }
}

// Heartbeat bookkeeping reset. Idempotent across test registries; runs
// before the wiring decision so a Disabled registry exits with the same
// state it would have had today (sink installed, nothing started).
void JobRegistry::resetHeartbeatState()
{
    std::lock_guard<std::mutex> lock(hb_mtx_);
    hb_admitted_ = true;
    hb_stopping_ = false;
    hb_running_ = false;
    hb_wake_ = false;
    degraded_ = false;
    incomplete_drain_ = false;
    hb_last_error_.clear();
    absences_.clear();
    suspect_ids_.clear();
    ended_executions_.clear();
    closing_executions_.clear();
    last_scan_start_ms_ = 0;
    last_scan_success_ms_ = 0;
    last_scan_complete_ = false;
    hb_passes_ = 0;
    hb_stop_errors_ = 0;
    hb_refresh_failures_ = 0;
}

// Arms the inventory components from the resolved configuration: one adapter
// per configured type, the automatic-selection policies, the production
// liveness probes and the kernel-attribution capability gate. Nothing is
// started here; the caller starts sources and the first pass afterwards, so
// the gate is guaranteed to exist before any attribution can activate.
JobRegistry::InventoryWiring
JobRegistry::armInventory(const JobRegistryOptions& options)
{
    // Symmetric resets first: every exit path (including failures) leaves the
    // same armed-state behind.
    adapters_.clear();
    auto_policies_.clear();
    auto_add_states_.clear();
    inventory_capture_all_ = true;

    // Test wiring: pre-built sources and injected probes, no configuration
    // and no automatic selection.
    if (!options.test_sources.empty() || options.test_heartbeat) {
        hb_sources_ = options.test_sources;
        process_probe_ = options.test_process_probe;
        cgroup_probe_ = options.test_cgroup_probe;
        if (options.test_auto_policy) {
            auto_policies_.clear();
            auto policy = options.test_auto_policy();
            load_rules_at_startup(options.test_auto_policy_adapter, policy);
            auto_policies_[options.test_auto_policy_adapter] = std::move(policy);
        }
        inventory_capture_all_ = options.test_capture_all;
        auto_add_states_ = options.test_auto_add_states;
        reconcile_interval_sec_ = 30;
        return InventoryWiring::Test;
    }

    InventoryMaintenanceConfig config;
    try {
        config = resolve_inventory_maintenance_config(Config::instance().dumpConfig());
    } catch (const std::exception& error) {
        spdlog::error("JobRegistry: invalid inventory configuration; inventory maintenance "
                      "is disabled: {}",
                      error.what());
        return InventoryWiring::Disabled;
    }
    reconcile_interval_sec_ = config.reconcile_interval_sec;
    // The admission gate follows the resolved configuration.
    inventory_capture_all_ = config.capture_all;
    for (const auto& [adapter_name, settings] : config.settings) {
        auto_add_states_[adapter_name] = settings.auto_add;
    }
    // Configuration wins over the construction default so the budget is a
    // deployment knob, not a rebuild.

    // Exactly one adapter per configured scheduler name, built through the
    // self-registration registry; the heartbeat drives them through
    // IDiscoverySource and owns no adapter details.
    for (const auto& name : config.types) {
        const auto& settings = config.settings.at(name);
        auto adapter = SchedulerAdapterRegistry::instance().build(name, settings);
        if (!adapter) {
            spdlog::error("JobRegistry: no adapter registered for inventory type '{}'; "
                          "registered: {}",
                          name,
                          fmt::join(SchedulerAdapterRegistry::instance().names(), ", "));
            adapters_.clear();
            return InventoryWiring::Disabled;
        }
        adapters_.push_back(std::move(adapter));
    }
    if (config.types.empty()) {
        spdlog::info("JobRegistry: no inventory types configured; discovery is disabled and "
                     "existing records are still maintained");
    }

    // Automatic selection is armed per adapter name from the resolved
    // tri-state: Default adds the configured defaults to every job, Rule
    // asks the rule engine per job (the first accepting rule selects; a rule
    // that accepts without naming collectors falls back to the configured
    // defaults, and without defaults the decision is refused). Off arms
    // nothing — the type is inventoried with no automatic collectors.
    for (const auto& [adapter_name, settings] : config.settings) {
        switch (settings.auto_add) {
            case AutoAdd::Off:
                break;
            case AutoAdd::Default:
                auto_policies_[adapter_name] =
                    AutoCollectorPolicy::defaults(settings.default_collectors);
                break;
            case AutoAdd::Rule: {
                auto policy = AutoCollectorPolicy::rules(
                    settings.rules_dir, settings.rules_prefix, settings.default_collectors);
                load_rules_at_startup(adapter_name, policy);
                auto_policies_[adapter_name] = std::move(policy);
                break;
            }
        }
    }

    hb_sources_.clear();
    for (const auto& adapter : adapters_) {
        hb_sources_.emplace_back(adapter->name(), &adapter->source());
    }

    // Adapter-owned identity->facts derivation needs no wiring anymore: the
    // registry dispatches through adapters_ itself while building record
    // images (see applyIdentityToFacts).

    // Liveness probes only: discovered Jobs arrive fully typed from the
    // watchers, so there is no observation->facts encoder to wire.
    process_probe_ = make_proc_liveness_probe();
    cgroup_probe_ = make_cgroup_membership_probe();

    // Kernel attribution capability gate: only adapters with the trait
    // register into pid2job/cgroup2job; manual rows (unknown adapter)
    // keep participating. Installed before the initial pass can activate
    // anything.
    if (attribution_) {
        attribution_->setCapabilityLookup([this](uint64_t internal_id) {
            auto record = ownRecord(internal_id);
            if (!record) return false;
            for (const auto& adapter : adapters_) {
                if (adapter->name() == record->job.subtype_name)
                    return adapter->traits().kernel_attribution;
            }
            return true;
        });
    }

    return InventoryWiring::Production;
}

void JobRegistry::startDiscoverySources(InventoryWiring wiring)
{
    source_started_.clear();
    source_start_notes_.clear();
    for (const auto& [name, source] : hb_sources_) {
        bool started = false;
        std::string note;
        try {
            started = source->start(heartbeat_sink_);
        } catch (const std::exception& error) {
            note = std::string("start failed: ") + error.what();
        }
        if (!started && note.empty())
            note = "event source unavailable; the manager pass drives discovery";
        source_started_.emplace_back(name, started);
        source_start_notes_.push_back(std::move(note));
        if (!started && wiring == InventoryWiring::Production)
            spdlog::warn("JobRegistry: source '{}' has no event source; discovery "
                         "continues through the manager pass: {}",
                         name, source_start_notes_.back());
    }
}

void JobRegistry::stopInventoryDiscovery() {
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        if (!hb_admitted_ && !hb_running_ &&
            !(heartbeat_thread_ && heartbeat_thread_->joinable()))
            return;
        hb_admitted_ = false;
        hb_stopping_ = true;
    }
    hb_cv_.notify_all();

    bool on_hb_thread = false;
    {
        std::unique_ptr<std::thread> thread;
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            thread = std::move(heartbeat_thread_);
            on_hb_thread = hb_worker_id_ != std::thread::id() &&
                           hb_worker_id_ == std::this_thread::get_id();
            hb_running_ = false;
            hb_worker_id_ = std::thread::id();
        }
        if (thread && thread->joinable()) {
            if (on_hb_thread) {
                // A callback from the heartbeat thread must never self-join;
                // shutdown is reported as incomplete instead of deadlocking.
                spdlog::error("JobRegistry: stop() called from the heartbeat thread; "
                              "detaching instead of self-joining");
                std::lock_guard<std::mutex> lock(hb_mtx_);
                ++hb_stop_errors_;
                thread->detach();
            } else {
                thread->join();
            }
        }
    }

    // Wait for an in-flight pass so no pass can touch the store after this
    // returns (the stop path runs before the DB is closed).
    {
        std::unique_lock<std::mutex> pass_lock(pass_mtx_, std::defer_lock);
        if (!on_hb_thread) pass_lock.lock();
    }

    // Stop discovery polling before finishing the queued work.
    for (const auto& [name, source] : hb_sources_) {
        try {
            source->stop();
        } catch (const std::exception& error) {
            spdlog::error("JobRegistry: source '{}' stop failed: {}", name, error.what());
            std::lock_guard<std::mutex> lock(hb_mtx_);
            ++hb_stop_errors_;
        }
    }

    // Discovery registration is synchronous on the producing threads, so
    // stopping the sources means no accepted work is left behind: there is no
    // queue to drain and nothing buffered to finish.
    spdlog::info("JobRegistry: discovery stopped");
}


std::optional<bool> JobRegistry::apply_auto_policy(const Job& job,
                                                   const std::string& adapter)
{
    const auto decision = autoSelectionFor(job, adapter);
    if (!decision) return std::nullopt;   // no enabled policy: no decision needed
    const bool committed =
        applyAutoSelection(job.JobID, decision->collectors, decision->policy_revision);
    return committed;
}

std::optional<JobRegistry::AutoDecision> JobRegistry::autoSelectionFor(
    const Job& job, const std::string& adapter) {
    const auto policy = auto_policies_.find(adapter);
    if (policy == auto_policies_.end() || !policy->second.enabled()) return std::nullopt;
    std::string revision;
    std::string error;
    const auto decision = policy->second.select(job, revision, error);
    if (!error.empty()) {
        spdlog::warn("JobRegistry: automatic selection failed for JobID {}; keeping the "
                     "previous decision: {}",
                     job.JobID, error);
        return std::nullopt;
    }
    // A rejecting rule resolves to an empty selection here: the job stays
    // inventoried with no automatic collectors (blanket-mode semantics). An
    // accept without collectors has already resolved to the configured
    // defaults inside the policy.
    AutoDecision resolved;
    resolved.collectors = decision.collectors;
    resolved.policy_revision = revision;
    return resolved;
}

bool JobRegistry::applyAutoSelection(uint64_t internal_id,
                                     const std::vector<std::string>& collectors,
                                     const std::string& policy_revision)
{
    const auto outcome = applyAutoPolicy(
        {AutoSelection{internal_id, collectors, policy_revision}});
    if (!outcome.ok) {
        spdlog::warn("JobRegistry: automatic selection for JobID {} was rejected: {}",
                     internal_id, outcome.message);
        return false;
    }
    // The decision is durable when the instance reached a stored intent. A
    // skipped target (unknown instance/collector) or a failed transaction is
    // not committed even though the batch reported ok.
    for (const auto& target : outcome.targets) {
        if (target.internal_id != internal_id) continue;
        return !target.skipped && target.revision > 0;
    }
    return false;   // no target: the selection was skipped, not committed
}

bool JobRegistry::deinit()
{
    if (deinit_done_) return deinit_ok_;

    // Shutdown order: discovery admission stopped first (the service calls
    // stopInventoryDiscovery() before draining the scheduler), then the
    // retirements are finalized, then attribution, then the store.
    stopInventoryDiscovery();

    bool ok = true;
    // Everything written without a fence becomes durable before the database
    // closes; archive runs (synced) after this so a closing record is archived
    // on top of an up-to-date row.
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        if (!store_->flushDirty("shutdown checkpoint")) {
            spdlog::warn("JobRegistry: the shutdown checkpoint could not be written");
            ok = false;
        }
    }
    // Archive leftover closing records first so a restored closing record is
    // not reported as an incomplete drain even though the archive succeeds.
    if (!cleanupClosingJobs()) ok = false;

    // Finalize: acknowledge the retirements this heartbeat made. The collector
    // drain has completed by now; a late acknowledgement for an old generation
    // is discarded.
    {
        std::vector<std::pair<uint64_t, ProcessIdentity>> closing;
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            closing.swap(closing_executions_);
        }
        for (const auto& [internal_id, generation] : closing) {
            const auto record = ownRecord(internal_id);
            if (record && !(manager_of(record->job) == generation)) {
                spdlog::info("JobRegistry: JobID {} no longer carries the retired "
                             "generation; acknowledgement discarded", internal_id);
                continue;
            }
            if (!record) continue;   // already archived
            // Shutdown is a single pass: it never retries a write whose last
            // outcome is unknown. Such a record stays closing and is resolved
            // by an explicit command or the next open.
            if (!finalizeClosingRemoval(internal_id, /*allow_recovery=*/false) &&
                ownRecord(internal_id)) {
                ok = false;
                std::lock_guard<std::mutex> lock(hb_mtx_);
                closing_executions_.emplace_back(internal_id, generation);
            }
        }
        if (ownClosingCount() > 0) ok = false;
        // A shutdown cannot claim success while a write outcome is still
        // unknown: the durable image may differ from what this process
        // published.
        if (store_->recoveryRequired()) {
            spdlog::error("JobRegistry: deinit leaves the store in recovery-required "
                          "state; the next start must reopen and reconcile it");
            ok = false;
        }
    }

    retractStatusProvider();
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        incomplete_drain_ = !ok || hb_stop_errors_ > 0;
    }
    // Attribution's reconcile thread consults the capability lookup, which
    // reads adapters_: stop it before the adapters go away.
    stop_pid_tracker();
    adapters_.clear();
    {
        // JobStore's destructor performs its own final checkpoint.
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        store_.reset();   // closes the db
    }

    deinit_done_ = true;
    deinit_ok_ = ok;
    if (!ok)
        spdlog::error("JobRegistry: deinit could not archive every closing record; "
                      "the incomplete state is persisted for recovery");
    return ok;
}

JobRegistry::~JobRegistry()
{
    if (!deinit_done_) {
        spdlog::warn("JobRegistry: destroyed without an explicit deinit; deiniting now");
        deinit();
    }
}

// ---------------------------------------------------------------------------
// KernelAttribution wiring
// ---------------------------------------------------------------------------

void JobRegistry::init_pid_tracker() {
    attribution_ = std::make_unique<KernelAttribution>(
        static_cast<KernelAttribution::Host&>(*this));
    if (!attribution_->start()) {
        spdlog::warn("JobRegistry: kernel attribution start failed, pid2job maintenance "
                     "disabled");
        attribution_.reset();
    }
}

void JobRegistry::stop_pid_tracker() {
    if (attribution_) {
        attribution_->stop();
        attribution_.reset();
    }
}

// --- KernelAttribution::Host (registry-side projection updates) -----------

bool JobRegistry::attributionJobExists(uint64_t internal_id) {
    return store_ && ownHasActive(internal_id);
}

void JobRegistry::attributionJobAddPid(uint64_t internal_id, pid_t pid) {
    // Runtime projection on the registry's own record: never persisted, the
    // kernel rebuilds it from events after a restart. The runtime wake happens
    // after the record lock is released, so a wake callback can read the
    // record it was woken for.
    bool changed = false;
    {
        std::unique_lock lg(records_mtx_);
        const auto it = records_.find(internal_id);
        if (it == records_.end()) return;
        auto& members = it->second.job.execution.members;
        const bool known = std::any_of(
            members.begin(), members.end(),
            [pid](const ProcessIdentity& member) { return member.pid == pid; });
        if (!known) {
            members.push_back(ProcessIdentity{pid, 0, std::string{}});
            std::sort(members.begin(), members.end(),
                      [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                          return lhs.pid < rhs.pid;
                      });
            spdlog::trace("JobRegistry: kernel FORK added pid={} to job_id={} (now {} pids)",
                          pid, internal_id, members.size());
            changed = true;
        }
    }
    if (changed) wake_runtime();
}

void JobRegistry::attributionJobRemovePid(uint64_t internal_id, pid_t pid) {
    bool changed = false;
    {
        std::unique_lock lg(records_mtx_);
        const auto it = records_.find(internal_id);
        if (it == records_.end()) return;
        auto& members = it->second.job.execution.members;
        const auto before = members.size();
        members.erase(std::remove_if(members.begin(), members.end(),
                                     [pid](const ProcessIdentity& member) {
                                         return member.pid == pid;
                                     }),
                      members.end());
        if (members.size() != before) {
            spdlog::trace("JobRegistry: kernel EXIT removed pid={} from job_id={} (now {} pids)",
                          pid, internal_id, members.size());
            changed = true;
        }
    }
    if (changed) wake_runtime();
}

void JobRegistry::attributionEnumerateJobs(
    const std::function<void(const Job&)>& fn) {
    // Active jobs only; snapshots are copies so the callback never sees a
    // concurrent mutation.
    for (const auto& record : ownFindActive(
             [](const Job&) { return true; })) {
        fn(record.job);
    }
}

void JobRegistry::attributionExitHint(pid_t pid) {
    std::function<void(pid_t)> observer;
    {
        std::lock_guard<std::mutex> lock(exit_observer_mtx_);
        observer = exit_observer_;
    }
    if (observer) {
        try {
            observer(pid);
        } catch (const std::exception& error) {
            spdlog::warn("JobRegistry: exit observer failed: {}", error.what());
        }
    }
}

std::string JobRegistry::attributionCgroupPath(uint64_t internal_id) {
    // Adapter-owned: cgroupPathOf is the only place that knows where the
    // cgroup lives in the typed facts. Manual/unknown adapters have none.
    const auto record = ownRecord(internal_id);
    if (!record) return {};
    for (const auto& adapter : adapters_)
        if (adapter->name() == record->job.subtype_name)
            return adapter->cgroupPathOf(record->job);
    return {};
}

bool JobRegistry::sync_job_to_kernel(const Job& job) {
    std::lock_guard<std::mutex> lock(kernel_owner_mtx_);
    if (!attribution_) return true;   // intentionally disabled: no kernel state
    if (!ownHasActive(job.JobID)) {
        // A stale Created/Updated snapshot whose instance has been retired
        // must not install a new owner.  KernelAttribution re-reads the full
        // eligibility under its own serial gate.
        spdlog::debug("JobRegistry: refusing kernel activation for non-active JobID {}",
                      job.JobID);
        return false;
    }
    // The kernel component seeds the root PIDs and the cgroup mapping; it
    // reports no failure on this API, so the registry keeps the record and lets
    // the periodic reconcile pass repair a degraded projection.
    attribution_->activate(job.JobID, job);
    return true;
}

void JobRegistry::ensure_kernel_owner(const Job& job) {
    if (!attribution_) return;
    // Idempotent re-seed: activation rewrites the same PIDs and cgroup mapping.
    sync_job_to_kernel(job);
}

bool JobRegistry::attributionRunning() const {
    return attribution_ && attribution_->running();
}

uint64_t JobRegistry::attributionPushedEvents() const {
    return attribution_ ? attribution_->pushedEvents() : 0;
}

uint64_t JobRegistry::attributionDroppedEvents() const {
    return attribution_ ? attribution_->droppedEvents() : 0;
}

void JobRegistry::remove_job_from_kernel(const Job& job, bool& ok, std::string& error) {
    ok = false;
    error.clear();
    std::function<bool(uint64_t, const Job&, std::string&)> cleanup;
    {
        std::lock_guard<std::mutex> lock(kernel_cleanup_mtx_);
        cleanup = kernel_cleanup_for_test_;
    }
    if (cleanup) {
        ok = cleanup(job.JobID, job, error);
        return;
    }
    std::lock_guard<std::mutex> lock(kernel_owner_mtx_);
    if (!attribution_) {
        // No kernel maps were ever installed by this process (the capability
        // is intentionally disabled): there is nothing to clean.
        ok = true;
        return;
    }
    // The baseline kernel API releases the cgroup mapping and clears the pids
    // without a status: this call cannot prove the kernel state clean, so it
    // reports success. Only the injected test seam above can withhold it.
    attribution_->deactivate(job.JobID, job);
    ok = true;
}

// ---------------------------------------------------------------------------
// Registration (the manual path — deliberately separate from the
// discovery upsert: manual input has no execution evidence to merge)
// ---------------------------------------------------------------------------

bool JobRegistry::createManualJob(const Job& job) {
    spdlog::info("JobRegistry: addJob entry JobID={}, native_id={}, collectors={}, PIDs={}",
                 job.JobID, job.NativeJobID, job.CollectorNames.size(), pids_of(job).size());
    // 错误检查
    if (job.CollectorNames.empty()) {
        spdlog::warn("JobRegistry: job with JobID {} has empty CollectorNames", job.JobID);
        return false;
    }
    // 自动生成JobID
    Job job_copy = job; // 先拷贝一份，避免修改外部对象
    std::string recovery_error;
    if (!recoverStoreIfNeeded(recovery_error)) {
        spdlog::error("JobRegistry: refusing registration: {}", recovery_error);
        return false;
    }
    if (job_copy.JobID == 0) {
        job_copy.JobID = getNextJobID();
        if (job_copy.JobID == 0) {
            spdlog::error("JobRegistry: durable JobID allocation failed, refusing to register the job");
            return false;
        }
        spdlog::info("JobRegistry: assigned JobID {} to new job", job_copy.JobID);
    }

    // Duplicate check over active records: same typed sub_attr or same id.
    // The screen is scheduler (schedd) discriminating: the same
    // cluster_id/proc_id under another schedd names a different job and is
    // allowed to create; an identity-bearing row counts as a duplicate only
    // when its scheduler attribution agrees with the request.
    const SubAttrMatcher matcher = duplicateMatcherOf(job_copy);
    const auto duplicates = scheduler_screened_duplicates(
        *this, job_copy, ownFindActive(
                      [&matcher](const Job& existing) { return matches_job(matcher, existing); }));
    if (!duplicates.empty()) {
        spdlog::warn("JobRegistry: job with matching sub_attr already exists, ignored");
        return false;
    }
    if (ownHasActive(job_copy.JobID)) {
        spdlog::warn("JobRegistry: duplicate jobID {}, ignored", job_copy.JobID);
        return false;
    }

    // The record image is registry-owned (moved from the store's manual
    // insert): a manual registration owns no discovery execution — its bound
    // members are the evidence, and an empty generation is expected.
    JobRecord record;
    record.job = job_copy;
    record.job.JobID = job_copy.JobID;
    // A manual registration IS a manual selection: the caller explicitly asked
    // for these collectors, so it can never be reported as an automatic
    // origin.
    if (!record.job.CollectorNames.empty()) {
        record.selection.manual = record.job.CollectorNames;
    }
    // Intent is the selection; the record image carries no dispatch list.
    record.job.CollectorNames.clear();
    record.revision = std::max<uint64_t>(record.revision, 1);
    record.source = FactSource::Manual;
    if (record.first_seen_us == 0) record.first_seen_us = now_us();
    if (record.last_seen_us == 0) record.last_seen_us = record.first_seen_us;
    // Manual registrations carry their identity only inside the typed facts:
    // the adapter owning those facts renders the projections the wire and the
    // collectors read. No configured adapter means the projections stay as
    // given — incomplete, never a guess.
    normalizeNamespaceFacts(record.job);
    projectRecordFacts(record.job);
    {
        // Bind each numeric PID to the process that holds it NOW. A manual
        // registration has no scheduler-side identity to copy, and a bare
        // number is not an execution identity: recording the current start
        // ticks makes the kernel path verify the same process later instead
        // of trusting a number that may be recycled. An unreadable stat
        // leaves the identity incomplete, which is reported as incomplete
        // ownership rather than guessed.
        const std::string boot_id = read_boot_id();
        const std::vector<pid_t> requested_pids = pids_of(record.job);
        ExecutionEvidence& execution = record.job.execution;
        execution.members.clear();
        for (const auto pid : requested_pids) {
            const auto ticks = read_process_start_ticks(pid);
            execution.members.push_back(ProcessIdentity{
                pid, ticks.value_or(0), ticks.has_value() ? boot_id : std::string{}});
        }
    }

    // One atomic durable batch: the row plus its identity index entry. A
    // duplicate live identity is refused twice — against the registry mirror
    // first (the map cannot own two live rows of one identity) and against
    // the durable index inside the commit (an ambiguous earlier create may
    // have landed without publishing anything).
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        {
            std::shared_lock lg(records_mtx_);
            const auto key = keyOf(record.job);
            if (key) {
                const auto owner = identity_index_.find(key->index_key());
                if (owner != identity_index_.end() && owner->second != record.job.JobID) {
                    spdlog::error("JobRegistry: identity for JobID {} is already registered "
                                  "as JobID {}; refusing a duplicate inventory record",
                                  record.job.JobID, owner->second);
                    return false;
                }
            }
        }
        // A registration that published nothing needs no recovery fence: the
        // identity lookup repairs an ambiguous landing.
        RecordTxn txn(*this, "register job");
        txn.putIndexed(std::move(record));
        if (!txn.commit()) {
            spdlog::error("JobRegistry: durable registration failed, JobID {} rejected ({})",
                          job_copy.JobID, txn.error());
            return false;
        }
    }
    spdlog::info("JobRegistry: persisted manual registration of JobID {}", job_copy.JobID);

    Job job_for_cb;
    if (auto stored = ownRecord(job_copy.JobID)) {
        job_for_cb = stored->job;
    } else {
        return false;
    }

    sync_job_to_kernel(job_for_cb);
    // The runtime pulls the record it must attach. Waking is a coalesced hint,
    // never a synchronous apply on this stack.
    wake_runtime();
    spdlog::info("JobRegistry: add job with JobID {}", job_copy.JobID);
    return true;
}

bool JobRegistry::addJob(const Job& job) {
    // Thin alias: the CREATE branch of the manual upsert, exactly the
    // historical registration semantics.
    return createManualJob(job);
}

JobRegistry::ManualTarget JobRegistry::manualUpdateTarget(
    const Job& job, const std::optional<NativeSelector>& selector) {
    ManualTarget target;
    std::vector<JobRecord> duplicates;
    std::optional<NamespaceFacts> selector_facts;
    std::optional<SchedulerKey> key;
    if (selector) {
        selector_facts = selectorFactsOf(*selector);
        key = canonicalize(*selector);
    }
    const auto request_facts = namespaceFactsOf(job);
    const SubAttrMatcher matcher = duplicateMatcherOf(job);
    for (const auto& record : ownRecords()) {
        if (record.job.JobID == kSystemSentinelJobId) continue;
        const auto record_key = keyOf(record.job);
        // The structured namespace facts decide whether a typed match is the
        // same job. Unknown components can survive target screening, but
        // UPDATE never adopts a new namespace into the creation snapshot.
        const bool typed_match = ownHasActive(record.job.JobID) &&
                                 matches_job(matcher, record.job);
        if (!typed_match) continue;
        const auto record_facts = namespaceFactsOf(record.job);
        const bool compatible =
            namespace_facts_compatible(request_facts, record_facts) &&
            (!selector_facts ||
             namespace_facts_compatible(*selector_facts, record_facts));
        if (!compatible) {
            if (key && record_key && *key == *record_key) {
                target.kind = ManualTarget::Kind::Ambiguous;
                target.message = "canonical identity collision between structured namespaces";
                target.candidates.push_back(format_internal_id(record.job.JobID));
                return target;
            }
            continue;
        }
        duplicates.push_back(record);
    }
    if (duplicates.size() > 1) {
        // Several surviving duplicates are an ambiguity the caller must
        // resolve (claim the scheduler attribution or pass an identity
        // selector), never a guess. Ids are reported ascending, exactly like
        // the selector route above.
        std::sort(duplicates.begin(), duplicates.end(),
                  [](const JobRecord& lhs, const JobRecord& rhs) {
                      return lhs.job.JobID < rhs.job.JobID;
                  });
        target.kind = ManualTarget::Kind::Ambiguous;
        for (const auto& match : duplicates)
            target.candidates.push_back(format_internal_id(match.job.JobID));
        target.message =
            fmt::format("typed sub_attr '{}' matches {} current local instances; "
                        "claim the scheduler attribution or use an identity selector",
                        job.NativeJobID, duplicates.size());
        return target;
    }
    if (!duplicates.empty()) {
        target.record = std::move(duplicates.front());
        target.kind = ownHasActive(target.record.job.JobID)
                          ? ManualTarget::Kind::Update : ManualTarget::Kind::Closing;
    }
    return target;
}

nlohmann::json JobRegistry::upsertJob(const Job& job, const nlohmann::json& request,
                                      std::optional<NativeSelector> identity_selector) {
    std::string recovery_error;
    if (!recoverStoreIfNeeded(recovery_error)) {
        nlohmann::json out;
        out["status"] = "error";
        out["msg"] = "registration refused: " + recovery_error;
        return out;
    }

    if (identity_selector) {
        if (selector_of(job).adapter != identity_selector->adapter ||
            !namespace_facts_compatible(namespaceFactsOf(job),
                                        selectorFactsOf(*identity_selector))) {
            return {{"status", "error"}, {"code", "namespace_conflict"},
                    {"message", "selector contradicts structured job facts"}};
        }
    }
    const ManualTarget target = manualUpdateTarget(job, identity_selector);
    if (target.kind == ManualTarget::Kind::Closing) {
        nlohmann::json out;
        out["status"] = "error";
        out["code"] = "generation_closing";
        out["message"] = "instance " + format_internal_id(target.record.job.JobID) +
                         " is closing and rejects new binding mutations";
        return out;
    }
    if (target.kind == ManualTarget::Kind::Ambiguous) {
        nlohmann::json out;
        out["status"] = "error";
        out["code"] = "ambiguous_selector";
        out["message"] = target.message;
        out["candidates"] = target.candidates;
        return out;
    }
    if (target.kind == ManualTarget::Kind::Update) {
        Job claimed_job = job;
        if (identity_selector) {
            // The selector's namespace folds into the typed facts (already
            // proven compatible above), then the common fields render.
            adoptNamespaceAliases(claimed_job, identity_selector->cluster_name,
                                  identity_selector->cluster_tag);
            projectRecordFacts(claimed_job);
        }
        return updateManualInstance(target.record, claimed_job, request);
    }

    // CREATE branch: the manual registration, verbatim. An empty
    // collector list is a CREATE-only rejection — an UPDATE with no
    // collectors is a valid no-op and keeps its creation metadata.
    Job create_job = job;
    if (identity_selector) {
        adoptNamespaceAliases(create_job, identity_selector->cluster_name,
                              identity_selector->cluster_tag);
        projectRecordFacts(create_job);
    }
    if (!createManualJob(create_job)) {
        nlohmann::json out;
        out["status"] = "error";
        out["msg"] = fmt::format(
            "failed to add job {} (type={}): empty collectors on create, duplicate "
            "JobID, or a rejected durable write — check server logs",
            job.JobID, job.NativeJobID);
        return out;
    }
    // The create response keeps the historical shape (status only): old
    // consumers of the manual add see no contract change on this path.
    nlohmann::json out;
    out["status"] = "success";
    return out;
}

nlohmann::json JobRegistry::updateManualInstance(const JobRecord& target, const Job& job,
                                                 const nlohmann::json& request) {
    const uint64_t internal_id = target.job.JobID;

    // Re-add only changes selection. Metadata and execution evidence remain
    // the creation/source snapshot; namespace claims cannot rebind that image.
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        const auto record = ownRecord(internal_id);
        if (!record) {
            return {{"status", "error"}, {"msg", "instance vanished while updating"}};
        }
        if (record->state == RecordState::Closing) {
            return {{"status", "error"}, {"code", "generation_closing"},
                    {"message", "the instance is closing and rejects manual re-add"}};
        }
        const auto requested = namespaceFactsOf(job);
        const auto recorded = namespaceFactsOf(record->job);
        bool matches = requested.size() == recorded.size();
        for (size_t i = 0; matches && i < requested.size(); ++i) {
            if (!requested[i].empty() && requested[i] != recorded[i]) matches = false;
        }
        if (!matches) {
            return {{"status", "error"}, {"code", "namespace_conflict"},
                    {"message", "manual re-add cannot change the creation namespace"}};
        }
    }

    // Collectors: union into the existing manual selection, never a wholesale
    // replace; an empty request list leaves the record untouched.
    std::optional<CollectionTargetOutcome> selection_outcome;
    if (!job.CollectorNames.empty()) {
        std::vector<std::string> merged;
        if (const auto current = ownSelection(internal_id); current.manual.has_value())
            merged = *current.manual;
        for (const auto& name : job.CollectorNames) {
            if (std::find(merged.begin(), merged.end(), name) == merged.end())
                merged.push_back(name);
        }
        CollectionRequest command;
        command.targets = {internal_id};
        command.collectors = std::move(merged);
        const auto outcome = setCollectors(command);
        if (!outcome.ok) {
            nlohmann::json out;
            out["status"] = "error";
            out["code"] = to_string(outcome.error);
            out["msg"] = outcome.message;
            return out;
        }
        selection_outcome = outcome.targets.front();
    }

    // Response in the add response shape.
    const auto record = ownRecord(internal_id);
    const auto selection = record ? record->selection : ownSelection(internal_id);
    const auto actual = actualCollection(internal_id);
    const uint64_t revision = record ? record->revision : 0;
    const bool changed = selection_outcome.has_value() && selection_outcome->changed;

    nlohmann::json out;
    out["status"] = "success";
    // Legacy numeric field for old consumers; the value is the internal id of
    // the existing instance, never a fresh allocation from the JobID hint.
    out["JobID"] = internal_id;
    out["internal_job_id"] = format_internal_id(internal_id);
    out["instance_id"] = format_internal_id(internal_id);
    out["record_revision"] = revision;
    out["mode"] = to_string(selection.mode());
    out["desired_collectors"] = selection.effectiveCollectors();
    // Accepted versus actual stays explicit: the request is accepted when the
    // record revision is durable, while collection only starts when the
    // runtime attaches.
    out["applied_collectors"] = actual.collectors;
    out["applied_attach_token"] = actual.attach_token;
    out["errors"] = nlohmann::json::array();
    if (!actual.error.empty()) {
        out["errors"].push_back(nlohmann::json{
            {"code", "apply_failed"},
            {"message", actual.error},
        });
    }
    out["duplicate_observation"] = !changed;

    if (request.contains("JobID") && !request["JobID"].is_null()) {
        const auto hint = request["JobID"];
        uint64_t hint_value = 0;
        bool hint_valid = false;
        if (hint.is_number_unsigned()) {
            hint_value = hint.get<uint64_t>();
            hint_valid = true;
        } else if (hint.is_string() && !hint.get<std::string>().empty() &&
                   hint.get<std::string>().find_first_not_of("0123456789") ==
                       std::string::npos) {
            hint_value = std::stoull(hint.get<std::string>());
            hint_valid = true;
        }
        if (hint_valid && hint_value != internal_id) {
            out["ignored_job_id_hint"] = format_internal_id(hint_value);
            spdlog::warn("JobRegistry: manual upsert for native job '{}' carried JobID {} "
                         "as a compatibility hint; resolved to inventory instance {}",
                         record ? record->job.NativeJobID : job.NativeJobID, hint_value,
                         internal_id);
        }
    }
    spdlog::info("JobRegistry: manual upsert resolved to inventory instance {} (internal "
                 "{}, revision {})",
                 format_internal_id(internal_id), internal_id, revision);
    return out;
}

bool JobRegistry::retireInstance(uint64_t jobID) {
    // A registered command is also the retry point after a failed write: the
    // store first verifies every uncertain row before any new mutation.
    if (store_ && store_->recoveryRequired()) {
        std::string recovery_error;
        if (!recoverStoreIfNeeded(recovery_error)) {
            spdlog::error("JobRegistry: cannot retire JobID {}: store recovery failed ({})",
                          jobID, recovery_error);
            return false;
        }
    }
    auto record = ownRecord(jobID);
    if (!record) return false;

    // The closing fact must be durable before anything destructive starts: a
    // failed write leaves the instance active and accepted, with no drain.
    if (record->state != RecordState::Closing) {
        // Capture the runtime's actual attachment BEFORE the drain begins (and
        // before any fault can interrupt the sequence): after the drain, or
        // after a restart, the fact of "was this ever collected" is gone. This
        // snapshot decides archive-versus-discard later, at cleanup time.
        {
            std::lock_guard<std::mutex> applied_lock(retire_applied_mtx_);
            retire_applied_[jobID] = actualCollection(jobID).collectors;
        }
        // The whole read-decide-commit sequence runs under the record
        // transaction lock; the registry image is the truth being marked
        // Closing. A failed commit publishes nothing, so the record stays
        // active and retryable.
        bool closing_persisted = false;
        {
            std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
            std::shared_lock rl(records_mtx_);
            const auto it = records_.find(jobID);
            if (it == records_.end() || it->second.state == RecordState::Closing) {
                // Already archived by a concurrent finalize, or already
                // Closing: nothing durable left to mark.
                closing_persisted = true;
            } else {
                JobRecord closing = it->second;
                rl.unlock();
                closing.state = RecordState::Closing;
                // The ONE revision orders memory and disk alike: the same
                // commit writes the row and advances the record.
                ++closing.revision;
                RecordTxn txn(*this, "persist record state");
                txn.touch(jobID);
                txn.replace(std::move(closing));
                closing_persisted = txn.commit();
            }
        }
        if (!closing_persisted) {
            spdlog::error("JobRegistry: could not persist the closing state for JobID {}; "
                          "retirement refused (the record stays active)", jobID);
            return false;
        }
        // The heartbeat path (retireIfConfirmed) records the ended generation
        // before retiring; an explicit retirement carries no such caller, so
        // the mark is made here. It is idempotent (a map insert) and uses the
        // record snapshot taken before the closing write, so the generation
        // remembered is the one that actually ended. Without it, a late
        // observation of the same execution would re-register the retired
        // instance instead of being refused by isObsolete().
        rememberEnded(*record);
        spdlog::info("JobRegistry: JobID {} is Closing; the collection runtime drains it",
                     jobID);
    }

    ICollectionRuntime* runtime = collectionRuntime();
    if (runtime == nullptr) {
        // Inventory-only core: no collectors can hold the instance, so the
        // checked cleanup may run immediately.
        if (!cleanupThenArchive(jobID)) {
            spdlog::error("JobRegistry: retirement of JobID {} stays closing until the "
                          "cleanup succeeds; it will be retried", jobID);
            return false;
        }
        spdlog::info("JobRegistry: retire instance with JobID {}", jobID);
        return true;
    }

    // The command is ACCEPTED here: the durable Closing revision is the
    // accepted intent. Convergence belongs to the runtime, which drains the
    // instance on its own worker and reports completion through onDrained().
    // The command must not wait for the detach and must not detach on this
    // caller's stack.
    wake_runtime();
    return true;
}

void JobRegistry::onDrained(uint64_t internal_id) {
    {
        std::lock_guard<std::mutex> lock(drain_mtx_);
        drain_complete_.insert(internal_id);
    }
    if (!finalizeClosingRemoval(internal_id, /*allow_recovery=*/true)) {
        spdlog::error("JobRegistry: closing JobID {} was not archived after the runtime "
                      "released it; kernel ownership and the Closing reservation are kept",
                      internal_id);
        return;
    }
    std::lock_guard<std::mutex> lock(drain_mtx_);
    drain_complete_.erase(internal_id);
}

void JobRegistry::delJob(uint64_t jobID) {
    retireInstance(jobID);
}

bool JobRegistry::cleanupThenArchive(uint64_t jobID) {
    // One finalization at a time: an ack, the maintenance retry and the
    // shutdown sweep may all try to finish the same closing record. The
    // archive is idempotent only when they are serialized.
    std::lock_guard<std::mutex> finalize_lock(closing_mtx_);
    // Re-read under the finalize lock: a concurrent path may already have
    // archived this instance (or a newer generation may own the id again).
    const auto current = ownRecord(jobID);
    if (!current) return true;   // already archived: the goal is reached
    if (current->state != RecordState::Closing) return false;

    // A live runtime's accepted tail requires its explicit completion. A
    // locally captured retirement also covers the window before the runtime
    // has reconciled the Closing row. Restored Closing rows have no such
    // capture: if the new runtime has no owner, preserve the restart fallback.
    if (auto* runtime = collectionRuntime()) {
        bool drained = false;
        {
            std::lock_guard<std::mutex> lock(drain_mtx_);
            drained = drain_complete_.count(jobID) != 0;
        }
        if (!drained) {
            bool locally_retired = false;
            {
                std::lock_guard<std::mutex> lock(retire_applied_mtx_);
                locally_retired = retire_applied_.count(jobID) != 0;
            }
            const auto actual = runtime->actual(jobID);
            if (locally_retired || actual.tracked || actual.draining) {
                spdlog::warn("JobRegistry: closing JobID {} has no proven runtime drain; "
                             "retaining the Closing reservation", jobID);
                return false;
            }
        }
    }
    const Job& closing_job = current->job;

    // Archive-versus-discard is a fact decision made here, not in the store:
    // the runtime snapshot captured when the retirement was accepted
    // ("applied"), or — for a closing restored from disk, whose capture died
    // with the previous process — the durable effective selection as the
    // documented fallback. An empty resolved set means the instance was never
    // collected and leaves no history row.
    std::vector<std::string> applied;
    const char* provenance = "effective";
    {
        std::lock_guard<std::mutex> applied_lock(retire_applied_mtx_);
        const auto captured = retire_applied_.find(jobID);
        if (captured != retire_applied_.end()) {
            applied = captured->second;
            provenance = "applied";
        }
    }
    if (applied.empty() && provenance != std::string("applied")) {
        applied = current->selection.effectiveCollectors();
    }

    // Ordering invariant (never reversed): durable closing reservation ->
    // checked owner-sensitive kernel cleanup -> archive. The archive is the
    // only step that releases the identity index, so a failed cleanup leaves a
    // closable record a retry can finish instead of an unreachable identity.
    std::string cleanup_error;
    bool cleaned = false;
    remove_job_from_kernel(closing_job, cleaned, cleanup_error);
    if (!cleaned) {
        spdlog::error("JobRegistry: kernel cleanup for JobID {} failed ({}); the durable "
                      "closing reservation is retained for a retry", jobID,
                      cleanup_error.empty() ? "unknown error" : cleanup_error);
        return false;
    }
    // One atomic durable batch, split by the collection fact the registry
    // decided above: an instance that WAS collected keeps a history row (the
    // final record plus the applied snapshot); a never-collected one is a
    // pure delete whose ambiguous outcome is confirmed by the row's absence.
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        // The i: release rides the delete: it releases the entry only while
        // this instance still owns it on disk, so a newer execution keeps its
        // index row.
        RecordTxn txn(*this, applied.empty() ? "discard job" : "archive job");
        txn.touch(jobID);
        txn.erase(*current);
        if (!applied.empty()) txn.history(*current, applied, provenance);
        if (!txn.commit()) {
            spdlog::error("JobRegistry: durable {} failed for closing JobID {} ({}), "
                          "keeping it pending (kernel ownership already released)",
                          applied.empty() ? "discard" : "archive", jobID, txn.error());
            return false;
        }
    }
    {
        std::lock_guard<std::mutex> applied_lock(retire_applied_mtx_);
        retire_applied_.erase(jobID);
    }
    return true;
}

bool JobRegistry::finalizeClosingRemoval(uint64_t jobID, bool allow_recovery) {
    if (allow_recovery && store_->recoveryRequired()) {
        // Recovery can change which rows exist (an ambiguous archive may have
        // landed), so it must run before the record is read.
        std::string recovery_error;
        if (!recoverStoreIfNeeded(recovery_error)) {
            spdlog::error("JobRegistry: cannot finalize closing JobID {}: recovery failed "
                          "({})", jobID, recovery_error);
            return false;
        }
    }
    const auto record = ownRecord(jobID);
    if (!record) return true;   // already archived (duplicate ack): idempotent
    if (record->state != RecordState::Closing) return false;
    return cleanupThenArchive(jobID);
}

size_t JobRegistry::closingJobCount() const {
    return store_ ? ownClosingCount() : 0;
}

bool JobRegistry::cleanupClosingJobs() {
    bool all_archived = true;
    if (!store_) return true;
    for (const auto& record : ownRecords()) {
        if (record.state != RecordState::Closing) continue;
        // Shutdown sweep: no implicit recovery. A store whose last write is
        // uncertain must not retry a write here; the record stays closing and
        // the next explicit command or a reopen resolves it.
        if (cleanupThenArchive(record.job.JobID)) {
            {
                std::lock_guard<std::mutex> lock(drain_mtx_);
                drain_complete_.erase(record.job.JobID);
            }
            spdlog::info("JobRegistry: archived closing JobID {}",
                         record.job.JobID);
        } else {
            all_archived = false;
            spdlog::error("JobRegistry: shutdown could not archive closing JobID {}; "
                          "the incomplete closing state is persisted for recovery",
                          record.job.JobID);
        }
    }
    return all_archived;
}

void JobRegistry::reconcileClosingRemovals() {
    if (!store_) return;
    ICollectionRuntime* runtime = collectionRuntime();
    for (const auto& record : ownRecords()) {
        if (record.state != RecordState::Closing) continue;
        bool drained = false;
        {
            std::lock_guard<std::mutex> lock(drain_mtx_);
            drained = drain_complete_.count(record.job.JobID) != 0;
        }
        if (drained) {
            // The runtime released the instance but the cleanup/archive failed
            // earlier: retry it. The finalize lock keeps the retry idempotent.
            if (finalizeClosingRemoval(record.job.JobID, /*allow_recovery=*/false)) {
                std::lock_guard<std::mutex> lock(drain_mtx_);
                drain_complete_.erase(record.job.JobID);
            }
            continue;
        }
        if (runtime == nullptr) {
            // No collectors exist on this core: cleanup can run directly.
            cleanupThenArchive(record.job.JobID);
            continue;
        }
        // A Closing record restored after a restart has no in-process drain.
        // Re-arm it: beginDrain is idempotent per instance.
        runtime->beginDrain(record.job.JobID);
    }
}

size_t JobRegistry::delJob(const SubAttrMatcher& matcher) {
    const auto matched = ownFindActive(
        [&matcher](const Job& job) { return matches_job(matcher, job); });

    for (const auto& record : matched) {
        // One retirement path: durable Closing, then the runtime's drain, then
        // checked kernel cleanup and the archive.
        if (retireInstance(record.job.JobID)) {
            spdlog::info("JobRegistry: remove job with JobID {} by sub_attr matcher",
                         record.job.JobID);
        }
    }

    spdlog::info("JobRegistry: removed {} jobs by sub_attr matcher", matched.size());
    return matched.size();
}

inline void JobRegistry::update_job(Job& job){
    // A row an adapter answers for is refreshed by that adapter's refreshFacts;
    // only Common/open-world rows reach here, and their only fact source is the
    // generic child-process update.
    if (job.subtype == JobSubType::Common) {
        if (const auto* attr = std::get_if<CommonJobAttr>(&job.sub_attr);
            attr != nullptr && attr->auto_update_child) {
            CommonJob::update_job_child_process(job);
        }
    }
}

std::optional<Job> JobRegistry::findJob(uint64_t jobID) {
    // Read-only snapshot: the heartbeat owns member refresh and retirement,
    // so reads never mutate the registry.
    if (jobID == kSystemSentinelJobId) return sentinel_job_;
    auto record = ownRecord(jobID);
    if (!record || record->state == RecordState::Closing) return std::nullopt;
    return record->job;
}

std::vector<Job> JobRegistry::findJob(const SubAttrMatcher& matcher) const {
    std::vector<Job> matched_jobs;
    for (const auto& record : ownFindActive(
             [&matcher](const Job& job) { return matches_job(matcher, job); })) {
        matched_jobs.push_back(record.job);
    }
    spdlog::debug("JobRegistry: found {} jobs matching the criteria", matched_jobs.size());
    return matched_jobs;
}

std::vector<Job> JobRegistry::snapshot() const {
    std::vector<Job> out;
    for (const auto& record : ownFindActive(
             [](const Job&) { return true; })) {
        out.push_back(record.job);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Kernel ownership admission for restored records
// ---------------------------------------------------------------------------

void JobRegistry::admitRestoredOwners() {
    // A record restored as active is verified-live, but this process starts
    // with an empty kernel namespace. Admit it through the same path
    // observations use, otherwise a restarted service only attributes the jobs
    // it happens to observe again.
    for (const auto& record : ownRecords()) {
        if (record.job.JobID == kSystemSentinelJobId) continue;
        if (record.state != RecordState::Active) continue;
        if (!record.verified) continue;
        ensure_kernel_owner(record.job);
    }
}

// ---------------------------------------------------------------------------
// Inventory store facade
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Registry-owned record map (the single source of truth)
// ---------------------------------------------------------------------------

std::optional<JobRecord> JobRegistry::ownRecord(uint64_t id) const {
    std::shared_lock lg(records_mtx_);
    const auto it = records_.find(id);
    if (it == records_.end()) return std::nullopt;
    return it->second;
}

std::vector<JobRecord> JobRegistry::ownRecords() const {
    std::shared_lock lg(records_mtx_);
    std::vector<JobRecord> out;
    out.reserve(records_.size());
    for (const auto& entry : records_)
        out.push_back(entry.second);
    return out;
}

std::vector<JobRecord> JobRegistry::ownFindActive(
    const std::function<bool(const Job&)>& pred) const {
    std::shared_lock lg(records_mtx_);
    std::vector<JobRecord> out;
    for (const auto& entry : records_) {
        if (entry.second.state == RecordState::Closing) continue;
        if (!pred(entry.second.job)) continue;
        out.push_back(entry.second);
    }
    return out;
}

bool JobRegistry::ownHasActive(uint64_t id) const {
    std::shared_lock lg(records_mtx_);
    const auto it = records_.find(id);
    return it != records_.end() && it->second.state != RecordState::Closing;
}

size_t JobRegistry::ownClosingCount() const {
    std::shared_lock lg(records_mtx_);
    size_t count = 0;
    for (const auto& entry : records_) {
        if (entry.second.state == RecordState::Closing) ++count;
    }
    return count;
}

std::optional<JobRecord> JobRegistry::ownRecordByKey(
    const SchedulerKey& key) const {
    std::shared_lock lg(records_mtx_);
    const auto it = identity_index_.find(key.index_key());
    if (it == identity_index_.end()) return std::nullopt;
    const auto record = records_.find(it->second);
    if (record == records_.end()) return std::nullopt;
    return record->second;
}

CollectionSelection JobRegistry::ownSelection(uint64_t id) const {
    std::shared_lock lg(records_mtx_);
    const auto it = records_.find(id);
    if (it != records_.end()) return it->second.selection;
    return CollectionSelection{};
}

void JobRegistry::publishRecord(JobRecord record) {
    const uint64_t id = record.job.JobID;
    if (record.index_key.empty()) {
        if (const auto key = keyOf(record.job)) record.index_key = key->index_key();
    }
    std::unique_lock lg(records_mtx_);
    records_[id] = std::move(record);
    if (!records_[id].index_key.empty()) {
        identity_index_[records_[id].index_key] = id;
    }
}

void JobRegistry::eraseRecordLocked(uint64_t id) {
    const auto it = records_.find(id);
    if (it == records_.end()) return;
    if (!it->second.index_key.empty()) {
        const auto idx = identity_index_.find(it->second.index_key);
        if (idx != identity_index_.end() && idx->second == id)
            identity_index_.erase(idx);
    }
    records_.erase(it);
}

void JobRegistry::applyRecovered(const std::vector<JobRecord>& adopted,
                                 const std::vector<uint64_t>& removed) {
    std::unique_lock lg(records_mtx_);
    for (const auto& row_in : adopted) {
        auto row = row_in;
        normalizeNamespaceFacts(row.job);
        if (const auto key = keyOf(row.job)) row.index_key = key->index_key();
        auto it = records_.find(row.job.JobID);
        if (it != records_.end() && !it->second.index_key.empty()) {
            const auto idx = identity_index_.find(it->second.index_key);
            if (idx != identity_index_.end() && idx->second == row.job.JobID)
                identity_index_.erase(idx);
        }
        if (!row.index_key.empty()) identity_index_[row.index_key] = row.job.JobID;
        records_[row.job.JobID] = row;
    }
    for (const auto id : removed) {
        auto it = records_.find(id);
        if (it == records_.end()) continue;
        if (!it->second.index_key.empty()) {
            const auto idx = identity_index_.find(it->second.index_key);
            if (idx != identity_index_.end() && idx->second == id)
                identity_index_.erase(idx);
        }
        records_.erase(it);
    }
}

// One observation against the current record image: pure decision, no
// durable write and no publish (moved verbatim from the store). The
// register/merge semantics of a repeated identity live here, so every caller
// gets the same result.
UpsertResult JobRegistry::mergeDiscoveredJob(const JobRecord& stored, const Job& job,
                                           const SchedulerKey& key,
                                           uint64_t internal_id, int64_t observed_at,
                                           JobRecord& updated_out, MergeKind kind) {
    UpsertResult result;
    result.internal_id = internal_id;
    result.instance_id = format_internal_id(internal_id);
    result.revision = stored.revision;

    const ProcessIdentity manager = manager_of(job);
    const std::vector<ProcessIdentity>& processes = members_of(job);
    const bool members_complete = members_authoritative_of(job);

    if (stored.state == RecordState::Closing) {
        // A retiring instance must never be resurrected into the active set by
        // a late observation: the drain owns it until it is archived.
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::InstanceClosing;
        result.error = "instance_closing";
        result.message = "the instance is retiring and no longer accepts observations";
        return result;
    }
    if (!stored.index_key.empty() && stored.index_key != key.index_key()) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::IdentityIndexMismatch;
        result.error = "identity_index_mismatch";
        result.message = "the identity index and the stored record disagree";
        spdlog::error("JobRegistry: identity index for JobID {} does not match the stored "
                      "record; refusing to merge", internal_id);
        return result;
    }
    // Out-of-order commits: an older source read must never overwrite newer
    // facts. read_order is process-monotonic and assigned before reading, so a
    // late scan loses to an event that was committed first.
    if (stored.job.execution.read_order != 0 && job.execution.read_order != 0 &&
        job.execution.read_order < stored.job.execution.read_order) {
        result.ok = true;
        result.disposition = UpsertDisposition::Unchanged;
        result.message = "older source read order; newer facts kept";
        updated_out = stored;
        return result;
    }

    const ProcessIdentity stored_manager = manager_of(stored.job);
    if (stored_manager.complete() && manager.complete() &&
        stored_manager != manager) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::GenerationConflict;
        result.error = "execution_generation_conflict";
        result.message = "a different live execution owns this identity";
        spdlog::warn("JobRegistry: observation for '{}' carries a different live generation "
                     "than JobID {}; not merged", key.native_job_id, internal_id);
        return result;
    }

    JobRecord updated = stored;
    // An observation records who owns the facts (a manual row that first meets
    // its source becomes Discovery) and re-verifies the execution. A periodic
    // refresh only moves execution evidence: provenance and the liveness
    // verdict stay with their owners.
    const bool observation = kind == MergeKind::Observation;
    if (observation) updated.source = FactSource::Discovery;
    // Discovery may acquire source-owned execution evidence for a manual row,
    // but its stable scheduler metadata remains the creation snapshot.
    updated.job.CollectorNames.clear();   // intent lives in selection
    // Keep stored execution evidence until the manager/member rules below
    // decide which observed evidence can replace it.
    updated.last_seen_us = observed_at;
    const bool execution_became_complete =
        !manager_of(stored.job).complete() && manager.complete();
    // The record's one evidence copy rides the Job: the discovery execution
    // group (normalized) replaces the stored one under the same rules. The
    // ordering token advances with every accepted observation so the
    // stale-pass guard compares against the newest read, not the first.
    ExecutionEvidence& updated_evidence = updated.job.execution;
    if (manager.complete()) updated_evidence.manager = manager;
    updated_evidence.read_order = job.execution.read_order;
    // Nonempty input replaces the set even when only partially known. An
    // empty input clears it only when the source confirms it is authoritative.
    if (!processes.empty()) {
        updated_evidence.members = normalized_processes(processes);
    } else if (members_complete) {
        updated_evidence.members.clear();
    }

    // A positive observation re-admits a record whose liveness could not be
    // verified. Unverified is not a lifecycle state. A refresh carries no such
    // verdict: the retirement pass owns it.
    const bool recovered = observation && !stored.verified;
    if (recovered) updated.verified = true;
    // Provenance is a durable fact of its own: a manual row that first meets a
    // discovery observation becomes a discovery row even when every other fact
    // is identical, so the next load rebuilds its evidence instead of treating
    // it as an operator binding.
    const bool provenance_changed = updated.source != stored.source;
    const bool changed = recovered || provenance_changed ||
        manager_of(stored.job) != manager_of(updated.job) ||
        members_of(stored.job) != members_of(updated.job);
    if (!changed) {
        updated.revision = stored.revision;
        result.ok = true;
        result.disposition = UpsertDisposition::Unchanged;
        result.revision = stored.revision;
        updated_out = std::move(updated);
        return result;
    }

    updated.revision = stored.revision + 1;
    result.ok = true;
    result.disposition = UpsertDisposition::Updated;
    result.revision = updated.revision;
    result.execution_became_complete = execution_became_complete;
    updated_out = std::move(updated);
    return result;
}

namespace {

CollectionOutcome selection_failure(CollectionError error, const std::string& message)
{
    CollectionOutcome outcome;
    outcome.error = error;
    outcome.message = message;
    return outcome;
}

} // namespace

CollectionOutcome JobRegistry::executeSelectionMutations(
    const std::vector<SelectionMutation>& mutations) {
    CollectionOutcome outcome;
    {
        // Checks and writes share the same transaction boundary, including a
        // recovery latch another writer may have set since caller validation.
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        if (!store_ || !store_->ready()) {
            return selection_failure(CollectionError::StoreUnavailable, "store_unavailable");
        }
        if (store_->recoveryRequired()) {
            return selection_failure(CollectionError::StoreUnavailable, "recovery_required");
        }
        if (mutations.empty()) {
            return selection_failure(CollectionError::StoreUnavailable, "no_targets");
        }
        std::vector<JobRecord> writes;
        std::set<uint64_t> seen;
        outcome.targets.reserve(mutations.size());

        for (const auto& mutation : mutations) {
            if (!seen.insert(mutation.job_id).second) {
                return selection_failure(CollectionError::StoreUnavailable, "duplicate_target");
            }
            auto record = ownRecord(mutation.job_id);
            if (!record) {
                return selection_failure(CollectionError::NotFound, "not_found");
            }
            // Closing discovered in-transaction retains the stale-revision
            // distinction from a target absent at caller validation time.
            if (record->state == RecordState::Closing) {
                return selection_failure(CollectionError::StaleRevision, "instance_closing");
            }
            if (mutation.expected_revision.has_value() &&
                record->revision != *mutation.expected_revision) {
                return selection_failure(CollectionError::StaleRevision, "stale_revision");
            }

            const auto& current = record->selection;
            CollectionSelection next = current;
            switch (mutation.operation) {
                case SelectionOperation::SetManual:
                    next.manual = mutation.collectors;
                    break;
                case SelectionOperation::ResetAuto:
                    next.manual.reset();
                    break;
                case SelectionOperation::SetAuto:
                    next.automatic = mutation.collectors;
                    next.automatic_source = mutation.automatic_source;
                    break;
            }
            const bool changed = !(next == current);
            CollectionTargetOutcome target;
            target.internal_id = mutation.job_id;
            target.instance_id = format_internal_id(mutation.job_id);
            target.changed = (current.manual != next.manual) ||
                             (!next.manual.has_value() && changed);
            outcome.unchanged += !changed;
            // Only changed automatic bookkeeping hidden by manual precedence
            // is a skip; an exact repeat remains an accepted unchanged command.
            outcome.skipped_manual_override += changed && !target.changed;
            if (changed) {
                record->selection = next;
                ++record->revision;
                writes.push_back(*record);
            }
            target.revision = record->revision;
            target.selection = std::move(next);
            outcome.targets.push_back(std::move(target));
        }

        if (!writes.empty()) {
            RecordTxn txn(*this, "mutate selection", /*durable=*/false);
            for (auto& record : writes) {
                txn.touch(record.job.JobID);
                txn.replace(std::move(record));
            }
            if (!txn.commit()) {
                // The batch may have landed. Leave recovery fencing intact and
                // return no planned revision or selection as accepted.
                return selection_failure(CollectionError::StoreUnavailable, "store_write_failed");
            }
        }
    }

    // Even a valid no-op must be retryable after a failed runtime attachment.
    // No callback or actual-state sampling runs under the registry locks.
    wake_runtime();
    for (auto& target : outcome.targets) {
        target.actual = actualCollection(target.internal_id);
    }
    outcome.ok = true;
    return outcome;
}

void JobRegistry::validateRestoredExecutions(std::vector<JobRecord>& rows) {
    // Evidence is durable, but it only proves "this was the run when the row was
    // written". One pass over the manager triple and the member pids keeps what
    // still exists and drops what a restart cannot re-bind; the first
    // observation re-establishes the generation. A record whose evidence does
    // not survive stays active but unverified, never retired on a guess.
    const std::string boot_id = read_boot_id();
    for (auto& record : rows) {
        if (record.state == RecordState::Closing) continue;   // the drain owns it
        ExecutionEvidence& execution = record.job.execution;
        record.verified = true;

        if (execution.manager.complete()) {
            const LivenessResult manager =
                verify_process_identity(execution.manager, boot_id, process_probe_);
            if (manager.state == LivenessState::Dead) {
                record.verified = false;
                execution.members.clear();
                spdlog::warn("JobRegistry: restored JobID {} has a stale execution "
                             "generation; liveness unknown", record.job.JobID);
                continue;
            }
            if (manager.state == LivenessState::Unknown) {
                record.verified = false;
                spdlog::warn("JobRegistry: restored JobID {} has an execution generation "
                             "that cannot be verified; liveness unknown, {} member(s) kept "
                             "as evidence", record.job.JobID, execution.members.size());
            }
        }

        std::vector<ProcessIdentity> kept;
        kept.reserve(execution.members.size());
        size_t dropped = 0;
        for (const auto& member : execution.members) {
            if (!member.complete()) {
                kept.push_back(member);   // unbound evidence: the next read fills it
                continue;
            }
            const LivenessResult state =
                verify_process_identity(member, boot_id, process_probe_);
            if (state.state == LivenessState::Dead) {
                ++dropped;
                continue;
            }
            kept.push_back(member);
        }
        if (dropped != 0) {
            execution.members = std::move(kept);
            spdlog::warn("JobRegistry: restored JobID {} dropped {} reused/gone member "
                         "process identity(ies)", record.job.JobID, dropped);
        }
    }
}

// True when an instance is live and policy-eligible but still has no durable
// intent: the initial decision never landed (write failure, crash) or the
// execution generation completed later. Derived from durable state, so it also
// holds after a restart.
bool JobRegistry::missing_initial_decision(const JobRecord& record) const
{
    if (record.state != RecordState::Active) return false;
    if (!execution_complete(record.job.execution)) return false;
    if (record.index_key.empty()) return false;
    // A type with no enabled policy owes no decision. Without this the
    // predicate stays true for every record of an auto_add=Off type (the
    // default), so every pass and every observation retries a no-op forever.
    // Policies are armed once, before any source starts, so this cannot hide a
    // policy that appears later at runtime.
    const auto policy = auto_policies_.find(record.job.subtype_name);
    if (policy == auto_policies_.end() || !policy->second.enabled()) return false;
    return !ownSelection(record.job.JobID).automatic.has_value();
}

std::optional<bool> JobRegistry::repair_initial_decision(const JobRecord& record,
                                                         UpsertResult& result)
{
    const auto committed = apply_auto_policy(record.job, record.job.subtype_name);
    if (committed == false) {
        result.code = RegistrationError::InitialIntentWriteFailed;
        result.error = to_string(result.code);
        result.message = "the initial automatic decision could not be committed";
    }
    return committed;
}

UpsertResult JobRegistry::upsertObservedJob(const Job& job,
                                            const InitialDecision* admission) {
    // A registration is also the record-driven repair point: verify every
    // row whose last write outcome is unknown before touching any record.
    std::string recovery_error;
    if (!recoverStoreIfNeeded(recovery_error)) {
        UpsertResult blocked;
        blocked.code = RegistrationError::StoreUnavailable;
        blocked.error = "recovery_required";
        blocked.message = recovery_error;
        return blocked;
    }
    UpsertResult result;
    if (!store_->ready()) {
        result.code = RegistrationError::StoreUnavailable;
        result.error = to_string(result.code);
        result.message = "inventory store is not ready";
        return result;
    }

    const auto key = keyOf(job);
    if (!key) {
        result.code = RegistrationError::IncompleteIdentity;
        result.error = "incomplete_identity";
        result.message = "scheduler type, cluster namespace and native job id are required";
        spdlog::debug("JobRegistry: rejected incomplete observation (native='{}', "
                      "cluster='{}')", job.NativeJobID, job.cluster_name);
        return result;
    }
    const std::string index_key = key->index_key();
    const ProcessIdentity manager = manager_of(job);
    const std::vector<ProcessIdentity>& processes = members_of(job);
    const bool members_complete = members_authoritative_of(job);
    const int64_t observed_at = now_us();

    JobRecord record;   // the authoritative image this call hands back

    // The whole resolve-decide-commit-publish sequence runs under the record
    // transaction lock: a decided revision cannot move before its commit
    // lands. records_mtx_ itself is taken only to read and (after a
    // successful commit) to publish, so the collection runtime keeps reading
    // the previous image while the store writes. The lock is scoped: kernel
    // admission, the runtime wake and the automatic-policy (Lua) evaluation
    // below never run under it.
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);

        uint64_t internal_id = 0;
        {
            std::shared_lock lg(records_mtx_);
            if (const auto indexed = identity_index_.find(index_key);
                indexed != identity_index_.end()) {
                internal_id = indexed->second;
            }
        }
        if (internal_id == 0) {
            if (auto durable = store_->readRowByIdentity(*key)) {
                // An earlier create landed ambiguously without publishing
                // anything: the durable row is the truth memory never held,
                // and this identity lookup is the documented repair. Adopt
                // it, then merge.
                internal_id = durable->job.JobID;
                normalizeNamespaceFacts(durable->job);
                std::unique_lock lg(records_mtx_);
                if (!durable->index_key.empty())
                    identity_index_[durable->index_key] = internal_id;
                records_[internal_id] = std::move(*durable);
            }
        }

    bool have_stored = false;
    JobRecord stored;
    if (internal_id != 0) {
        std::shared_lock lg(records_mtx_);
        const auto it = records_.find(internal_id);
        if (it != records_.end()) {
            stored = it->second;
            have_stored = true;
        }
    }

    if (have_stored) {
        if (!namespace_facts_compatible(namespaceFactsOf(job),
                                        namespaceFactsOf(stored.job))) {
            result.disposition = UpsertDisposition::Conflict;
            result.code = RegistrationError::IdentityIndexMismatch;
            result.error = to_string(result.code);
            result.message = "canonical identity key aliases conflicting structured namespaces";
            return result;
        }
        JobRecord updated;
        result = mergeDiscoveredJob(stored, job, *key, internal_id, observed_at, updated);
        if (!result.ok) return result;
        if (result.disposition == UpsertDisposition::Unchanged) {
            // The facts did not move, but the observed image still refreshes
            // memory-only bookkeeping (last_seen_us); publication matches the
            // previous store-out publishing behavior.
            record = updated;
            std::unique_lock lg(records_mtx_);
            records_[internal_id] = std::move(updated);
        } else {
            // Updated: one unsynced batch; the row joins the dirty set. A
            // failed or ambiguous write publishes nothing (the latch fences
            // the row).
            RecordTxn txn(*this, "update inventory facts", /*durable=*/false);
            txn.touch(internal_id);
            txn.put(std::move(updated));
            if (!txn.commit()) {
                result.ok = false;
                result.code = RegistrationError::StoreWriteFailed;
                result.error = "store_write_failed";
                result.message = txn.error();
                return result;
            }
            if (const auto published = ownRecord(internal_id)) record = *published;
        }
    } else {
        // Create a new inventory record. Nothing is activated and no caller
        // binding is accepted here. The id reservation rides along in the
        // same batch as the record and its index entry: one commit, not two.
        std::string allocation_error;
        uint64_t durable_counter = 0;
        bool durable_exhausted = false;
        uint64_t allocated = 0;
        if (!store_->reserveId(allocated, durable_counter, durable_exhausted,
                               allocation_error)) {
            result.code = allocation_error == "id_exhausted"
                              ? RegistrationError::IdExhausted
                              : RegistrationError::StoreWriteFailed;
            result.error = result.code == RegistrationError::IdExhausted
                               ? allocation_error
                               : to_string(result.code);
            result.message = "durable internal id allocation failed";
            return result;
        }

        JobRecord created;
        created.job = job;
        created.job.JobID = allocated;
        created.job.CollectorNames.clear();   // intent lives in selection
        normalizeNamespaceFacts(created.job);
        projectRecordFacts(created.job);
        created.source = FactSource::Discovery;
        created.index_key = index_key;
        created.job.execution.manager = manager;
        created.job.execution.members_authoritative = members_complete;
        // The ordering token is record business (the stale-pass guard
        // compares against it); the observation's reasons/notes stay
        // producer-side diagnostics and are not copied.
        created.job.execution.read_order = job.execution.read_order;
        created.revision = 1;
        created.state = RecordState::Active;
        created.first_seen_us = observed_at;
        created.last_seen_us = observed_at;
        created.job.execution.members = normalized_processes(processes);

        // A create has no published memory image to revert: identity
        // lookup repairs an ambiguous create, so it does not fence
        // recovery.
        RecordTxn txn(*this, "create inventory record", /*durable=*/false);
        txn.reserveCounter(durable_counter, durable_exhausted);
        txn.put(std::move(created));
        txn.indexPut(*key, allocated);
        if (!txn.commit()) {
            result.code = RegistrationError::StoreWriteFailed;
            result.error = "store_write_failed";
            result.message = txn.error();
            return result;
        }

        result.ok = true;
        result.disposition = UpsertDisposition::Created;
        result.internal_id = allocated;
        result.instance_id = format_internal_id(allocated);
        std::shared_lock lg(records_mtx_);
        result.revision = records_[allocated].revision;
        record = records_[allocated];
        spdlog::info("JobRegistry: created inventory instance {} for {} '{}'",
                     allocated, key->adapter, key->native_job_id);
        }
    }   // mutation_mtx_ released: kernel admission, the runtime wake and the
        // automatic-policy evaluation below never run under a registry lock.

    if (result.disposition == UpsertDisposition::Created) {
        // Kernel attribution is initialized here, exactly like manual
        // registration: eBPF attribution and the cgroup map must not wait for
        // the reconcile pass, and the cgroup-id cache must exist for the
        // later removal.
        sync_job_to_kernel(record.job);
        // Facts only: an unbound instance creates no collector work, but the
        // collection runtime must learn the instance exists (System collectors
        // are its concern). A coalesced wake, never a synchronous apply here.
        wake_runtime();
        // First complete registration is an automatic-selection trigger; Lua
        // evaluation happens outside every registry lock. An
        // observation whose execution generation is not verified yet is never
        // auto-collected: the update that completes the generation re-runs
        // the selection instead. An admission decision that carried its own
        // evaluation is applied verbatim: the rule already ran once at the
        // gate and must not run twice for one registration.
        if (manager_of(job).complete()) {
            result.intent_committed =
                admission != nullptr && admission->initial_collectors.has_value()
                    ? applyAutoSelection(record.job.JobID, *admission->initial_collectors,
                                         admission->policy_revision)
                    : apply_auto_policy(record.job, record.job.subtype_name);
            if (result.intent_committed == false) {
                result.code = RegistrationError::InitialIntentWriteFailed;
                result.error = to_string(result.code);
                result.message = "the initial automatic decision could not be committed";
            }
        }
    } else if (result.disposition == UpsertDisposition::Updated) {
        // Fact changes reach already bound consumers (fresh PIDs/attributes)
        // through the runtime, which re-reads the record it converges to. A
        // coalesced wake: the source stack never runs a collector.
        wake_runtime();
        // A record registered from an incomplete observation has never seen
        // an automatic selection. The pass that completes its execution
        // generation is its first complete registration. A record whose
        // initial decision never became durable is repaired here too, not
        // only on the Created path.
        if (result.execution_became_complete || missing_initial_decision(record))
            result.intent_committed = repair_initial_decision(record, result);
        // An observation is also the admission repair point: a live instance
        // with no owner (restored row, capacity refusal) gets one here through
        // the single KernelAttribution path.  Facts changed, so this is not a
        // no-op even for an existing owner: newly observed members must be
        // seeded (activate() only seeds identities the admission gate verified).
        sync_job_to_kernel(record.job);
    } else if (result.disposition == UpsertDisposition::Unchanged) {
        // A repeated observation is the record-driven repair path: if the
        // initial automatic decision is still missing, decide now.
        if (missing_initial_decision(record))
            result.intent_committed = repair_initial_decision(record, result);
        // Repeated observation of an attested-live instance is the retry path
        // for kernel ownership (a restarted process starts with an empty
        // namespace even though the record was restored as active).
        ensure_kernel_owner(record.job);
    }
    return result;
}

std::optional<JobRecord> JobRegistry::inventoryRecord(uint64_t jobID) const {
    return ownRecord(jobID);
}

std::optional<JobRecord> JobRegistry::inventoryRecordByKey(
    const SchedulerKey& key) const {
    return ownRecordByKey(key);
}

std::vector<JobRecord> JobRegistry::inventoryRecords() const {
    return ownRecords();
}

CollectionSelection JobRegistry::selectionRecord(uint64_t jobID) const {
    return ownSelection(jobID);
}

namespace {

std::vector<std::string> normalized_selection(std::vector<std::string> names)
{
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    return names;
}

std::vector<uint64_t> deduplicated_targets(std::vector<uint64_t> ids)
{
    std::vector<uint64_t> out;
    std::unordered_set<uint64_t> seen;
    out.reserve(ids.size());
    for (const auto id : ids) {
        if (seen.insert(id).second) out.push_back(id);
    }
    return out;
}

CollectionOutcome reentrant_refusal(const char* what)
{
    CollectionOutcome outcome;
    outcome.error = CollectionError::ReentrantMutation;
    outcome.message = std::string(what) +
        " was requested from inside a collection mutation; refused without "
        "committing anything";
    spdlog::warn("JobRegistry: reentrant {} refused", what);
    return outcome;
}

}  // namespace

void JobRegistry::setCollectorValidator(
    std::function<bool(const std::string&)> validator) {
    std::lock_guard<std::mutex> lock(selection_validation_mtx_);
    collector_validator_ = std::move(validator);
}

bool JobRegistry::validate_collectors(const std::vector<std::string>& collectors,
                                      std::string& bad_name) const {
    std::function<bool(const std::string&)> validator;
    {
        std::lock_guard<std::mutex> lock(selection_validation_mtx_);
        validator = collector_validator_;
    }
    for (const auto& name : collectors) {
        if (name.empty()) {
            bad_name = "<empty>";
            return false;
        }
        if (validator && !validator(name)) {
            bad_name = name;
            return false;
        }
    }
    return true;
}

CollectionOutcome JobRegistry::setCollectors(const CollectionRequest& request) {
    if (joblens::in_mutation_gate()) return reentrant_refusal("setCollectors");
    return mutateSelection(request, /*manual=*/true);
}

CollectionOutcome JobRegistry::resetAuto(const CollectionRequest& request) {
    if (joblens::in_mutation_gate()) return reentrant_refusal("resetAuto");
    return mutateSelection(request, /*manual=*/false);
}

CollectionOutcome JobRegistry::mutateSelection(const CollectionRequest& request,
                                               bool manual) {
    CollectionOutcome outcome;
    if (request.targets.empty()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "at least one resolved target is required";
        return outcome;
    }
    // `set` requires the list (an explicit [] is a persistent stop); a missing
    // list is not the same request as an empty one.
    if (manual && !request.collectors.has_value()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "set requires an explicit collectors list ([] stops collection)";
        return outcome;
    }
    if (!manual && request.collectors.has_value()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "reset_auto does not accept a collectors list";
        return outcome;
    }
    if (request.expected_revision.has_value() && request.targets.size() != 1) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "expected_revision is only valid for a single-instance request";
        return outcome;
    }

    const auto targets = deduplicated_targets(request.targets);
    const auto names = manual ? normalized_selection(*request.collectors)
                              : std::vector<std::string>{};
    if (manual) {
        std::string bad_name;
        if (!validate_collectors(names, bad_name)) {
            outcome.error = CollectionError::UnknownCollector;
            outcome.message = "unknown collector name: " + bad_name;
            return outcome;
        }
    }

    // A selection command is the retry point after an ambiguous write: verify
    // the uncertain rows before any target is read or transformed.
    std::string recovery_error;
    if (!recoverStoreIfNeeded(recovery_error)) {
        outcome.error = CollectionError::StoreUnavailable;
        outcome.message = "binding transaction rejected: " + recovery_error;
        return outcome;
    }

    std::vector<SelectionMutation> mutations;
    mutations.reserve(targets.size());
    for (const auto id : targets) {
        if (!ownHasActive(id)) {
            outcome.error = CollectionError::NotFound;
            outcome.message = "no verified inventory instance with internal id " +
                              std::to_string(id);
            return outcome;
        }
        SelectionMutation mutation;
        mutation.job_id = id;
        mutation.operation = manual ? SelectionOperation::SetManual
                                    : SelectionOperation::ResetAuto;
        mutation.collectors = names;
        mutation.expected_revision = request.expected_revision;
        mutations.push_back(std::move(mutation));
    }

    outcome = executeSelectionMutations(mutations);
    if (!outcome.ok) {
        outcome.message = "binding transaction rejected: " + outcome.message;
    }
    return outcome;
}

CollectionOutcome JobRegistry::applyAutoPolicy(
    const std::vector<AutoSelection>& selections) {
    if (joblens::in_mutation_gate()) return reentrant_refusal("applyAutoPolicy");
    CollectionOutcome outcome;
    if (selections.empty()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "at least one automatic decision is required";
        return outcome;
    }

    std::unordered_set<uint64_t> seen;
    for (const auto& selection : selections) {
        if (!seen.insert(selection.internal_id).second) {
            outcome.error = CollectionError::InvalidRequest;
            outcome.message = "duplicate target in one automatic policy batch";
            return outcome;
        }
    }

    // Automatic decisions are validated per target: an invalid result keeps
    // the previous valid decision instead of failing the whole batch.
    std::string recovery_error;
    if (!recoverStoreIfNeeded(recovery_error)) {
        outcome.error = CollectionError::StoreUnavailable;
        outcome.message = "automatic policy transaction rejected: " + recovery_error;
        return outcome;
    }
    std::vector<SelectionMutation> mutations;
    mutations.reserve(selections.size());
    outcome.targets.reserve(selections.size());
    for (const auto& selection : selections) {
        CollectionTargetOutcome target;
        target.internal_id = selection.internal_id;
        target.instance_id = format_internal_id(selection.internal_id);

        if (!ownHasActive(selection.internal_id)) {
            ++outcome.skipped_invalid;
            outcome.message = "no verified inventory instance with internal id " +
                              std::to_string(selection.internal_id);
            target.selection = ownSelection(selection.internal_id);
            target.actual.error = "no verified inventory instance";
            target.skipped = true;
            outcome.targets.push_back(std::move(target));
            continue;
        }

        const auto names = normalized_selection(selection.collectors);
        std::string bad_name;
        if (!validate_collectors(names, bad_name)) {
            ++outcome.skipped_invalid;
            outcome.message = "unknown automatic collector name: " + bad_name;
            spdlog::warn("JobRegistry: ignoring invalid automatic selection for "
                         "instance {} (collector '{}')", selection.internal_id, bad_name);
            target.selection = ownSelection(selection.internal_id);
            target.actual.error = "unknown automatic collector name: " + bad_name;
            target.skipped = true;
            outcome.targets.push_back(std::move(target));
            continue;
        }

        SelectionMutation mutation;
        mutation.job_id = selection.internal_id;
        mutation.operation = SelectionOperation::SetAuto;
        mutation.collectors = names;
        mutation.automatic_source = selection.policy_revision;
        mutations.push_back(std::move(mutation));
        outcome.targets.push_back(std::move(target));
    }

    if (!mutations.empty()) {
        auto accepted = executeSelectionMutations(mutations);
        if (!accepted.ok) {
            outcome.error = accepted.error;
            outcome.message = "automatic policy transaction rejected: " + accepted.message;
            for (auto& target : outcome.targets) {
                // Preserve validation attribution without claiming the write
                // did not land: it may require recovery before any retry.
                if (target.actual.error.empty()) {
                    target.actual.error = "intent write failed: " + accepted.message;
                }
            }
            return outcome;
        }

        outcome.unchanged = accepted.unchanged;
        outcome.skipped_manual_override = accepted.skipped_manual_override;
        auto result = accepted.targets.begin();
        for (auto& target : outcome.targets) {
            // Mutations preserve input order with invalid entries removed, so
            // one linear merge keeps skipped errors intact without an ID map.
            if (!target.skipped) target = std::move(*result++);
        }
    }

    outcome.ok = true;
    return outcome;
}

bool JobRegistry::recoverStoreIfNeeded(std::string& error) {
    error.clear();
    if (!store_) {
        error = "store_unavailable";
        return false;
    }
    if (!store_->recoveryRequired()) return true;
    // Recovery may also checkpoint row-provider snapshots. Keep its fence and
    // publication in the same transaction boundary as ordinary checkpoints.
    std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
    if (!store_) {
        error = "store_unavailable";
        return false;
    }
    if (!store_->recoveryRequired()) return true;
    std::vector<JobRecord> adopted;
    std::vector<uint64_t> removed;
    const bool recovered = store_->recoverPending(adopted, removed, error);
    // Adoption is what memory shows, even when the durability fence failed and
    // the latch stays: the durable images are authoritative the moment they
    // are proven, and the latch only tracks the fence obligation.
    applyRecovered(adopted, removed);
    if (!recovered) {
        if (error.empty()) error = "recovery_required";
        return false;
    }
    return true;
}

StoreHealth JobRegistry::storeHealth() const {
    auto health = store_ ? store_->health() : StoreHealth{};
    // The record-derived fields describe the registry's map (it owns the
    // records); the persistence fields come from the store.
    {
        std::shared_lock lg(records_mtx_);
        health.active_instances = records_.size();
        for (const auto& entry : records_) {
            if (entry.second.index_key.empty()) ++health.incomplete_identities;
        }
    }
    return health;
}

void JobRegistry::injectStoreFaultForTest(StoreFaultMode mode, size_t skip, size_t count) {
    store_->injectFaultForTest(mode, skip, count);
}

void JobRegistry::clearStoreFaultsForTest() {
    store_->clearFaultsForTest();
}

void JobRegistry::setKernelCleanupForTest(
    std::function<bool(uint64_t, const Job&, std::string&)> cleanup) {
    std::lock_guard<std::mutex> lock(kernel_cleanup_mtx_);
    kernel_cleanup_for_test_ = std::move(cleanup);
}

uint64_t JobRegistry::getNextJobID() {
    return store_->nextJobId();
}

ICollectionRuntime* JobRegistry::collectionRuntime() const {
    std::lock_guard<std::mutex> lock(runtime_mtx_);
    return collection_runtime_;
}

void JobRegistry::setCollectionRuntime(ICollectionRuntime* runtime) {
    std::lock_guard<std::mutex> lock(runtime_mtx_);
    collection_runtime_ = runtime;
}

ActualCollectionState JobRegistry::actualCollection(uint64_t jobID) {
    ICollectionRuntime* runtime = collectionRuntime();
    if (runtime == nullptr) return ActualCollectionState{};
    return runtime->actual(jobID);
}

void JobRegistry::wake_runtime() {
    ICollectionRuntime* runtime = collectionRuntime();
    if (runtime == nullptr) return;
    // The only call into the runtime on a command stack. A runtime that tries
    // to mutate intent from here would otherwise recurse; the gate turns that
    // into a typed refusal.
    joblens::MutationGateScope gate;
    runtime->wake();
}

bool JobRegistry::runtimeRecords(std::vector<JobRecord>& out) const {
    out.clear();
    // Unavailable is not empty: the runtime must keep its attachments instead
    // of detaching everything because the store cannot be read. The same holds
    // while a write outcome is unknown: memory and disk may disagree, so the
    // runtime must not act on either image.
    if (!store_ || !store_->ready() || store_->recoveryRequired()) return false;
    out = ownRecords();
    return true;
}

bool JobRegistry::runtimeRecord(uint64_t internal_id,
                                std::optional<JobRecord>& out) const {
    out.reset();
    if (!store_ || !store_->ready() || store_->recoveryRequired()) return false;
    out = ownRecord(internal_id);
    return true;
}

void JobRegistry::setExecutionVerified(uint64_t jobID, bool verified) {
    // Liveness evidence only. It never changes the lifecycle state and never
    // authorizes retirement; it gates kernel admission. Runtime state: not
    // persisted, rebuilt by observation after a restart.
    std::unique_lock lg(records_mtx_);
    const auto it = records_.find(jobID);
    if (it != records_.end()) it->second.verified = verified;
}

void JobRegistry::regRPCHandle() {
    registerInventoryRpc(*this);
    RPCServer::instance().register_method("JobRegistry/group_probe_stats",
        [](const nlohmann::json&) -> nlohmann::json {
            return {{"status", "ok"}, {"stats", GroupProbeStats::instance().to_json()}};
        });
}

nlohmann::json JobRegistry::heartbeatStatusJson() const {
    // Registry/store queries happen without holding the heartbeat mutex; they
    // take their own locks briefly and never re-enter here.
    const StoreHealth store = storeHealth();
    const auto records = inventoryRecords();
    const std::size_t closing_records = closingJobCount();

    std::vector<std::pair<uint64_t, ProcessIdentity>> closing_ids;
    std::set<uint64_t> suspect_ids;
    std::uint64_t events_received = 0;
    std::uint64_t rescan_requests = 0;
    std::uint64_t obsolete_hints = 0;
    std::uint64_t incomplete_observations = 0;
    std::uint64_t upsert_conflicts = 0;
    std::uint64_t dropped_observations = 0;
    std::uint64_t rejected_observations = 0;
    std::uint64_t admission_errors = 0;
    std::uint64_t decision_repairs = 0;
    std::uint64_t decision_failures = 0;
    std::uint64_t refresh_failures = 0;
    std::uint64_t retirements = 0;
    std::uint64_t passes = 0;
    std::int64_t last_scan_start_ms = 0;
    std::int64_t last_scan_success_ms = 0;
    bool last_scan_complete = false;
    bool incomplete_drain = false;
    bool admitted = false;
    bool degraded = false;
    std::string last_error;
    bool tracker_unavailable = false;
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        closing_ids = closing_executions_;
        suspect_ids = suspect_ids_;
        events_received = hb_events_received_;
        rescan_requests = hb_rescan_requests_;
        obsolete_hints = hb_obsolete_hints_;
        incomplete_observations = hb_incomplete_observations_;
        upsert_conflicts = hb_upsert_conflicts_;
        dropped_observations = hb_dropped_observations_;
        rejected_observations = hb_rejected_observations_;
        admission_errors = hb_admission_errors_;
        decision_repairs = hb_decision_repairs_;
        decision_failures = hb_decision_failures_;
        refresh_failures = hb_refresh_failures_;
        retirements = hb_retirements_;
        passes = hb_passes_;
        last_scan_start_ms = last_scan_start_ms_;        last_scan_success_ms = last_scan_success_ms_;
        last_scan_complete = last_scan_complete_;
        incomplete_drain = incomplete_drain_;
        admitted = hb_admitted_;
        degraded = degraded_;
        last_error = hb_last_error_;
    }
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        for (const auto& started : source_started_) {
            if (!started.second) tracker_unavailable = true;
        }
    }

    std::size_t closing_present = 0;
    for (const auto& entry : closing_ids) {
        // Closing records stay in the store until the acknowledgement archives
        // them, so this is the accurate overlap with the active count.
        if (inventoryRecord(entry.first).has_value()) ++closing_present;
    }

    std::size_t active_count = 0;
    std::size_t suspect_count = 0;
    std::size_t incomplete_count = 0;
    std::size_t pending_decisions = 0;
    for (const auto& record : records) {
        if (record.job.JobID == 0) continue;   // system sentinel is never inventory
        if (record.index_key.empty()) ++incomplete_count;
        // The same durable predicate the repair path uses, evaluated on this
        // snapshot: a live record whose automatic decision never landed.
        if (record.state == RecordState::Active && !record.index_key.empty() &&
            execution_complete(record.job.execution) &&
            !record.selection.automatic.has_value()) {
            const auto policy = auto_policies_.find(record.job.subtype_name);
            if (policy != auto_policies_.end() && policy->second.enabled())
                ++pending_decisions;
        }
        const bool suspect = !record.verified ||
                             suspect_ids.count(record.job.JobID) != 0;
        if (suspect) ++suspect_count;
        else if (record.state == RecordState::Active) ++active_count;
    }
    if (active_count >= closing_present) active_count -= closing_present;
    else active_count = 0;

    std::string state;
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        if (!admitted) state = "stopped";
        else if (passes == 0) state = "initializing";
        else if (degraded || !last_scan_complete) state = "degraded";
        else state = "healthy";
    }

    nlohmann::json configured_types = nlohmann::json::array();
    for (const auto& adapter : adapters_) configured_types.push_back(adapter->name());

    nlohmann::json sources = nlohmann::json::array();
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        for (std::size_t index = 0; index < source_started_.size(); ++index) {
            nlohmann::json entry;
            entry["name"] = source_started_[index].first;
            entry["started"] = source_started_[index].second;
            entry["start_note"] = index < source_start_notes_.size()
                                      ? source_start_notes_[index]
                                      : std::string();
            sources.push_back(std::move(entry));
        }
    }

    nlohmann::json out;
    out["state"] = state;
    out["configured_types"] = std::move(configured_types);
    out["reconcile_interval_sec"] = reconcile_interval_sec_;
    out["last_scan_start_ms"] = last_scan_start_ms;
    out["last_scan_success_ms"] = last_scan_success_ms;
    out["last_scan_complete"] = last_scan_complete;
    out["last_error"] = last_error;
    out["active_count"] = active_count;
    out["suspect_count"] = suspect_count;
    out["closing_count"] = closing_records;
    out["incomplete_count"] = incomplete_count;
    out["quarantined_count"] = 0;   // no quarantine tier: nothing is withheld
    out["events_received"] = events_received;
    out["events_dropped"] = 0;   // no ingest queue: nothing is dropped
    out["queue_depth"] = 0;   // no ingest queue
    // Removed subsystems whose status keys stay for wire compatibility: the
    // exec ingest worker, the observation hint queue and the per-batch policy
    // budget. They report zero because they no longer exist.
    out["exec_ingest_depth"] = 0;
    out["policy_pending"] = 0;
    out["policy_deferred_total"] = 0;
    out["auto_policy_budget_us"] = 0;
    out["rescan_requests"] = rescan_requests;
    out["obsolete_hints"] = obsolete_hints;
    out["incomplete_observations"] = incomplete_observations;
    out["upsert_conflicts"] = upsert_conflicts;
    out["capture_all"] = inventory_capture_all_;
    out["dropped_observations"] = dropped_observations;
    out["rejected_observations"] = rejected_observations;
    out["admission_errors"] = admission_errors;
    out["pending_decisions"] = pending_decisions;
    out["decision_repairs"] = decision_repairs;
    out["decision_repair_failures"] = decision_failures;
    out["refresh_failures"] = refresh_failures;
    out["retirements"] = retirements;
    out["pass_count"] = passes;
    out["tracker_unavailable"] = tracker_unavailable;
    out["incomplete_drain"] = incomplete_drain;
    out["store"] = nlohmann::json{
        {"db_running", store.db_running},
        {"migration_complete", store.migration_complete},
        {"migration_state", store.migration_state},
        {"schema_version", store.schema_version},
        {"degraded", store.degraded},
        {"write_failures", store.write_failures},
        {"last_write_error", store.last_write_error},
        {"active_instances", store.active_instances},
    };
    out["sources"] = std::move(sources);
    return out;
}
