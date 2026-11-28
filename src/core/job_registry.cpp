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
// KernelAttribution; the RPC surface lives in core/rpc/. This file
// keeps the CollectorScheduler-facing seams: lifecycle callbacks, the
// collection applier, removal/drain flow and the automatic-selection
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
#include "core/maintenance_config.hpp"
#include "core/rpc/register_all.hpp"
#include "core/collector_registry.hpp"
#include "common/utils.hpp"
#include "common/common_job.hpp"
#include "common/local_rpc.hpp"
#include "job_watcher/group_probe_stats.hpp"
#include <algorithm>

#include <set>
#include <unordered_set>
#include <type_traits>

// Maintenance-side source health (source/queue/reconciliation state) is
// published by the heartbeat through this seam. When no provider
// is registered, the status handler serves the explicit "not_configured"
// marker instead of guessing a source state.
#if __has_include("core/status_provider.hpp")
#include "core/status_provider.hpp"
#define JOBLENS_HAS_STATUS_PROVIDER 1
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
// Job record helpers
// ---------------------------------------------------------------------------

namespace {

int64_t now_us()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// Stable map key for one manager identity lives on JobRegistry
// (managerKey): the triple is what makes a pid a process, and comparing
// bare pids would let a recycled number alias an old manager.

JobStore::WriteOp put_record_op(const JobRecord& record) {
    JobStore::WriteOp op;
    op.kind = JobStore::WriteOp::PutRecord;
    op.job_id = record.job.JobID;
    op.record = record;
    return op;
}

JobStore::WriteOp delete_record_op(uint64_t job_id) {
    JobStore::WriteOp op;
    op.kind = JobStore::WriteOp::DeleteRecord;
    op.job_id = job_id;
    return op;
}

JobStore::WriteOp identity_op(JobStore::WriteOp::Kind kind, uint64_t job_id,
                              SchedulerKey key) {
    JobStore::WriteOp op;
    op.kind = kind;
    op.job_id = job_id;
    op.key = std::move(key);
    return op;
}

// Screen native fields here; adapters screen structured namespaces below.

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
        const uint64_t job_id = record.job.JobID;
        std::string index_key;
        std::optional<SchedulerKey> key = registry_.keyOf(record.job);
        if (key) {
            index_key = key->index_key();
            record.index_key = index_key;
        }
        put(std::move(record));
        if (key) {
            index_put_.emplace_back(std::move(index_key), job_id);
            ops_.push_back(
                identity_op(JobStore::WriteOp::PutIdentity, job_id, std::move(*key)));
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
        const uint64_t job_id = record.job.JobID;
        ops_.push_back(delete_record_op(job_id));
        if (const auto key = registry_.keyOf(record.job)) {
            ops_.push_back(
                identity_op(JobStore::WriteOp::DeleteIdentity, job_id, *key));
        }
        erased_.push_back(job_id);
        return *this;
    }

    // i: maintenance that carries no record image of its own.
    RecordTxn& indexPut(const SchedulerKey& key, uint64_t job_id)
    {
        index_put_.emplace_back(key.index_key(), job_id);
        ops_.push_back(identity_op(JobStore::WriteOp::PutIdentity, job_id, key));
        return *this;
    }

    RecordTxn& indexRelease(const SchedulerKey& key, uint64_t job_id)
    {
        index_release_.emplace_back(key.index_key(), job_id);
        ops_.push_back(identity_op(JobStore::WriteOp::DeleteIdentity, job_id, key));
        return *this;
    }

    // h: archive row of a deleting batch.
    RecordTxn& history(const JobRecord& record, std::vector<std::string> applied,
                       const std::string& provenance)
    {
        JobStore::WriteOp op;
        op.kind = JobStore::WriteOp::PutHistory;
        op.job_id = record.job.JobID;
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
    RecordTxn& touch(uint64_t job_id)
    {
        touched_.push_back(job_id);
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
        for (const auto job_id : erased_)
            registry_.eraseRecordLocked(job_id);
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
    return {job.clusterName, job.clusterTag};
}

NamespaceFacts JobRegistry::selectorFactsOf(const NativeSelector& selector) const {
    const auto* adapter = namespaceAdapterNamed(selector.adapter);
    if (adapter != nullptr) return adapter->selectorNamespaceFacts(selector);
    return {selector.clusterName, selector.clusterTag};
}

const ISchedulerAdapter* JobRegistry::adapterNamed(const std::string& name) const {
    return namespaceAdapterNamed(name);
}

bool JobRegistry::adoptNamespaceAliases(Job& job, const std::string& clusterName,
                                        const std::string& clusterTag) const {
    if (const auto* adapter = namespaceAdapterOf(job))
        return adapter->adoptNamespaceAliases(job, clusterName, clusterTag);
    // No registered adapter answers for this scheduler: the generic business
    // keeps the aliases in the common fields — the same fields the generic
    // comparisons read.
    job.clusterName = clusterName;
    job.clusterTag = clusterTag;
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
        adapter->adoptNamespaceAliases(job, job.clusterName, job.clusterTag);
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
    // Fallback for test wiring (which bypasses configuration) and a scheduler
    // registered after the adapter set was assembled. Built but never armed:
    // the watcher carries no state until a discovery source starts it, so this
    // instance answers facts questions only.
    auto built = SchedulerAdapterRegistry::instance().build(name, SchedulerAdapterSettings{});
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
    // goes through the assembled adapter set (or the test/unknown fallback
    // cache), so it never depends on which discovery types are enabled.
    // Without an adapter the generic selector is the best the facts can say —
    // and nullopt is reported, never a guess.
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

    // The adapter set is assembled BEFORE the store restores rows: a persisted
    // row decodes through the same settings-driven adapter the runtime uses,
    // never through a lazily built default-configured one. Every registered
    // scheduler gets exactly one adapter whether or not its discovery source
    // is enabled — the adapter is the facts/interface layer for jobs from any
    // source, `auto_discovery_types` only arms the watcher.
    MaintenanceConfig maintenance;
    bool maintenance_ready = false;
    const bool production_wiring =
        !options.test_static_registry && options.test_sources.empty() &&
        !options.test_heartbeat;
    if (production_wiring) {
        try {
            maintenance = resolve_maintenance_config(Config::instance().dumpConfig());
            maintenance_ready = true;
        } catch (const std::exception& error) {
            spdlog::error("JobRegistry: invalid maintenance configuration; registry "
                          "maintenance is disabled: {}", error.what());
        }
        if (maintenance_ready && !buildAdapters(maintenance))
            maintenance_ready = false;
    }

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
    // is installed by armDiscovery below.
    if (options.enable_pid_tracker) init_pid_tracker();

    // Discovery startup, in the order that matters: reset the heartbeat
    // bookkeeping and install the registration sink (both BEFORE the wiring
    // decision, so a Disabled registry exits in the same state as today),
    // then arm the components (policies, probes, capability gate, sources),
    // then admit the restored owners through that gate, then start the sources
    // (the sink must exist before any poll thread), run the first
    // authoritative pass (blocking: the record set is settled before the RPC
    // surface opens), publish the status providers and finally start the
    // maintenance worker. Production always arms this stack (a registered job
    // needs removal/checkpoint upkeep); the purely static form is a
    // test-only escape hatch (test_static_registry).
    DiscoveryWiring wiring = DiscoveryWiring::Disabled;
    if (!options.test_static_registry) {
        resetHeartbeatState();
        installHeartbeatSink();
        wiring = armDiscovery(options, maintenance, maintenance_ready);
    }

    // Restored active records must be re-seeded into the kernel namespace this
    // process now owns, before any collector can attach to them. This runs
    // after arming: the capability gate decides which adapters may participate,
    // and a record admitted before the gate exists would be seeded
    // unconditionally (capable() defaults to true while no lookup is armed).
    admitRestoredOwners();

    if (!options.test_static_registry && wiring != DiscoveryWiring::Disabled) {
        startDiscoverySources(wiring);
        runInitialPass();
        publishStatusProvider();
        startHeartbeat();
        if (wiring == DiscoveryWiring::Test)
            spdlog::info("JobRegistry: heartbeat running with {} test source(s)",
                         hb_sources_.size());
        else
            spdlog::info("JobRegistry: registry heartbeat running with {} discovery type(s)",
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
    job.JobNativeID = row.value("JobNativeID", "");
    job.clusterName = row.value("clusterName", "");
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
    for (auto& row : rows) {
        normalizeNamespaceFacts(row.job);
        row.job.group_probed = !getGroups(row.job).empty();
    }
    for (auto& row : rows) publishRecord(std::move(row));
}

void JobRegistry::installCollectorValidator()
{
    const auto known = known_collector_names();
    setCollectorValidator(
        [known](const std::string& name) { return known.count(name) != 0; });
}

void JobRegistry::runInitialPass()
{
    if (!runPass(true, /*wait=*/true)) {
        spdlog::warn("JobRegistry: the initial maintenance pass did not run");
    }
}

JobRegistry& JobRegistry::instance() {
    static JobRegistry reg;
    return reg;
}

// ---------------------------------------------------------------------------
// Registry heartbeat — the maintenance function of the registry.
// Thread-free policy components (probes, removal rules, absence counters)
// are driven from one periodic worker; there is no separate service object.
// ---------------------------------------------------------------------------

namespace {
constexpr int kAbsencesBeforeRetirement = 2;
constexpr std::size_t kEndedManagerCapacity = 4096;
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
                      key->job_native_id);
        result.message = "obsolete execution manager";
        return result;
    }

    // Readiness: the facts name a job AND its execution manager is complete.
    if (!key || !manager.complete()) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_incomplete_observations_;
        spdlog::debug("JobRegistry: incomplete discovery '{}'", job.JobNativeID);
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
                        "the admission policy refused this observation");
            return result;
        }
        result = upsertObservedJob(job, &*admission);
    }
    if (result.ok) {
        observed.insert(result.job_id);
        if (result.disposition == UpsertDisposition::Created)
            created.insert(result.job_id);
        std::lock_guard<std::mutex> lock(hb_mtx_);
        auto& entry = absences_[result.job_id];
        entry.manager = manager;
        entry.complete_absences = 0;
        entry.last_seen_ms = clockNow();
        entry.last_seen_seq = ++observation_seq_;
        suspect_ids_.erase(result.job_id);
        return result;
    }

    if (result.error == "incomplete_identity") {
        // An unresolvable identity is not an error of the pass: it stays out
        // of the identity index and is retried on the next observation.
        return result;
    }

    if (result.error == "execution_manager_conflict" ||
        result.error == "identity_quarantined" ||
        result.error == "instance_closing" ||
        result.error == "identity_index_mismatch") {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_upsert_conflicts_;
        hb_last_error_ = result.error + (result.message.empty() ? "" : ": " + result.message);
        spdlog::warn("JobRegistry: discovery for '{}' rejected: {}",
                     job.JobNativeID, result.error);
        return result;
    }

    noteHeartbeatError("job upsert failed: " + result.error +
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
        // The snapshot may predate a removal this pass already applied:
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
    // one) and the recorded pid identities are the only evidence. This is the
    // pre-existing pid identity rule, now reported as a RefreshResult so the
    // removal decision reads one shape.
    RefreshResult result;
    const ProcessIdentity& manager = manager_of(job);
    if (manager.complete()) {
        const LivenessResult liveness =
            process_probe_ ? process_probe_(manager) : LivenessResult{};
        result.manager = liveness.state;
    }
    probePidIdentities(job, result);
    return result;
}

void JobRegistry::probePidIdentities(const Job& job, RefreshResult& evidence) const {
    for (const auto& pid_identity : pid_identities_of(job)) {
        if (evidence.any_pid_identity_live) break;
        const LivenessResult liveness =
            process_probe_ ? process_probe_(pid_identity) : LivenessResult{};
        if (liveness.state == LivenessState::Alive) {
            evidence.any_pid_identity_live = true;
        } else if (liveness.state == LivenessState::Unknown) {
            // An unreadable pid identity might be alive: never death evidence.
            evidence.pid_identity_inconclusive = true;
        }
    }
}

void JobRegistry::probeSurvivingPidIdentities(const Job& stored, const Job& refreshed,
                                              RefreshResult& evidence) const {
    if (refreshed.execution.pid_identities_authoritative) return;  // a confirmed read wins
    if (!pid_identities_of(refreshed).empty()) return;             // the read produced evidence
    probePidIdentities(stored, evidence);
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
            outcome.has_pid_identity_evidence = !pid_identities_of(updated.job).empty();
            {
                std::unique_lock lg(records_mtx_);
                records_[current->job.JobID] = std::move(updated);
            }
            return outcome;
        }

        RecordTxn txn(*this, "refresh job evidence", /*durable=*/false);
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
        outcome.has_pid_identity_evidence =
            published ? !pid_identities_of(published->job).empty() : true;
        changed = true;
    }
    // Evidence changes reach bound consumers through the runtime, which re-reads
    // the record. Outside every registry lock, on an unsynced batch.
    if (changed) wake_runtime();
    return outcome;
}

bool JobRegistry::removeIfConfirmed(const JobRecord& record, const std::string& reason) {
    // Only remove the manager the evidence was gathered for. A late
    // decision for an older execution must never remove newer ownership.
    const auto current = ownRecord(record.job.JobID);
    if (!current) return false;
    if (!(manager_of(current->job) == manager_of(record.job))) {
        spdlog::info("JobRegistry: JobID {} changed execution identity before "
                     "removal; keeping current ownership", record.job.JobID);
        return false;
    }

    rememberEnded(record);
    if (!removeJob(record.job.JobID)) {
        noteHeartbeatError("job removal failed for JobID " +
                           std::to_string(record.job.JobID));
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_removals_;
        closing_managers_.emplace_back(record.job.JobID,
                                           manager_of(record.job));
        absences_.erase(record.job.JobID);
        suspect_ids_.erase(record.job.JobID);
    }
    spdlog::info("JobRegistry: removed JobID {}: {}", record.job.JobID, reason);
    return true;
}

std::optional<JobRegistry::InitialDecision> JobRegistry::admissionDecision(const Job& job) {
    // capture_all on: every captured job is admitted; the automatic selection
    // stays with the post-registration path (applyAutoPolicyForJob), so no
    // decision is pre-evaluated here.
    if (capture_all_) {
        InitialDecision admitted;
        return admitted;
    }

    // capture_all off: the type's auto_add tri-state decides. A type without
    // an entry — including subtypes no configured adapter owns — behaves as
    // Off.
    const auto state = auto_add_states_.find(job.subtype_name);
    if (state == auto_add_states_.end() || state->second == AutoAdd::Off) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_not_admitted_capture_all_off_;
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
        ++hb_not_admitted_capture_all_off_;
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
                     job.JobNativeID, error);
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            ++hb_admission_errors_;
        }
        noteHeartbeatError("admission evaluation failed for '" + job.JobNativeID +
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
    // at all when capture_all is off, unlike capture_all mode where a reject
    // keeps the job inventoried with an empty selection. An accept without
    // collectors has already resolved to the configured defaults inside the
    // policy (or was refused there as an error).
    if (!decision.accepted) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_not_admitted_by_rule_;
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
    ended_managers_[key->key()] = manager_of(record.job);
    while (ended_managers_.size() > kEndedManagerCapacity) {
        ended_managers_.erase(ended_managers_.begin());
    }
}

bool JobRegistry::isObsolete(const SchedulerKey& key,
                             const ProcessIdentity& manager) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    const auto it = ended_managers_.find(key.key());
    return it != ended_managers_.end() && it->second == manager;
}

void JobRegistry::evaluateRemovals(const PassRun& pass) {
    const auto records = ownRecords();
    const std::int64_t now = clockNow();
    // A targeted pass (`enumerate` false) has no authoritative source coverage,
    // so it never counts as an absence; only positive termination evidence
    // applies.
    const std::map<std::string, bool> no_coverage;
    const std::map<std::string, bool>& source_complete =
        pass.enumerate ? pass.source_complete : no_coverage;

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
            seen_this_pass = pass.observed.count(record.job.JobID) != 0 ||
                             entry.last_seen_seq > pass.scan_seq;
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
        // stays Active, and an unverified instance is refused kernel admission
        // and never removed. A presence observation restores the verdict; the
        // call is outside hb_mtx_ (records_mtx_ is taken under it, never the
        // other way) and before the seen/no-evidence short-circuits.
        setExecutionPresumedLive(record.job.JobID, absences == 0);
        if (seen_this_pass) continue;

        // The eager refresh this pass already read is the record's evidence;
        // re-probing here would read the same facts twice. A record without a
        // refresh this pass (created now, or one whose refresh lost to newer
        // facts) has no verdict to apply.
        const auto refreshed_it = pass.refreshed.find(record.job.JobID);
        if (refreshed_it == pass.refreshed.end()) continue;
        const RefreshedEvidence& evidence = refreshed_it->second;

        // Records without a complete execution manager (manual
        // registrations, unresolved observations) have no manager to probe.
        // The pid identity list is their only evidence: live or unreadable pids keep
        // the record, an all-dead list removes it, and an empty list is "no
        // evidence yet", never "all dead".
        const ProcessIdentity manager = manager_of(record.job);
        if (!manager.complete()) {
            if (!evidence.has_pid_identity_evidence) continue;
            if (evidence.evidence.any_pid_identity_live || evidence.evidence.pid_identity_inconclusive)
                continue;
            removeIfConfirmed(record,
                              "no live process for a manually registered instance");
            continue;
        }

        // Remove only on positive termination evidence: a dead manager plus a
        // cgroup that was read and is confirmed empty. A readable but
        // non-empty cgroup whose pids could not be stat'ed is NOT an empty
        // cgroup, and an unreadable cgroup is no evidence at all.
        if (evidence.evidence.manager == LivenessState::Dead &&
            evidence.evidence.cgroup_empty_confirmed) {
            removeIfConfirmed(
                record, "positive termination evidence (manager ended, execution cgroup empty)");
            continue;
        }

        if (absences < kAbsencesBeforeRetirement) continue;
        // Unknown is not death: an unreadable manager blocks the absence rule
        // exactly like a live one does.
        if (evidence.evidence.manager != LivenessState::Dead) continue;
        if (evidence.evidence.any_pid_identity_live || evidence.evidence.pid_identity_inconclusive)
            continue;

        removeIfConfirmed(
            record, "two complete source passes without an observation and no live pid identity");
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

    auto pass = beginPass(do_scan, refresh_facts);
    if (!pass) return false;

    // Phase 1: discovery — which managers exist, which of them are unknown.
    discoverManagers(*pass);
    // Phase 2: evidence — one refresh per active record, applied once.
    refreshActiveRecords(*pass);
    // Phase 3: the record-driven obligation that needs no new observation.
    repairMissingInitialDecisions();
    // Phase 4: one fsynced batch, then the status the scan justifies.
    checkpointPass();
    publishPassStatus(*pass);
    // Phase 5: remove what the evidence no longer supports.
    evaluateRemovals(*pass);
    // Phase 6: a Closing record is not "done" just because it left the active
    // map — finalize restored closings and retry cleanups that failed.
    reconcileClosingRemovals();

    return true;
}

std::optional<JobRegistry::PassRun> JobRegistry::beginPass(bool do_scan, bool refresh_facts) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    if (!hb_admitted_ || hb_stopping_) return std::nullopt;
    // Anything noteHeartbeatError() records from here on belongs to this pass
    // and must survive its own "scan complete" status update. The observation
    // sequence is captured before the scan: a registration accepted during the
    // pass must not be read as an absence.
    PassRun pass;
    pass.enumerate = do_scan;
    pass.refresh = refresh_facts;
    pass.error_epoch = hb_error_epoch_;
    pass.scan_seq = observation_seq_;
    pass.started_ms = clockNow();
    return pass;
}

void JobRegistry::discoverManagers(PassRun& pass) {
    if (!pass.enumerate) return;

    // Manager triple -> record id from the current image: a manager this
    // registry already tracks costs no observe() at all.
    ManagerIndex manager_index;
    for (const auto& record : ownRecords()) {
        if (record.state == RecordState::Closing) continue;
        const ProcessIdentity& manager = manager_of(record.job);
        if (!manager.complete()) continue;
        manager_index.emplace(managerKey(manager), record.job.JobID);
    }

    for (const auto& [name, source] : hb_sources_) {
        // The pass only discovers managers: no job ad, no pid identity read, no fact
        // merge into an existing record.
        ManagerSet managers;
        try {
            managers = source->enumerateManagers();
        } catch (const std::exception& exception) {
            managers.complete = false;
            managers.error = std::string("manager enumeration threw: ") + exception.what();
        }
        noteSourceCoverage(pass, name, managers);

        for (const auto& manager : managers.managers) {
            if (!manager.complete()) continue;   // a partial entry names nobody
            const auto tracked = manager_index.find(managerKey(manager));
            if (tracked != manager_index.end()) {
                pass.observed.insert(tracked->second);
                continue;
            }
            observeManager(name, *source, manager, pass, manager_index);
        }
    }
}

void JobRegistry::observeManager(const std::string& source_name, IDiscoverySource& source,
                                 const ProcessIdentity& manager, PassRun& pass,
                                 ManagerIndex& manager_index) {
    std::optional<Job> job;
    try {
        job = source.observe(manager);
    } catch (const std::exception& exception) {
        spdlog::warn("JobRegistry: observe() for a '{}' manager failed: {}", source_name,
                     exception.what());
        return;
    }
    if (!job) return;   // gone/unreadable: retried, never guessed

    const auto observation = processObservation(*job, pass.observed, pass.created);
    // Only an accepted observation may make this manager "tracked". An identity
    // conflict (a different live execution owns the job) must keep it unknown:
    // mapping it to the old record would hide the new manager and keep the old
    // record permanently "seen", so it could never remove.
    if (!observation.ok) return;

    uint64_t record_id = 0;
    if (observation.disposition == UpsertDisposition::Created) {
        record_id = observation.job_id;
    } else if (const auto key = keyOf(*job)) {
        if (const auto record = ownRecordByKey(*key);
            record && manager_of(record->job) == manager) {
            record_id = record->job.JobID;
        }
    }
    if (record_id != 0) manager_index.emplace(managerKey(manager), record_id);
}

void JobRegistry::noteSourceCoverage(PassRun& pass, const std::string& name,
                                     const ManagerSet& managers) {
    if (!managers.complete) {
        const std::string detail =
            managers.error.empty() ? "incomplete manager pass" : managers.error;
        if (pass.error.empty()) pass.error = name.empty() ? detail : name + ": " + detail;
    }
    pass.complete = pass.complete && managers.complete;
    // Coverage is keyed by adapter name; a record of one adapter is only
    // absence-eligible when that adapter's source covered its inputs (records
    // with no adapter — manual registrations — never are). Two sources sharing
    // a name must both have covered it.
    const auto inserted = pass.source_complete.emplace(name, managers.complete);
    if (!inserted.second)
        inserted.first->second = inserted.first->second && managers.complete;
}

void JobRegistry::refreshActiveRecords(PassRun& pass) {
    // Eager update: every active record, whatever produced it, goes through the
    // same evidence reader and the same apply. A record created by this pass
    // already carries the observe() image and is skipped. An idle retry tick
    // (refresh_facts=false) re-runs the bookkeeping only: the fact readers are
    // cadence-bound, not 100 ms-tick-bound.
    if (!pass.refresh) return;

    // The adapter answering for a subtype is fixed once the registry is armed,
    // so one lookup per subtype replaces a lookup (and its identity scan) per
    // record. A legacy row without a subtype name has no stable cache key and
    // falls back to the direct call.
    std::map<std::string, ISchedulerAdapter*> adapters;
    const auto adapter_for = [this, &adapters](const Job& job) -> ISchedulerAdapter* {
        if (job.subtype_name.empty()) return namespaceAdapterOf(job);
        const auto cached = adapters.find(job.subtype_name);
        if (cached != adapters.end()) return cached->second;
        ISchedulerAdapter* const adapter = namespaceAdapterOf(job);
        adapters.emplace(job.subtype_name, adapter);
        return adapter;
    };

    for (const auto& record : ownRecords()) {
        if (record.job.JobID == 0 || record.state == RecordState::Closing) continue;
        if (pass.created.count(record.job.JobID) != 0) continue;
        Job job = record.job;
        RefreshResult evidence;
        try {
            // Reads run strictly outside every registry lock; the adapter owns
            // the scheduler-specific part, the registry passes the probes.
            if (ISchedulerAdapter* const adapter = adapter_for(job); adapter != nullptr) {
                evidence = adapter->refreshFacts(job, process_probe_, cgroup_probe_);
                probeSurvivingPidIdentities(record.job, job, evidence);
            } else {
                update_job(job);
                evidence = probeRecordEvidence(job);
            }
        } catch (const std::exception& exception) {
            {
                std::lock_guard<std::mutex> lock(hb_mtx_);
                ++hb_refresh_failures_;
            }
            spdlog::warn("JobRegistry: fact refresh for JobID {} failed: {}", record.job.JobID,
                         exception.what());
            continue;
        }
        if (const auto applied = applyRefreshedRecord(record, std::move(job), evidence))
            pass.refreshed.emplace(record.job.JobID, *applied);
    }
}

void JobRegistry::checkpointPass() {
    // Facts and intent are written without a fence on the hot path, so one
    // fsynced batch per pass bounds what a process crash can lose. The pass
    // cadence (reconcile_interval_sec) is the checkpoint cadence on purpose:
    // no extra knob, no extra thread. The row-provider snapshot and its
    // checkpoint write stay inside the same transaction boundary as
    // record/index mutations.
    std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
    if (!store_->flushDirty("maintenance checkpoint")) {
        spdlog::warn("JobRegistry: the maintenance checkpoint could not be written");
    }
}

void JobRegistry::publishPassStatus(const PassRun& pass) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    ++hb_passes_;
    if (!pass.enumerate) return;

    last_scan_start_ms_ = pass.started_ms;
    last_scan_complete_ = pass.complete;
    if (pass.complete) {
        last_scan_success_ms_ = clockNow();
        // A registration/admission error raised while this pass was running is
        // not a scan failure, but the stack is not healthy either: keep it
        // visible until a later pass observes no new error (a fresh scan error
        // is recorded below).
        if (hb_error_epoch_ == pass.error_epoch) {
            degraded_ = false;
            hb_last_error_.clear();
        }
        return;
    }
    degraded_ = true;
    hb_last_error_ = pass.error.empty() ? "source pass incomplete" : pass.error;
}

bool JobRegistry::reconcileNow() {
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
        const auto note_seen = [this, &job](uint64_t job_id) {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            auto& entry = absences_[job_id];
            entry.manager = manager_of(job);
            entry.complete_absences = 0;
            entry.last_seen_ms = clockNow();
            entry.last_seen_seq = ++observation_seq_;
            suspect_ids_.erase(job_id);
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
            if (result.ok) note_seen(result.job_id);
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
                         "the admission policy refused this observation");
            return refused;
        }
        auto result = upsertObservedJob(job, &*admission);
        if (result.ok) note_seen(result.job_id);
        return result;
    };
}

void JobRegistry::publishStatusProvider() {
    set_status_provider([this]() { return heartbeatStatusJson(); });
    set_refresh_provider([this]() { return reconcileNow(); });
    {
        std::lock_guard<std::mutex> lock(exit_observer_mtx_);
        exit_observer_ = [this](pid_t pid) { notifyExit(pid); };
    }
    std::lock_guard<std::mutex> lock(hb_mtx_);
    status_published_ = true;
}

void JobRegistry::retractStatusProvider() {
    if (status_published_) {
        set_status_provider({});
        status_published_ = false;
    }
    set_refresh_provider({});
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
    ended_managers_.clear();
    closing_managers_.clear();
    last_scan_start_ms_ = 0;
    last_scan_success_ms_ = 0;
    last_scan_complete_ = false;
    hb_passes_ = 0;
    hb_stop_errors_ = 0;
    hb_refresh_failures_ = 0;
}

// Assembles exactly one settings-driven adapter per registered scheduler,
// before anything can restore, decode or register a job. The adapter is the
// scheduler's facts/interface layer for jobs from every source; whether its
// discovery watcher runs is decided later, per `auto_discovery_types`.
bool JobRegistry::buildAdapters(const MaintenanceConfig& config)
{
    adapters_.clear();
    for (const auto& [adapter_name, settings] : config.settings) {
        // A configuration section for a scheduler with no registered factory
        // stays inert (resolve_maintenance_config already warned); it cannot
        // arm a source or answer facts.
        if (!SchedulerAdapterRegistry::instance().known(adapter_name)) continue;
        auto adapter = SchedulerAdapterRegistry::instance().build(adapter_name, settings);
        if (!adapter) {
            spdlog::error("JobRegistry: no factory built the registered adapter '{}'; "
                          "registered: {}",
                          adapter_name,
                          fmt::join(SchedulerAdapterRegistry::instance().names(), ", "));
            adapters_.clear();
            return false;
        }
        adapters_.push_back(std::move(adapter));
    }
    std::string assembled;
    for (const auto& adapter : adapters_)
        assembled += (assembled.empty() ? "" : ", ") + adapter->name();
    spdlog::info("JobRegistry: {} scheduler adapter(s) assembled: {}", adapters_.size(),
                 assembled);
    return true;
}

// Arms the registry components from the resolved configuration: the
// automatic-selection policies, the discovery sources of the enabled types,
// the production liveness probes and the kernel-attribution capability gate.
// The adapter set itself was built by buildAdapters() before the store
// restored anything. Nothing is started here; the caller starts sources and
// the first pass afterwards, so the gate is guaranteed to exist before any
// attribution can activate.
JobRegistry::DiscoveryWiring
JobRegistry::armDiscovery(const JobRegistryOptions& options,
                          const MaintenanceConfig& config,
                          bool maintenance_ready)
{
    // Symmetric resets first: every exit path (including failures) leaves the
    // same armed-state behind. adapters_ belongs to buildAdapters(): it is
    // cleared there (or never built under test wiring), never here.
    auto_policies_.clear();
    auto_add_states_.clear();
    auto_discovery_types_.clear();
    capture_all_ = true;

    // Test wiring: pre-built sources and injected probes, no configuration
    // and no automatic selection.
    if (!options.test_sources.empty() || options.test_heartbeat) {
        adapters_.clear();   // test wiring installs no production adapter set
        hb_sources_ = options.test_sources;
        process_probe_ = options.test_process_probe;
        cgroup_probe_ = options.test_cgroup_probe;
        if (options.test_auto_policy) {
            auto_policies_.clear();
            auto policy = options.test_auto_policy();
            load_rules_at_startup(options.test_auto_policy_adapter, policy);
            auto_policies_[options.test_auto_policy_adapter] = std::move(policy);
        }
        capture_all_ = options.test_capture_all;
        auto_add_states_ = options.test_auto_add_states;
        reconcile_interval_sec_ = 30;
        return DiscoveryWiring::Test;
    }

    // The configuration was resolved (and the adapters built) before the
    // store restored rows; an invalid document or a failed adapter build
    // disables the runtime here.
    if (!maintenance_ready) return DiscoveryWiring::Disabled;

    reconcile_interval_sec_ = config.reconcile_interval_sec;
    // The admission gate follows the resolved configuration.
    capture_all_ = config.capture_all;
    auto_discovery_types_ = config.auto_discovery_types;
    for (const auto& [adapter_name, settings] : config.settings) {
        auto_add_states_[adapter_name] = settings.auto_add;
    }
    // Configuration wins over the construction default so the budget is a
    // deployment knob, not a rebuild.

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

    // Discovery sources: only the adapters whose type is listed in
    // `auto_discovery_types`. Every other adapter stays available for facts,
    // identity, refresh and kernel attribution of jobs that arrive another
    // way (manual registration, RPC, restored rows).
    hb_sources_.clear();
    for (const auto& adapter : adapters_) {
        const auto settings = config.settings.find(adapter->name());
        if (settings == config.settings.end() ||
            !settings->second.auto_discovery_enabled)
            continue;
        hb_sources_.emplace_back(adapter->name(), &adapter->source());
    }
    if (auto_discovery_types_.empty()) {
        spdlog::info("JobRegistry: no auto discovery types configured; no watcher starts, "
                     "and every registered adapter still serves manual registrations, "
                     "refresh and kernel attribution");
    } else {
        spdlog::info("JobRegistry: auto discovery enabled for {} type(s): {}",
                     hb_sources_.size(),
                     fmt::join(auto_discovery_types_, ", "));
    }

    // Adapter-owned identity->facts derivation needs no wiring anymore: the
    // registry dispatches through adapters_ itself while building record
    // images (see applyIdentityToFacts).

    // Liveness probes only: discovered Jobs arrive fully typed from the
    // watchers, so there is no observation->facts encoder to wire.
    process_probe_ = make_proc_liveness_probe();
    cgroup_probe_ = make_cgroup_membership_probe();

    // Kernel attribution capability gate: only adapters with the trait
    // register into pid2job/cgroup2job. An open-world subtype with no adapter
    // has no trait to consult and keeps the legacy participation. Installed
    // before the initial pass can activate anything.
    if (attribution_) {
        attribution_->setCapabilityLookup([this](uint64_t job_id) {
            auto record = ownRecord(job_id);
            if (!record) return false;
            if (const auto* const adapter = namespaceAdapterOf(record->job))
                return adapter->traits().kernel_attribution;
            return true;
        });
    }

    return DiscoveryWiring::Production;
}

void JobRegistry::startDiscoverySources(DiscoveryWiring wiring)
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
        if (!started && wiring == DiscoveryWiring::Production)
            spdlog::warn("JobRegistry: source '{}' has no event source; discovery "
                         "continues through the manager pass: {}",
                         name, source_start_notes_.back());
    }
}

void JobRegistry::stopDiscovery() {
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


std::optional<bool> JobRegistry::applyAutoPolicyForJob(const Job& job,
                                                       const std::string& adapter)
{
    const auto decision = autoSelectionFor(job, adapter);
    if (!decision) return std::nullopt;   // no enabled policy: no decision needed
    const bool committed =
        applyOneAutoSelection(job.JobID, decision->collectors, decision->policy_revision);
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

bool JobRegistry::applyOneAutoSelection(uint64_t job_id,
                                        const std::vector<std::string>& collectors,
                                        const std::string& policy_revision)
{
    const auto outcome = applyAutoPolicy(
        {AutoSelection{job_id, collectors, policy_revision}});
    if (!outcome.ok) {
        spdlog::warn("JobRegistry: automatic selection for JobID {} was rejected: {}",
                     job_id, outcome.message);
        return false;
    }
    // The decision is durable when the instance reached a stored intent. A
    // skipped target (unknown instance/collector) or a failed transaction is
    // not committed even though the batch reported ok.
    for (const auto& target : outcome.targets) {
        if (target.job_id != job_id) continue;
        return !target.skipped && target.revision > 0;
    }
    return false;   // no target: the selection was skipped, not committed
}

bool JobRegistry::deinit()
{
    if (deinit_done_) return deinit_ok_;

    // Shutdown order: discovery admission stopped first (the service calls
    // stopDiscovery() before draining the scheduler), then the
    // removals are finalized, then attribution, then the store.
    stopDiscovery();

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

    // Finalize: acknowledge the removals this heartbeat made. The collector
    // drain has completed by now; a late acknowledgement for an old manager
    // is discarded.
    {
        std::vector<std::pair<uint64_t, ProcessIdentity>> closing;
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            closing.swap(closing_managers_);
        }
        for (const auto& [job_id, manager] : closing) {
            const auto record = ownRecord(job_id);
            if (record && !(manager_of(record->job) == manager)) {
                spdlog::info("JobRegistry: JobID {} no longer carries the removed "
                             "manager; acknowledgement discarded", job_id);
                continue;
            }
            if (!record) continue;   // already archived
            // Shutdown is a single pass: it never retries a write whose last
            // outcome is unknown. Such a record stays closing and is resolved
            // by an explicit command or the next open.
            if (!finalizeClosingRemoval(job_id, /*allow_recovery=*/false) &&
                ownRecord(job_id)) {
                ok = false;
                std::lock_guard<std::mutex> lock(hb_mtx_);
                closing_managers_.emplace_back(job_id, manager);
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

bool JobRegistry::attributionJobExists(uint64_t job_id) {
    return store_ && ownHasActive(job_id);
}

void JobRegistry::attributionJobAddPid(uint64_t job_id, pid_t pid) {
    // Runtime projection on the registry's own record: never persisted, the
    // kernel rebuilds it from events after a restart. The runtime wake happens
    // after the record lock is released, so a wake callback can read the
    // record it was woken for.
    bool changed = false;
    {
        std::unique_lock lg(records_mtx_);
        const auto it = records_.find(job_id);
        if (it == records_.end()) return;
        auto& pid_identities = it->second.job.execution.pid_identities;
        const bool known = std::any_of(
            pid_identities.begin(), pid_identities.end(),
            [pid](const ProcessIdentity& pid_identity) { return pid_identity.pid == pid; });
        if (!known) {
            pid_identities.push_back(ProcessIdentity{pid, 0, std::string{}});
            std::sort(pid_identities.begin(), pid_identities.end(),
                      [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                          return lhs.pid < rhs.pid;
                      });
            spdlog::trace("JobRegistry: kernel FORK added pid={} to job_id={} (now {} pids)",
                          pid, job_id, pid_identities.size());
            changed = true;
        }
    }
    if (changed) wake_runtime();
}

void JobRegistry::attributionJobRemovePid(uint64_t job_id, pid_t pid) {
    bool changed = false;
    {
        std::unique_lock lg(records_mtx_);
        const auto it = records_.find(job_id);
        if (it == records_.end()) return;
        auto& pid_identities = it->second.job.execution.pid_identities;
        const auto before = pid_identities.size();
        pid_identities.erase(std::remove_if(pid_identities.begin(), pid_identities.end(),
                                            [pid](const ProcessIdentity& pid_identity) {
                                                return pid_identity.pid == pid;
                                            }),
                             pid_identities.end());
        if (pid_identities.size() != before) {
            spdlog::trace("JobRegistry: kernel EXIT removed pid={} from job_id={} (now {} pids)",
                          pid, job_id, pid_identities.size());
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

std::string JobRegistry::attributionCgroupPath(uint64_t job_id) {
    // Adapter-owned: cgroupPathOf is the only place that knows where the
    // cgroup lives in the typed facts. The adapter answers through the
    // namespace cache, so a manual registration works on a node that
    // configures no discovery type (the armed list is empty there).
    const auto record = ownRecord(job_id);
    if (!record) return {};
    if (const auto* const adapter = namespaceAdapterOf(record->job))
        return adapter->cgroupPathOf(record->job);
    return {};
}

bool JobRegistry::sync_job_to_kernel(const Job& job) {
    std::lock_guard<std::mutex> lock(kernel_owner_mtx_);
    if (!attribution_) return true;   // intentionally disabled: no kernel state
    if (!ownHasActive(job.JobID)) {
        // A stale Created/Updated snapshot whose instance has been removed
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

JobRegistry::BuiltRecord JobRegistry::buildJobRecord(
    Job job, uint64_t job_id, FactSource source,
    std::optional<std::vector<std::string>> manual_collectors,
    const std::optional<SchedulerKey>& known_key, int64_t observed_at) {
    JobRecord record;
    record.job = std::move(job);
    record.job.JobID = job_id;
    // Intent lives in the selection; the record image never carries a dispatch
    // list.
    if (manual_collectors) record.selection.manual = std::move(*manual_collectors);
    record.job.CollectorNames.clear();
    record.state = RecordState::Active;
    record.source = source;
    record.revision = 1;
    record.first_seen_us = observed_at;
    record.last_seen_us = observed_at;
    // The adapter owning the facts renders the projections the wire and the
    // collectors read. No adapter means they stay as given — incomplete, never
    // a guess.
    normalizeNamespaceFacts(record.job);
    projectRecordFacts(record.job);

    ExecutionEvidence& execution = record.job.execution;
    if (!execution.pid_identities.empty()) {
        execution.pid_identities = normalized_pid_identities(std::move(execution.pid_identities));
    } else if (!pids_of(record.job).empty()) {
        // No observation behind this registration: bind the pids it names to
        // the processes that hold them NOW. A bare number is not an execution
        // identity — recording the start ticks lets the kernel path verify the
        // same process later instead of trusting a recycled number.
        const std::string boot_id = read_boot_id();
        for (const auto pid : pids_of(record.job)) {
            const auto ticks = read_process_start_ticks(pid);
            execution.pid_identities.push_back(ProcessIdentity{
                pid, ticks.value_or(0), ticks.has_value() ? boot_id : std::string{}});
        }
    }

    BuiltRecord built;
    built.record = std::move(record);
    built.key = known_key ? known_key : keyOf(built.record.job);
    if (built.key) built.record.index_key = built.key->index_key();
    return built;
}

std::optional<JobRegistry::NewRecordId> JobRegistry::reserveJobId(uint64_t requested,
                                                                  std::string& error) {
    NewRecordId id;
    if (requested != 0) {
        // Explicit caller id: a primary-key write, no allocator movement.
        id.id = requested;
        return id;
    }
    // The allocator knows its own history, not the registry's rows: an explicit
    // create may already own the number it answers with. Skip such an id and
    // ask again — the next answer is the colliding id + 1, and the high-water
    // counter that rides the create batch already covers the skipped value.
    for (;;) {
        if (!store_->reserveId(id.id, id.counter, id.exhausted, error)) return std::nullopt;
        bool taken = false;
        {
            std::shared_lock lg(records_mtx_);
            taken = records_.count(id.id) != 0;
        }
        if (!taken) break;
        spdlog::warn("JobRegistry: the allocator answered JobID {} which an explicit create "
                     "already owns; skipping it", id.id);
    }
    id.writes_counter = true;
    return id;
}

bool JobRegistry::commitNewRecord(BuiltRecord built, const NewRecordId& id, bool durable,
                                  std::string& error) {
    {
        std::shared_lock lg(records_mtx_);
        // Every new id is checked, allocated or explicit: the allocator only
        // knows its own history, so its answer is not proof the primary key is
        // free. A race here refuses the create (a retry allocates again) instead
        // of overwriting a live row.
        if (records_.count(id.id) != 0) {
            error = "duplicate_job_id";
            return false;
        }
        if (!built.record.index_key.empty() &&
            identity_index_.count(built.record.index_key) != 0) {
            error = "duplicate_identity";
            return false;
        }
    }
    // The row, its identity cache and (for an allocated id) the counter ride
    // one batch. A create has no published image to revert, so it passes no
    // recovery fence; the identity lookup repairs an ambiguous landing.
    RecordTxn txn(*this, "create job record", durable);
    if (id.writes_counter) txn.reserveCounter(id.counter, id.exhausted);
    txn.put(std::move(built.record));
    if (built.key) txn.indexPut(*built.key, id.id);
    if (!txn.commit()) {
        error = txn.error();
        return false;
    }
    return true;
}

UpsertResult JobRegistry::createManualJob(Job& job) {
    UpsertResult result;
    spdlog::info("JobRegistry: createJob entry JobID={}, native_id={}, collectors={}, PIDs={}",
                 job.JobID, job.JobNativeID, job.CollectorNames.size(), pids_of(job).size());
    if (job.CollectorNames.empty()) {
        spdlog::warn("JobRegistry: job with JobID {} has empty CollectorNames", job.JobID);
        result.fail(RegistrationError::EmptyCollectors,
                    "a manual create requires at least one collector");
        return result;
    }
    std::string error;
    if (!recoverStoreIfNeeded(error)) {
        spdlog::error("JobRegistry: refusing registration: {}", error);
        result.fail(RegistrationError::StoreUnavailable, "registration refused: " + error);
        return result;
    }
    // Registration-time fact completion: a manual request names member pids,
    // not the execution scope. The adapter owning the facts derives whatever
    // the request did not carry (the slot cgroup, the starter pid) from that
    // evidence, BEFORE the record is built, so kernel attribution's first
    // activation already has a cgroup mapping and the refresh pass sees the
    // whole scope instead of the named pids.
    if (ISchedulerAdapter* const adapter = namespaceAdapterOf(job))
        adapter->deriveExecutionFacts(job);
    const auto id = reserveJobId(job.JobID, error);
    if (!id) {
        spdlog::error("JobRegistry: durable JobID allocation failed, refusing to register the "
                      "job: {}", error);
        result.fail(error == "id_exhausted" ? RegistrationError::IdExhausted
                                            : RegistrationError::StoreUnavailable,
                    "durable JobID allocation failed: " + error);
        return result;
    }
    auto built = buildJobRecord(job, id->id, FactSource::Manual, job.CollectorNames,
                                std::nullopt, now_us());
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        if (!commitNewRecord(std::move(built), *id, /*durable=*/false, error)) {
            spdlog::error("JobRegistry: manual registration of JobID {} was refused ({})",
                          id->id, error);
            // commitNewRecord's internal contract is its error string; this is
            // the one boundary where it becomes the typed code callers branch
            // on. The durable index conflict is the ambiguous-create repair
            // path: the row exists on disk while memory never published it,
            // so the answer is a conflict, not a store failure.
            if (error == "duplicate_job_id")
                result.fail(RegistrationError::DuplicateJobId,
                            "another record already owns JobID " + format_job_id(id->id));
            else if (error == "duplicate_identity" || error == "identity_conflict")
                result.fail(RegistrationError::DuplicateIdentity,
                            "another record already owns this scheduler identity");
            else
                result.fail(RegistrationError::StoreWriteFailed,
                            "durable write refused: " + error);
            return result;
        }
    }
    spdlog::info("JobRegistry: registered manual job JobID {} (committed; the maintenance "
                 "checkpoint persists it)", id->id);

    Job job_for_cb;
    if (auto stored = ownRecord(id->id)) {
        job_for_cb = stored->job;
    } else {
        result.fail(RegistrationError::StoreReadFailed,
                    "the committed record vanished before publication");
        return result;
    }
    sync_job_to_kernel(job_for_cb);
    // The runtime pulls the record it must attach. Waking is a coalesced hint,
    // never a synchronous apply on this stack.
    wake_runtime();
    spdlog::info("JobRegistry: add job with JobID {}", id->id);
    job.JobID = id->id;
    result.ok = true;
    result.disposition = UpsertDisposition::Created;
    result.job_id = id->id;
    return result;
}

bool JobRegistry::addJob(const Job& job) {
    // Strict create: an existing identity or a surviving native-id candidate is
    // a refusal, never an update. Routing is the same one the upsert entry
    // uses, so a create carries no second duplicate screen.
    Job copy = job;
    if (manualUpdateTarget(copy, std::nullopt).kind != ManualTarget::Kind::None) {
        spdlog::warn("JobRegistry: duplicate manual registration ignored");
        return false;
    }
    return createManualJob(copy).ok;
}

JobRegistry::ManualTarget JobRegistry::manualUpdateTarget(
    const Job& job, const std::optional<NativeSelector>& selector) {
    ManualTarget target;
    // The derived identity is the first and authoritative route: a full key
    // names exactly one record, so nothing has to be screened.
    std::optional<SchedulerKey> request_key;
    if (selector) request_key = canonicalize(*selector);
    if (!request_key) request_key = keyOf(job);
    if (request_key) {
        if (const auto record = ownRecordByKey(*request_key)) {
            // The index key is derived from the rendered namespace, so two
            // different structured splits can alias one canonical string. A
            // selector that disagrees with the stored facts must never update
            // the row: it is the same ambiguity the candidate route reports.
            const NamespaceFacts request_facts =
                selector ? selectorFactsOf(*selector) : namespaceFactsOf(job);
            if (!namespace_facts_compatible(request_facts,
                                            namespaceFactsOf(record->job))) {
                target.kind = ManualTarget::Kind::Ambiguous;
                target.message = "canonical identity collision between structured namespaces";
                target.candidates.push_back(format_job_id(record->job.JobID));
                return target;
            }
            target.record = *record;
            target.kind = record->state == RecordState::Closing
                              ? ManualTarget::Kind::Closing
                              : ManualTarget::Kind::Update;
            return target;
        }
    }

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
                target.candidates.push_back(format_job_id(record.job.JobID));
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
            target.candidates.push_back(format_job_id(match.job.JobID));
        target.message =
            fmt::format("typed sub_attr '{}' matches {} current local instances; "
                        "claim the scheduler attribution or use an identity selector",
                        job.JobNativeID, duplicates.size());
        return target;
    }
    if (!duplicates.empty()) {
        target.record = std::move(duplicates.front());
        target.kind = ownHasActive(target.record.job.JobID)
                          ? ManualTarget::Kind::Update : ManualTarget::Kind::Closing;
    }
    return target;
}

nlohmann::json JobRegistry::addOrUpdateManualJob(const Job& job,
                                                 const nlohmann::json& request,
                                                 std::optional<NativeSelector> identity_selector,
                                                 bool create_only) {
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
    const auto already_exists = [](uint64_t job_id, uint64_t revision) {
        nlohmann::json out;
        out["status"] = "error";
        out["code"] = "already_exists";
        out["message"] = "job instance " + format_job_id(job_id) +
                         " already exists";
        out["JobID"] = format_job_id(job_id);
        out["revision"] = revision;
        return out;
    };

    if (create_only && job.JobID != 0) {
        if (const auto existing = ownRecord(job.JobID)) {
            if (existing->state == RecordState::Closing) {
                return {{"status", "error"}, {"code", "instance_closing"},
                        {"message", "instance " + format_job_id(job.JobID) +
                                        " is closing and rejects new binding mutations"}};
            }
            return already_exists(job.JobID, existing->revision);
        }
    }

    const ManualTarget target = manualUpdateTarget(job, identity_selector);
    if (target.kind == ManualTarget::Kind::Closing) {
        nlohmann::json out;
        out["status"] = "error";
        out["code"] = "instance_closing";
        out["message"] = "instance " + format_job_id(target.record.job.JobID) +
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
        if (create_only) return already_exists(target.record.job.JobID, target.record.revision);
        Job claimed_job = job;
        if (identity_selector) {
            // The selector's namespace folds into the typed facts (already
            // proven compatible above), then the common fields render.
            adoptNamespaceAliases(claimed_job, identity_selector->clusterName,
                                  identity_selector->clusterTag);
            projectRecordFacts(claimed_job);
        }
        return updateManualInstance(target.record, claimed_job, request);
    }

    // CREATE branch: the manual registration, verbatim. An empty
    // collector list is a CREATE-only rejection — an UPDATE with no
    // collectors is a valid no-op and keeps its creation metadata.
    Job create_job = job;
    if (identity_selector) {
        adoptNamespaceAliases(create_job, identity_selector->clusterName,
                              identity_selector->clusterTag);
        projectRecordFacts(create_job);
    }
    const auto created = createManualJob(create_job);
    if (!created.ok) {
        // A refused create answers with its typed code so the trigger maps a
        // real status (409 already_exists / 400 invalid_collectors / the
        // store family keeps its own code on a 503) instead of the
        // unknown-code 503 a codeless error produced (JL-05).
        if (created.code == RegistrationError::DuplicateJobId ||
            created.code == RegistrationError::DuplicateIdentity) {
            // Name the instance the request collided with when the registry
            // can still see it: the explicit id first, then the identity
            // index (the create race loser's row).
            if (create_job.JobID != 0) {
                if (const auto existing = ownRecord(create_job.JobID))
                    return already_exists(existing->job.JobID, existing->revision);
            }
            if (const auto key = keyOf(create_job)) {
                if (const auto existing = ownRecordByKey(*key))
                    return already_exists(existing->job.JobID, existing->revision);
            }
        }
        return {{"status", "error"}, {"code", created.error},
                {"message", created.message}};
    }
    // Return the created resource identity. The legacy trigger adapter ignores
    // these fields; the v2 job resource uses them to address the new record.
    nlohmann::json out;
    out["status"] = "success";
    // JobID is the decimal string form: uint64-safe for JavaScript consumers.
    out["JobID"] = format_job_id(create_job.JobID);
    if (const auto record = jobRecord(create_job.JobID)) {
        out["revision"] = record->revision;
        out["mode"] = to_string(record->selection.mode());
        out["desired_collectors"] = record->selection.effectiveCollectors();
    }
    return out;
}

nlohmann::json JobRegistry::updateManualInstance(const JobRecord& target, const Job& job,
                                                 const nlohmann::json& request) {
    const uint64_t job_id = target.job.JobID;

    // Re-add only changes selection. Metadata and execution evidence remain
    // the creation/source snapshot; namespace claims cannot rebind that image.
    {
        std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
        const auto record = ownRecord(job_id);
        if (!record) {
            return {{"status", "error"}, {"msg", "instance vanished while updating"}};
        }
        if (record->state == RecordState::Closing) {
            return {{"status", "error"}, {"code", "instance_closing"},
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
        if (const auto current = ownSelection(job_id); current.manual.has_value())
            merged = *current.manual;
        for (const auto& name : job.CollectorNames) {
            if (std::find(merged.begin(), merged.end(), name) == merged.end())
                merged.push_back(name);
        }
        CollectionRequest command;
        command.targets = {job_id};
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
    const auto record = ownRecord(job_id);
    const auto selection = record ? record->selection : ownSelection(job_id);
    const auto actual = actualCollection(job_id);
    const uint64_t revision = record ? record->revision : 0;
    const bool changed = selection_outcome.has_value() && selection_outcome->changed;

    nlohmann::json out;
    out["status"] = "success";
    // The existing record's JobID, never a fresh allocation from the JobID
    // hint; the decimal string form keeps uint64 precision.
    out["JobID"] = format_job_id(job_id);
    out["revision"] = revision;
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
        if (hint_valid && hint_value != job_id) {
            out["ignored_job_id_hint"] = format_job_id(hint_value);
            spdlog::warn("JobRegistry: manual upsert for native job '{}' carried JobID {} "
                         "as a compatibility hint; resolved to job instance {}",
                         record ? record->job.JobNativeID : job.JobNativeID, hint_value,
                         job_id);
        }
    }
    spdlog::info("JobRegistry: manual upsert resolved to job instance {} (internal "
                 "{}, revision {})",
                 format_job_id(job_id), job_id, revision);
    return out;
}

bool JobRegistry::removeJob(uint64_t jobID) {
    // A registered command is also the retry point after a failed write: the
    // store first verifies every uncertain row before any new mutation.
    if (store_ && store_->recoveryRequired()) {
        std::string recovery_error;
        if (!recoverStoreIfNeeded(recovery_error)) {
            spdlog::error("JobRegistry: cannot remove JobID {}: store recovery failed ({})",
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
            std::lock_guard<std::mutex> applied_lock(removal_applied_mtx_);
            removal_applied_[jobID] = actualCollection(jobID).collectors;
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
                          "removal refused (the record stays active)", jobID);
            return false;
        }
        // The heartbeat path (removeIfConfirmed) records the ended manager
        // before removing; an explicit removal carries no such caller, so
        // the mark is made here. It is idempotent (a map insert) and uses the
        // record snapshot taken before the closing write, so the manager
        // remembered is the one that actually ended. Without it, a late
        // observation of the same execution would re-register the removed
        // instance instead of being refused by isObsolete().
        rememberEnded(*record);
        spdlog::info("JobRegistry: JobID {} is Closing; the collection runtime drains it",
                     jobID);
    }

    ICollectionRuntime* runtime = collectionRuntime();
    if (runtime == nullptr) {
        // Registry-only core: no collectors can hold the instance, so the
        // checked cleanup may run immediately.
        if (!cleanupThenArchive(jobID)) {
            spdlog::error("JobRegistry: removal of JobID {} stays closing until the "
                          "cleanup succeeds; it will be retried", jobID);
            return false;
        }
        spdlog::info("JobRegistry: remove instance with JobID {}", jobID);
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

void JobRegistry::onDrained(uint64_t job_id) {
    {
        std::lock_guard<std::mutex> lock(drain_mtx_);
        drain_complete_.insert(job_id);
    }
    if (!finalizeClosingRemoval(job_id, /*allow_recovery=*/true)) {
        spdlog::error("JobRegistry: closing JobID {} was not archived after the runtime "
                      "released it; kernel ownership and the Closing reservation are kept",
                      job_id);
        return;
    }
    std::lock_guard<std::mutex> lock(drain_mtx_);
    drain_complete_.erase(job_id);
}

bool JobRegistry::cleanupThenArchive(uint64_t jobID) {
    // One finalization at a time: an ack, the maintenance retry and the
    // shutdown sweep may all try to finish the same closing record. The
    // archive is idempotent only when they are serialized.
    std::lock_guard<std::mutex> finalize_lock(closing_mtx_);
    // Re-read under the finalize lock: a concurrent path may already have
    // archived this instance (or a newer manager may own the id again).
    const auto current = ownRecord(jobID);
    if (!current) return true;   // already archived: the goal is reached
    if (current->state != RecordState::Closing) return false;

    // A live runtime's accepted tail requires its explicit completion. A
    // locally captured removal also covers the window before the runtime
    // has reconciled the Closing row. Restored Closing rows have no such
    // capture: if the new runtime has no owner, preserve the restart fallback.
    if (auto* runtime = collectionRuntime()) {
        bool drained = false;
        {
            std::lock_guard<std::mutex> lock(drain_mtx_);
            drained = drain_complete_.count(jobID) != 0;
        }
        if (!drained) {
            bool locally_removed = false;
            {
                std::lock_guard<std::mutex> lock(removal_applied_mtx_);
                locally_removed = removal_applied_.count(jobID) != 0;
            }
            const auto actual = runtime->actual(jobID);
            if (locally_removed || actual.tracked || actual.draining) {
                spdlog::warn("JobRegistry: closing JobID {} has no proven runtime drain; "
                             "retaining the Closing reservation", jobID);
                return false;
            }
        }
    }
    const Job& closing_job = current->job;

    // Archive-versus-discard is a fact decision made here, not in the store:
    // the runtime snapshot captured when the removal was accepted
    // ("applied"), or — for a closing restored from disk, whose capture died
    // with the previous process — the durable effective selection as the
    // documented fallback. An empty resolved set means the instance was never
    // collected and leaves no history row.
    std::vector<std::string> applied;
    const char* provenance = "effective";
    {
        std::lock_guard<std::mutex> applied_lock(removal_applied_mtx_);
        const auto captured = removal_applied_.find(jobID);
        if (captured != removal_applied_.end()) {
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
        std::lock_guard<std::mutex> applied_lock(removal_applied_mtx_);
        removal_applied_.erase(jobID);
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

size_t JobRegistry::removeMatchingJobs(const SubAttrMatcher& matcher) {
    const auto matched = ownFindActive(
        [&matcher](const Job& job) { return matches_job(matcher, job); });

    size_t accepted = 0;
    for (const auto& record : matched) {
        // The same removal path every caller takes: durable Closing, then
        // the runtime's drain, then checked kernel cleanup and the archive.
        if (removeJob(record.job.JobID)) {
            ++accepted;
            spdlog::info("JobRegistry: remove job with JobID {} by sub_attr matcher",
                         record.job.JobID);
        }
    }

    spdlog::info("JobRegistry: removed {} of {} matching jobs", accepted, matched.size());
    return accepted;
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
    // Read-only snapshot: the heartbeat owns pid identity refresh and removal,
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
    // A record restored as active is presumed live, but this process starts
    // with an empty kernel namespace. Admit it through the same path
    // observations use, otherwise a restarted service only attributes the jobs
    // it happens to observe again.
    for (const auto& record : ownRecords()) {
        if (record.job.JobID == kSystemSentinelJobId) continue;
        if (record.state != RecordState::Active) continue;
        if (!record.presumed_live) continue;
        ensure_kernel_owner(record.job);
    }
}

// ---------------------------------------------------------------------------
// JobStore facade
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
                                           uint64_t job_id, int64_t observed_at,
                                           JobRecord& updated_out, MergeKind kind) {
    UpsertResult result;
    result.job_id = job_id;
    result.revision = stored.revision;

    const ProcessIdentity manager = manager_of(job);
    const std::vector<ProcessIdentity>& processes = pid_identities_of(job);
    const bool pid_identities_authoritative = pid_identities_authoritative_of(job);

    if (stored.state == RecordState::Closing) {
        // A removing instance must never be resurrected into the active set by
        // a late observation: the drain owns it until it is archived.
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::InstanceClosing;
        result.error = "instance_closing";
        result.message = "the instance is removing and no longer accepts observations";
        return result;
    }
    if (!stored.index_key.empty() && stored.index_key != key.index_key()) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::IdentityIndexMismatch;
        result.error = "identity_index_mismatch";
        result.message = "the identity index and the stored record disagree";
        spdlog::error("JobRegistry: identity index for JobID {} does not match the stored "
                      "record; refusing to merge", job_id);
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
        result.code = RegistrationError::ManagerConflict;
        result.error = "execution_manager_conflict";
        result.message = "a different live execution owns this identity";
        spdlog::warn("JobRegistry: observation for '{}' carries a different live manager "
                     "than JobID {}; not merged", key.job_native_id, job_id);
        return result;
    }

    JobRecord updated = stored;
    // An observation records who owns the facts (a manual row that first meets
    // its source becomes Discovery) and re-verifies the execution. A periodic
    // refresh only moves execution evidence: provenance and the liveness
    // verdict stay with their owners.
    const bool observation = kind == MergeKind::Observation;
    if (observation) updated.source = FactSource::Discovery;
    // Group backfill is limited to missing group fields in this execution;
    // unrelated scheduler metadata remains the creation snapshot.
    const bool groups_changed = fill_missing_groups(updated.job, job);
    // A refresh may have DERIVED an execution-scope fact the record never
    // carried (a registration made before its member pid was readable). The
    // adapter completes only empty slots, so identity, namespace and every
    // existing value stay frozen; a completion is persisted like any other
    // change so kernel attribution's reconcile can map the cgroup.
    bool facts_completed = false;
    if (!observation) {
        if (ISchedulerAdapter* const adapter = namespaceAdapterOf(updated.job))
            facts_completed = adapter->completeRefreshedFacts(updated.job, job);
    }
    updated.job.CollectorNames.clear();   // intent lives in selection
    // Keep stored execution evidence until the manager/pid identity rules below
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
        updated_evidence.pid_identities = normalized_pid_identities(processes);
    } else if (pid_identities_authoritative) {
        updated_evidence.pid_identities.clear();
    }

    // A positive observation re-admits a record whose liveness could not be
    // verified. Unverified is not a lifecycle state. A refresh carries no such
    // verdict: the removal pass owns it.
    const bool recovered = observation && !stored.presumed_live;
    if (recovered) updated.presumed_live = true;
    // Provenance is a durable fact of its own: a manual row that first meets a
    // discovery observation becomes a discovery row even when every other fact
    // is identical, so the next load rebuilds its evidence instead of treating
    // it as an operator binding.
    const bool provenance_changed = updated.source != stored.source;
    const bool changed = recovered || provenance_changed || groups_changed ||
        facts_completed ||
        manager_of(stored.job) != manager_of(updated.job) ||
        pid_identities_of(stored.job) != pid_identities_of(updated.job);
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
            target.job_id = mutation.job_id;
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
        target.actual = actualCollection(target.job_id);
    }
    outcome.ok = true;
    return outcome;
}

void JobRegistry::validateRestoredExecutions(std::vector<JobRecord>& rows) {
    // Evidence is durable, but it only proves "this was the run when the row was
    // written". One pass over the manager triple and the pid identities keeps what
    // still exists and drops what a restart cannot re-bind; the first
    // observation re-establishes the manager. A record whose evidence does
    // not survive stays active but unverified, never removed on a guess.
    const std::string boot_id = read_boot_id();
    for (auto& record : rows) {
        if (record.state == RecordState::Closing) continue;   // the drain owns it
        ExecutionEvidence& execution = record.job.execution;
        record.presumed_live = true;

        if (execution.manager.complete()) {
            const LivenessResult manager =
                verify_process_identity(execution.manager, boot_id, process_probe_);
            if (manager.state == LivenessState::Dead) {
                record.presumed_live = false;
                execution.pid_identities.clear();
                spdlog::warn("JobRegistry: restored JobID {} has a stale execution "
                             "manager; liveness unknown", record.job.JobID);
                continue;
            }
            if (manager.state == LivenessState::Unknown) {
                record.presumed_live = false;
                spdlog::warn("JobRegistry: restored JobID {} has an execution manager "
                             "that cannot be verified; liveness unknown, {} pid identity(ies) "
                             "kept as evidence", record.job.JobID,
                             execution.pid_identities.size());
            }
        }

        std::vector<ProcessIdentity> kept;
        kept.reserve(execution.pid_identities.size());
        size_t dropped = 0;
        for (const auto& pid_identity : execution.pid_identities) {
            if (!pid_identity.complete()) {
                kept.push_back(pid_identity);   // unbound evidence: the next read fills it
                continue;
            }
            const LivenessResult state =
                verify_process_identity(pid_identity, boot_id, process_probe_);
            if (state.state == LivenessState::Dead) {
                ++dropped;
                continue;
            }
            kept.push_back(pid_identity);
        }
        if (dropped != 0) {
            execution.pid_identities = std::move(kept);
            spdlog::warn("JobRegistry: restored JobID {} dropped {} reused/gone "
                         "process identity(ies)", record.job.JobID, dropped);
        }
    }
}

// True when an instance is live and policy-eligible but still has no durable
// intent: the initial decision never landed (write failure, crash) or the
// manager identity completed later. Derived from durable state, so it also
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
    const auto committed = applyAutoPolicyForJob(record.job, record.job.subtype_name);
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
        result.message = "job store is not ready";
        return result;
    }

    const auto key = keyOf(job);
    if (!key) {
        result.code = RegistrationError::IncompleteIdentity;
        result.error = "incomplete_identity";
        result.message = "scheduler type, cluster namespace and native job id are required";
        spdlog::debug("JobRegistry: rejected incomplete observation (native='{}', "
                      "cluster='{}')", job.JobNativeID, job.clusterName);
        return result;
    }
    const std::string index_key = key->index_key();
    const int64_t observed_at = now_us();

    JobRecord record;   // the authoritative image this call hands back

    // One observation, two lock scopes: the registry state that decides
    // create-vs-merge is read and committed under the record transaction lock,
    // while allocation and construction (no registry state, possibly /proc
    // reads) run outside it. A commit that loses a create race re-enters the
    // merge path instead of overwriting the winner. Inside every lock,
    // records_mtx_ is taken only to read and (after a successful commit) to
    // publish, so the collection runtime keeps reading the previous image while
    // the store writes; kernel admission, the runtime wake and the
    // automatic-policy (Lua) evaluation below never run under one.
    for (;;) {
        {
            std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);

            uint64_t job_id = 0;
            {
                std::shared_lock lg(records_mtx_);
                if (const auto indexed = identity_index_.find(index_key);
                    indexed != identity_index_.end()) {
                    job_id = indexed->second;
                }
            }
            if (job_id == 0) {
                if (auto durable = store_->readRowByIdentity(*key)) {
                    // An earlier create landed ambiguously without publishing
                    // anything: the durable row is the truth memory never held,
                    // and this identity lookup is the documented repair. Adopt
                    // it, then merge.
                    job_id = durable->job.JobID;
                    normalizeNamespaceFacts(durable->job);
                    std::unique_lock lg(records_mtx_);
                    if (!durable->index_key.empty())
                        identity_index_[durable->index_key] = job_id;
                    records_[job_id] = std::move(*durable);
                }
            }
            JobRecord stored;
            bool have_stored = false;
            if (job_id != 0) {
                std::shared_lock lg(records_mtx_);
                const auto it = records_.find(job_id);
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
                    result.message =
                        "canonical identity key aliases conflicting structured namespaces";
                    return result;
                }
                JobRecord updated;
                result = mergeDiscoveredJob(stored, job, *key, job_id, observed_at, updated);
                if (!result.ok) return result;
                if (result.disposition == UpsertDisposition::Unchanged) {
                    // The facts did not move, but the observed image still
                    // refreshes memory-only bookkeeping (last_seen_us);
                    // publication matches the previous store-out behavior.
                    record = updated;
                    std::unique_lock lg(records_mtx_);
                    records_[job_id] = std::move(updated);
                } else {
                    // Updated: one unsynced batch; the row joins the dirty set.
                    // A failed or ambiguous write publishes nothing (the latch
                    // fences the row).
                    RecordTxn txn(*this, "update job facts", /*durable=*/false);
                    txn.touch(job_id);
                    txn.put(std::move(updated));
                    if (!txn.commit()) {
                        result.ok = false;
                        result.code = RegistrationError::StoreWriteFailed;
                        result.error = "store_write_failed";
                        result.message = txn.error();
                        return result;
                    }
                    if (const auto published = ownRecord(job_id)) record = *published;
                }
                break;
            }
            // No record owns this identity: the create image is prepared
            // outside every registry lock, and the identity is re-checked at
            // commit time.
        }

        // Allocation and construction are not registry state: the id comes from
        // the store's allocator and the image from the observation alone.
        std::string create_error;
        const auto id = reserveJobId(0, create_error);
        if (!id) {
            result.code = create_error == "id_exhausted"
                              ? RegistrationError::IdExhausted
                              : RegistrationError::StoreWriteFailed;
            result.error = result.code == RegistrationError::IdExhausted
                               ? create_error
                               : to_string(result.code);
            result.message = "durable job id allocation failed";
            return result;
        }
        auto built = buildJobRecord(job, id->id, FactSource::Discovery, std::nullopt, *key,
                                    observed_at);
        {
            std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
            // The id, its identity entry and (for an allocated id) the counter
            // ride one batch; a create has no published image to revert, so it
            // passes no recovery fence.
            if (!commitNewRecord(std::move(built), *id, /*durable=*/false, create_error)) {
                // Another producer registered this identity (or took this id)
                // while the image was built outside the lock: merge the row it
                // published instead of overwriting it.
                if (create_error == "duplicate_identity" || create_error == "duplicate_job_id")
                    continue;
                result.code = RegistrationError::StoreWriteFailed;
                result.error = "store_write_failed";
                result.message = create_error;
                return result;
            }

            result.ok = true;
            result.disposition = UpsertDisposition::Created;
            result.job_id = id->id;
            std::shared_lock lg(records_mtx_);
            result.revision = records_[id->id].revision;
            record = records_[id->id];
            spdlog::info("JobRegistry: created job instance {} for {} '{}'",
                         id->id, key->adapter, key->job_native_id);
        }
        break;
    }   // registry locks released: kernel admission, the runtime wake and the
        // automatic-policy evaluation below never run under one.

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
        // observation whose execution manager is not complete yet is never
        // auto-collected: the update that completes the manager re-runs
        // the selection instead. An admission decision that carried its own
        // evaluation is applied verbatim: the rule already ran once at the
        // gate and must not run twice for one registration.
        if (manager_of(job).complete()) {
            result.intent_committed =
                admission != nullptr && admission->initial_collectors.has_value()
                    ? applyOneAutoSelection(record.job.JobID, *admission->initial_collectors,
                                            admission->policy_revision)
                    : applyAutoPolicyForJob(record.job, record.job.subtype_name);
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
        // manager is its first complete registration. A record whose
        // initial decision never became durable is repaired here too, not
        // only on the Created path.
        if (result.execution_became_complete || missing_initial_decision(record))
            result.intent_committed = repair_initial_decision(record, result);
        // An observation is also the admission repair point: a live instance
        // with no owner (restored row, capacity refusal) gets one here through
        // the single KernelAttribution path.  Facts changed, so this is not a
        // no-op even for an existing owner: newly observed pid identities must be
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

std::optional<JobRecord> JobRegistry::jobRecord(uint64_t jobID) const {
    return ownRecord(jobID);
}

std::optional<JobRecord> JobRegistry::jobRecordByKey(
    const SchedulerKey& key) const {
    return ownRecordByKey(key);
}

std::vector<JobRecord> JobRegistry::jobRecords() const {
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
            outcome.message = "no active instance with job id " +
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
        if (!seen.insert(selection.job_id).second) {
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
        target.job_id = selection.job_id;

        if (!ownHasActive(selection.job_id)) {
            ++outcome.skipped_invalid;
            outcome.message = "no active instance with job id " +
                              std::to_string(selection.job_id);
            target.selection = ownSelection(selection.job_id);
            target.actual.error = "no active instance";
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
                         "instance {} (collector '{}')", selection.job_id, bad_name);
            target.selection = ownSelection(selection.job_id);
            target.actual.error = "unknown automatic collector name: " + bad_name;
            target.skipped = true;
            outcome.targets.push_back(std::move(target));
            continue;
        }

        SelectionMutation mutation;
        mutation.job_id = selection.job_id;
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

bool JobRegistry::runtimeRecord(uint64_t job_id,
                                std::optional<JobRecord>& out) const {
    out.reset();
    if (!store_ || !store_->ready() || store_->recoveryRequired()) return false;
    out = ownRecord(job_id);
    return true;
}

void JobRegistry::setExecutionPresumedLive(uint64_t jobID, bool presumed_live) {
    // Liveness evidence only. It never changes the lifecycle state and never
    // authorizes removal; it gates kernel admission. Runtime state: not
    // persisted, rebuilt by observation after a restart.
    std::unique_lock lg(records_mtx_);
    const auto it = records_.find(jobID);
    if (it != records_.end()) it->second.presumed_live = presumed_live;
}

void JobRegistry::regRPCHandle() {
    registerJobRpc(*this);
    RPCServer::instance().register_method("JobRegistry/group_probe_stats",
        [](const nlohmann::json&) -> nlohmann::json {
            return {{"status", "ok"}, {"stats", GroupProbeStats::instance().to_json()}};
        });
}

nlohmann::json JobRegistry::heartbeatStatusJson() const {
    // Registry/store queries happen without holding the heartbeat mutex; they
    // take their own locks briefly and never re-enter here.
    const StoreHealth store = storeHealth();
    const auto records = jobRecords();
    const std::size_t closing_records = closingJobCount();

    std::vector<std::pair<uint64_t, ProcessIdentity>> closing_ids;
    std::set<uint64_t> suspect_ids;
    std::uint64_t events_received = 0;
    std::uint64_t rescan_requests = 0;
    std::uint64_t obsolete_hints = 0;
    std::uint64_t incomplete_observations = 0;
    std::uint64_t upsert_conflicts = 0;
    std::uint64_t not_admitted_capture_all_off = 0;
    std::uint64_t not_admitted_by_rule = 0;
    std::uint64_t admission_errors = 0;
    std::uint64_t decision_repairs = 0;
    std::uint64_t decision_failures = 0;
    std::uint64_t refresh_failures = 0;
    std::uint64_t removals = 0;
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
        closing_ids = closing_managers_;
        suspect_ids = suspect_ids_;
        events_received = hb_events_received_;
        rescan_requests = hb_rescan_requests_;
        obsolete_hints = hb_obsolete_hints_;
        incomplete_observations = hb_incomplete_observations_;
        upsert_conflicts = hb_upsert_conflicts_;
        not_admitted_capture_all_off = hb_not_admitted_capture_all_off_;
        not_admitted_by_rule = hb_not_admitted_by_rule_;
        admission_errors = hb_admission_errors_;
        decision_repairs = hb_decision_repairs_;
        decision_failures = hb_decision_failures_;
        refresh_failures = hb_refresh_failures_;
        removals = hb_removals_;
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
        if (jobRecord(entry.first).has_value()) ++closing_present;
    }

    std::size_t active_count = 0;
    std::size_t suspect_count = 0;
    std::size_t incomplete_count = 0;
    std::size_t pending_decisions = 0;
    for (const auto& record : records) {
        if (record.job.JobID == 0) continue;   // system sentinel is never a job record
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
        const bool suspect = !record.presumed_live ||
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

    nlohmann::json configured_adapters = nlohmann::json::array();
    for (const auto& adapter : adapters_)
        configured_adapters.push_back(adapter->name());
    nlohmann::json auto_discovery_types = nlohmann::json::array();
    for (const auto& name : auto_discovery_types_)
        auto_discovery_types.push_back(name);

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
    out["configured_adapters"] = std::move(configured_adapters);
    out["auto_discovery_types"] = std::move(auto_discovery_types);
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
    out["capture_all"] = capture_all_;
    out["not_admitted_capture_all_off"] = not_admitted_capture_all_off;
    out["not_admitted_by_rule"] = not_admitted_by_rule;
    out["admission_errors"] = admission_errors;
    out["pending_decisions"] = pending_decisions;
    out["decision_repairs"] = decision_repairs;
    out["decision_repair_failures"] = decision_failures;
    out["refresh_failures"] = refresh_failures;
    out["removals"] = removals;
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
