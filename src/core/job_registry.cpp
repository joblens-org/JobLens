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
// job_registry.cpp — the registry facade (v2 stage S2/S3).
//
// Durable state lives in JobStore (JobRecord); kernel attribution lives in
// KernelAttribution; the RPC surface lives in inventory_rpc.cpp. This file
// keeps the CollectorScheduler-facing seams: lifecycle callbacks, the
// collection applier, retirement/drain flow and the automatic-selection
// trigger.
#include "core/job_registry.hpp"
#include <spdlog/spdlog.h>
#include "common/streamer_watcher.hpp"
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
#include "core/job_collection_manager.hpp"
#include "core/job_inventory_maintenance.hpp"
#include "core/inventory_rpc.hpp"
#include "core/collector_registry.hpp"
#include "common/utils.hpp"
#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "common/common_job.hpp"
#include "common/local_rpc.hpp"
#include "core/group_probe_stats.hpp"
#include <algorithm>

#include <set>
#include <type_traits>

// Maintenance-side inventory health (source/queue/reconciliation state) is
// published by the maintenance controller through this seam. When no provider
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

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

JobRegistry::JobRegistry() : JobRegistry(JobRegistryOptions{}) {}

JobRegistry::JobRegistry(const JobRegistryOptions& options) {
    Job fake_job;
    fake_job.JobID = 0;
    fake_job.JobPIDs = {1};
    fake_job.jobtype = JobType::Sys;
    fake_job.subtype = JobSubType::Common;
    fake_job.sub_attr = CommonJobAttr{
        .auto_update_child = false
    };
    fake_job.CollectorNames = {"Easter egg"}; //实际上这里的添加方式不一样，所以无所谓这里的名字
    sentinel_job_ = fake_job; // 直接持有避免拉起回调

    // The store is the single durable representation; it opens the db, runs
    // the legacy migration and restores the records in its constructor.
    JobStore::Options store_options;
    store_options.db_path = options.db_path;
    store_options.migration_batch_limit = options.migration_batch_limit;
    store_ = std::make_unique<JobStore>(store_options);
    if (store_->ready()) {
        const auto health = store_->health();
        spdlog::info("JobRegistry: store ready (schema {}, migration {})",
                     health.schema_version, health.migration_state);
    } else {
        spdlog::warn("JobRegistry: job db not running");
    }

    // The collection manager is shared by the RPC surface and the automatic
    // selection path; one instance per registry.
    collection_manager_ = std::make_shared<JobCollectionManager>(*this);
    {
        const auto known = known_collector_names();
        collection_manager_->setCollectorValidator(
            [known](const std::string& name) { return known.count(name) != 0; });
    }

    if (options.enable_inventory_maintenance) init_inventory();

    if (options.enable_pid_tracker) init_pid_tracker();
    if (options.register_rpc) regRPCHandle();
}

JobRegistry& JobRegistry::instance() {
    static JobRegistry reg;
    return reg;
}

// ---------------------------------------------------------------------------
// Inventory stack wiring (replaced by the heartbeat in stage S5)
// ---------------------------------------------------------------------------

void JobRegistry::init_inventory()
{
    InventoryMaintenanceConfig config;
    try {
        config = resolve_inventory_maintenance_config(Config::instance().dumpConfig());
    } catch (const std::exception& error) {
        spdlog::error("JobRegistry: invalid inventory configuration; inventory maintenance "
                      "is disabled: {}",
                      error.what());
        return;
    }

    // Exactly one adapter per configured scheduler type, built through the
    // self-registration registry; the controller drives them through
    // IDiscoverySource and owns no adapter details.
    std::vector<IDiscoverySource*> sources;
    adapters_.clear();
    for (const auto type : config.types) {
        const auto& settings = config.settings.at(type);
        auto adapter = SchedulerAdapterRegistry::instance().build(
            adapter_name_of(type), settings);
        if (!adapter) {
            spdlog::error("JobRegistry: no adapter registered for inventory type '{}'; "
                          "registered: {}",
                          adapter_name_of(type),
                          fmt::join(SchedulerAdapterRegistry::instance().names(), ", "));
            adapters_.clear();
            return;
        }
        sources.push_back(&adapter->source());
        adapters_.push_back(std::move(adapter));
    }
    if (config.types.empty()) {
        spdlog::info("JobRegistry: no inventory types configured; discovery is disabled and "
                     "existing records are still maintained");
    }

    // Automatic selection is armed per scheduler type: use_rules=false adds the
    // configured defaults to every job, use_rules=true asks the rule engine
    // per job (reject = empty selection, the job stays inventoried).
    auto_policies_.clear();
    for (const auto& [type, settings] : config.settings) {
        if (!settings.auto_collect) continue;
        const SchedulerType scheduler =
            type == InventoryType::Condor ? SchedulerType::Condor : SchedulerType::Slurm;
        if (settings.use_rules)
            auto_policies_[scheduler] =
                AutoCollectorPolicy::rules(settings.rules_dir, settings.rules_prefix);
        else
            auto_policies_[scheduler] =
                AutoCollectorPolicy::defaults(settings.default_collectors);
    }

    // Kernel attribution capability gate: only adapters with the trait
    // register into pid2job/cgroup2job; legacy/manual rows (unknown adapter)
    // keep participating. Installed before the initial pass can activate
    // anything.
    if (attribution_) {
        attribution_->setCapabilityLookup([this](uint64_t internal_id) {
            auto record = store_->record(internal_id);
            if (!record) return false;
            for (const auto& adapter : adapters_) {
                if (adapter->name() == record->adapter)
                    return adapter->traits().kernel_attribution;
            }
            return true;
        });
    }

    inventory_ = std::make_unique<JobInventoryMaintenance>(*this, config, std::move(sources));
    // Observation -> typed facts goes through the adapters (the controller
    // itself stays scheduler-agnostic).
    inventory_->setFactsEncoder([this](const Observation& observation) -> Job {
        for (const auto& adapter : adapters_) {
            if (adapter->source().schedulerType() == observation.scheduler_type)
                return adapter->encodeFacts(observation);
        }
        return Job{};   // no adapter: facts stay empty, the store normalizes
    });
    if (!inventory_->start()) {
        spdlog::error("JobRegistry: inventory maintenance did not start; "
                      "inventory status will report it as unavailable");
        inventory_.reset();
        adapters_.clear();
        return;
    }
    spdlog::info("JobRegistry: inventory maintenance running with {} discovery type(s)",
                 config.types.size());
}

void JobRegistry::apply_auto_policy(const Job& job, SchedulerType type)
{
    const auto policy = auto_policies_.find(type);
    if (policy == auto_policies_.end() || !policy->second.enabled()) return;

    std::string revision;
    std::string error;
    const auto collectors = policy->second.select(job, revision, error);
    if (!error.empty()) {
        spdlog::warn("JobRegistry: automatic selection failed for JobID {}; keeping the "
                     "previous decision: {}",
                     job.JobID, error);
        return;
    }
    applyAutoSelection(job.JobID, collectors, revision);
}

bool JobRegistry::applyAutoSelection(uint64_t internal_id,
                                     const std::vector<std::string>& collectors,
                                     const std::string& policy_revision)
{
    if (!collection_manager_) return false;
    const auto outcome = collection_manager_->applyAutoPolicy(
        {AutoSelection{internal_id, collectors, policy_revision}});
    if (!outcome.ok) {
        spdlog::warn("JobRegistry: automatic selection for JobID {} was rejected: {}",
                     internal_id, outcome.message);
        return false;
    }
    return true;
}

void JobRegistry::stopInventoryDiscovery()
{
    if (inventory_) inventory_->stopDiscovery();
}

bool JobRegistry::deinit()
{
    if (deinit_done_) return deinit_ok_;

    // Plan 5.3 shutdown order: discovery admission stopped first (the service
    // calls stopInventoryDiscovery() before draining the scheduler), then the
    // retirements are finalized, then attribution, then the store.
    if (inventory_ && !inventory_->stoppedDiscovery()) stopInventoryDiscovery();

    bool ok = true;
    // Archive leftover closing records before the controller's finalize so a
    // restored closing record is not reported as an incomplete drain even
    // though the archive below succeeds.
    if (!cleanupClosingJobs()) ok = false;
    if (inventory_ && !inventory_->finalize()) ok = false;
    inventory_.reset();
    adapters_.clear();

    stop_pid_tracker();
    store_.reset();   // closes the db

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
    return store_ && store_->hasActive(internal_id);
}

void JobRegistry::attributionJobAddPid(uint64_t internal_id, pid_t pid) {
    store_->mutateRecord(internal_id, [internal_id, pid](JobRecord& record) {
        auto& pids = record.job.JobPIDs;
        if (std::find(pids.begin(), pids.end(), pid) == pids.end()) {
            pids.push_back(pid);
            spdlog::trace("JobRegistry: kernel FORK added pid={} to job_id={} (now {} pids)",
                          pid, internal_id, pids.size());
        }
    });
}

void JobRegistry::attributionJobRemovePid(uint64_t internal_id, pid_t pid) {
    store_->mutateRecord(internal_id, [internal_id, pid](JobRecord& record) {
        auto& pids = record.job.JobPIDs;
        const auto before = pids.size();
        pids.erase(std::remove(pids.begin(), pids.end(), pid), pids.end());
        if (pids.size() != before) {
            spdlog::trace("JobRegistry: kernel EXIT removed pid={} from job_id={} (now {} pids)",
                          pid, internal_id, pids.size());
        }
    });
}

void JobRegistry::attributionEnumerateJobs(
    const std::function<void(const Job&)>& fn) {
    // Active jobs only; snapshots are copies so the callback never sees a
    // concurrent mutation.
    for (const auto& record : store_->findActive(
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

// 内核登记入口（Added/Removed 由生命周期路径调用）。
void JobRegistry::sync_job_to_kernel(const Job& job) {
    if (attribution_) attribution_->activate(job.JobID, job);
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

void JobRegistry::remove_job_from_kernel(const Job& job) {
    if (attribution_) attribution_->deactivate(job.JobID, job);
}

// ---------------------------------------------------------------------------
// Registration (the manual/legacy path — deliberately separate from the
// discovery upsert; doc/job_inventory_v2_architecture.md §4.2)
// ---------------------------------------------------------------------------

bool JobRegistry::addJob(const Job& job) {
    spdlog::info("JobRegistry: addJob entry JobID={}, native_id={}, collectors={}, PIDs={}",
                 job.JobID, job.NativeJobID, job.CollectorNames.size(), job.JobPIDs.size());
    // 错误检查
    if (job.CollectorNames.empty()) {
        spdlog::warn("JobRegistry: job with JobID {} has empty CollectorNames", job.JobID);
        return false;
    }
    // 自动生成JobID
    Job job_copy = job; // 先拷贝一份，避免修改外部对象
    if (job_copy.JobID == 0) {
        job_copy.JobID = getNextJobID();
        if (job_copy.JobID == 0) {
            spdlog::error("JobRegistry: durable JobID allocation failed, refusing to register the job");
            return false;
        }
        spdlog::info("JobRegistry: assigned JobID {} to new job", job_copy.JobID);
    }

    // Duplicate check over active records: same typed sub_attr or same id.
    SubAttrMatcher matcher;
    matcher.subtype = job_copy.subtype;
    if (std::holds_alternative<CondorJobAttr>(job_copy.sub_attr)) {
        const auto& attr = std::get<CondorJobAttr>(job_copy.sub_attr);
        matcher.subtype = JobSubType::Condor;
        matcher.condor_cluster_id = attr.cluster_id;
        matcher.condor_proc_id = attr.proc_id;
    } else if (std::holds_alternative<SlurmJobAttr>(job_copy.sub_attr)) {
        const auto& attr = std::get<SlurmJobAttr>(job_copy.sub_attr);
        matcher.subtype = JobSubType::Slurm;
        matcher.slurm_job_id = attr.job_id;
        matcher.slurm_step_id = attr.step_id;
    }
    if (!store_->findActive([&matcher](const Job& existing) {
            return matcher.matches(existing.sub_attr); }).empty()) {
        spdlog::warn("JobRegistry: job with matching sub_attr already exists, ignored");
        return false;
    }
    if (store_->hasActive(job_copy.JobID)) {
        spdlog::warn("JobRegistry: duplicate jobID {}, ignored", job_copy.JobID);
        return false;
    }

    JobRecord record;
    record.job = job_copy;
    record.internal_id = job_copy.JobID;
    if (!store_->insertManual(std::move(record))) {
        spdlog::error("JobRegistry: durable registration failed, JobID {} rejected",
                      job_copy.JobID);
        return false;
    }

    Job job_for_cb;
    if (auto stored = store_->record(job_copy.JobID)) {
        job_for_cb = stored->job;
    } else {
        return false;
    }

    broadcast_lifecycle(JobEvent::Added, job_for_cb);
    // The Added broadcast performs the runtime application; the serialized
    // reconcile records the *verified* runtime state afterwards instead of
    // assuming the broadcast succeeded (a plain observer callback proves
    // nothing about collector initialization).
    reconcileCollectionIntent(job_copy.JobID);
    sync_job_to_kernel(job_for_cb);
    spdlog::info("JobRegistry: add job with JobID {}", job_copy.JobID);
    return true;
}

bool JobRegistry::retireInstance(uint64_t jobID) {
    auto record = store_->record(jobID);
    if (!record) return false;

    const bool has_callbacks = !lifecycle_callbacks().empty();

    if (record->state == kStateClosing) {
        // A pending drain owns the record. With active subscribers a repeated
        // removal must not skip the drain ack; without them the archive can
        // be retried right here.
        if (has_callbacks) return false;
        remove_job_from_kernel(record->job);
        if (!store_->archive(jobID)) {
            spdlog::error("JobRegistry: durable archive failed for JobID {}, instance "
                          "stays closing", jobID);
            return false;
        }
        spdlog::info("JobRegistry: retire instance with JobID {}", jobID);
        return true;
    }

    if (has_callbacks) {
        // The closing fact is durable before the drain starts: a crash (or a
        // failed archive) followed by a restart must not resurrect the
        // instance as active while its retirement was already accepted.
        if (!store_->persistState(jobID, kStateClosing)) {
            spdlog::error("JobRegistry: could not persist the closing state for JobID {}; "
                          "a crash before the archive would resurrect the instance",
                          jobID);
        }
        store_->recordApplied(jobID, AppliedCollection{});
        // Collectors retain the snapshot and acknowledge after their accepted
        // tail has drained; kernel attribution and history wait for that ack.
        broadcast_lifecycle(JobEvent::Removed, record->job);
        return true;
    }

    remove_job_from_kernel(record->job);
    if (!store_->archive(jobID)) {
        spdlog::error("JobRegistry: durable archive failed for JobID {}, instance stays "
                      "active", jobID);
        return false;
    }
    spdlog::info("JobRegistry: retire instance with JobID {}", jobID);
    return true;
}

void JobRegistry::delJob(uint64_t jobID) {
    retireInstance(jobID);
}

bool JobRegistry::acknowledgeJobRemoval(uint64_t jobID) {
    auto record = store_->record(jobID);
    if (!record || record->state != kStateClosing) return false;
    remove_job_from_kernel(record->job);
    if (!store_->archive(jobID)) {
        spdlog::error("JobRegistry: durable archive failed for closing JobID {}, keeping "
                      "it pending", jobID);
        return false;
    }
    return true;
}

size_t JobRegistry::closingJobCount() const {
    return store_ ? store_->closingCount() : 0;
}

bool JobRegistry::cleanupClosingJobs() {
    bool all_archived = true;
    if (!store_) return true;
    for (const auto& record : store_->records()) {
        if (record.state != kStateClosing) continue;
        remove_job_from_kernel(record.job);
        if (store_->archive(record.internal_id)) {
            spdlog::info("JobRegistry: archived closing JobID {}",
                         record.internal_id);
        } else {
            all_archived = false;
            spdlog::error("JobRegistry: shutdown could not archive closing JobID {}; "
                          "the incomplete closing state is persisted for recovery",
                          record.internal_id);
        }
    }
    return all_archived;
}

bool JobRegistry::finishJobRemoval(const Job& job) {
    remove_job_from_kernel(job);
    if (!store_->archive(job.JobID)) {
        spdlog::error("JobRegistry: failed to archive removed JobID {}", job.JobID);
        return false;
    }
    spdlog::info("JobRegistry: remove job with JobID {}", job.JobID);
    return true;
}

size_t JobRegistry::delJob(const SubAttrMatcher& matcher) {
    const auto matched = store_->findActive(
        [&matcher](const Job& job) { return matcher.matches(job.sub_attr); });
    const bool has_callbacks = !lifecycle_callbacks().empty();

    for (const auto& record : matched) {
        if (has_callbacks) {
            // The closing fact is durable before any drain starts (see
            // retireInstance).
            if (!store_->persistState(record.internal_id, kStateClosing)) {
                spdlog::error("JobRegistry: could not persist the closing state for "
                              "JobID {}", record.internal_id);
            }
            store_->recordApplied(record.internal_id, AppliedCollection{});
            broadcast_lifecycle(JobEvent::Removed, record.job);
        } else if (finishJobRemoval(record.job)) {
            spdlog::info("JobRegistry: remove job with JobID {} by sub_attr matcher",
                         record.internal_id);
        }
    }

    spdlog::info("JobRegistry: removed {} jobs by sub_attr matcher", matched.size());
    return matched.size();
}

size_t JobRegistry::delJobByRequest(const nlohmann::json& req) {
    // 优先检查 JobID，如果存在则按 JobID 删除
    if (req.contains("JobID")) {
        uint64_t jobID = req["JobID"].get<uint64_t>();
        if (findJob(jobID).has_value()) {
            delJob(jobID);
            return 1;
        }
        return 0;
    }

    // 检查 matcher 字段，解析 sub_attr 匹配条件
    if (req.contains("matcher")) {
        const auto& matcher_json = req["matcher"];
        SubAttrMatcher matcher;

        // 解析 subtype
        if (matcher_json.contains("subtype")) {
            std::string subtype_str = matcher_json["subtype"].get<std::string>();
            if (subtype_str == "Condor") {
                matcher.subtype = JobSubType::Condor;
            } else if (subtype_str == "Slurm") {
                matcher.subtype = JobSubType::Slurm;
            } else if (subtype_str == "Common") {
                matcher.subtype = JobSubType::Common;
            }
        }

        // 解析 Condor 匹配字段
        if (matcher_json.contains("condor_cluster_id")) {
            matcher.condor_cluster_id = matcher_json["condor_cluster_id"].get<size_t>();
        }
        if (matcher_json.contains("condor_proc_id")) {
            matcher.condor_proc_id = matcher_json["condor_proc_id"].get<size_t>();
        }
        if (matcher_json.contains("condor_owner")) {
            matcher.condor_owner = matcher_json["condor_owner"].get<std::string>();
        }
        if (matcher_json.contains("condor_starter_pid")) {
            matcher.condor_starter_pid = matcher_json["condor_starter_pid"].get<pid_t>();
        }

        // 解析 Slurm 匹配字段
        if (matcher_json.contains("slurm_job_id")) {
            matcher.slurm_job_id = matcher_json["slurm_job_id"].get<size_t>();
        }
        if (matcher_json.contains("slurm_step_id")) {
            matcher.slurm_step_id = matcher_json["slurm_step_id"].get<uint32_t>();
        }
        if (matcher_json.contains("slurm_user")) {
            matcher.slurm_user = matcher_json["slurm_user"].get<std::string>();
        }
        if (matcher_json.contains("slurm_job_name")) {
            matcher.slurm_job_name = matcher_json["slurm_job_name"].get<std::string>();
        }
        if (matcher_json.contains("slurm_stepd_pid")) {
            matcher.slurm_stepd_pid = matcher_json["slurm_stepd_pid"].get<pid_t>();
        }

        return delJob(matcher);
    }

    // 既没有 JobID 也没有 matcher，返回 0
    spdlog::warn("JobRegistry::delJobByRequest: request must contain 'JobID' or 'matcher' field");
    return 0;
}

inline void JobRegistry::update_job(Job& job){
    if(job.subtype == JobSubType::Condor){
        CondorJob::update_job_info(job);
        if (!CondorJob::update_job_pids(job)) {
            CommonJob::update_job_child_process(job);
        }
    }
    if(job.subtype == JobSubType::Slurm){
        SlurmJob::update_job_info(job);
        CommonJob::update_job_child_process(job);
    }
    if(job.subtype == JobSubType::Common){
        if (std::get<CommonJobAttr>(job.sub_attr).auto_update_child){
            auto old_pids = job.JobPIDs;
            CommonJob::update_job_child_process(job);
        }
    }
}

std::optional<Job> JobRegistry::findJob(uint64_t jobID) {
    // Read-only snapshot: the heartbeat owns PID/attribute refresh and
    // retirement, so reads never mutate the registry (plan section 3.2).
    if (jobID == kSystemSentinelJobId) return sentinel_job_;
    auto record = store_->record(jobID);
    if (!record || record->state == kStateClosing) return std::nullopt;
    return record->job;
}

std::vector<Job> JobRegistry::findJob(const SubAttrMatcher& matcher) const {
    std::vector<Job> matched_jobs;
    for (const auto& record : store_->findActive(
             [&matcher](const Job& job) { return matcher.matches(job.sub_attr); })) {
        matched_jobs.push_back(record.job);
    }
    spdlog::debug("JobRegistry: found {} jobs matching the criteria", matched_jobs.size());
    return matched_jobs;
}

std::vector<Job> JobRegistry::snapshot() const {
    std::vector<Job> out;
    for (const auto& record : store_->findActive(
             [](const Job&) { return true; })) {
        out.push_back(record.job);
    }
    return out;
}

// ---------------------------------------------------------------------------
// Lifecycle fan-out and collection runtime seam
// ---------------------------------------------------------------------------

std::vector<JobLifecycleCb> JobRegistry::lifecycle_callbacks() const {
    std::shared_lock lg(mtx_);
    return cbs_;
}

void JobRegistry::broadcast_lifecycle(JobEvent event, const Job& job) {
    for (const auto& cb : lifecycle_callbacks()) cb(event, job);
}

void JobRegistry::addLifecycleCb(JobLifecycleCb cb) {
    // Copy the restored records first (store lock only), then take the
    // registry lock — no nested locking.
    const auto records = store_->records();
    std::vector<Job> replay;
    {
        std::unique_lock lg(mtx_);
        cbs_.push_back(cb);
        // Restored records are already published; replay them once to this
        // subscriber instead of re-adding them.
        //
        // A record restored as suspect was already judged unverifiable (stale
        // execution generation, no live process). Replaying it would re-arm
        // collection on an execution nobody has revalidated, so the replay
        // skips it; the heartbeat re-admits it (state back to active) as soon
        // as a fresh observation proves the execution again. Closing records
        // belong to the drain.
        replay.reserve(records.size());
        for (const auto& record : records) {
            if (record.internal_id == kSystemSentinelJobId) continue;
            if (record.state != kStateActive) continue;
            replay.push_back(record.job);
        }
    }

    for (const auto& job : replay) {
        cb(JobEvent::Added, job);
    }

    // The replay is how persisted intent is applied after a restart. Record
    // the verified runtime state per instance instead of assuming the replay
    // succeeded (the subscriber may lack a runtime or reject the binding).
    for (const auto& job : replay) {
        if (job.CollectorNames.empty()) continue;
        reconcileCollectionIntent(job.JobID);
    }
}

// ---------------------------------------------------------------------------
// Inventory store facade
// ---------------------------------------------------------------------------

UpsertResult JobRegistry::upsertObservedJob(const ObservedExecution& observation) {
    JobRecord record;
    UpsertResult result = store_->upsert(observation, record);
    if (!result.ok) return result;

    if (result.disposition == UpsertDisposition::Created) {
        // Kernel attribution is initialized here, exactly like the legacy
        // registration: eBPF attribution and the cgroup map must not wait for
        // the reconcile pass, and the cgroup-id cache must exist for the
        // later removal.
        sync_job_to_kernel(record.job);
        // Facts only: an unbound instance creates no collector work, but
        // System collectors must still learn the job exists (plan 6.3).
        broadcast_lifecycle(JobEvent::Added, record.job);
        // First complete registration is an automatic-selection trigger (plan
        // 6.2); Lua evaluation happens outside every registry lock. An
        // observation whose execution generation is not verified yet is never
        // auto-collected: the update that completes the generation re-runs
        // the selection instead.
        if (observation.generation.complete())
            apply_auto_policy(record.job, record.identity.scheduler_type);
    } else if (result.disposition == UpsertDisposition::Updated) {
        // Fact changes reach already bound consumers (fresh PIDs/attributes)
        // and the collection runtime outside every registry lock. The
        // application re-reads the durable binding inside a serialized
        // section, so a fact update can never re-apply a binding a newer
        // commit has already superseded.
        applyLatestCollectionIntent(record.internal_id);
        // A record registered from an incomplete observation has never seen
        // an automatic selection. The pass that completes its execution
        // generation is its first complete registration.
        if (result.generation_became_complete)
            apply_auto_policy(record.job, record.identity.scheduler_type);
    }
    return result;
}

std::optional<JobRecord> JobRegistry::inventoryRecord(uint64_t jobID) const {
    return store_->record(jobID);
}

std::optional<JobRecord> JobRegistry::inventoryRecordByIdentity(
    const CanonicalIdentity& identity) const {
    return store_->recordByIdentity(identity);
}

std::vector<JobRecord> JobRegistry::inventoryRecords() const {
    return store_->records();
}

BindingRecord JobRegistry::bindingRecord(uint64_t jobID) const {
    return store_->binding(jobID);
}

bool JobRegistry::commitBindings(const std::vector<BindingRecord>& bindings,
                                 std::string& error) {
    std::vector<std::pair<JobRecord, bool>> updated;
    if (!store_->commitBindings(bindings, updated, error)) return false;

    // Publish to the collection runtime only after the durable commit
    // succeeded. A runtime failure is recorded per instance and reported by
    // the API; the intent itself stays accepted (plan section 6.1). Auto
    // bookkeeping can move without changing the effective selection (manual
    // override), so only real membership changes reach the runtime. The
    // serialized application re-reads the binding inside the apply section,
    // so a racing fact-update application converges on this commit, never
    // the reverse.
    for (const auto& [record, changed] : updated) {
        if (!changed) continue;
        applyLatestCollectionIntent(record.internal_id);
    }
    return true;
}

bool JobRegistry::hasInstance(uint64_t jobID) const {
    return store_->hasActive(jobID);
}

std::string JobRegistry::instanceId(uint64_t jobID) const {
    return store_->instanceId(jobID);
}

StoreHealth JobRegistry::storeHealth() const {
    return store_->health();
}

void JobRegistry::injectStoreFaultForTest(StoreFaultMode mode, size_t skip, size_t count) {
    store_->injectFaultForTest(mode, skip, count);
}

void JobRegistry::clearStoreFaultsForTest() {
    store_->clearFaultsForTest();
}

uint64_t JobRegistry::getNextJobID() {
    return store_->nextJobId();
}

void JobRegistry::setCollectionApplier(CollectionApplier applier) {
    std::lock_guard<std::mutex> lock(applier_mtx_);
    collection_applier_ = std::move(applier);
}

void JobRegistry::applyLatestCollectionIntent(uint64_t internal_id) {
    CollectionApplier applier;
    {
        std::lock_guard<std::mutex> lock(applier_mtx_);
        applier = collection_applier_;
    }

    // Serialize all runtime applications. Whoever holds apply_mtx_ re-reads
    // the durable binding and derives the desired set from it, so concurrent
    // applications always converge on the latest committed intent: a stale
    // fact-update can apply only what is current when it finally runs, never
    // the snapshot it started with.
    std::lock_guard<std::mutex> apply_lock(apply_mtx_);

    const AppliedCollection previous = store_->applied(internal_id);
    const BindingRecord binding = store_->binding(internal_id);

    if (binding.revision == 0) {
        // No intent was ever recorded: nothing to converge, and inventing an
        // error state for an unbound instance would misreport it.
        return;
    }
    if (previous.applied && previous.revision == binding.revision &&
        previous.collectors == binding.effectiveCollectors()) {
        return;   // already converged
    }

    Job job;
    {
        auto record = store_->record(internal_id);
        if (!record || record->state == kStateClosing) {
            return;   // retired or draining meanwhile: nothing to apply
        }
        job = record->job;
    }

    AppliedCollection applied;
    applied.revision = binding.revision;
    if (!applier) {
        // No collection runtime on this Agent (inventory-only core, tests).
        // The intent is durable, but nothing was applied: say so.
        applied.error = "no_collection_runtime";
    } else {
        // The desired set comes from the binding, never from a job snapshot
        // that may have been read before a newer binding committed.
        Job desired = job;
        desired.CollectorNames = binding.effectiveCollectors();
        std::vector<std::string> collectors;
        std::string error;
        bool ok = false;
        try {
            ok = applier(desired, collectors, error);
        } catch (const std::exception& failure) {
            error = std::string("collection runtime threw: ") + failure.what();
        }
        // A partially successful application stays visible: collectors holds
        // exactly what the runtime reported as bound, applied=false says the
        // binding is not fully in effect.
        applied.applied = ok;
        applied.collectors = collectors;
        if (!ok && error.empty()) error = "collection runtime rejected the binding";
        applied.error = error;
        const bool changed_state =
            previous.applied != applied.applied || previous.collectors != applied.collectors ||
            previous.revision != applied.revision || previous.error != applied.error;
        if (!ok && !changed_state)
            spdlog::debug("JobRegistry: collection intent for JobID {} still not applied: {}",
                          job.JobID, error);
        else if (!ok)
            spdlog::warn("JobRegistry: collection intent for JobID {} not applied: {}",
                         job.JobID, error);
    }

    // recordApplied is a no-op when the instance was retired meanwhile; its
    // applied state then no longer has an owner.
    store_->recordApplied(internal_id, applied);
}

void JobRegistry::reconcileCollectionIntent(uint64_t internal_id) {
    // Retry/repair semantics live in the serialized application; this wrapper
    // exists so callers (heartbeat, collection manager) express intent
    // ("make the runtime match the store") instead of mechanism.
    applyLatestCollectionIntent(internal_id);
}

AppliedCollection JobRegistry::appliedCollection(uint64_t jobID) const {
    return store_->applied(jobID);
}

void JobRegistry::setInstanceState(uint64_t jobID, const std::string& state) {
    // In-memory only: suspect is a transient heartbeat verdict (restore
    // re-derives it), while closing goes through persistState.
    store_->mutateRecord(jobID, [&state](JobRecord& record) { record.state = state; });
}

void JobRegistry::setExitObserver(std::function<void(pid_t)> observer) {
    std::lock_guard<std::mutex> lock(exit_observer_mtx_);
    exit_observer_ = std::move(observer);
}

bool JobRegistry::refreshJobFacts(uint64_t jobID) {
    // /proc and scheduler metadata I/O run inside `refresh` strictly outside
    // every store lock; only the result write happens under it.
    return store_->refreshFacts(jobID, [this](Job& job) { update_job(job); });
}

void JobRegistry::regRPCHandle() {
    registerInventoryRpc(*this, collection_manager_);
    RPCServer::instance().register_method("JobRegistry/group_probe_stats",
        [](const nlohmann::json&) -> nlohmann::json {
            return {{"status", "ok"}, {"stats", GroupProbeStats::instance().to_json()}};
        });
}
