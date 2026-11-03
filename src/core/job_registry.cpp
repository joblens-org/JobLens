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

#include "core/reentrancy_gate.hpp"
#include "core/inventory_config.hpp"
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
#include <unordered_set>
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
// Inventory merge rules (moved from the store; registry-owned business)
// ---------------------------------------------------------------------------

namespace {

int64_t now_us()
{
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

bool merge_text(std::string& target, const std::string& source) {
    if (source.empty() || target == source) return false;
    target = source;
    return true;
}

template <typename T>
bool merge_scalar(T& target, T source) {
    if (source == T{} || target == source) return false;
    target = source;
    return true;
}

bool merge_flag(bool& target, bool source) {
    if (!source || target) return false;
    target = true;
    return true;
}

bool merge_condor_attr(CondorJobAttr& target, const CondorJobAttr& source) {
    bool changed = false;
    changed |= merge_scalar(target.starter_pid, source.starter_pid);
    changed |= merge_scalar(target.cluster_id, source.cluster_id);
    changed |= merge_scalar(target.proc_id, source.proc_id);
    changed |= merge_text(target.scheduler_host, source.scheduler_host);
    changed |= merge_text(target.scheduler_name, source.scheduler_name);
    changed |= merge_text(target.slots_cgroup_path, source.slots_cgroup_path);
    changed |= merge_text(target.owner, source.owner);
    changed |= merge_text(target.job_ad_path, source.job_ad_path);
    changed |= merge_text(target.collector_host, source.collector_host);
    changed |= merge_flag(target.auto_update_child, source.auto_update_child);
    return changed;
}

bool merge_slurm_attr(SlurmJobAttr& target, const SlurmJobAttr& source) {
    bool changed = false;
    changed |= merge_scalar(target.stepd_pid, source.stepd_pid);
    changed |= merge_scalar(target.job_id, source.job_id);
    changed |= merge_scalar(target.step_id, source.step_id);
    changed |= merge_text(target.cluster_name, source.cluster_name);
    changed |= merge_text(target.cgroup_path, source.cgroup_path);
    changed |= merge_text(target.user, source.user);
    changed |= merge_text(target.job_name, source.job_name);
    changed |= merge_text(target.partition, source.partition);
    changed |= merge_flag(target.auto_update_child, source.auto_update_child);
    return changed;
}

bool merge_common_attr(CommonJobAttr& target, const CommonJobAttr& source) {
    return merge_flag(target.auto_update_child, source.auto_update_child);
}

// Merge observation facts into the stored facts. Empty/default observation
// fields never erase known facts; CollectorNames is never taken from an
// observation because selection is intent, not a discovery fact.
//
// `replace_members` upgrades the PID list to an authoritative replacement: an
// empty list then means "the source read the membership and found none", which
// must clear stale PIDs instead of silently keeping them. Without the flag
// (membership only partially known) an empty list is ignored.
bool merge_job_facts(Job& target, const Job& observed, bool replace_members) {
    bool changed = false;

    if (observed.subtype != JobSubType::Unknown && target.subtype != observed.subtype) {
        target.subtype = observed.subtype;
        changed = true;
    }
    if (observed.jobtype != JobType::Unknown && target.jobtype != observed.jobtype) {
        target.jobtype = observed.jobtype;
        changed = true;
    }
    changed |= merge_text(target.NativeJobID, observed.NativeJobID);
    changed |= merge_text(target.cluster_name, observed.cluster_name);
    changed |= merge_text(target.clusterTag, observed.clusterTag);

    std::vector<pid_t> observed_pids = observed.JobPIDs;
    std::sort(observed_pids.begin(), observed_pids.end());
    observed_pids.erase(std::unique(observed_pids.begin(), observed_pids.end()),
                        observed_pids.end());
    // Authoritative replacement (possibly empty), or a non-empty merge list;
    // a partial member set never erases stored members.
    if (replace_members || !observed_pids.empty()) {
        if (observed_pids != target.JobPIDs) {
            target.JobPIDs = std::move(observed_pids);
            changed = true;
        }
    }

    std::visit([&changed, &target](const auto& source) {
        using T = std::decay_t<decltype(source)>;
        if constexpr (std::is_same_v<T, CondorJobAttr>) {
            if (auto* current = std::get_if<CondorJobAttr>(&target.sub_attr)) {
                changed |= merge_condor_attr(*current, source);
            } else {
                target.sub_attr = source;
                changed = true;
            }
        } else if constexpr (std::is_same_v<T, SlurmJobAttr>) {
            if (auto* current = std::get_if<SlurmJobAttr>(&target.sub_attr)) {
                changed |= merge_slurm_attr(*current, source);
            } else {
                target.sub_attr = source;
                changed = true;
            }
        } else {
            if (auto* current = std::get_if<CommonJobAttr>(&target.sub_attr)) {
                changed |= merge_common_attr(*current, source);
            } else {
                target.sub_attr = source;
                changed = true;
            }
        }
    }, observed.sub_attr);

    return changed;
}

nlohmann::json facts_fingerprint(const Job& job, const JobRecord& record) {
    nlohmann::json out;
    out["jobtype"] = static_cast<int>(job.jobtype);
    out["subtype"] = static_cast<int>(job.subtype);
    out["native_job_id"] = job.NativeJobID;
    out["cluster_name"] = job.cluster_name;
    out["clusterTag"] = job.clusterTag;

    auto pids = job.JobPIDs;
    std::sort(pids.begin(), pids.end());
    pids.erase(std::unique(pids.begin(), pids.end()), pids.end());
    out["pids"] = pids;

    nlohmann::json members = nlohmann::json::array();
    for (const auto& member : normalized_processes(record.members)) {
        nlohmann::json entry;
        entry["pid"] = member.pid;
        entry["start_ticks"] = member.start_ticks;
        entry["boot_id"] = member.boot_id;
        members.push_back(std::move(entry));
    }
    out["members"] = std::move(members);

    std::visit([&out](const auto& attr) {
        out["sub_attr"] = nlohmann::json(attr);
    }, job.sub_attr);
    nlohmann::json generation;
    generation["boot_id"] = record.generation.boot_id;
    generation["manager_pid"] = record.generation.manager_pid;
    generation["manager_start_ticks"] = record.generation.manager_start_ticks;
    generation["cgroup_identity"] = record.generation.cgroup_identity;
    out["generation"] = std::move(generation);
    return out;
}

// Best-effort selector reconstruction for legacy (schema < 2) records. A
// missing namespace or native id yields an incomplete selector, never a guess.
NativeSelector selector_from_legacy_job(const Job& job) {
    NativeSelector selector;
    if (job.subtype == JobSubType::Condor) {
        selector.adapter = kAdapterCondor;
        selector.cluster_name = job.cluster_name;
        selector.cluster_tag = job.clusterTag;
        selector.native_job_id = job.NativeJobID;
        if (selector.native_job_id.empty()) {
            if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
                selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
            }
        }
    } else if (job.subtype == JobSubType::Slurm) {
        selector.adapter = kAdapterSlurm;
        selector.cluster_name = job.cluster_name;
        selector.native_job_id = job.NativeJobID;
        if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
            if (selector.cluster_name.empty()) selector.cluster_name = attr->cluster_name;
            if (selector.native_job_id.empty()) {
                selector.native_job_id = std::to_string(attr->job_id);
            }
            if (attr->step_id != 0) {
                if (const auto name = slurm_step_name(attr->step_id)) {
                    selector.step_id = *name;
                } else {
                    selector.step_id = std::to_string(attr->step_id);
                }
            }
        }
    }
    return selector;
}

JobStore::WriteOp put_record_op(const JobRecord& record) {
    JobStore::WriteOp op;
    op.kind = JobStore::WriteOp::PutRecord;
    op.internal_id = record.internal_id;
    op.record = record;
    return op;
}

}  // namespace

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

    // Liveness probe: the admission gate and restore validation both need a
    // positive process-identity check, so the production probe is installed
    // before the store restores anything.  A test may substitute it.
    process_probe_ = options.test_process_probe ? options.test_process_probe
                                                : make_proc_liveness_probe();

    // The registry owns the records; the store is the persistence executor
    // (it opens the db and admits the schema). Rows are loaded and validated
    // HERE with the same probe the admission gate uses, so a row cannot be
    // published as verified here and refused there (or worse, vice versa).
    JobStore::Options store_options;
    store_options.db_path = options.db_path;
    store_ = std::make_unique<JobStore>(store_options);
    store_->setRowProvider([this](uint64_t id) -> std::optional<JobRecord> {
        std::shared_lock lg(records_mtx_);
        const auto it = records_.find(id);
        if (it == records_.end()) return std::nullopt;
        return it->second;
    });
    {
        auto rows = store_->loadAll();
        validateRestoredExecutions(rows);
        for (auto& row : rows) publishRecord(std::move(row));
    }
    if (store_->ready()) {
        const auto health = store_->health();
        spdlog::info("JobRegistry: store ready (schema {}, migration {})",
                     health.schema_version, health.migration_state);
    } else {
        spdlog::warn("JobRegistry: job db not running");
    }

    // Selection validation is registry-owned: one gate for the RPC surface and
    // the automatic-selection path.
    {
        const auto known = known_collector_names();
        setCollectorValidator(
            [known](const std::string& name) { return known.count(name) != 0; });
    }


    // Attribution owns the kernel map namespace, so it must exist before any
    // discovery admission or collector attachment. The capability gate is
    // installed by resolve_inventory_components before the initial pass.
    if (options.enable_pid_tracker) init_pid_tracker();

    // Restored active records must be re-seeded into the kernel namespace this
    // process now owns, before any collector can attach to them.
    admitRestoredOwners();

    if (options.enable_inventory_maintenance) init_inventory(options);

    if (options.register_rpc) regRPCHandle();
}

JobRegistry& JobRegistry::instance() {
    static JobRegistry reg;
    return reg;
}

// ---------------------------------------------------------------------------
// Inventory heartbeat (v2 §2.3) — the maintenance function of the registry.
// Thread-free policy components (probes, retirement rules, absence counters)
// are driven from one periodic worker; there is no separate service object.
// ---------------------------------------------------------------------------

namespace {
constexpr int kAbsencesBeforeRetirement = 2;
constexpr std::size_t kEndedGenerationCapacity = 4096;
constexpr std::size_t kIngestQueueCapacity = 1024;
constexpr std::size_t kIngestRetryCapacity = 256;
// The exec-hint retry ladder: scheduler state (job ad, spool dir) may not be
// written yet at exec time. Sub-second retries cover the race; the ladder's
// exhaustion hands the pid to one coalesced authoritative rescan.
constexpr int kExecRetryDelaysMs[] = {5, 25, 100, 500};
constexpr std::uint32_t kWaitSliceMs = 100;
}  // namespace

std::int64_t JobRegistry::clockNow() const {
    return now_epoch_ms();
}

void JobRegistry::noteHeartbeatError(const std::string& error) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    degraded_ = true;
    hb_last_error_ = error;
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
        // storm (plan §6.3).
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

void JobRegistry::processObservation(const Job& job, std::set<uint64_t>& observed) {
    const auto identity = canonicalize(selector_of(job));
    const ExecutionGeneration generation = generation_of(job);
    if (identity && generation.complete() && isObsolete(*identity, generation)) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_obsolete_hints_;
        spdlog::debug("JobRegistry: discarded hint for the ended execution of '{}'",
                      identity->native_job_id);
        return;
    }

    // The old two-level readiness rule: resolvable identity AND complete
    // execution generation.
    const bool ready = identity && generation.complete();
    if (!ready) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_incomplete_observations_;
        spdlog::debug("JobRegistry: incomplete discovery '{}': {}",
                      job.NativeJobID,
                      job.execution ? job.execution->incomplete_reason : std::string{});
    }

    const auto result = upsertObservedJob(job);
    if (result.ok) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        observed.insert(result.internal_id);
        auto& entry = absences_[result.internal_id];
        entry.generation = generation;
        entry.complete_absences = 0;
        entry.last_seen_ms = clockNow();
        suspect_ids_.erase(result.internal_id);
        return;
    }

    if (result.error == "incomplete_identity") {
        // An unresolvable identity is not an error of the pass: it stays out
        // of the identity index and is retried on the next observation.
        return;
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
        return;
    }

    noteHeartbeatError("inventory upsert failed: " + result.error +
                       (result.message.empty() ? "" : ": " + result.message));
}

void JobRegistry::repairMissingInitialDecisions() {
    // Bounded per pass: a large backlog converges over several passes instead
    // of stalling one.
    constexpr std::size_t kMaxPerPass = 128;
    std::size_t repaired = 0;
    for (const auto& record : ownRecords()) {
        if (repaired >= kMaxPerPass) break;
        if (!missing_initial_decision(record)) continue;
        auto policy_targets = std::vector<uint64_t>{record.internal_id};
        std::vector<AutoSelection> selections;
        evaluateAutoPolicies(policy_targets, selections);
        if (selections.empty()) continue;   // no enabled policy for this adapter
        const auto outcome = applyAutoPolicy(selections);
        if (!outcome.ok) {
            spdlog::warn("JobRegistry: initial decision repair for JobID {} was rejected: {}",
                         record.internal_id, outcome.message);
            continue;
        }
        ++repaired;
        spdlog::info("JobRegistry: repaired the missing initial decision of JobID {}",
                     record.internal_id);
    }
}

void JobRegistry::refreshLegacyRecords() {
    for (const auto& record : ownRecords()) {
        if (record.internal_id == kSystemSentinelJobId) continue;
        if (record.state == kStateClosing) continue;   // the drain owns it
        // Discovery owns its execution facts.  A discovery record whose
        // generation never completed must not be re-read as a legacy row: the
        // legacy refresh replaces the saved member identities with bare PIDs,
        // which would erase the only evidence the record has.
        if (record.execution_observed) continue;
        if (record.generation.complete()) continue;    // nothing to refresh
        if (record.members.empty()) continue;          // no liveness evidence yet
        refreshJobFacts(record.internal_id);
    }
}

bool JobRegistry::retireIfConfirmed(const JobRecord& record, const std::string& reason) {
    // Only retire the generation the evidence was gathered for. A late
    // decision for an older execution must never remove newer ownership.
    const auto current = ownRecord(record.internal_id);
    if (!current) return false;
    if (!(current->generation == record.generation)) {
        spdlog::info("JobRegistry: JobID {} changed execution generation before "
                     "retirement; keeping current ownership", record.internal_id);
        return false;
    }

    rememberEnded(record);
    if (!retireInstance(record.internal_id)) {
        noteHeartbeatError("inventory retirement failed for JobID " +
                           std::to_string(record.internal_id));
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_retirements_;
        closing_generations_.emplace_back(record.internal_id, record.generation);
        absences_.erase(record.internal_id);
        suspect_ids_.erase(record.internal_id);
    }
    spdlog::info("JobRegistry: retired JobID {}: {}", record.internal_id, reason);
    return true;
}

void JobRegistry::rememberEnded(const JobRecord& record) {
    const auto identity = canonicalize(record.selector);
    if (!identity) return;
    std::lock_guard<std::mutex> lock(hb_mtx_);
    ended_generations_[identity->key()] = record.generation;
    while (ended_generations_.size() > kEndedGenerationCapacity) {
        ended_generations_.erase(ended_generations_.begin());
    }
}

bool JobRegistry::isObsolete(const CanonicalIdentity& identity,
                             const ExecutionGeneration& generation) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    const auto it = ended_generations_.find(identity.key());
    return it != ended_generations_.end() && it->second == generation;
}

void JobRegistry::evaluateRetirements(
    const std::set<uint64_t>& observed_ids,
    const std::map<std::string, bool>& source_complete) {
    const auto records = ownRecords();
    const std::int64_t now = clockNow();

    for (const auto& record : records) {
        if (record.internal_id == 0) continue;
        if (record.state == kStateClosing) continue;

        int absences = 0;
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            auto& entry = absences_[record.internal_id];
            if (!(entry.generation == record.generation)) {
                entry.generation = record.generation;
                entry.complete_absences = 0;
            }
            if (observed_ids.count(record.internal_id) != 0) {
                entry.complete_absences = 0;
                entry.last_seen_ms = now;
                suspect_ids_.erase(record.internal_id);
                continue;
            }
            const auto coverage = source_complete.find(record.job.subtype_name);
            const bool absence_allowed =
                coverage != source_complete.end() && coverage->second;
            if (absence_allowed && entry.complete_absences < kAbsencesBeforeRetirement) {
                ++entry.complete_absences;
            }
            absences = entry.complete_absences;
            if (absences > 0) suspect_ids_.insert(record.internal_id);
            else suspect_ids_.erase(record.internal_id);
        }

        // Persisted lifecycle state is heartbeat-owned; expose it through the
        // inventory API instead of keeping it only here.
        // Absence is liveness evidence, not a lifecycle state: the record
        // stays Open, and an unverified instance is refused kernel admission
        // and never retired.
        setExecutionVerified(record.internal_id, absences == 0);

        // Records without a validated execution generation (legacy/manual
        // registrations, unresolved observations) have no manager to probe.
        // The refreshed member list is their only evidence: live or unreadable
        // pids keep the record, an all-dead list retires it.
        if (!record.generation.complete()) {
            std::vector<ProcessIdentity> pids = record.members;
            if (pids.empty()) {
                if (const auto job = findJob(record.internal_id)) {
                    for (const auto pid : job->JobPIDs)
                        pids.push_back(ProcessIdentity{pid, 0, ""});
                }
            }
            if (pids.empty()) continue;   // no evidence: retry on the next pass

            bool any_live = false;
            for (const auto& process : pids) {
                const LivenessResult liveness = process_probe_
                                                    ? process_probe_(process)
                                                    : LivenessResult{};
                if (liveness.state != LivenessState::Dead) {
                    any_live = true;
                    break;
                }
            }
            if (!any_live)
                retireIfConfirmed(record, "no live process for a legacy/manual registration");
            continue;
        }

        const ProcessIdentity manager{record.generation.manager_pid,
                                      record.generation.manager_start_ticks,
                                      record.generation.boot_id};
        const LivenessResult manager_liveness = process_probe_
                                                    ? process_probe_(manager)
                                                    : LivenessResult{};

        // A live manager always wins, whatever the PID list or cgroup says.
        if (manager_liveness.state == LivenessState::Alive) continue;

        bool cgroup_empty_confirmed = false;
        bool cgroup_live = false;
        bool cgroup_inconclusive = false;
        if (!record.generation.cgroup_identity.empty() && cgroup_probe_) {
            const CgroupMembership membership =
                cgroup_probe_(record.generation.cgroup_identity);
            if (membership.readable) {
                if (membership.members.empty()) {
                    // The read succeeded and found nothing: positive "cgroup
                    // is empty" evidence, not missing data.
                    cgroup_empty_confirmed = true;
                }
                for (const pid_t pid : membership.members) {
                    const LivenessResult member = process_probe_
                                                      ? process_probe_(ProcessIdentity{pid, 0, ""})
                                                      : LivenessResult{};
                    if (member.state == LivenessState::Alive) {
                        cgroup_live = true;
                        break;
                    }
                    if (member.state == LivenessState::Unknown)
                        cgroup_inconclusive = true;
                }
            }
        }

        // Retire only on positive termination evidence: a dead manager plus a
        // cgroup that was read and is confirmed empty. A readable but
        // non-empty cgroup whose members could not be stat'ed is NOT an empty
        // cgroup, and an unreadable cgroup is no evidence at all.
        if (manager_liveness.state == LivenessState::Dead && cgroup_empty_confirmed) {
            retireIfConfirmed(
                record, "positive termination evidence (manager ended, execution cgroup empty)");
            continue;
        }

        if (absences < kAbsencesBeforeRetirement) continue;
        // Unknown is not death: an unreadable manager blocks the absence rule
        // exactly like a live one does.
        if (manager_liveness.state != LivenessState::Dead) continue;
        if (cgroup_live || cgroup_inconclusive) continue;

        bool live_member = false;
        bool member_inconclusive = false;
        for (const auto& member : record.members) {
            const LivenessResult liveness = process_probe_ ? process_probe_(member)
                                                           : LivenessResult{};
            if (liveness.state == LivenessState::Alive) {
                live_member = true;
                break;
            }
            // An unreadable member (including one with an unvalidated
            // identity) might be alive: it can never be read as death
            // evidence, so it blocks the absence rule like a live member.
            if (liveness.state == LivenessState::Unknown) member_inconclusive = true;
        }
        if (live_member || member_inconclusive) continue;

        retireIfConfirmed(
            record, "two complete source passes without an observation and no live member");
    }

    // Prune heartbeat state for records that no longer exist.
    {
        std::set<uint64_t> live;
        for (const auto& record : records) live.insert(record.internal_id);
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

bool JobRegistry::runPass(bool do_scan, bool wait) {
    // Passes are serialized by pass_mtx_. The worker never waits for a pass
    // (it retries on the next slice), while an on-demand reconcile waits so
    // that returning means "a pass completed".
    std::unique_lock<std::mutex> pass_lock(pass_mtx_, std::defer_lock);
    if (wait) {
        pass_lock.lock();
    } else if (!pass_lock.try_lock()) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        if (!hb_admitted_ || hb_stopping_) return false;
    }

    const std::int64_t started_ms = clockNow();
    std::vector<std::pair<std::string, ScanResult>> results;
    std::map<std::string, bool> source_complete;
    bool complete = true;
    std::string error;

    if (do_scan) {
        for (const auto& [name, source] : hb_sources_) {
            ScanResult result;
            try {
                result = source->scan();
            } catch (const std::exception& exception) {
                result.complete = false;
                result.error = std::string("scan threw: ") + exception.what();
            }
            if (!result.complete) {
                const std::string detail =
                    result.error.empty() ? "incomplete source pass" : result.error;
                if (error.empty()) {
                    error = name.empty() ? detail : name + ": " + detail;
                }
            }
            complete = complete && result.complete;
            // Coverage is keyed by adapter name; a record of one adapter is
            // only absence-eligible when that adapter's source covered its
            // inputs (records with no adapter — manual/legacy — never are).
            const auto inserted =
                source_complete.emplace(name, result.complete);
            if (!inserted.second)
                inserted.first->second = inserted.first->second && result.complete;
            results.emplace_back(name, std::move(result));
        }
    }

    std::set<uint64_t> observed;
    for (const auto& entry : results) {
        for (const auto& job : entry.second.jobs) {
            processObservation(job, observed);
        }
    }

    // Automatic policy is synchronous with registration; there is no deferred
    // backlog to drain here (plan §1.1.3).

    // Exec events and the sources' own scans never reach this pass: they
    // register synchronously on their producing threads through on_job.

    // Records no discovery source owns (legacy/manual registrations, or types
    // whose inventory source was removed) are refreshed here; reads never do.
    if (do_scan) refreshLegacyRecords();

    // Missing initial decisions are a record-driven obligation: repair them on
    // every pass, even when no new observation arrived (plan §5.5).
    repairMissingInitialDecisions();

    // Checkpoint: facts and intent are written without a fence on the hot path,
    // so one fsynced batch per pass bounds what a process crash can lose. The
    // pass cadence (reconcile_interval_sec) is the checkpoint cadence on
    // purpose: no extra knob, no extra thread.
    if (!store_->flushDirty("maintenance checkpoint")) {
        spdlog::warn("JobRegistry: the maintenance checkpoint could not be written");
    }

    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_passes_;
        if (do_scan) {
            last_scan_start_ms_ = started_ms;
            last_scan_complete_ = complete;
            if (complete) {
                last_scan_success_ms_ = clockNow();
                degraded_ = false;
                hb_last_error_.clear();
            } else {
                degraded_ = true;
                hb_last_error_ = error.empty() ? "source pass incomplete" : error;
            }
        }
    }

    // A targeted pass has no authoritative source coverage, so it never counts
    // as an absence; only positive termination evidence applies.
    static const std::map<std::string, bool> no_coverage;
    evaluateRetirements(observed, do_scan ? source_complete : no_coverage);

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
            // storm cannot turn into a /proc scan storm.
            const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            const bool allowed =
                last_forced_scan_ms_ == 0 || now_ms - last_forced_scan_ms_ >= 1000;
            if (allowed) {
                last_forced_scan_ms_ = now_ms;
                executed = runPass(true, false);
            } else {
                executed = runPass(false, false);
            }
        } else {
            executed = runPass(false, false);
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
        return upsertObservedJob(job);
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

void JobRegistry::init_inventory(const JobRegistryOptions& options)
{
    // Prepare heartbeat state (idempotent across test registries).
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
        ended_generations_.clear();
        closing_generations_.clear();
        last_scan_start_ms_ = 0;
        last_scan_success_ms_ = 0;
        last_scan_complete_ = false;
        hb_passes_ = 0;
        hb_stop_errors_ = 0;
    }
    installHeartbeatSink();

    const InventoryWiring wiring = resolve_inventory_components(options);
    if (wiring == InventoryWiring::Disabled) return;

    // Sink first, then the poll threads, the initial authoritative pass, the
    // status provider and finally the worker.
    startDiscoverySources(wiring);
    if (!runPass(true, /*wait=*/true)) {
        spdlog::warn("JobRegistry: the initial inventory pass did not run");
    }
    publishStatusProvider();
    startHeartbeat();

    if (wiring == InventoryWiring::Test)
        spdlog::info("JobRegistry: heartbeat running with {} test source(s)",
                     hb_sources_.size());
    else
        spdlog::info("JobRegistry: inventory heartbeat running with {} discovery type(s)",
                     hb_sources_.size());
}

JobRegistry::InventoryWiring
JobRegistry::resolve_inventory_components(const JobRegistryOptions& options)
{
    // Test wiring: pre-built sources and injected probes, no configuration
    // and no automatic selection.
    if (!options.test_sources.empty() || options.test_heartbeat) {
        hb_sources_ = options.test_sources;
        process_probe_ = options.test_process_probe;
        cgroup_probe_ = options.test_cgroup_probe;
        if (options.test_auto_policy) {
            auto_policies_.clear();
            auto_policies_[options.test_auto_policy_adapter] = options.test_auto_policy();
        }
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
    // Configuration wins over the construction default so the budget is a
    // deployment knob, not a rebuild.

    // Exactly one adapter per configured scheduler name, built through the
    // self-registration registry; the heartbeat drives them through
    // IDiscoverySource and owns no adapter details.
    adapters_.clear();
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

    // Automatic selection is armed per adapter name: use_rules=false adds the
    // configured defaults to every job, use_rules=true asks the rule engine
    // per job (reject = empty selection, the job stays inventoried).
    auto_policies_.clear();
    for (const auto& [adapter_name, settings] : config.settings) {
        if (!settings.auto_collect) continue;
        if (settings.use_rules)
            auto_policies_[adapter_name] =
                AutoCollectorPolicy::rules(settings.rules_dir, settings.rules_prefix);
        else
            auto_policies_[adapter_name] =
                AutoCollectorPolicy::defaults(settings.default_collectors);
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
    // register into pid2job/cgroup2job; legacy/manual rows (unknown adapter)
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
            note = "event source unavailable (scan-only)";
        source_started_.emplace_back(name, started);
        source_start_notes_.push_back(std::move(note));
        if (!started && wiring == InventoryWiring::Production)
            spdlog::warn("JobRegistry: source '{}' has no event source, continuing with "
                         "scans only: {}",
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
    const auto collectors = policy->second.select(job, revision, error);
    if (!error.empty()) {
        spdlog::warn("JobRegistry: automatic selection failed for JobID {}; keeping the "
                     "previous decision: {}",
                     job.JobID, error);
        return std::nullopt;
    }
    AutoDecision decision;
    decision.collectors = collectors;
    decision.policy_revision = revision;
    return decision;
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
        return !target.skipped && target.revision > 0 &&
               target.actual.error.rfind("intent write failed", 0) != 0;
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
    if (!store_->flushDirty("shutdown checkpoint")) {
        spdlog::warn("JobRegistry: the shutdown checkpoint could not be written");
        ok = false;
    }
    // Archive leftover closing records first so a restored closing record is
    // not reported as an incomplete drain even though the archive succeeds.
    if (!cleanupClosingJobs()) ok = false;

    // Finalize: acknowledge the retirements this heartbeat made. The collector
    // drain has completed by now; a late acknowledgement for an old generation
    // is discarded.
    {
        std::vector<std::pair<uint64_t, ExecutionGeneration>> closing;
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            closing.swap(closing_generations_);
        }
        for (const auto& [internal_id, generation] : closing) {
            const auto record = ownRecord(internal_id);
            if (record && !(record->generation == generation)) {
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
                closing_generations_.emplace_back(internal_id, generation);
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
    return store_ && ownHasActive(internal_id);
}

void JobRegistry::attributionJobAddPid(uint64_t internal_id, pid_t pid) {
    // Runtime projection on the registry's own record: never persisted, the
    // kernel rebuilds it from events after a restart.
    std::unique_lock lg(records_mtx_);
    const auto it = records_.find(internal_id);
    if (it == records_.end()) return;
    auto& pids = it->second.job.JobPIDs;
    if (std::find(pids.begin(), pids.end(), pid) == pids.end()) {
        pids.push_back(pid);
        spdlog::trace("JobRegistry: kernel FORK added pid={} to job_id={} (now {} pids)",
                      pid, internal_id, pids.size());
    }
}

void JobRegistry::attributionJobRemovePid(uint64_t internal_id, pid_t pid) {
    std::unique_lock lg(records_mtx_);
    const auto it = records_.find(internal_id);
    if (it == records_.end()) return;
    auto& pids = it->second.job.JobPIDs;
    const auto before = pids.size();
    pids.erase(std::remove(pids.begin(), pids.end(), pid), pids.end());
    auto& members = it->second.members;
    members.erase(std::remove_if(members.begin(), members.end(),
                                  [pid](const ProcessIdentity& member) {
                                      return member.pid == pid;
                                  }),
                   members.end());
    if (pids.size() != before) {
        spdlog::trace("JobRegistry: kernel EXIT removed pid={} from job_id={} (now {} pids)",
                      pid, internal_id, pids.size());
    }
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
    if (!ownFindActive([&matcher](const Job& existing) {
            return matches_job(matcher, existing); }).empty()) {
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
    record.internal_id = job_copy.JobID;
    record.instance_id = store_->instanceId(job_copy.JobID);
    // A legacy register IS a manual selection: the caller explicitly asked
    // for these collectors, so it can never be reported as an automatic
    // origin.
    if (!record.job.CollectorNames.empty()) {
        record.selection.manual = record.job.CollectorNames;
    }
    record.revision = std::max<uint64_t>(record.revision, 1);
    record.execution_observed = false;
    if (record.state.empty()) record.state = kStateActive;
    if (record.first_seen_us == 0) record.first_seen_us = now_us();
    if (record.last_seen_us == 0) record.last_seen_us = record.first_seen_us;
    if (record.selector.adapter.empty()) {
        // Legacy registrations carry their identity only inside the typed
        // facts; reconstruct the selector, never a guess.
        record.selector = selector_from_legacy_job(record.job);
        // One copy of the adapter name: on the Job itself.
        record.job.subtype_name = record.selector.adapter;
        if (const auto identity = canonicalize(record.selector)) {
            record.identity = *identity;
            record.identity_complete = true;
        }
    }
    {
        // Bind each numeric PID to the process that holds it NOW. A manual
        // registration has no scheduler-side identity to copy, and a bare
        // number is not an execution identity: recording the current start
        // ticks makes the kernel path verify the same process later instead
        // of trusting a number that may be recycled. An unreadable stat
        // leaves the identity incomplete, which is reported as incomplete
        // ownership rather than guessed.
        const std::string boot_id = read_boot_id();
        record.members.clear();
        for (const auto pid : record.job.JobPIDs) {
            const auto ticks = read_process_start_ticks(pid);
            record.members.push_back(ProcessIdentity{
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
            if (record.identity_complete) {
                const auto owner = identity_index_.find(identity_index_key(record.identity));
                if (owner != identity_index_.end() && owner->second != record.internal_id) {
                    spdlog::error("JobRegistry: identity for JobID {} is already registered "
                                  "as JobID {}; refusing a duplicate inventory record",
                                  record.internal_id, owner->second);
                    return false;
                }
            }
        }
        std::vector<JobStore::WriteOp> ops;
        ops.push_back(put_record_op(record));
        if (record.identity_complete) {
            JobStore::WriteOp index;
            index.kind = JobStore::WriteOp::PutIdentity;
            index.internal_id = record.internal_id;
            index.identity = record.identity;
            ops.push_back(std::move(index));
        }
        // A registration that published nothing needs no recovery fence: the
        // identity lookup repairs an ambiguous landing.
        std::vector<uint64_t> touched;
        std::string error;
        if (!store_->commit(std::move(ops), /*durable=*/true, touched, "register job",
                            error)) {
            spdlog::error("JobRegistry: durable registration failed, JobID {} rejected ({})",
                          job_copy.JobID, error);
            return false;
        }
        {
            std::unique_lock lg(records_mtx_);
            const uint64_t registered_id = record.internal_id;
            records_[registered_id] = std::move(record);
            if (records_[registered_id].identity_complete &&
                !records_[registered_id].quarantined)
                identity_index_[identity_index_key(records_[registered_id].identity)] =
                    registered_id;
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
    if (record->state != kStateClosing) {
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
            if (it == records_.end() || it->second.state == kStateClosing) {
                // Already archived by a concurrent finalize, or already
                // Closing: nothing durable left to mark.
                closing_persisted = true;
            } else {
                JobRecord closing = it->second;
                rl.unlock();
                closing.state = kStateClosing;
                // The ONE revision orders memory and disk alike: the same
                // commit writes the row and advances the record.
                ++closing.revision;
                std::vector<JobStore::WriteOp> ops{put_record_op(closing)};
                std::vector<uint64_t> touched{jobID};
                std::string error;
                if (store_->commit(std::move(ops), /*durable=*/true, touched,
                                   "persist record state", error)) {
                    std::unique_lock wl(records_mtx_);
                    const auto current = records_.find(jobID);
                    if (current != records_.end()) current->second = std::move(closing);
                    closing_persisted = true;
                }
            }
        }
        if (!closing_persisted) {
            spdlog::error("JobRegistry: could not persist the closing state for JobID {}; "
                          "retirement refused (the record stays active)", jobID);
            return false;
        }
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
    if (current->state != kStateClosing) return false;
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
        std::vector<JobStore::WriteOp> ops;
        JobStore::WriteOp delete_row;
        delete_row.kind = JobStore::WriteOp::DeleteRecord;
        delete_row.internal_id = jobID;
        ops.push_back(std::move(delete_row));
        if (current->identity_complete) {
            // DeleteIdentity releases the entry only while this instance
            // still owns it on disk; a newer execution keeps its index row.
            JobStore::WriteOp release;
            release.kind = JobStore::WriteOp::DeleteIdentity;
            release.internal_id = jobID;
            release.identity = current->identity;
            ops.push_back(std::move(release));
        }
        const char* reason = "discard job";
        if (!applied.empty()) {
            JobStore::WriteOp history;
            history.kind = JobStore::WriteOp::PutHistory;
            history.internal_id = jobID;
            history.record = *current;
            history.applied_collectors = applied;
            history.history_provenance = provenance;
            ops.push_back(std::move(history));
            reason = "archive job";
        }
        std::vector<uint64_t> touched{jobID};
        std::string error;
        if (!store_->commit(std::move(ops), /*durable=*/true, touched, reason, error)) {
            spdlog::error("JobRegistry: durable {} failed for closing JobID {} ({}), "
                          "keeping it pending (kernel ownership already released)",
                          applied.empty() ? "discard" : "archive", jobID, error);
            return false;
        }
    }
    eraseRecord(jobID);
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
    if (record->state != kStateClosing) return false;
    return cleanupThenArchive(jobID);
}

size_t JobRegistry::closingJobCount() const {
    return store_ ? ownClosingCount() : 0;
}

bool JobRegistry::cleanupClosingJobs() {
    bool all_archived = true;
    if (!store_) return true;
    for (const auto& record : ownRecords()) {
        if (record.state != kStateClosing) continue;
        // Shutdown sweep: no implicit recovery. A store whose last write is
        // uncertain must not retry a write here; the record stays closing and
        // the next explicit command or a reopen resolves it.
        if (cleanupThenArchive(record.internal_id)) {
            {
                std::lock_guard<std::mutex> lock(drain_mtx_);
                drain_complete_.erase(record.internal_id);
            }
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

void JobRegistry::reconcileClosingRemovals() {
    if (!store_) return;
    ICollectionRuntime* runtime = collectionRuntime();
    for (const auto& record : ownRecords()) {
        if (record.state != kStateClosing) continue;
        bool drained = false;
        {
            std::lock_guard<std::mutex> lock(drain_mtx_);
            drained = drain_complete_.count(record.internal_id) != 0;
        }
        if (drained) {
            // The runtime released the instance but the cleanup/archive failed
            // earlier: retry it. The finalize lock keeps the retry idempotent.
            if (finalizeClosingRemoval(record.internal_id, /*allow_recovery=*/false)) {
                std::lock_guard<std::mutex> lock(drain_mtx_);
                drain_complete_.erase(record.internal_id);
            }
            continue;
        }
        if (runtime == nullptr) {
            // No collectors exist on this core: cleanup can run directly.
            cleanupThenArchive(record.internal_id);
            continue;
        }
        // A Closing record restored after a restart has no in-process drain.
        // Re-arm it: beginDrain is idempotent per instance.
        runtime->beginDrain(record.internal_id);
    }
}

size_t JobRegistry::delJob(const SubAttrMatcher& matcher) {
    const auto matched = ownFindActive(
        [&matcher](const Job& job) { return matches_job(matcher, job); });

    for (const auto& record : matched) {
        // One retirement path: durable Closing, then the runtime's drain, then
        // checked kernel cleanup and the archive.
        if (retireInstance(record.internal_id)) {
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
    auto record = ownRecord(jobID);
    if (!record || record->state == kStateClosing) return std::nullopt;
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
        if (record.internal_id == kSystemSentinelJobId) continue;
        if (record.state != kStateActive) continue;
        if (!record.execution_verified) continue;
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
    auto record = it->second;
    record.instance_id = store_->instanceId(id);
    return record;
}

std::vector<JobRecord> JobRegistry::ownRecords() const {
    std::shared_lock lg(records_mtx_);
    std::vector<JobRecord> out;
    out.reserve(records_.size());
    for (const auto& entry : records_) {
        auto record = entry.second;
        record.instance_id = store_->instanceId(entry.first);
        out.push_back(std::move(record));
    }
    return out;
}

std::vector<JobRecord> JobRegistry::ownFindActive(
    const std::function<bool(const Job&)>& pred) const {
    std::shared_lock lg(records_mtx_);
    std::vector<JobRecord> out;
    for (const auto& entry : records_) {
        if (entry.second.state == kStateClosing) continue;
        if (!pred(entry.second.job)) continue;
        auto record = entry.second;
        record.instance_id = store_->instanceId(entry.first);
        out.push_back(std::move(record));
    }
    return out;
}

bool JobRegistry::ownHasActive(uint64_t id) const {
    std::shared_lock lg(records_mtx_);
    const auto it = records_.find(id);
    return it != records_.end() && it->second.state != kStateClosing;
}

size_t JobRegistry::ownClosingCount() const {
    std::shared_lock lg(records_mtx_);
    size_t count = 0;
    for (const auto& entry : records_) {
        if (entry.second.state == kStateClosing) ++count;
    }
    return count;
}

std::optional<JobRecord> JobRegistry::ownRecordByIdentity(
    const CanonicalIdentity& identity) const {
    std::shared_lock lg(records_mtx_);
    const auto it = identity_index_.find(identity_index_key(identity));
    if (it == identity_index_.end()) return std::nullopt;
    const auto record = records_.find(it->second);
    if (record == records_.end()) return std::nullopt;
    auto value = record->second;
    value.instance_id = store_->instanceId(it->second);
    return value;
}

CollectionSelection JobRegistry::ownSelection(uint64_t id) const {
    std::shared_lock lg(records_mtx_);
    const auto it = records_.find(id);
    if (it != records_.end()) return it->second.selection;
    return CollectionSelection{};
}

void JobRegistry::publishRecord(JobRecord record) {
    const uint64_t id = record.internal_id;
    const bool index_it = record.identity_complete && !record.quarantined;
    std::unique_lock lg(records_mtx_);
    records_[id] = std::move(record);
    if (index_it) {
        identity_index_[identity_index_key(records_[id].identity)] = id;
    }
}

void JobRegistry::eraseRecord(uint64_t id) {
    std::unique_lock lg(records_mtx_);
    const auto it = records_.find(id);
    if (it == records_.end()) return;
    if (it->second.identity_complete) {
        const auto key = identity_index_key(it->second.identity);
        const auto idx = identity_index_.find(key);
        if (idx != identity_index_.end() && idx->second == id)
            identity_index_.erase(idx);
    }
    records_.erase(it);
}

void JobRegistry::applyRecovered(const std::vector<JobRecord>& adopted,
                                 const std::vector<uint64_t>& removed) {
    std::unique_lock lg(records_mtx_);
    for (const auto& row : adopted) {
        auto it = records_.find(row.internal_id);
        if (it != records_.end() && it->second.identity_complete) {
            const auto key = identity_index_key(it->second.identity);
            const auto idx = identity_index_.find(key);
            if (idx != identity_index_.end() && idx->second == row.internal_id)
                identity_index_.erase(idx);
        }
        if (row.identity_complete && !row.quarantined)
            identity_index_[identity_index_key(row.identity)] = row.internal_id;
        records_[row.internal_id] = row;
    }
    for (const auto id : removed) {
        auto it = records_.find(id);
        if (it == records_.end()) continue;
        if (it->second.identity_complete) {
            const auto key = identity_index_key(it->second.identity);
            const auto idx = identity_index_.find(key);
            if (idx != identity_index_.end() && idx->second == id)
                identity_index_.erase(idx);
        }
        records_.erase(it);
    }
}

void JobRegistry::applyIdentityToFacts(Job& job, const NativeSelector& selector,
                                       const CanonicalIdentity& identity) const {
    // Generic derivation: the authoritative native string and namespace. The
    // scheduler typed-attr derivation dispatches through the adapters by
    // identity.adapter; without adapters (store-only tests) the observed
    // facts are stored verbatim — no branch, no misfiling of an open-world
    // scheduler into a built-in schema.
    job.NativeJobID = identity.native_job_id;
    if (!selector.cluster_name.empty()) job.cluster_name = selector.cluster_name;
    if (!selector.cluster_tag.empty()) job.clusterTag = selector.cluster_tag;
    if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys) {
        job.jobtype = JobType::Job;
    }
    for (const auto& adapter : adapters_) {
        if (adapter->name() == identity.adapter) {
            adapter->applyIdentityToFacts(job, selector, identity);
            break;
        }
    }
}

// One observation against the current record image: pure decision, no
// durable write and no publish (moved verbatim from the store). The
// register/merge semantics of a repeated identity live here, so every caller
// gets the same result.
UpsertResult JobRegistry::mergeDiscoveredJob(const JobRecord& stored, const Job& job,
                                           const NativeSelector& selector,
                                           const CanonicalIdentity& identity,
                                           uint64_t internal_id, int64_t observed_at,
                                           JobRecord& updated_out) {
    UpsertResult result;
    result.internal_id = internal_id;
    result.instance_id = store_->instanceId(internal_id);
    result.revision = stored.revision;

    const ExecutionGeneration generation = generation_of(job);
    const std::vector<ProcessIdentity>& processes = members_of(job);
    const bool members_complete = members_authoritative_of(job);

    if (stored.quarantined) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::IdentityQuarantined;
        result.error = "identity_quarantined";
        result.message = "record is quarantined after an identity conflict";
        return result;
    }
    if (stored.state == kStateClosing) {
        // A retiring instance must never be resurrected into the active set by
        // a late observation: the drain owns it until it is archived.
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::InstanceClosing;
        result.error = "instance_closing";
        result.message = "the instance is retiring and no longer accepts observations";
        return result;
    }
    if (stored.identity_complete && stored.identity != identity) {
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
    // late scan loses to an event that was committed first (plan §4.1, A09).
    if (stored.job.execution && job.execution &&
        stored.job.execution->read_order != 0 && job.execution->read_order != 0 &&
        job.execution->read_order < stored.job.execution->read_order) {
        result.ok = true;
        result.disposition = UpsertDisposition::Unchanged;
        result.message = "older source read order; newer facts kept";
        updated_out = stored;
        return result;
    }

    if (stored.generation.complete() && generation.complete() &&
        stored.generation != generation) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::GenerationConflict;
        result.error = "execution_generation_conflict";
        result.message = "a different live execution generation owns this identity";
        spdlog::warn("JobRegistry: observation for '{}' carries a different live generation "
                     "than JobID {}; not merged", identity.native_job_id, internal_id);
        return result;
    }

    // Merge facts idempotently. The member list is authoritative only when the
    // source says so: an empty list from a partially-known membership must
    // never erase stored members.
    Job observed_job = job;
    observed_job.JobID = internal_id;
    observed_job.CollectorNames = stored.job.CollectorNames;
    applyIdentityToFacts(observed_job, selector, identity);
    const bool authoritative_members = members_complete;
    if (!processes.empty()) {
        observed_job.JobPIDs.clear();
        for (const auto& process : processes) observed_job.JobPIDs.push_back(process.pid);
    } else if (members_complete) {
        // Read succeeded, no members: an authoritative empty set.
        observed_job.JobPIDs.clear();
    }

    Job merged = stored.job;
    merge_job_facts(merged, observed_job, authoritative_members);
    merged.JobID = internal_id;
    merged.CollectorNames = stored.job.CollectorNames;

    JobRecord updated = stored;
    // This record now carries a discovery observation, so its execution facts
    // are owned by the source: an incomplete generation is an unverified
    // execution from here on, never a manual row that merely lacks one.
    updated.execution_observed = true;
    updated.job = merged;
    if (job.execution) updated.job.execution = job.execution;
    updated.selector = selector;
    updated.last_seen_us = observed_at;
    const bool generation_became_complete =
        !stored.generation.complete() && generation.complete();
    if (generation.complete()) updated.generation = generation;
    if (!processes.empty()) {
        updated.members = normalized_processes(processes);
    } else if (members_complete) {
        updated.members.clear();
    } else if (!observed_job.JobPIDs.empty()) {
        updated.members.clear();
        for (const auto pid : observed_job.JobPIDs)
            updated.members.push_back(ProcessIdentity{pid, 0, ""});
    }

    // A record whose liveness could not be verified is re-admitted by positive
    // observation evidence. Unverified is not a lifecycle state.
    const bool recovered = !stored.execution_verified;
    if (recovered) updated.execution_verified = true;
    // Provenance is a durable fact of its own: a manual row that first meets a
    // discovery observation stays "manual" on disk when every other fact is
    // identical, and a restart would replay its incomplete generation as
    // Verified.  The delta rides the same write and revision as any fact change.
    const bool provenance_changed =
        updated.execution_observed != stored.execution_observed;
    const bool changed = recovered || provenance_changed ||
        facts_fingerprint(merged, updated) != facts_fingerprint(stored.job, stored);
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
    result.generation_became_complete = generation_became_complete;
    updated_out = std::move(updated);
    (void)observed_at;
    return result;
}

// The selection command transaction (moved from the store): the records are
// read under records_mtx_, the revision CAS is checked there, and every
// changed row is written in ONE unsynced batch. A write failure commits and
// publishes nothing.
bool JobRegistry::executeSelectionMutations(
    const std::vector<SelectionMutation>& mutations,
    std::vector<SelectionMutationResult>& out, std::string& error) {
    out.clear();
    error.clear();
    if (!store_ || !store_->ready()) {
        error = "store_unavailable";
        return false;
    }
    if (store_->recoveryRequired()) {
        // The stored row may already carry a write memory did not publish.
        // Refusing is the only answer that cannot diverge further.
        error = "recovery_required";
        return false;
    }
    if (mutations.empty()) {
        error = "no_targets";
        return false;
    }

    // The whole read-transform-write runs under the record transaction lock:
    // the revision guard is evaluated against the same image that is written,
    // so no caller can pass a stale revision between a check and a commit.
    // records_mtx_ is taken to read and (after the commit) to publish.
    std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);

    struct PendingWrite {
        JobRecord record;
    };
    std::vector<SelectionMutationResult> planned;
    std::vector<PendingWrite> writes;
    std::set<uint64_t> seen;
    planned.reserve(mutations.size());

    for (const auto& mutation : mutations) {
        if (!seen.insert(mutation.internal_id).second) {
            error = "duplicate_target";
            return false;
        }

        JobRecord record;
        {
            std::shared_lock lg(records_mtx_);
            const auto it = records_.find(mutation.internal_id);
            if (it == records_.end()) {
                error = "not_found";
                return false;
            }
            record = it->second;
        }
        if (record.quarantined) {
            error = "identity_quarantined";
            return false;
        }
        if (record.state == kStateClosing) {
            error = "instance_closing";
            return false;
        }
        if (mutation.expected_revision.has_value() &&
            record.revision != *mutation.expected_revision) {
            error = "stale_revision";
            return false;
        }

        const CollectionSelection current = record.selection;
        const auto before = current.effectiveCollectors();
        CollectionSelection next = current;
        bool intent_changed = false;
        bool stored_changed = false;
        bool skipped_manual_override = false;

        switch (mutation.operation) {
            case SelectionOperation::SetManual:
                if (!current.manual.has_value() ||
                    *current.manual != mutation.collectors) {
                    next.manual = mutation.collectors;
                    intent_changed = true;
                    stored_changed = true;
                }
                break;
            case SelectionOperation::ResetAuto:
                if (current.manual.has_value()) {
                    next.manual.reset();
                    intent_changed = true;
                    stored_changed = true;
                }
                break;
            case SelectionOperation::SetAuto:
                if (current.automatic != mutation.collectors ||
                    current.automatic_source != mutation.automatic_source) {
                    next.automatic = mutation.collectors;
                    next.automatic_source = mutation.automatic_source;
                    stored_changed = true;
                    if (current.manual.has_value()) {
                        // Manual precedence, including an empty manual stop:
                        // record the decision for a later reset_auto without
                        // moving the client-visible intent or the runtime.
                        skipped_manual_override = true;
                    } else {
                        intent_changed = true;
                    }
                }
                break;
        }

        SelectionMutationResult result;
        result.internal_id = mutation.internal_id;
        result.intent_changed = intent_changed;
        result.stored_changed = stored_changed;
        result.skipped_manual_override = skipped_manual_override;
        result.runtime_changed = before != next.effectiveCollectors();
        // ONE revision orders every record mutation, bookkeeping included: an
        // automatic decision recorded under a manual override still advances
        // the record. It does not detach anything (runtime_changed stays false).
        result.revision = stored_changed ? record.revision + 1 : record.revision;
        if (stored_changed) {
            record.selection = next;
            record.revision = result.revision;
            record.job.CollectorNames = next.effectiveCollectors();
            writes.push_back(PendingWrite{std::move(record)});
            result.selection = std::move(next);
        } else {
            result.selection = current;
        }
        planned.push_back(std::move(result));
    }

    if (!writes.empty()) {
        std::vector<JobStore::WriteOp> ops;
        std::vector<uint64_t> touched;
        ops.reserve(writes.size());
        touched.reserve(writes.size());
        for (const auto& write : writes) {
            ops.push_back(put_record_op(write.record));
            touched.push_back(write.record.internal_id);
        }
        // Intent is hot path: unsynced, checkpointed by maintenance/shutdown.
        if (!store_->commit(std::move(ops), /*durable=*/false, touched,
                            "mutate selection", error)) {
            // A reported failure publishes nothing: the in-memory records and
            // the caller-visible outcome stay at their previous values.
            error = "store_write_failed";
            return false;
        }
        // Publish after the batch landed: every planned row becomes
        // authoritative at once (all-or-nothing). The transaction lock held
        // here guarantees no other mutation moved these rows in between.
        std::unique_lock lg(records_mtx_);
        for (const auto& write : writes) {
            const auto it = records_.find(write.record.internal_id);
            if (it != records_.end()) it->second = write.record;
        }
    }

    // The results carry the authoritative revision and selection; the map is
    // already up to date.
    out = std::move(planned);
    return true;
}

void JobRegistry::validateRestoredExecutions(std::vector<JobRecord>& rows) {
    const std::string boot_id = read_boot_id();
    for (auto& record : rows) {
        if (record.state == kStateClosing) continue;   // the drain owns it
        switch (verify_execution(record.generation, record.execution_observed,
                                 boot_id, process_probe_)) {
            case ExecutionVerdict::Stale:
                record.execution_verified = false;
                record.job.JobPIDs.clear();
                record.members.clear();
                spdlog::warn("JobRegistry: restored JobID {} has a stale execution "
                             "generation; liveness unknown", record.internal_id);
                break;
            case ExecutionVerdict::Unknown:
                record.execution_verified = false;
                spdlog::warn("JobRegistry: restored JobID {} has an execution "
                             "generation that cannot be verified (unreadable "
                             "process or incomplete discovery identity); liveness "
                             "unknown, {} member(s) kept as evidence and never "
                             "replayed",
                             record.internal_id, record.members.size());
                break;
            case ExecutionVerdict::Verified: {
                std::vector<ProcessIdentity> kept;
                kept.reserve(record.members.size());
                size_t dropped = 0;
                for (const auto& member : record.members) {
                    if (!member.complete()) {
                        kept.push_back(member);
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
                    record.members = std::move(kept);
                    spdlog::warn("JobRegistry: restored JobID {} dropped {} "
                                 "reused/gone member process identity(ies)",
                                 record.internal_id, dropped);
                }
                if (!record.members.empty()) {
                    record.job.JobPIDs.clear();
                    for (const auto& member : record.members)
                        record.job.JobPIDs.push_back(member.pid);
                }
                if (record.members.empty() && dropped != 0)
                    record.execution_verified = false;
                break;
            }
        }
    }
}

// True when an instance is live and policy-eligible but still has no durable
// intent: the initial decision never landed (write failure, crash) or the
// execution generation completed later. Derived from durable state, so it also
// holds after a restart.
bool JobRegistry::missing_initial_decision(const JobRecord& record) const
{
    if (record.state != kStateActive) return false;
    if (record.job.execution && !record.job.execution->generation.complete()) return false;
    if (!record.identity_complete) return false;
    return !ownSelection(record.internal_id).automatic.has_value();
}

std::optional<bool> JobRegistry::repair_initial_decision(const JobRecord& record,
                                                         UpsertResult& result)
{
    const auto committed = apply_auto_policy(record.job, record.identity.adapter);
    if (committed == false) {
        result.code = RegistrationError::InitialIntentWriteFailed;
        result.error = to_string(result.code);
        result.message = "the initial automatic decision could not be committed";
    }
    return committed;
}

UpsertResult JobRegistry::upsertObservedJob(const Job& job) {
    // A registration is also the record-driven repair point (A14): verify every
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

    const auto selector = selector_of(job);
    const auto identity = canonicalize(selector);
    if (!identity) {
        result.code = RegistrationError::IncompleteIdentity;
        result.error = "incomplete_identity";
        result.message = "scheduler type, cluster namespace and native job id are required";
        spdlog::debug("JobRegistry: rejected incomplete observation (native='{}', "
                      "cluster='{}')",
                      selector.native_job_id, selector.cluster_name);
        return result;
    }
    const std::string index_key = identity_index_key(*identity);
    const ExecutionGeneration generation = generation_of(job);
    const std::vector<ProcessIdentity>& processes = members_of(job);
    const bool members_complete = members_authoritative_of(job);
    const int64_t observed_at = read_at_us_of(job) != 0 ? read_at_us_of(job) : now_us();

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
            if (auto durable = store_->readRowByIdentity(*identity)) {
                // An earlier create landed ambiguously without publishing
                // anything: the durable row is the truth memory never held,
                // and this identity lookup is the documented repair. Adopt
                // it, then merge.
                internal_id = durable->internal_id;
                std::unique_lock lg(records_mtx_);
                if (durable->identity_complete && !durable->quarantined)
                    identity_index_[index_key] = internal_id;
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
        JobRecord updated;
        result = mergeDiscoveredJob(stored, job, selector, *identity, internal_id,
                                  observed_at, updated);
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
            std::vector<JobStore::WriteOp> ops{put_record_op(updated)};
            std::vector<uint64_t> touched{internal_id};
            std::string error;
            if (!store_->commit(std::move(ops), /*durable=*/false, touched,
                                "update inventory facts", error)) {
                result.ok = false;
                result.code = RegistrationError::StoreWriteFailed;
                result.error = "store_write_failed";
                result.message = error;
                return result;
            }
            std::unique_lock lg(records_mtx_);
            records_[internal_id] = std::move(updated);
            record = records_[internal_id];
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
        created.job.CollectorNames.clear();
        applyIdentityToFacts(created.job, selector, *identity);
        created.job.JobPIDs.clear();
        for (const auto& process : processes)
            created.job.JobPIDs.push_back(process.pid);
        std::sort(created.job.JobPIDs.begin(), created.job.JobPIDs.end());
        created.job.JobPIDs.erase(
            std::unique(created.job.JobPIDs.begin(), created.job.JobPIDs.end()),
            created.job.JobPIDs.end());

        created.internal_id = allocated;
        created.instance_id = store_->instanceId(allocated);
        created.selector = selector;
        created.identity = *identity;
        created.identity_complete = true;
        created.execution_observed = true;   // created by a discovery observation
        created.generation = generation;
        created.members_authoritative = members_complete;
        created.revision = 1;
        created.state = kStateActive;
        created.first_seen_us = observed_at;
        created.last_seen_us = observed_at;
        if (!processes.empty()) {
            created.members = normalized_processes(processes);
        } else {
            for (const auto pid : created.job.JobPIDs) {
                created.members.push_back(ProcessIdentity{pid, 0, ""});
            }
        }

        std::vector<JobStore::WriteOp> ops;
        JobStore::WriteOp counter;
        counter.kind = JobStore::WriteOp::ReserveCounter;
        counter.durable_counter = durable_counter;
        counter.counter_exhausted = durable_exhausted;
        ops.push_back(std::move(counter));
        ops.push_back(put_record_op(created));
        JobStore::WriteOp index;
        index.kind = JobStore::WriteOp::PutIdentity;
        index.internal_id = allocated;
        index.identity = *identity;
        ops.push_back(std::move(index));
        // A create has no published memory image to revert: identity
        // lookup repairs an ambiguous create, so it does not fence
        // recovery.
        std::vector<uint64_t> touched;
        std::string error;
        if (!store_->commit(std::move(ops), /*durable=*/false, touched,
                            "create inventory record", error)) {
            result.code = RegistrationError::StoreWriteFailed;
            result.error = "store_write_failed";
            result.message = error;
            return result;
        }
        {
            std::unique_lock lg(records_mtx_);
            records_[allocated] = std::move(created);
            identity_index_[index_key] = allocated;
        }

        result.ok = true;
        result.disposition = UpsertDisposition::Created;
        result.internal_id = allocated;
        result.instance_id = store_->instanceId(allocated);
        std::shared_lock lg(records_mtx_);
        result.revision = records_[allocated].revision;
        record = records_[allocated];
        spdlog::info("JobRegistry: created inventory instance {} for {} '{}'",
                     allocated, identity->adapter, identity->native_job_id);
        }
    }   // mutation_mtx_ released: kernel admission, the runtime wake and the
        // automatic-policy evaluation below never run under a registry lock.

    if (result.disposition == UpsertDisposition::Created) {
        // Kernel attribution is initialized here, exactly like the legacy
        // registration: eBPF attribution and the cgroup map must not wait for
        // the reconcile pass, and the cgroup-id cache must exist for the
        // later removal.
        sync_job_to_kernel(record.job);
        // Facts only: an unbound instance creates no collector work, but the
        // collection runtime must learn the instance exists (System collectors
        // are its concern). A coalesced wake, never a synchronous apply here.
        wake_runtime();
        // First complete registration is an automatic-selection trigger (plan
        // 6.2); Lua evaluation happens outside every registry lock. An
        // observation whose execution generation is not verified yet is never
        // auto-collected: the update that completes the generation re-runs
        // the selection instead.
        if (generation_of(job).complete()) {
            result.intent_committed =
                apply_auto_policy(record.job, record.identity.adapter);
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
        // only on the Created path (plan §5.5).
        if (result.generation_became_complete || missing_initial_decision(record))
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

void JobRegistry::evaluateAutoPolicies(const std::vector<uint64_t>& internal_ids,
                                       std::vector<AutoSelection>& out) {
    if (internal_ids.empty()) return;

    for (const uint64_t internal_id : internal_ids) {
        const auto record = ownRecord(internal_id);
        if (!record || record->state != kStateActive) continue;   // retired meanwhile
        auto decision = autoSelectionFor(record->job, record->identity.adapter);
        if (!decision) continue;   // no enabled policy for this adapter
        out.push_back(AutoSelection{internal_id, std::move(decision->collectors),
                                    std::move(decision->policy_revision)});
    }
}

std::optional<JobRecord> JobRegistry::inventoryRecord(uint64_t jobID) const {
    return ownRecord(jobID);
}

std::optional<JobRecord> JobRegistry::inventoryRecordByIdentity(
    const CanonicalIdentity& identity) const {
    return ownRecordByIdentity(identity);
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

CollectionError selection_error_from_store(const std::string& error)
{
    if (error == "not_found") return CollectionError::NotFound;
    // A closing instance cannot accept a new selection; report it like a
    // stale revision (the caller must re-read the instance, not retry blindly).
    if (error == "instance_closing" || error == "stale_revision")
        return CollectionError::StaleRevision;
    return CollectionError::StoreUnavailable;
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
        mutation.internal_id = id;
        mutation.operation = manual ? SelectionOperation::SetManual
                                    : SelectionOperation::ResetAuto;
        mutation.collectors = names;
        mutation.expected_revision = request.expected_revision;
        mutations.push_back(std::move(mutation));
    }

    std::vector<SelectionMutationResult> results;
    std::string error;
    if (!executeSelectionMutations(mutations, results, error)) {
        outcome.error = selection_error_from_store(error);
        outcome.message = "binding transaction rejected: " + error;
        return outcome;
    }

    outcome.targets.reserve(results.size());
    for (const auto& result : results) {
        CollectionTargetOutcome target;
        target.internal_id = result.internal_id;
        target.instance_id = store_->instanceId(result.internal_id);
        target.selection = result.selection;
        target.revision = result.revision;
        target.changed = result.intent_changed;
        if (!result.intent_changed && !result.stored_changed &&
            !result.skipped_manual_override) {
            ++outcome.unchanged;
        }
        outcome.skipped_manual_override += result.skipped_manual_override ? 1 : 0;
        outcome.targets.push_back(std::move(target));
    }

    // The command is committed. Convergence belongs to the runtime; this only
    // wakes it. The reported state is sampled afterwards and may honestly
    // still show the previous attachment.
    wake_runtime();
    for (auto& target : outcome.targets) {
        target.actual = actualCollection(target.internal_id);
    }
    outcome.ok = true;
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
    std::vector<CollectionTargetOutcome> planned;
    for (const auto& selection : selections) {
        CollectionTargetOutcome target;
        target.internal_id = selection.internal_id;
        target.instance_id = store_->instanceId(selection.internal_id);

        if (!ownHasActive(selection.internal_id)) {
            ++outcome.skipped_invalid;
            outcome.message = "no verified inventory instance with internal id " +
                              std::to_string(selection.internal_id);
            target.selection = ownSelection(selection.internal_id);
            target.actual.error = "no verified inventory instance";
            target.skipped = true;
            planned.push_back(std::move(target));
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
            planned.push_back(std::move(target));
            continue;
        }

        SelectionMutation mutation;
        mutation.internal_id = selection.internal_id;
        mutation.operation = SelectionOperation::SetAuto;
        mutation.collectors = names;
        mutation.automatic_source = selection.policy_revision;
        mutations.push_back(std::move(mutation));
        planned.push_back(std::move(target));
    }

    if (!mutations.empty()) {
        std::vector<SelectionMutationResult> results;
        std::string error;
        if (!executeSelectionMutations(mutations, results, error)) {
            outcome.error = selection_error_from_store(error);
            outcome.message = "automatic policy transaction rejected: " + error;
            for (auto& target : planned) {
                // Attribution survives a batch-wide failure: every item says
                // the intent write did not land.
                if (target.actual.error.empty()) {
                    target.actual.error = "intent write failed: " + error;
                }
            }
            outcome.targets = std::move(planned);
            return outcome;
        }

        std::map<uint64_t, const SelectionMutationResult*> by_id;
        for (const auto& result : results) by_id[result.internal_id] = &result;
        for (auto& target : planned) {
            const auto it = by_id.find(target.internal_id);
            if (it == by_id.end()) continue;
            const auto& result = *it->second;
            target.selection = result.selection;
            target.revision = result.revision;
            target.changed = result.intent_changed;
            if (!result.intent_changed && !result.stored_changed &&
                !result.skipped_manual_override) {
                ++outcome.unchanged;
            }
            outcome.skipped_manual_override += result.skipped_manual_override ? 1 : 0;
        }

        wake_runtime();
        for (auto& target : planned) target.actual = actualCollection(target.internal_id);
    }

    outcome.ok = true;
    outcome.targets = std::move(planned);
    return outcome;
}

bool JobRegistry::recoverStoreIfNeeded(std::string& error) {
    error.clear();
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

std::string JobRegistry::instanceId(uint64_t jobID) const {
    return store_->instanceId(jobID);
}

StoreHealth JobRegistry::storeHealth() const {
    auto health = store_ ? store_->health() : StoreHealth{};
    // The record-derived fields describe the registry's map (it owns the
    // records); the persistence fields come from the store.
    {
        std::shared_lock lg(records_mtx_);
        health.active_instances = records_.size();
        for (const auto& entry : records_) {
            if (!entry.second.identity_complete) ++health.incomplete_identities;
            if (entry.second.quarantined) ++health.quarantined_identities;
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

void JobRegistry::setInstanceState(uint64_t jobID, const std::string& state) {
    // In-memory only: the durable Closing transition goes through the store.
    std::unique_lock lg(records_mtx_);
    const auto it = records_.find(jobID);
    if (it != records_.end()) it->second.state = state;
}

void JobRegistry::setExecutionVerified(uint64_t jobID, bool verified) {
    // Liveness evidence only. It never changes the lifecycle state and never
    // authorizes retirement; it gates kernel admission. Runtime state: not
    // persisted, rebuilt by observation after a restart.
    std::unique_lock lg(records_mtx_);
    const auto it = records_.find(jobID);
    if (it != records_.end()) it->second.execution_verified = verified;
}

bool JobRegistry::refreshJobFacts(uint64_t jobID) {
    std::string recovery_error;
    if (!recoverStoreIfNeeded(recovery_error)) return false;

    // /proc and scheduler metadata I/O run strictly outside every lock; only
    // the result write happens under them.
    Job job;
    {
        std::shared_lock lg(records_mtx_);
        const auto it = records_.find(jobID);
        if (it == records_.end()) return false;
        job = it->second.job;
    }
    update_job(job);

    std::lock_guard<std::mutex> mutation_lock(mutation_mtx_);
    JobRecord record;
    {
        std::shared_lock lg(records_mtx_);
        const auto it = records_.find(jobID);
        if (it == records_.end()) return false;
        record = it->second;
    }
    record.job = job;
    record.job.JobID = jobID;   // CollectorNames stay intent-owned, untouched
    record.last_seen_us = now_us();
    // Records without a validated execution generation keep their member list
    // as the only liveness evidence. Only replace it while the refresh still
    // sees processes: an empty result is itself the termination evidence the
    // retirement pass has to evaluate.
    if (!record.generation.complete() && !job.JobPIDs.empty()) {
        record.members.clear();
        for (const auto pid : job.JobPIDs)
            record.members.push_back(ProcessIdentity{pid, 0, ""});
    }
    if (record.revision == 0) {
        record.revision = 1;
        record.first_seen_us = now_us();
        record.selector = selector_from_legacy_job(record.job);
        if (const auto identity = canonicalize(record.selector)) {
            record.identity = *identity;
            record.identity_complete = true;
        }
    }
    // A durable record mutation advances the one record revision. This is the
    // same counter the selection CAS compares.
    ++record.revision;
    std::vector<JobStore::WriteOp> ops{put_record_op(record)};
    std::vector<uint64_t> touched{jobID};
    std::string error;
    if (!store_->commit(std::move(ops), /*durable=*/true, touched, "update job facts",
                        error)) {
        return false;
    }
    {
        std::unique_lock lg(records_mtx_);
        const auto it = records_.find(jobID);
        if (it != records_.end()) it->second = std::move(record);
    }
    return true;
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

    std::vector<std::pair<uint64_t, ExecutionGeneration>> closing_ids;
    std::set<uint64_t> suspect_ids;
    std::uint64_t events_received = 0;
    std::uint64_t rescan_requests = 0;
    std::uint64_t obsolete_hints = 0;
    std::uint64_t incomplete_observations = 0;
    std::uint64_t upsert_conflicts = 0;
    std::uint64_t retirements = 0;
    std::uint64_t passes = 0;
    std::size_t exec_ingest_depth = 0;
    std::size_t policy_pending = 0;
    std::uint64_t policy_deferred_total = 0;
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
        closing_ids = closing_generations_;
        suspect_ids = suspect_ids_;
        events_received = hb_events_received_;
        rescan_requests = hb_rescan_requests_;
        obsolete_hints = hb_obsolete_hints_;
        incomplete_observations = hb_incomplete_observations_;
        upsert_conflicts = hb_upsert_conflicts_;
        retirements = hb_retirements_;
        passes = hb_passes_;
        last_scan_start_ms = last_scan_start_ms_;        last_scan_success_ms = last_scan_success_ms_;
        last_scan_complete = last_scan_complete_;
        incomplete_drain = incomplete_drain_;
        admitted = hb_admitted_;
        degraded = degraded_;
        last_error = hb_last_error_;
    }
    // Deprecated: the exec ingest worker and the observation hint queue are
    // gone (plan P4). The keys stay for status compatibility and are always
    // zero; a fake queue is not reported.
    (void)exec_ingest_depth;
    // The deferred-policy FIFO was removed with the experimental budget: the
    // keys stay for status compatibility and always report zero.
    (void)policy_pending;
    (void)policy_deferred_total;
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
    std::size_t quarantined_count = 0;
    for (const auto& record : records) {
        if (record.internal_id == 0) continue;   // system sentinel is never inventory
        if (!record.identity_complete) ++incomplete_count;
        if (record.quarantined) ++quarantined_count;
        const bool suspect = !record.execution_verified ||
                             suspect_ids.count(record.internal_id) != 0;
        if (suspect) ++suspect_count;
        else if (record.state == kStateActive) ++active_count;
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
    out["quarantined_count"] = quarantined_count;
    out["events_received"] = events_received;
    out["events_dropped"] = 0;   // no queue since P4: nothing is dropped
    out["queue_depth"] = 0;   // no queue since P4
    out["exec_ingest_depth"] = exec_ingest_depth;
    out["policy_pending"] = policy_pending;
    out["policy_deferred_total"] = policy_deferred_total;
    // Deprecated: the per-batch policy budget experiment was removed; policy
    // is evaluated synchronously and only timed for diagnostics.
    out["auto_policy_budget_us"] = 0;
    out["rescan_requests"] = rescan_requests;
    out["obsolete_hints"] = obsolete_hints;
    out["incomplete_observations"] = incomplete_observations;
    out["upsert_conflicts"] = upsert_conflicts;
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
