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

    // The store is the single durable representation; it opens the db, runs
    // the legacy migration and restores the records in its constructor.
    JobStore::Options store_options;
    store_options.db_path = options.db_path;
    // Restore must apply the same identity verification the admission gate
    // uses, otherwise a row could be published as verified here and refused
    // there (or worse, vice versa).
    store_options.identity_probe = process_probe_;
    store_ = std::make_unique<JobStore>(store_options);
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
constexpr std::size_t kHintQueueCapacity = 512;
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

void JobRegistry::enqueueHint(HeartbeatHint hint) {
    bool overflow = false;
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_events_received_;
        if (!hb_admitted_ || hb_stopping_) {
            ++hb_events_dropped_;
            return;
        }
        if (hints_.size() >= kHintQueueCapacity) {
            ++hb_events_dropped_;
            overflow = true;
        } else {
            hints_.push_back(std::move(hint));
            hint_wakeup_ = true;
        }
    }
    if (overflow) {
        // Bounded queue: the producer is never blocked; the drop is counted,
        // the heartbeat degrades and an authoritative rescan repairs whatever
        // the dropped hint would have refreshed.
        noteHeartbeatError("invariant hint queue overflow; awaiting authoritative rescan");
        spdlog::warn("JobRegistry: hint queue full, dropping event; requesting a rescan");
        requestRescan("hint queue overflow");
        return;
    }
    hb_cv_.notify_one();
}

void JobRegistry::notifyExit(pid_t pid) {
    HeartbeatHint hint;
    hint.kind = HeartbeatHint::Kind::Exit;
    hint.pid = pid;
    enqueueHint(std::move(hint));
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

void JobRegistry::drainHints(std::deque<HeartbeatHint>& out) {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    while (!hints_.empty()) {
        out.push_back(std::move(hints_.front()));
        hints_.pop_front();
    }
}

Job JobRegistry::toJob(const Observation& observation) const {
    // The Job is the registration unit (plan §4.1): facts plus one execution
    // evidence group, no second envelope. The encoder still owns the
    // scheduler-specific attribute projection.
    Job job = facts_encoder_(observation);
    job.subtype_name = observation.selector.adapter;
    job.cluster_name = observation.selector.cluster_name;
    job.clusterTag = observation.selector.cluster_tag;
    job.NativeJobID = observation.selector.native_job_id;
    JobExecution execution;
    execution.generation = observation.generation;
    execution.members = observation.members;
    execution.members_authoritative = observation.members_complete;
    if (!observation.selector.step_id.empty())
        execution.step_id = observation.selector.step_id;
    execution.read_at_us = observation.source_timestamp_ms > 0
                               ? observation.source_timestamp_ms * 1000
                               : 0;
    execution.read_order = observation.read_order;
    execution.incomplete_reason = observation.incomplete_reason;
    job.execution = std::move(execution);
    return job;
}

void JobRegistry::processObservation(const Observation& observation,
                                     std::set<uint64_t>& observed) {
    const auto identity = canonicalize(observation.selector);
    if (identity && observation.generation.complete() &&
        isObsolete(*identity, observation.generation)) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_obsolete_hints_;
        spdlog::debug("JobRegistry: discarded hint for the ended execution of '{}'",
                      identity->native_job_id);
        return;
    }

    if (!observation.identity_complete) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_incomplete_observations_;
        spdlog::debug("JobRegistry: incomplete observation '{}': {}",
                      observation.selector.native_job_id, observation.incomplete_reason);
    }

    const auto result = upsertObservedJob(toJob(observation));
    if (result.ok) {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        observed.insert(result.internal_id);
        auto& entry = absences_[result.internal_id];
        entry.generation = observation.generation;
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
        spdlog::warn("JobRegistry: observation for '{}' rejected: {}",
                     observation.selector.native_job_id, result.error);
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
    for (const auto& record : store_->records()) {
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
    for (const auto& record : store_->records()) {
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
    const auto current = store_->record(record.internal_id);
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
    const auto records = store_->records();
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
            const auto coverage = source_complete.find(record.adapter);
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
        for (const auto& observation : entry.second.observations) {
            processObservation(observation, observed);
        }
    }

    std::deque<HeartbeatHint> hints;
    drainHints(hints);
    bool needs_authoritative_pass = false;
    for (auto& hint : hints) {
        if (hint.kind == HeartbeatHint::Kind::Observation) {
            processObservation(hint.observation, observed);
        } else {
            // Exit/refresh hints are cheap by design: they ask for one
            // coalesced authoritative pass instead of doing discovery I/O on
            // the producing callback.
            needs_authoritative_pass = true;
        }
    }
    if (needs_authoritative_pass) requestRescan("process/attribute hint");

    // Automatic policy is synchronous with registration; there is no deferred
    // backlog to drain here (plan §1.1.3).

    // Exec hints never reach this pass: the source resolves them on its own
    // poll thread and registers the resulting Job synchronously.

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

    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        // Only clear the wake-up when this pass consumed everything; a hint
        // that arrived after the drain must wake the next slice.
        if (hints_.empty()) hint_wakeup_ = false;
    }
    return true;
}

bool JobRegistry::runInventoryPass(bool do_scan) {
    const bool result = runPass(do_scan, /*wait=*/true);
    return result;
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
            if ((!rescan && !hint_wakeup_) || hb_stopping_) continue;
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
        // the queued hints; retry on the next slice.
        if (!executed) hint_wakeup_ = true;
    }
}

void JobRegistry::startHeartbeat() {
    std::lock_guard<std::mutex> lock(hb_mtx_);
    if (hb_stopping_ || hb_running_) return;
    hb_running_ = true;
    heartbeat_thread_ = std::make_unique<std::thread>([this]() { heartbeatLoop(); });
}

void JobRegistry::installHeartbeatSink() {
    // The sink entries are the same bounded producers tests and out-of-band
    // callers use; adapters go through them too.
    heartbeat_sink_.on_observation = [this](const Observation& observation) {
        onDiscoveryObservation(observation);
    };
    // Event path: the source hands one resolved observation over on its own
    // poll thread and gets the registration result back. No worker, no queue,
    // and no fsync on this path.
    heartbeat_sink_.on_job = [this](const Observation& observation) {
        {
            std::lock_guard<std::mutex> lock(hb_mtx_);
            ++hb_events_received_;
        }
        return upsertObservedJob(toJob(observation));
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
        hint_wakeup_ = false;
        hb_wake_ = false;
        degraded_ = false;
        incomplete_drain_ = false;
        hb_last_error_.clear();
        hints_.clear();
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
    // Test wiring: pre-built sources, injected probes and encoder, no
    // configuration and no automatic selection.
    if (!options.test_sources.empty() || options.test_heartbeat) {
        hb_sources_ = options.test_sources;
        process_probe_ = options.test_process_probe;
        cgroup_probe_ = options.test_cgroup_probe;
        facts_encoder_ = options.test_facts_encoder;
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

    // Adapter-owned identity->facts derivation: the store never branches on
    // a scheduler (test-wired registries share the same injection).
    store_->setIdentityApplier(
        [this](Job& facts, const NativeSelector& selector, const CanonicalIdentity& identity) {
            for (const auto& adapter : adapters_)
                if (adapter->name() == identity.adapter)
                    adapter->applyIdentityToFacts(facts, selector, identity);
        });

    // Liveness probes and the adapter facts encoder.
    process_probe_ = make_proc_liveness_probe();
    cgroup_probe_ = make_cgroup_membership_probe();
    facts_encoder_ = [this](const Observation& observation) -> Job {
        for (const auto& adapter : adapters_) {
            if (adapter->name() == observation.adapter)
                return adapter->encodeFacts(observation);
        }
        return Job{};   // no adapter: facts stay empty, the store normalizes
    };

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

    // "Finish queued bounded maintenance": accepted observations are committed
    // while the store is still open, never silently discarded.
    std::deque<HeartbeatHint> hints;
    drainHints(hints);
    std::set<uint64_t> observed;
    for (auto& hint : hints) {
        if (hint.kind == HeartbeatHint::Kind::Observation)
            processObservation(hint.observation, observed);
    }
    if (!hints.empty()) {
        spdlog::info("JobRegistry: discovery stopped, {} buffered hint(s) completed",
                     hints.size());
    } else {
        spdlog::info("JobRegistry: discovery stopped");
    }
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
            const auto record = store_->record(internal_id);
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
                store_->record(internal_id)) {
                ok = false;
                std::lock_guard<std::mutex> lock(hb_mtx_);
                closing_generations_.emplace_back(internal_id, generation);
            }
        }
        if (store_->closingCount() > 0) ok = false;
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
        auto& members = record.members;
        members.erase(std::remove_if(members.begin(), members.end(),
                                     [pid](const ProcessIdentity& member) {
                                         return member.pid == pid;
                                     }),
                      members.end());
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

std::string JobRegistry::attributionCgroupPath(uint64_t internal_id) {
    // Adapter-owned: cgroupPathOf is the only place that knows where the
    // cgroup lives in the typed facts. Manual/unknown adapters have none.
    const auto record = store_->record(internal_id);
    if (!record) return {};
    for (const auto& adapter : adapters_)
        if (adapter->name() == record->adapter)
            return adapter->cgroupPathOf(record->job);
    return {};
}

bool JobRegistry::sync_job_to_kernel(const Job& job) {
    std::lock_guard<std::mutex> lock(kernel_owner_mtx_);
    if (!attribution_) return true;   // intentionally disabled: no kernel state
    if (!store_->hasActive(job.JobID)) {
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
    if (!store_->findActive([&matcher](const Job& existing) {
            return matches_job(matcher, existing); }).empty()) {
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
        if (!store_->recoverPending(recovery_error)) {
            spdlog::error("JobRegistry: cannot retire JobID {}: store recovery failed ({})",
                          jobID, recovery_error);
            return false;
        }
    }
    auto record = store_->record(jobID);
    if (!record) return false;

    // The closing fact must be durable before anything destructive starts: a
    // failed write leaves the instance active and accepted, with no drain.
    if (record->state != kStateClosing) {
        if (!store_->persistState(jobID, kStateClosing)) {
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
    const auto current = store_->record(jobID);
    if (!current) return true;   // already archived: the goal is reached
    if (current->state != kStateClosing) return false;
    const Job& closing_job = current->job;

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
    if (!store_->archive(jobID)) {
        spdlog::error("JobRegistry: durable archive failed for closing JobID {}, keeping "
                      "it pending (kernel ownership already released)", jobID);
        return false;
    }
    return true;
}

bool JobRegistry::finalizeClosingRemoval(uint64_t jobID, bool allow_recovery) {
    if (allow_recovery && store_->recoveryRequired()) {
        // Recovery can change which rows exist (an ambiguous archive may have
        // landed), so it must run before the record is read.
        std::string recovery_error;
        if (!store_->recoverPending(recovery_error)) {
            spdlog::error("JobRegistry: cannot finalize closing JobID {}: recovery failed "
                          "({})", jobID, recovery_error);
            return false;
        }
    }
    const auto record = store_->record(jobID);
    if (!record) return true;   // already archived (duplicate ack): idempotent
    if (record->state != kStateClosing) return false;
    return cleanupThenArchive(jobID);
}

size_t JobRegistry::closingJobCount() const {
    return store_ ? store_->closingCount() : 0;
}

bool JobRegistry::cleanupClosingJobs() {
    bool all_archived = true;
    if (!store_) return true;
    for (const auto& record : store_->records()) {
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
    for (const auto& record : store_->records()) {
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
    const auto matched = store_->findActive(
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
    auto record = store_->record(jobID);
    if (!record || record->state == kStateClosing) return std::nullopt;
    return record->job;
}

std::vector<Job> JobRegistry::findJob(const SubAttrMatcher& matcher) const {
    std::vector<Job> matched_jobs;
    for (const auto& record : store_->findActive(
             [&matcher](const Job& job) { return matches_job(matcher, job); })) {
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
// Kernel ownership admission for restored records
// ---------------------------------------------------------------------------

void JobRegistry::admitRestoredOwners() {
    // A record restored as active is verified-live, but this process starts
    // with an empty kernel namespace. Admit it through the same path
    // observations use, otherwise a restarted service only attributes the jobs
    // it happens to observe again.
    for (const auto& record : store_->records()) {
        if (record.internal_id == kSystemSentinelJobId) continue;
        if (record.state != kStateActive) continue;
        if (!record.execution_verified) continue;
        ensure_kernel_owner(record.job);
    }
}

// ---------------------------------------------------------------------------
// Inventory store facade
// ---------------------------------------------------------------------------

// True when an instance is live and policy-eligible but still has no durable
// intent: the initial decision never landed (write failure, crash) or the
// execution generation completed later. Derived from durable state, so it also
// holds after a restart.
bool JobRegistry::missing_initial_decision(const JobRecord& record) const
{
    if (record.state != kStateActive) return false;
    if (record.job.execution && !record.job.execution->generation.complete()) return false;
    if (!record.identity_complete) return false;
    return !store_->selection(record.internal_id).automatic.has_value();
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
    JobRecord record;
    UpsertResult result = store_->upsert(job, record);
    if (!result.ok) return result;

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
        const auto record = store_->record(internal_id);
        if (!record || record->state != kStateActive) continue;   // retired meanwhile
        auto decision = autoSelectionFor(record->job, record->identity.adapter);
        if (!decision) continue;   // no enabled policy for this adapter
        out.push_back(AutoSelection{internal_id, std::move(decision->collectors),
                                    std::move(decision->policy_revision)});
    }
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

CollectionSelection JobRegistry::selectionRecord(uint64_t jobID) const {
    return store_->selection(jobID);
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
        if (!store_->hasActive(id)) {
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
    if (!store_->mutateSelections(mutations, results, error)) {
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

        if (!store_->hasActive(selection.internal_id)) {
            ++outcome.skipped_invalid;
            outcome.message = "no verified inventory instance with internal id " +
                              std::to_string(selection.internal_id);
            target.selection = store_->selection(selection.internal_id);
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
            target.selection = store_->selection(selection.internal_id);
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
        if (!store_->mutateSelections(mutations, results, error)) {
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

bool JobRegistry::recoverStore(std::string& error) {
    return recoverStoreIfNeeded(error);
}

bool JobRegistry::recoverStoreIfNeeded(std::string& error) {
    error.clear();
    if (!store_) {
        error = "store_unavailable";
        return false;
    }
    if (!store_->recoveryRequired()) return true;
    if (!store_->recoverPending(error)) {
        if (error.empty()) error = "recovery_required";
        return false;
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
    // Callable after deinit(): the store was reset, which reads as "closed".
    if (!store_) return StoreHealth{};
    return store_->health();
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
    out = store_->records();
    return true;
}

bool JobRegistry::runtimeRecord(uint64_t internal_id,
                                std::optional<JobRecord>& out) const {
    out.reset();
    if (!store_ || !store_->ready() || store_->recoveryRequired()) return false;
    out = store_->record(internal_id);
    return true;
}

void JobRegistry::setInstanceState(uint64_t jobID, const std::string& state) {
    // In-memory only: the durable Closing transition goes through the store.
    store_->mutateRecord(jobID, [&state](JobRecord& record) { record.state = state; });
}

void JobRegistry::setExecutionVerified(uint64_t jobID, bool verified) {
    // Liveness evidence only. It never changes the lifecycle state and never
    // authorizes retirement; it gates kernel admission.
    store_->mutateRecord(jobID, [verified](JobRecord& record) {
        record.execution_verified = verified;
    });
}

void JobRegistry::setExitObserver(std::function<void(pid_t)> observer) {
    std::lock_guard<std::mutex> lock(exit_observer_mtx_);
    exit_observer_ = std::move(observer);
}

bool JobRegistry::refreshJobFacts(uint64_t jobID) {
    std::string recovery_error;
    if (!recoverStoreIfNeeded(recovery_error)) return false;
    // /proc and scheduler metadata I/O run inside `refresh` strictly outside
    // every store lock; only the result write happens under it.
    return store_->refreshFacts(jobID, [this](Job& job) { update_job(job); });
}

void JobRegistry::regRPCHandle() {
    registerInventoryRpc(*this);
    RPCServer::instance().register_method("JobRegistry/group_probe_stats",
        [](const nlohmann::json&) -> nlohmann::json {
            return {{"status", "ok"}, {"stats", GroupProbeStats::instance().to_json()}};
        });
}

void JobRegistry::onDiscoveryObservation(const Observation& observation) {
    HeartbeatHint hint;
    hint.kind = HeartbeatHint::Kind::Observation;
    hint.observation = observation;
    enqueueHint(std::move(hint));
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
    std::uint64_t events_dropped = 0;
    std::uint64_t rescan_requests = 0;
    std::uint64_t obsolete_hints = 0;
    std::uint64_t incomplete_observations = 0;
    std::uint64_t upsert_conflicts = 0;
    std::uint64_t retirements = 0;
    std::uint64_t passes = 0;
    std::size_t queue_depth = 0;
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
        events_dropped = hb_events_dropped_;
        rescan_requests = hb_rescan_requests_;
        obsolete_hints = hb_obsolete_hints_;
        incomplete_observations = hb_incomplete_observations_;
        upsert_conflicts = hb_upsert_conflicts_;
        retirements = hb_retirements_;
        passes = hb_passes_;
        queue_depth = hints_.size();
        last_scan_start_ms = last_scan_start_ms_;        last_scan_success_ms = last_scan_success_ms_;
        last_scan_complete = last_scan_complete_;
        incomplete_drain = incomplete_drain_;
        admitted = hb_admitted_;
        degraded = degraded_;
        last_error = hb_last_error_;
    }
    // Deprecated: the exec ingest worker is gone (plan P4). The field stays for
    // status compatibility and is always zero; a fake queue is not reported.
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
    out["events_dropped"] = events_dropped;
    out["queue_depth"] = queue_depth;
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
