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

    if (options.enable_inventory_maintenance) init_inventory(options);

    if (options.enable_pid_tracker) init_pid_tracker();
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

ObservedExecution JobRegistry::toExecution(const Observation& observation) const {
    ObservedExecution execution;
    execution.selector = observation.selector;
    execution.generation = observation.generation;
    execution.processes = observation.members;
    execution.members_complete = observation.members_complete;
    if (facts_encoder_) execution.facts = facts_encoder_(observation);
    return execution;
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

    const auto result = upsertObservedJob(toExecution(observation));
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

JobRegistry::ExecResolution JobRegistry::resolveExecHintOnce(pid_t pid) {
    for (const auto& [name, source] : hb_sources_) {
        std::optional<Observation> observation;
        try {
            observation = source->observeForHint(pid);
        } catch (const std::exception& error) {
            spdlog::debug("JobRegistry: source '{}' could not resolve hint {}: {}",
                          name, pid, error.what());
            continue;
        }
        if (!observation) continue;   // not this source's execution
        std::set<uint64_t> observed;
        processObservation(*observation, observed);
        return observation->identity_complete ? ExecResolution::Resolved
                                              : ExecResolution::Incomplete;
    }
    // Unresolvable hints are not errors: the pid may be gone already or belong
    // to no execution; the authoritative scan covers whatever remains.
    std::lock_guard<std::mutex> lock(hb_mtx_);
    ++hb_unresolved_hints_;
    return ExecResolution::Unresolved;
}

void JobRegistry::ingestLoop() {
    {
        std::lock_guard<std::mutex> lock(ingest_mtx_);
        ingest_worker_id_ = std::this_thread::get_id();
    }
    std::unique_lock<std::mutex> lock(ingest_mtx_);
    while (!ingest_stopping_) {
        std::int64_t wait_ms = -1;
        if (!ingest_retries_.empty()) {
            std::int64_t next_due = std::numeric_limits<std::int64_t>::max();
            for (const auto& retry : ingest_retries_)
                next_due = std::min(next_due, retry.due_ms);
            const std::int64_t now = now_epoch_ms();
            wait_ms = next_due > now ? next_due - now : 0;
            wait_ms = std::min<std::int64_t>(wait_ms, 1000);
        }
        ingest_cv_.wait_for(lock, std::chrono::milliseconds(wait_ms), [this]() {
            return ingest_stopping_ || !ingest_queue_.empty();
        });
        if (ingest_stopping_) break;

        // One batch: fresh hints plus retries whose deadline passed. Each
        // entry carries its attempt so failures ladder without re-counting.
        std::vector<ExecRetry> batch;
        for (const pid_t pid : ingest_queue_) batch.push_back(ExecRetry{pid, 0, 0});
        ingest_queue_.clear();
        const std::int64_t now = now_epoch_ms();
        std::vector<ExecRetry> pending;
        for (auto& retry : ingest_retries_) {
            if (retry.due_ms <= now) batch.push_back(retry);
            else pending.push_back(retry);
        }
        ingest_retries_.swap(pending);
        lock.unlock();

        for (const auto& item : batch) {
            if (ingest_stopping_) break;
            const auto resolution = resolveExecHintOnce(item.pid);
            if (resolution != ExecResolution::Incomplete) {
                std::lock_guard<std::mutex> guard(ingest_mtx_);
                ingest_inflight_.erase(item.pid);
                continue;
            }
            // Incomplete (scheduler state not yet written): ladder or hand
            // over to one coalesced authoritative rescan.
            const std::size_t next = static_cast<std::size_t>(item.attempt);
            bool laddered = false;
            {
                std::lock_guard<std::mutex> guard(ingest_mtx_);
                if (next < std::size(kExecRetryDelaysMs) &&
                    ingest_retries_.size() < kIngestRetryCapacity) {
                    ingest_retries_.push_back(ExecRetry{
                        item.pid, item.attempt + 1,
                        now_epoch_ms() + kExecRetryDelaysMs[next]});
                    laddered = true;
                } else {
                    ingest_inflight_.erase(item.pid);
                }
            }
            if (!laddered) requestRescan("exec hint unresolved after retries");
        }
        if (ingest_stopping_) break;
        // Re-acquire before the next wait; the break paths exit with the
        // unique_lock unlocked, which its destructor handles state-aware.
        lock.lock();
    }
}

void JobRegistry::startExecIngest() {
    std::lock_guard<std::mutex> lock(ingest_mtx_);
    if (ingest_stopping_ || (ingest_thread_ && ingest_thread_->joinable())) return;
    ingest_running_ = true;
    ingest_thread_ = std::make_unique<std::thread>([this]() { ingestLoop(); });
}

void JobRegistry::stopExecIngest() {
    std::unique_ptr<std::thread> thread;
    {
        std::lock_guard<std::mutex> lock(ingest_mtx_);
        ingest_stopping_ = true;
        if (!ingest_running_ && !(ingest_thread_ && ingest_thread_->joinable()))
            return;
        thread = std::move(ingest_thread_);
        ingest_running_ = false;
    }
    ingest_cv_.notify_all();
    if (thread && thread->joinable()) {
        const bool on_ingest_thread =
            ingest_worker_id_ != std::thread::id() &&
            ingest_worker_id_ == std::this_thread::get_id();
        if (on_ingest_thread) {
            // Same rule as the heartbeat worker: a callback from the ingest
            // thread must never self-join; report incomplete instead.
            spdlog::error("JobRegistry: exec ingest stop() called from its own "
                          "worker; shutdown reported incomplete instead of "
                          "deadlocking");
            thread.release();
        } else {
            thread->join();
        }
    }
    {
        std::lock_guard<std::mutex> lock(ingest_mtx_);
        ingest_worker_id_ = std::thread::id();
    }
}

void JobRegistry::refreshLegacyRecords() {
    for (const auto& record : store_->records()) {
        if (record.internal_id == kSystemSentinelJobId) continue;
        if (record.state == kStateClosing) continue;   // the drain owns it
        if (record.generation.complete()) continue;    // discovery owns these facts
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

        // Desired/applied reconciliation runs for every *active* instance on
        // every pass: a binding whose runtime application failed, or whose
        // application was lost to a crash, converges here without waiting for
        // an observation to change facts. A suspect execution is never
        // (re)armed from here — it must be observed again first.
        if (record.state == kStateActive)
            reconcileCollectionIntent(record.internal_id);

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
        if (absences > 0) {
            setInstanceState(record.internal_id, kStateSuspect);
        } else if (record.state == kStateSuspect) {
            setInstanceState(record.internal_id, kStateActive);
        }

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

    // Exec hints never reach the pass: the ingest worker owns them (the
    // fast path) and resolves pids through observeForHint directly.

    // Records no discovery source owns (legacy/manual registrations, or types
    // whose inventory source was removed) are refreshed here; reads never do.
    if (do_scan) refreshLegacyRecords();

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
        // A slice-bounded wait keeps stop() bounded; hints are picked up on
        // the next slice (at most wait_slice_ms) without ever spinning.
        hb_cv_.wait_for(lock, slice, [this]() { return hb_stopping_; });
        if (hb_stopping_) break;

        const bool due = std::chrono::steady_clock::now() >= next_full;
        bool rescan = false;
        if (!due && !hint_wakeup_) {
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
    heartbeat_sink_.on_exec_hint = [this](pid_t pid) {
        onDiscoveryExecHint(pid);
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
    {
        std::lock_guard<std::mutex> lock(ingest_mtx_);
        ingest_stopping_ = false;
        ingest_queue_.clear();
        ingest_inflight_.clear();
        ingest_retries_.clear();
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
    startExecIngest();

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

    // Exactly one adapter per configured scheduler type, built through the
    // self-registration registry; the heartbeat drives them through
    // IDiscoverySource and owns no adapter details.
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
    for (const auto& [type, settings] : config.settings) {
        if (!settings.auto_collect) continue;
        const std::string adapter = adapter_name_of(type);
        if (settings.use_rules)
            auto_policies_[adapter] =
                AutoCollectorPolicy::rules(settings.rules_dir, settings.rules_prefix);
        else
            auto_policies_[adapter] =
                AutoCollectorPolicy::defaults(settings.default_collectors);
    }

    hb_sources_.clear();
    for (const auto& adapter : adapters_) {
        hb_sources_.emplace_back(adapter->name(), &adapter->source());
    }

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
    // The ingest worker produces upserts: stop it before the heartbeat so a
    // final pass never races a targeted resolution.
    stopExecIngest();
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


void JobRegistry::apply_auto_policy(const Job& job, const std::string& adapter)
{
    const auto policy = auto_policies_.find(adapter);
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

bool JobRegistry::deinit()
{
    if (deinit_done_) return deinit_ok_;

    // Shutdown order: discovery admission stopped first (the service calls
    // stopInventoryDiscovery() before draining the scheduler), then the
    // retirements are finalized, then attribution, then the store.
    stopInventoryDiscovery();

    bool ok = true;
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
            if (!acknowledgeJobRemoval(internal_id) && store_->record(internal_id)) {
                ok = false;
                std::lock_guard<std::mutex> lock(hb_mtx_);
                closing_generations_.emplace_back(internal_id, generation);
            }
        }
        if (store_->closingCount() > 0) ok = false;
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
            apply_auto_policy(record.job, record.identity.adapter);
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
            apply_auto_policy(record.job, record.identity.adapter);
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
    // Stamp the runtime-reported holders onto the stored projection (in-memory
    // only: collection status never survives a restart as true — the replay
    // re-attaches and the next application re-stamps it).
    store_->mutateRecord(internal_id, [&applied](JobRecord& record) {
        record.job.collection.in_pipeline = !applied.collectors.empty();
        record.job.collection.collectors = applied.collectors;
    });
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

void JobRegistry::onDiscoveryObservation(const Observation& observation) {
    HeartbeatHint hint;
    hint.kind = HeartbeatHint::Kind::Observation;
    hint.observation = observation;
    enqueueHint(std::move(hint));
}

void JobRegistry::onDiscoveryExecHint(pid_t pid) {
    if (pid <= 0) return;
    {
        std::lock_guard<std::mutex> lock(hb_mtx_);
        ++hb_events_received_;
        if (!hb_admitted_ || hb_stopping_) {
            ++hb_events_dropped_;
            return;
        }
    }
    // The ingest worker resolves the pid immediately (the fast path); the
    // dedup keeps one entry per pid while it is queued or retrying.
    bool queued = false;
    {
        std::lock_guard<std::mutex> lock(ingest_mtx_);
        if (!ingest_stopping_ && ingest_inflight_.insert(pid).second) {
            if (ingest_queue_.size() < kIngestQueueCapacity) {
                ingest_queue_.push_back(pid);
                queued = true;
            } else {
                ingest_inflight_.erase(pid);
            }
        }
    }
    if (queued) {
        ingest_cv_.notify_one();
        return;
    }
    // Duplicate or overflowing hint: counted, never blocking the producer.
    std::lock_guard<std::mutex> lock(hb_mtx_);
    ++hb_events_dropped_;
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
    {
        std::lock_guard<std::mutex> lock(ingest_mtx_);
        exec_ingest_depth = ingest_queue_.size() + ingest_retries_.size();
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
    std::size_t quarantined_count = 0;
    for (const auto& record : records) {
        if (record.internal_id == 0) continue;   // system sentinel is never inventory
        if (!record.identity_complete) ++incomplete_count;
        if (record.quarantined) ++quarantined_count;
        const bool suspect = record.state == kStateSuspect ||
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
    out["rescan_requests"] = rescan_requests;
    out["obsolete_hints"] = obsolete_hints;
    out["unresolved_hints"] = hb_unresolved_hints_;
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
