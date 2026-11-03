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
#pragma once
#include "common/slurm_job.hpp"
#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include "common/ebpf_common.hpp"
#include "common/utils.hpp"
#include "core/job_discovery.hpp"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include "ebpf/trace_slurm_stepd.h"

// Slurm discovery adapter (doc/job_inventory_collection_decoupling_plan.md
// section 3.1). Same single construction path as CondorJobWatcher: explicit
// DiscoveryOptions only (no Config, no eBPF, no thread in the constructor),
// start(DiscoverySink) begins operation, scan() is the authoritative pass and
// observeForHint(pid) the targeted pass for exec events.
//
// Discovery inputs are slurmstepd processes and their children in the process
// table plus the optional slurmd spool, read through an injectable
// DiscoveryEnvironment so an unprivileged host can present a fake tree.
class SlurmJobWatcher final : public IDiscoverySource{
public:
    // Options only: nothing is loaded and nothing polls until start().
    explicit SlurmJobWatcher(DiscoveryOptions options);

    ~SlurmJobWatcher() override { stop(); }

    SlurmJobWatcher(const SlurmJobWatcher&) = delete;
    SlurmJobWatcher& operator=(const SlurmJobWatcher&) = delete;

    // ---- IDiscoverySource -------------------------------------------------
    const std::string& name() const override { return name_; }
    bool start(const DiscoverySink& sink) override;
    void stop() override;
    ScanResult scan() override;
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Job> observeForHint(pid_t hint_pid) override;

    // ---- Owner/test observability ------------------------------------------
    bool polling() const;
    bool thread_joinable() const;

    // Legacy spelling kept for existing callers.
    void request_rescan() { requestRescan(); }

    // Discovery inputs (paths, namespaces) may be re-resolved between passes.
    void set_discovery_environment(const DiscoveryEnvironment& environment);
    DiscoveryEnvironment discovery_environment() const;

private:
    bool start_polling();
    void poll_loop();
    // The source's own periodic scan registers its Jobs synchronously through
    // the on_job sink, exactly like exec events (no queue, no worker).
    void register_scan_jobs(const ScanResult& result);
    // Exec events only hand over the pid; building the discovered Job is the
    // consumer's job (worker thread), never the polling thread's.
    // Handled on the polling thread: resolve, offer to this turn's batch, and
    // remember the pid when the scheduler metadata is not ready yet.
    void handle_exec_event(uint32_t pid);
    // One sample ended the turn: libbpf must stop consuming so the tail batch,
    // the due re-reads and the stop check can run (plan §6.2).
    bool stop_requested();
    // Samples handled in the current poll call; the turn ends at
    // kSamplesPerTurn so the stop/scan checks always get a chance (plan §6.2).
    static constexpr std::size_t kSamplesPerTurn = 32;
    std::size_t samples_this_turn_ = 0;
    std::atomic<bool> yield_requested_{false};
    ScanResult finish_scan(ScanResult result);

    // Builds the discovered Job for one slurmstepd: manager identity from the
    // stepd, member processes from its children and the step cgroup, logical
    // identity from SLURM_* of the members.
    Job observe_stepd(const DiscoveryEnvironment& env, const std::string& boot_id,
                      pid_t stepd_pid,
                      const std::map<pid_t, std::vector<pid_t>>& children);

    // A slurmd spool entry can only corroborate presence: it proves a job
    // directory exists but carries no process or step evidence, so it is always
    // an incomplete discovery with an explicit reason.
    Job observe_spool_entry(const DiscoveryEnvironment& env,
                            const std::string& dir_name);

    bool init_event_source();
    // Idempotent counterpart of init_event_source().
    void deinit_event_source();

    std::string name_{"slurm"};
    DiscoveryOptions options_;
    std::string rb_name = "slurm_exec_events";

    // Lifecycle of the poll thread.
    mutable std::mutex life_mutex_;
    std::condition_variable poll_cv_;
    bool polling_running_{false};
    std::unique_ptr<std::thread> poll_thread_;
    DiscoverySink sink_;
    // Serializes stop(): shutdown can arrive from the service lifecycle and
    // from the destructor.
    std::mutex stop_mutex_;

    // Health and re-resolvable discovery inputs.
    mutable std::mutex state_mutex_;
    SourceHealth health_;
    std::mutex scan_mutex_;

    std::vector<bpf_link*> bpf_links_;
    bpf_object* bpf_obj_{nullptr};
    ring_buffer* bpf_rb_{nullptr};
    bool event_source_ready_{false};
    std::string event_source_note_;
};

// ---------------------------------------------------------------------------
// Inline definitions (header-only adapter, no .cpp in the monolithic build)
// ---------------------------------------------------------------------------

inline SlurmJobWatcher::SlurmJobWatcher(DiscoveryOptions options)
    : options_(std::move(options))
{
    if (!options_.enable_event_source)
        event_source_note_ =
            "eBPF event source disabled by configuration (scan-only adapter)";
}

inline bool SlurmJobWatcher::start(const DiscoverySink& sink)
{
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        sink_ = sink;
    }
    if (options_.enable_event_source && !event_source_ready_) {
        if (!init_event_source()) {
            deinit_event_source();
            spdlog::warn("SlurmJobWatcher: event source unavailable ({}); "
                         "continuing with scans and targeted hints only",
                         event_source_note_);
        }
    }
    return start_polling();
}

inline void SlurmJobWatcher::stop()
{
    std::lock_guard<std::mutex> stop_lock(stop_mutex_);
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        polling_running_ = false;
    }
    poll_cv_.notify_all();

    std::unique_ptr<std::thread> thread;
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        thread = std::move(poll_thread_);
    }
    if (thread && thread->joinable()) thread->join();

    samples_this_turn_ = 0;
    yield_requested_ = false;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        health_.polling = false;
    }
    deinit_event_source();
}

inline bool SlurmJobWatcher::polling() const
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return polling_running_;
}

inline bool SlurmJobWatcher::thread_joinable() const
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return poll_thread_ && poll_thread_->joinable();
}

inline ScanResult SlurmJobWatcher::scan()
{
    std::lock_guard<std::mutex> scan_lock(scan_mutex_);
    ScanResult result;
    result.started_at_ms = scan_clock_now(options_.clock);
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        health_.last_scan_start_ms = result.started_at_ms;
        health_.rescan_requested = false;
    }

    const auto env = discovery_environment();
    result.notes.push_back(event_source_ready_
        ? "event source: eBPF ring buffer available (scan itself uses the process table)"
        : (event_source_note_.empty() ? std::string("event source unavailable")
                                      : event_source_note_));

    std::string error;
    if (!ensure_readable_directory(env.proc_root, error)) {
        result.error = "slurm discovery: " + error;
        return finish_scan(std::move(result));
    }

    const std::string boot_id = read_boot_id(env.proc_root);
    if (boot_id.empty())
        result.notes.push_back("boot id unavailable: no process identity can be complete");

    // One process-table pass: parent map and stepd candidates together, so
    // /proc is not re-scanned once per manager.
    std::map<pid_t, std::vector<pid_t>> children;
    std::vector<pid_t> stepds;
    std::size_t unreadable_entries = 0;
    for (pid_t pid : list_proc_pids(env.proc_root)) {
        std::string cmdline;
        std::string cmdline_error;
        if (!read_proc_cmdline(env.proc_root, pid, cmdline, cmdline_error)) {
            ++unreadable_entries;
            continue;
        }
        if (cmdline.find("slurmstepd") != std::string::npos) stepds.push_back(pid);
        if (const auto ppid = read_proc_ppid(env.proc_root, pid)) children[*ppid].push_back(pid);
    }
    if (unreadable_entries > 0) {
        result.notes.push_back(fmt::format(
            "{} process entries unreadable (permission denied or exited mid-scan); "
            "they cannot be attributed to slurm", unreadable_entries));
    }
    result.counts.skipped = unreadable_entries;

    // One discovered Job per stepd/spool entry, built directly (the store is
    // the identity idempotency point, so no second merge model).
    std::set<std::string> live_native_job_ids;
    result.jobs.reserve(stepds.size());
    for (pid_t stepd : stepds) {
        auto discovered = observe_stepd(env, boot_id, stepd, children);
        if (!discovered.NativeJobID.empty())
            live_native_job_ids.insert(discovered.NativeJobID);
        result.jobs.push_back(std::move(discovered));
    }

    std::size_t spool_candidates = 0;
    if (!env.slurm_spool_dir.empty()) {
        std::string spool_error;
        if (!ensure_readable_directory(env.slurm_spool_dir, spool_error)) {
            result.error = "slurm discovery: spool root " + spool_error;
            return finish_scan(std::move(result));
        }
        std::error_code ec;
        std::vector<std::string> spool_dirs;
        for (const auto& entry : std::filesystem::directory_iterator(env.slurm_spool_dir, ec)) {
            if (ec) break;
            std::error_code type_ec;
            if (!entry.is_directory(type_ec) || type_ec) continue;
            spool_dirs.push_back(entry.path().filename().string());
        }
        if (ec) {
            result.error = "slurm discovery: cannot enumerate spool root '" +
                           env.slurm_spool_dir + "': " + ec.message();
            return finish_scan(std::move(result));
        }
        std::sort(spool_dirs.begin(), spool_dirs.end());
        for (const auto& dir_name : spool_dirs) {
            ++spool_candidates;
            auto spool_job = observe_spool_entry(env, dir_name);
            if (spool_job.NativeJobID.empty()) {
                result.counts.skipped++;
                result.notes.push_back("skipped spool entry '" + dir_name +
                                       "': not a job<id> directory");
                continue;
            }
            if (live_native_job_ids.count(spool_job.NativeJobID) > 0) continue;
            result.jobs.push_back(std::move(spool_job));
        }
    }

    result.counts.candidates = stepds.size() + spool_candidates;
    return finish_scan(std::move(result));
}

inline SourceHealth SlurmJobWatcher::health() const
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    return health_;
}

inline void SlurmJobWatcher::requestRescan()
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    health_.rescan_requested = true;
}

inline void SlurmJobWatcher::set_discovery_environment(const DiscoveryEnvironment& environment)
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    options_.discovery = environment;
}

inline DiscoveryEnvironment SlurmJobWatcher::discovery_environment() const
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    return options_.discovery;
}

inline std::optional<Job> SlurmJobWatcher::observeForHint(pid_t hint_pid)
{
    if (hint_pid <= 0) return std::nullopt;
    const auto env = discovery_environment();
    const auto ppid = read_proc_ppid(env.proc_root, hint_pid);
    if (!ppid || *ppid <= 0) return std::nullopt;
    // Real events guarantee the parent is a slurmstepd; the cmdline check
    // keeps a recycled pid from fabricating a discovery.
    std::string cmdline;
    std::string error;
    if (!read_proc_cmdline(env.proc_root, *ppid, cmdline, error) ||
        cmdline.find("slurmstepd") == std::string::npos)
        return std::nullopt;
    const std::string boot_id = read_boot_id(env.proc_root);
    // The exec'd task seeds the identity derivation; the step cgroup members
    // (merged by observe_stepd) complete the process list.
    std::map<pid_t, std::vector<pid_t>> children;
    children[*ppid] = {hint_pid};
    return observe_stepd(env, boot_id, *ppid, children);
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

inline bool SlurmJobWatcher::start_polling()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    if (poll_thread_ && poll_thread_->joinable()) return true;
    if (!event_source_ready_ && options_.scan_interval_ms == 0) {
        spdlog::info("SlurmJobWatcher: start requested without an event source "
                     "or scan interval, nothing to poll");
        return false;
    }
    polling_running_ = true;
    {
        std::lock_guard<std::mutex> state_lock(state_mutex_);
        health_.polling = true;
    }
    poll_thread_ = std::make_unique<std::thread>([this](){ poll_loop(); });
    return true;
}

inline void SlurmJobWatcher::poll_loop()
{
    auto next_scan = std::chrono::steady_clock::now();
    while (true) {
        {
            std::lock_guard<std::mutex> lock(life_mutex_);
            if (!polling_running_) return;
        }

        // Block inside libbpf's own epoll: data arrives at kernel-wakeup
        // speed — the timeout never delays an event, it only bounds how long
        // a stop() can go unnoticed and fires the self-scan deadline.
        int timeout_ms = 100;   // stop-check cadence
        if (options_.scan_interval_ms > 0) {
            const auto now = std::chrono::steady_clock::now();
            timeout_ms = now >= next_scan
                ? 0
                : std::min<int>(100,
                                static_cast<int>(std::chrono::duration_cast<
                                    std::chrono::milliseconds>(
                                        next_scan - now).count()));
        }
        samples_this_turn_ = 0;
        if (bpf_rb_) {
            const int err = ring_buffer__poll(bpf_rb_, timeout_ms);
            if (err == -ECANCELED && yield_requested_.exchange(false)) {
                // Normal turn end requested by the sample callback; the sample
                // itself was already handled before the yield.
            } else if (err == -EINTR) {
                // Plain signal wakeup; fall through to the stop/scan checks.
            } else if (err < 0) {
                spdlog::error("SlurmJobWatcher: error {}", err);
            }
        } else {
            // No event source: wait out the same timeout on the cv so stop()
            // stays immediate and the scan deadline still fires.
            std::unique_lock<std::mutex> lock(life_mutex_);
            poll_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                              [this](){ return !polling_running_; });
        }

        if (options_.scan_interval_ms > 0) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_scan) {
                register_scan_jobs(scan());
                next_scan = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(options_.scan_interval_ms);
            }
        }
    }
}

inline void SlurmJobWatcher::register_scan_jobs(const ScanResult& result)
{
    JobSink sink;
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        sink = sink_.on_job;
    }
    if (!sink) return;
    for (const auto& job : result.jobs) {
        try {
            const UpsertResult registered = sink(job);
            // A facts/metadata failure asks the maintenance pass to look
            // again; a post-commit failure is repaired from record state.
            if (!registered.ok && registered.retry_registration()) {
                requestRescan();
                return;
            }
        } catch (const std::exception& error) {
            spdlog::error("SlurmJobWatcher: registration sink failed: {}", error.what());
            requestRescan();
            return;
        }
    }
}

inline bool SlurmJobWatcher::stop_requested()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return !polling_running_;
}

inline void SlurmJobWatcher::handle_exec_event(uint32_t pid)
{
    // Runs on the polling thread. The event is only a hint: the discovered Job
    // is built from current procfs/ad evidence and registered right here, in
    // memory. Nothing on this path waits for persistence, so a burst of job
    // starts cannot block the ring.
    JobSink sink;
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        sink = sink_.on_job;
    }
    if (!sink) return;

    std::optional<Job> discovered;
    try {
        discovered = observeForHint(static_cast<pid_t>(pid));
    } catch (const std::exception& error) {
        spdlog::debug("SlurmJobWatcher: exec hint {} could not be resolved: {}", pid, error.what());
    }
    if (!discovered) return;   // not a JobLens execution, or it already exited

    if (discovery_job_ready(*discovered)) {
        UpsertResult result;
        try {
            result = sink(*discovered);
        } catch (const std::exception& error) {
            spdlog::error("SlurmJobWatcher: registration sink failed: {}", error.what());
            requestRescan();
            return;
        }
        // A facts/metadata failure asks the maintenance pass to look again; a
        // post-commit failure is repaired from record state.
        if (!result.ok && result.retry_registration()) requestRescan();
        return;
    }

    // The scheduler metadata is not written yet (the ad/spool appears a moment
    // after exec). One coalesced rescan request converges it in tens of ms
    // without any per-pid state on this thread.
    requestRescan();
}

inline ScanResult SlurmJobWatcher::finish_scan(ScanResult result)
{
    result.finished_at_ms = scan_clock_now(options_.clock);
    result.complete = result.error.empty();
    result.counts.complete = 0;
    result.counts.incomplete = 0;
    for (const auto& job : result.jobs) {
        if (discovery_job_ready(job)) ++result.counts.complete;
        else ++result.counts.incomplete;
    }

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        health_.last_note = result.notes.empty() ? std::string() : result.notes.front();
        if (result.complete) {
            health_.state = SourceState::Healthy;
            health_.last_scan_success_ms = result.finished_at_ms;
            health_.consecutive_failures = 0;
        } else {
            health_.state = SourceState::Degraded;
            health_.last_error = result.error;
            health_.consecutive_failures++;
        }
    }

    spdlog::debug("SlurmJobWatcher: scan complete={} candidates={} complete={} incomplete={} skipped={}",
                  result.complete, result.counts.candidates, result.counts.complete,
                  result.counts.incomplete, result.counts.skipped);
    return result;
}

inline Job SlurmJobWatcher::observe_stepd(const DiscoveryEnvironment& env,
                                          const std::string& boot_id,
                                          pid_t stepd_pid,
                                          const std::map<pid_t, std::vector<pid_t>>& children)
{
    // The Job is the registration unit: typed facts (filled here, where the
    // sources are parsed) plus the execution evidence group. No second
    // envelope — the registry derives selector/identity on admission.
    Job job;
    JobExecution& execution = job.execution.emplace();
    // Assigned before the first read so the store can reject an older pass
    // overwriting newer facts (plan §4.1).
    execution.read_order = next_discovery_read_order();
    std::vector<std::string> reasons;
    job.subtype_name = name_;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Slurm;
    execution.read_at_us = scan_clock_now(options_.clock) * 1000;

    const auto manager = read_process_identity(env.proc_root, boot_id, stepd_pid);
    execution.generation.boot_id = manager.boot_id;
    execution.generation.manager_pid = stepd_pid;
    execution.generation.manager_start_ticks = manager.start_ticks;

    // Typed attrs the scan fills from the members' environment; the identity
    // derivations join once the env pass below agrees.
    SlurmJobAttr attributes{};
    attributes.auto_update_child = false;
    attributes.stepd_pid = stepd_pid;

    std::vector<pid_t> member_pids;
    const auto found = children.find(stepd_pid);
    if (found != children.end()) member_pids = found->second;

    // Logical identity comes from the members' environment, exactly like
    // SlurmJob::getJobID()/getStepID().
    std::optional<std::uint64_t> job_id;
    std::optional<std::uint32_t> step_id;
    for (pid_t member_pid : member_pids) {
        std::string job_env;
        std::string env_error;
        if (!read_proc_env(env.proc_root, member_pid, "SLURM_JOB_ID", job_env, env_error)) {
            execution.notes.push_back(fmt::format(
                "child pid {} environment unreadable: {}", member_pid, env_error));
            continue;
        }
        if (job_env.empty()) continue;  // not a job process

        std::string parse_error;
        const auto candidate_job = SlurmJob::parse_job_id(job_env, parse_error);
        if (!candidate_job) {
            reasons.push_back(fmt::format("child pid {}: {}", member_pid, parse_error));
            continue;
        }
        if (job_id && *job_id != *candidate_job) {
            execution.notes.push_back(fmt::format(
                "slurmstepd {} children disagree on SLURM_JOB_ID; using {}",
                stepd_pid, *job_id));
            continue;
        }
        job_id = candidate_job;

        std::string step_env;
        if (!read_proc_env(env.proc_root, member_pid, "SLURM_STEP_ID", step_env, env_error)) {
            reasons.push_back(fmt::format("child pid {} SLURM_STEP_ID unreadable: {}",
                                          member_pid, env_error));
            continue;
        }
        std::string step_error;
        const auto candidate_step = SlurmJob::parse_step_id(step_env, step_error);
        if (!candidate_step) {
            reasons.push_back(fmt::format("child pid {}: {}", member_pid, step_error));
            continue;
        }
        if (step_id && *step_id != *candidate_step) {
            execution.notes.push_back(fmt::format(
                "slurmstepd {} children disagree on SLURM_STEP_ID; using {}",
                stepd_pid, SlurmJob::step_id_name(*step_id)));
            continue;
        }
        step_id = candidate_step;

        if (attributes.user.empty()) {
            std::string value;
            if (read_proc_env(env.proc_root, member_pid, "SLURM_JOB_USER", value, env_error) && !value.empty())
                attributes.user = value;
        }
        if (attributes.job_name.empty()) {
            std::string value;
            if (read_proc_env(env.proc_root, member_pid, "SLURM_JOB_NAME", value, env_error) && !value.empty())
                attributes.job_name = value;
        }
        if (attributes.partition.empty()) {
            std::string value;
            if (read_proc_env(env.proc_root, member_pid, "SLURM_JOB_PARTITION", value, env_error) && !value.empty())
                attributes.partition = value;
        }
        if (job.cluster_name.empty()) {
            std::string value;
            if (read_proc_env(env.proc_root, member_pid, "SLURM_CLUSTER_NAME", value, env_error))
                job.cluster_name = value;
        }
    }

    if (!job_id) {
        reasons.push_back(fmt::format(
            "slurmstepd {} has no attributable job members (SLURM_JOB_ID missing)", stepd_pid));
    } else {
        job.NativeJobID = std::to_string(*job_id);
        attributes.job_id = *job_id;
    }
    if (!step_id) {
        if (job_id) reasons.push_back(fmt::format(
            "slurmstepd {} step identity unresolved (SLURM_STEP_ID missing or unparsable)",
            stepd_pid));
    } else {
        execution.step_id = SlurmJob::step_id_name(*step_id);
        attributes.step_id = *step_id;
    }
    if (job.cluster_name.empty())
        job.cluster_name = env.cluster_name;
    if (job.cluster_name.empty())
        reasons.push_back("slurm cluster namespace unresolved (SLURM_CLUSTER_NAME unavailable)");
    attributes.cluster_name = job.cluster_name;

    // Member processes: stepd children plus step-cgroup members, so a child
    // created before registration is still attributed. The member list is
    // authoritative (members_authoritative stays true) only when the step
    // cgroup was actually readable: a failed read must never be reported as a
    // confirmed member set, an empty list included.
    std::string cgroup_path =
        read_proc_cgroup_path(env.proc_root, stepd_pid, env.cgroup_root);
    if (cgroup_path.empty() && !member_pids.empty()) {
        cgroup_path =
            read_proc_cgroup_path(env.proc_root, member_pids.front(), env.cgroup_root);
        if (!cgroup_path.empty())
            execution.notes.push_back(fmt::format(
                "stepd cgroup unknown; used member pid {} cgroup", member_pids.front()));
    }
    execution.generation.cgroup_identity = cgroup_path;
    attributes.cgroup_path = cgroup_path;

    std::vector<pid_t> effective_members = member_pids;
    std::vector<pid_t> cgroup_members;
    std::string cgroup_error;
    if (!cgroup_path.empty()) {
        if (read_cgroup_members(cgroup_path, cgroup_members, cgroup_error)) {
            for (pid_t cgroup_pid : cgroup_members) {
                if (std::find(effective_members.begin(), effective_members.end(),
                              cgroup_pid) == effective_members.end()) {
                    effective_members.push_back(cgroup_pid);
                }
            }
        } else {
            execution.members_authoritative = false;
            execution.notes.push_back("step cgroup membership unreadable: " + cgroup_error);
        }
    } else if (!member_pids.empty()) {
        // Members are known from children only; the cgroup may hold more.
        execution.members_authoritative = false;
    }
    for (pid_t member_pid : effective_members) {
        if (member_pid == stepd_pid) continue;
        const auto member = read_process_identity(env.proc_root, boot_id, member_pid);
        if (!member.complete()) execution.members_authoritative = false;
        execution.members.push_back(member);
    }

    job.sub_attr = attributes;
    finalize_discovery_job(job, std::move(reasons));
    return job;
}

inline Job SlurmJobWatcher::observe_spool_entry(const DiscoveryEnvironment& env,
                                                const std::string& dir_name)
{
    // Spool entries never become collectable instances: no process evidence,
    // no step identity. The Job carries the reason explicitly.
    Job job;
    JobExecution& execution = job.execution.emplace();
    execution.read_order = next_discovery_read_order();
    std::vector<std::string> reasons;
    job.subtype_name = name_;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Slurm;
    execution.read_at_us = scan_clock_now(options_.clock) * 1000;
    job.cluster_name = env.cluster_name;

    const bool has_prefix = dir_name.size() > 3 && dir_name.compare(0, 3, "job") == 0;
    if (has_prefix) {
        const auto id_text = dir_name.substr(3);
        std::string parse_error;
        if (const auto spool_job_id = SlurmJob::parse_job_id(id_text, parse_error))
            job.NativeJobID = std::to_string(*spool_job_id);
    }
    if (job.cluster_name.empty())
        reasons.push_back("slurm cluster namespace unresolved (SLURM_CLUSTER_NAME unavailable)");
    reasons.push_back("no live slurmstepd observed for spool directory '" + dir_name +
                      "' in '" + env.slurm_spool_dir + "'");
    execution.notes.push_back(
        "spool entry only proves the job directory exists; no step or process evidence");

    finalize_discovery_job(job, std::move(reasons));
    return job;
}

inline bool SlurmJobWatcher::init_event_source()
{
    auto path = EbpfCommon::resolve_bpf_obj("trace_slurm_stepd.bpf.o");
    if (path.empty()) {
        event_source_note_ = "eBPF object trace_slurm_stepd.bpf.o not found: "
                             "event-driven discovery disabled, scan uses process-table discovery";
        return false;
    }
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    if (!bpf_obj_) {
        event_source_note_ = "eBPF object trace_slurm_stepd.bpf.o could not be loaded "
                             "(privilege or kernel support): event-driven discovery disabled, "
                             "scan uses process-table discovery";
        return false;
    }

    ring_buffer_sample_fn fn  = [](void *ctx, void *data, size_t /*size*/){
        auto ptr = static_cast<SlurmJobWatcher*>(ctx);
        auto event_ptr = static_cast<struct slurm_event*>(data);
        spdlog::trace("SlurmJobWatcher: ebpf got exec event pid {} ppid {} comm {}",
                      event_ptr->pid, event_ptr->ppid, event_ptr->comm);
        // Cheap by contract: the pid is handed to the sink and nothing else
        // happens on this thread (no /proc reads, no policy, no storage).
        // Admission closed: do not start a new sample, end the turn so the
        // poll thread can finish and join.
        if (ptr->stop_requested()) return -ECANCELED;
        // Handle the CURRENT sample first: libbpf has already advanced the
        // consumer position, so a sample that is not taken now is lost.
        ptr->handle_exec_event(event_ptr->pid);
        // Bound the turn: the sample above is already handled, so yielding now
        // only ends libbpf's consume loop and lets the stop/scan checks run.
        ++ptr->samples_this_turn_;
        if (ptr->samples_this_turn_ >= kSamplesPerTurn || ptr->stop_requested()) {
            ptr->yield_requested_ = true;
            return -ECANCELED;
        }
        return 0;
    };

    bpf_rb_ = EbpfCommon::new_rb(bpf_obj_, rb_name, fn, static_cast<void*>(this));

    if(!bpf_rb_){
        event_source_note_ = "eBPF ring buffer for trace_slurm_stepd.bpf.o unavailable: "
                             "event-driven discovery disabled, scan uses process-table discovery";
        return false;
    }
    event_source_ready_ = true;
    return true;
}

inline void SlurmJobWatcher::deinit_event_source()
{
    EbpfCommon::free_rb(bpf_rb_);
    bpf_rb_ = nullptr;
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
    bpf_obj_ = nullptr;
    event_source_ready_ = false;
    spdlog::info("SlurmJobWatcher: deinit_ebpf");
}
