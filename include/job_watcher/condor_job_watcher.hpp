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
#include "common/condor_job.hpp"
#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include "common/ebpf_common.hpp"
#include "common/utils.hpp"
#include "core/job_discovery.hpp"
#include "job_watcher/registration_turn.hpp"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include "ebpf/trace_condor_starter.h"

// Condor discovery adapter (doc/job_inventory_collection_decoupling_plan.md
// section 3.1). One construction path for broad discovery and legacy auto-add
// alike:
//
//   * explicit DiscoveryOptions only: no Config access, no eBPF load and no
//     thread in the constructor;
//   * start(DiscoverySink) installs the sink before anything can call back,
//     then loads the event source on demand and starts polling;
//   * scan() is the authoritative pass, observeForHint(pid) the O(job)
//     targeted pass used for exec events.
//
// The exec-event callback only hands the pid to the sink: policy, storage and
// /proc work never run on the polling thread (plan section 5.2).
class CondorJobWatcher final : public IDiscoverySource{
public:
    // Options only: nothing is loaded and nothing polls until start().
    explicit CondorJobWatcher(DiscoveryOptions options);

    ~CondorJobWatcher() override { stop(); }

    CondorJobWatcher(const CondorJobWatcher&) = delete;
    CondorJobWatcher& operator=(const CondorJobWatcher&) = delete;

    // ---- IDiscoverySource -------------------------------------------------
    const std::string& name() const override { return name_; }
    bool start(const DiscoverySink& sink) override;
    void stop() override;
    ScanResult scan() override;
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Observation> observeForHint(pid_t hint_pid) override;

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
    void emit_observations(const ScanResult& result);
    // An exec event is handled on the polling thread: resolve the pid, offer
    // the built Job to this turn's batch, and remember the pid when the
    // scheduler metadata is not ready yet (bounded re-read, plan §6.3).
    void handle_exec_event(uint32_t pid);
    // One sample ended the turn: libbpf must stop consuming so the tail batch,
    // the due re-reads and the stop check can run (plan §6.2).
    bool stop_requested();
    // One bounded re-read pass over the pids whose deadline expired.
    void process_due_rewrites();
    void flush_turn();
    RegistrationTurn turn_;
    std::atomic<bool> yield_requested_{false};
    ScanResult finish_scan(ScanResult result);

    // Builds one observation from a condor_starter candidate using the same
    // sources the scan path reads (starter environ, _CONDOR_JOB_AD file, slot
    // cgroup), through the injectable discovery roots. `preferred_member_pid`
    // (exec-hint path) is probed for the job ad before the other members.
    Observation observe_starter(const DiscoveryEnvironment& env, const std::string& boot_id,
                                pid_t starter_pid, pid_t preferred_member_pid = 0);

    // Member pids of one starter: cgroup members when the cgroup is readable,
    // otherwise the starter's live children in /proc (the classic layout where
    // the job ad is exported to the job process instead of the starter).
    std::vector<pid_t> collect_member_pids(const DiscoveryEnvironment& env,
                                           Observation& observation,
                                           pid_t starter_pid, bool& membership_known);

    bool init_event_source();
    // Idempotent counterpart of init_event_source().
    void deinit_event_source();

    std::string name_{"condor"};
    DiscoveryOptions options_;
    std::string rb_name = "exec_events";

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

inline CondorJobWatcher::CondorJobWatcher(DiscoveryOptions options)
    : options_(std::move(options))
{
    if (!options_.enable_event_source)
        event_source_note_ =
            "eBPF event source disabled by configuration (scan-only adapter)";
}

inline bool CondorJobWatcher::start(const DiscoverySink& sink)
{
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        sink_ = sink;
    }
    turn_.configure(sink, options_.registration_batch_size);
    // Load the event source on first start: an adapter that is never started
    // costs nothing (no privileges used, no kernel objects).
    if (options_.enable_event_source && !event_source_ready_) {
        if (!init_event_source()) {
            deinit_event_source();
            spdlog::warn("CondorJobWatcher: event source unavailable ({}); "
                         "continuing with scans and targeted hints only",
                         event_source_note_);
        }
    }
    return start_polling();
}

inline void CondorJobWatcher::stop()
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

    // The turn is only touched by the (now joined) poll thread.
    turn_.clear();
    yield_requested_ = false;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        health_.polling = false;
    }
    deinit_event_source();
}

inline bool CondorJobWatcher::polling() const
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return polling_running_;
}

inline bool CondorJobWatcher::thread_joinable() const
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return poll_thread_ && poll_thread_->joinable();
}

inline ScanResult CondorJobWatcher::scan()
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
        result.error = "condor discovery: " + error;
        return finish_scan(std::move(result));
    }

    const std::string boot_id = read_boot_id(env.proc_root);
    if (boot_id.empty())
        result.notes.push_back("boot id unavailable: no process identity can be complete");

    std::size_t unreadable_entries = 0;
    std::vector<pid_t> starters;
    for (pid_t pid : list_proc_pids(env.proc_root)) {
        std::string cmdline;
        std::string cmdline_error;
        if (!read_proc_cmdline(env.proc_root, pid, cmdline, cmdline_error)) {
            ++unreadable_entries;
            continue;
        }
        if (cmdline.find("condor_starter") != std::string::npos) starters.push_back(pid);
    }
    result.counts.candidates = starters.size();
    result.counts.skipped = unreadable_entries;
    if (unreadable_entries > 0) {
        result.notes.push_back(fmt::format(
            "{} process entries unreadable (permission denied or exited mid-scan); "
            "they cannot be attributed to condor", unreadable_entries));
    }

    // One observation per manager, built directly: the store is the identity
    // idempotency point, so the accumulator (a second merge model) is gone.
    result.observations.reserve(starters.size());
    for (pid_t starter : starters) {
        result.observations.push_back(observe_starter(env, boot_id, starter));
    }
    return finish_scan(std::move(result));
}

inline SourceHealth CondorJobWatcher::health() const
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    return health_;
}

inline void CondorJobWatcher::requestRescan()
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    health_.rescan_requested = true;
}

inline void CondorJobWatcher::set_discovery_environment(const DiscoveryEnvironment& environment)
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    options_.discovery = environment;
}

inline DiscoveryEnvironment CondorJobWatcher::discovery_environment() const
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    return options_.discovery;
}

inline std::optional<Observation> CondorJobWatcher::observeForHint(pid_t hint_pid)
{
    if (hint_pid <= 0) return std::nullopt;
    const auto env = discovery_environment();
    const auto ppid = read_proc_ppid(env.proc_root, hint_pid);
    if (!ppid || *ppid <= 0) return std::nullopt;
    // The BPF filter guarantees the parent is a starter for real events; the
    // cmdline check keeps a recycled pid from fabricating an observation.
    std::string cmdline;
    std::string error;
    if (!read_proc_cmdline(env.proc_root, *ppid, cmdline, error) ||
        cmdline.find("condor_starter") == std::string::npos)
        return std::nullopt;
    const std::string boot_id = read_boot_id(env.proc_root);
    // The hinted pid is the exec'd job process: probe it for the job ad first,
    // mirroring the old exec-event watcher that read _CONDOR_JOB_AD there.
    return observe_starter(env, boot_id, *ppid, hint_pid);
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

inline bool CondorJobWatcher::start_polling()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    if (poll_thread_ && poll_thread_->joinable()) return true;
    if (!event_source_ready_ && options_.scan_interval_ms == 0) {
        spdlog::info("CondorJobWatcher: start requested without an event source "
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

inline void CondorJobWatcher::poll_loop()
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
        // A pending re-read must not wait behind a 100 ms stop-check tick.
        const auto until_due = turn_.msUntilNextDue(RegistrationTurn::Clock::now());
        if (until_due >= 0) timeout_ms = std::min<int>(timeout_ms, static_cast<int>(until_due));
        if (bpf_rb_) {
            int err = ring_buffer__poll(bpf_rb_, timeout_ms);
            if (err == -EINTR) continue;   // plain signal wakeup; re-check stop
            if (err < 0) spdlog::error("CondorJobWatcher: error {}", err);
        } else {
            // No event source: wait out the same timeout on the cv so stop()
            // stays immediate and the scan deadline still fires.
            std::unique_lock<std::mutex> lock(life_mutex_);
            poll_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                              [this](){ return !polling_running_; });
        }

        // End of turn: the tail batch is registered here (never waits for a
        // new event), then the due re-reads are serviced by this same thread.
        flush_turn();
        process_due_rewrites();

        if (options_.scan_interval_ms > 0) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_scan) {
                emit_observations(scan());
                next_scan = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(options_.scan_interval_ms);
            }
        }
    }
}

inline void CondorJobWatcher::emit_observations(const ScanResult& result)
{
    ObservationSink sink;
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        sink = sink_.on_observation;
    }
    if (!sink) return;
    for (const auto& observation : result.observations) {
        // The sink is caller-owned code on the polling thread: a throwing
        // consumer must not take the discovery adapter down with it.
        try {
            sink(observation);
        } catch (const std::exception& error) {
            spdlog::error("CondorJobWatcher: observation sink failed: {}", error.what());
        }
    }
}

inline bool CondorJobWatcher::stop_requested()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return !polling_running_;
}

inline void CondorJobWatcher::handle_exec_event(uint32_t pid)
{
    // Runs on the polling thread. The event is only a hint: the observation is
    // built from current procfs/ad evidence, and metadata that is not ready yet
    // is re-read by this same loop instead of being handed to a worker.
    const auto now = RegistrationTurn::Clock::now();
    std::optional<Observation> observation;
    try {
        observation = observeForHint(static_cast<pid_t>(pid));
    } catch (const std::exception& error) {
        spdlog::debug("CondorJobWatcher: exec hint {} could not be resolved: {}", pid,
                      error.what());
    }
    if (!observation) return;   // not a JobLens execution, or it already exited
    if (observation->identity_complete) {
        turn_.offer(*observation);
        return;
    }
    // Incomplete metadata: keep the pid for a bounded re-read. The full scan
    // still covers whatever outlives the window.
    turn_.rememberPid(static_cast<pid_t>(pid),
                      RegistrationTurn::childIdentity(*observation, static_cast<pid_t>(pid)),
                      now);
    requestRescan();
}

inline void CondorJobWatcher::process_due_rewrites()
{
    const auto now = RegistrationTurn::Clock::now();
    const auto due = turn_.take_due(now, 32);
    for (const auto& entry : due) {
        const pid_t pid = entry.pid;
        std::optional<Observation> observation;
        try {
            observation = observeForHint(pid);
        } catch (const std::exception& error) {
            spdlog::debug("CondorJobWatcher: re-read of hint {} failed: {}", pid, error.what());
        }
        if (observation && observation->identity_complete) {
            // A reused pid or a different process now must not be registered as
            // the original hint.
            if (!RegistrationTurn::sameChild(entry.child, *observation, pid)) {
                spdlog::debug("CondorJobWatcher: dropping hint {} (pid reused or execution ended)", pid);
                continue;
            }
            turn_.offer(*observation);
            continue;
        }
        // Still incomplete: keep the ORIGINAL deadline and advance the
        // attempt, so the 30 s window is never restarted.
        turn_.reschedule(pid, entry.first, entry.attempt + 1, RegistrationTurn::Clock::now());
    }
    flush_turn();   // a tail batch never waits for the next event
}

inline void CondorJobWatcher::flush_turn()
{
    turn_.flush();
}

inline ScanResult CondorJobWatcher::finish_scan(ScanResult result)
{
    result.finished_at_ms = scan_clock_now(options_.clock);
    result.complete = result.error.empty();
    result.counts.complete = 0;
    result.counts.incomplete = 0;
    for (const auto& observation : result.observations) {
        if (observation.identity_complete) ++result.counts.complete;
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

    spdlog::debug("CondorJobWatcher: scan complete={} candidates={} complete={} incomplete={} skipped={}",
                  result.complete, result.counts.candidates, result.counts.complete,
                  result.counts.incomplete, result.counts.skipped);
    return result;
}

inline std::vector<pid_t> CondorJobWatcher::collect_member_pids(
    const DiscoveryEnvironment& env, Observation& observation, pid_t starter_pid,
    bool& membership_known)
{
    membership_known = false;
    std::vector<pid_t> members;
    std::string members_error;
    if (!observation.cgroup_path.empty() &&
        read_cgroup_members(observation.cgroup_path, members, members_error)) {
        // A job-scoped cgroup contains the starter and its job processes, never
        // the daemon that spawned the starter. Nodes without per-slot cgroups
        // put the starter in the shared service cgroup, where all Condor
        // daemons (and every other job's processes) live; treating that as the
        // job's membership would attribute the daemon tree to the job.
        const auto starter_parent = read_proc_ppid(env.proc_root, starter_pid);
        const bool shared_with_daemons =
            starter_parent && *starter_parent > 0 &&
            std::find(members.begin(), members.end(), *starter_parent) != members.end();
        if (!shared_with_daemons) {
            membership_known = true;  // the read succeeded, an empty list included
            return members;
        }
        observation.notes.push_back(fmt::format(
            "cgroup {} is shared with the daemon tree (contains starter parent {}); "
            "members resolved from the starter's children",
            observation.cgroup_path, *starter_parent));
        observation.cgroup_path.clear();
        observation.generation.cgroup_identity.clear();
        members.clear();
    }

    // No readable cgroup: fall back to the starter's live children so member
    // PIDs (and the job ad that usually lives on them) survive without cgroup
    // v2. The enumeration cannot prove completeness, so consumers must treat
    // the member list as partial (members_complete == false).
    if (!observation.cgroup_path.empty())
        observation.notes.push_back("cgroup membership unreadable: " + members_error);
    for (pid_t pid : list_proc_pids(env.proc_root)) {
        if (pid == starter_pid) continue;
        const auto ppid = read_proc_ppid(env.proc_root, pid);
        if (ppid && *ppid == starter_pid) members.push_back(pid);
    }
    std::sort(members.begin(), members.end());
    if (!members.empty())
        observation.notes.push_back(fmt::format(
            "cgroup unavailable; {} member pid(s) resolved from the starter's children",
            members.size()));
    return members;
}

inline Observation CondorJobWatcher::observe_starter(const DiscoveryEnvironment& env,
                                                     const std::string& boot_id,
                                                     pid_t starter_pid,
                                                     pid_t preferred_member_pid)
{
    Observation observation;
    // Assigned before the first read so the store can reject an older pass
    // overwriting newer facts (plan §4.1).
    observation.read_order = next_discovery_read_order();
    std::vector<std::string> reasons;
    observation.adapter = name_;
    observation.selector.adapter = name_;
    observation.source_timestamp_ms = scan_clock_now(options_.clock);

    std::string cluster_name = env.cluster_name;
    if (cluster_name.empty()) cluster_name = Utils::get_condor_collector_host();
    if (cluster_name.empty())
        reasons.push_back("condor cluster namespace unresolved (COLLECTOR_HOST unavailable)");
    observation.selector.cluster_name = cluster_name;

    const auto manager = read_process_identity(env.proc_root, boot_id, starter_pid);
    observation.generation.boot_id = manager.boot_id;
    observation.generation.manager_pid = starter_pid;
    observation.generation.manager_start_ticks = manager.start_ticks;

    observation.cgroup_path =
        read_proc_cgroup_path(env.proc_root, starter_pid, env.cgroup_root);
    observation.generation.cgroup_identity = observation.cgroup_path;
    if (observation.cgroup_path.empty()) {
        observation.notes.push_back(fmt::format(
            "cgroup path unavailable for starter pid {}", starter_pid));
    }

    // Members first: the job ad usually lives on a member process, so the
    // membership must be known before the ad resolution can fall back to it.
    bool membership_known = false;
    const auto member_pids =
        collect_member_pids(env, observation, starter_pid, membership_known);
    observation.members_complete = membership_known;
    for (pid_t member_pid : member_pids) {
        if (member_pid == starter_pid) continue;
        const auto member =
            read_process_identity(env.proc_root, boot_id, member_pid);
        if (!member.complete()) observation.members_complete = false;
        observation.members.push_back(member);
    }
    if (member_pids.empty() && membership_known)
        observation.notes.push_back("cgroup " + observation.cgroup_path +
                                    " has no member processes");

    // Job ad resolution: the starter's own _CONDOR_JOB_AD first, then the job
    // child processes. HTCondor exports the ad to the job environment, and the
    // previous exec-event watcher read it from there; requiring it on the
    // starter regressed that discovery path.
    std::string ad_path;
    std::string env_error;
    bool ad_from_starter = false;
    if (read_proc_env(env.proc_root, starter_pid, "_CONDOR_JOB_AD", ad_path, env_error) &&
        !ad_path.empty()) {
        ad_from_starter = true;
    } else {
        std::vector<pid_t> ad_candidates;
        if (preferred_member_pid > 0) ad_candidates.push_back(preferred_member_pid);
        for (pid_t member_pid : member_pids) {
            if (member_pid != preferred_member_pid && member_pid != starter_pid)
                ad_candidates.push_back(member_pid);
        }
        for (pid_t candidate : ad_candidates) {
            std::string candidate_error;
            if (read_proc_env(env.proc_root, candidate, "_CONDOR_JOB_AD", ad_path,
                              candidate_error) &&
                !ad_path.empty()) {
                observation.notes.push_back(fmt::format(
                    "_CONDOR_JOB_AD read from job process {} (starter has none)", candidate));
                break;
            }
            ad_path.clear();
        }
    }

    if (ad_path.empty()) {
        if (ad_from_starter) {
            reasons.push_back(fmt::format(
                "condor job ad path unavailable for pid {}: _CONDOR_JOB_AD missing",
                starter_pid));
        } else {
            reasons.push_back(fmt::format(
                "condor job ad unavailable: _CONDOR_JOB_AD missing on starter {} and on "
                "every job process",
                starter_pid));
        }
    } else {
        std::string ad_content;
        std::string ad_error;
        if (!CondorJob::read_ad_content(ad_path, ad_content, ad_error)) {
            reasons.push_back("condor job ad unreadable: " + ad_error);
        } else {
            std::string parse_error;
            const auto ad = CondorJob::parse_ad_identity(ad_content, parse_error);
            if (!ad) {
                reasons.push_back("condor job ad unparsable: " + parse_error);
            } else {
                observation.selector.native_job_id = ad->native_job_id;
                observation.selector.cluster_tag = ad->scheduler_name;
                observation.owner = ad->owner;
                observation.scheduler_host = ad->scheduler_host;
            }
        }
    }

    finalize_observation(observation, std::move(reasons));
    return observation;
}

inline bool CondorJobWatcher::init_event_source()
{
    auto path = EbpfCommon::resolve_bpf_obj("trace_condor_starter.bpf.o");
    if (path.empty()) {
        event_source_note_ = "eBPF object trace_condor_starter.bpf.o not found: "
                             "event-driven discovery disabled, scan uses process-table discovery";
        return false;
    }
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    if (!bpf_obj_) {
        event_source_note_ = "eBPF object trace_condor_starter.bpf.o could not be loaded "
                             "(privilege or kernel support): event-driven discovery disabled, "
                             "scan uses process-table discovery";
        return false;
    }

    ring_buffer_sample_fn fn  = [](void *ctx, void *data, size_t /*size*/){
        auto ptr = static_cast<CondorJobWatcher*>(ctx);
        auto event_ptr = static_cast<struct event*>(data);
        spdlog::trace("CondorJobWatcher: ebpf got exec event pid {} ppid {} comm {}",
                      event_ptr->pid, event_ptr->ppid, event_ptr->comm);
        // The event is resolved and registered on this thread by the turn;
        // a turn-level batch only ever grows inside one poll call.
        // Admission closed: do not start a new sample, end the turn so the
        // poll thread can finish and join.
        if (ptr->stop_requested()) return -ECANCELED;
        // Handle the CURRENT sample first: libbpf has already advanced the
        // consumer position, so a sample that is not taken now is lost.
        ptr->handle_exec_event(event_ptr->pid);
        const auto now = RegistrationTurn::Clock::now();
        if (ptr->turn_.should_yield(now, ptr->stop_requested())) {
            ptr->yield_requested_ = true;
            return -ECANCELED;
        }
        return 0;
    };

    bpf_rb_ = EbpfCommon::new_rb(bpf_obj_, rb_name, fn, static_cast<void*>(this));

    if(!bpf_rb_){
        event_source_note_ = "eBPF ring buffer for trace_condor_starter.bpf.o unavailable: "
                             "event-driven discovery disabled, scan uses process-table discovery";
        return false;
    }
    event_source_ready_ = true;
    return true;
}

inline void CondorJobWatcher::deinit_event_source()
{
    EbpfCommon::free_rb(bpf_rb_);
    bpf_rb_ = nullptr;
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
    bpf_obj_ = nullptr;
    event_source_ready_ = false;
    spdlog::info("CondorJobWatcher: deinit_ebpf");
}
