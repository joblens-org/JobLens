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
    // An exec event is handled on the polling thread: resolve the pid, offer
    // the built Job to this turn's batch, and remember the pid when the
    // scheduler metadata is not ready yet (bounded re-read, plan §6.3).
    void handle_exec_event(uint32_t pid);
    // One sample ended the turn: libbpf must stop consuming so the tail batch,
    // the due re-reads and the stop check can run (plan §6.2).
    bool stop_requested();
    // One bounded re-read pass over the pids whose deadline expired.
    // Samples handled in the current poll call; the turn ends at
    // kSamplesPerTurn so the stop/scan checks always get a chance (plan §6.2).
    static constexpr std::size_t kSamplesPerTurn = 32;
    std::size_t samples_this_turn_ = 0;
    std::atomic<bool> yield_requested_{false};
    ScanResult finish_scan(ScanResult result);

    // Builds one discovered Job from a condor_starter candidate using the same
    // sources the scan path reads (starter environ, _CONDOR_JOB_AD file, slot
    // cgroup), through the injectable discovery roots. `preferred_member_pid`
    // (exec-hint path) is probed for the job ad before the other members.
    Job observe_starter(const DiscoveryEnvironment& env, const std::string& boot_id,
                        pid_t starter_pid, pid_t preferred_member_pid = 0);

    // Member pids of one starter: cgroup members when the cgroup is readable,
    // otherwise the starter's live children in /proc (the classic layout where
    // the job ad is exported to the job process instead of the starter).
    // `cgroup_path` may be cleared (cgroup shared with the daemon tree); notes
    // explain whichever fallback was taken.
    std::vector<pid_t> collect_member_pids(const DiscoveryEnvironment& env,
                                           std::string& cgroup_path,
                                           std::vector<std::string>& notes,
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

    // Only touched by the (now joined) poll thread.
    samples_this_turn_ = 0;
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

    // One discovered Job per manager, built directly: the store is the identity
    // idempotency point, so the accumulator (a second merge model) is gone.
    result.jobs.reserve(starters.size());
    for (pid_t starter : starters) {
        result.jobs.push_back(observe_starter(env, boot_id, starter));
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

inline std::optional<Job> CondorJobWatcher::observeForHint(pid_t hint_pid)
{
    if (hint_pid <= 0) return std::nullopt;
    const auto env = discovery_environment();
    const auto ppid = read_proc_ppid(env.proc_root, hint_pid);
    if (!ppid || *ppid <= 0) return std::nullopt;
    // The BPF filter guarantees the parent is a starter for real events; the
    // cmdline check keeps a recycled pid from fabricating a discovery.
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
        samples_this_turn_ = 0;
        samples_this_turn_ = 0;
        if (bpf_rb_) {
            const int err = ring_buffer__poll(bpf_rb_, timeout_ms);
            if (err == -ECANCELED && yield_requested_.exchange(false)) {
                // Normal turn end requested by the sample callback; the sample
                // itself was already handled before the yield.
            } else if (err == -EINTR) {
                // Plain signal wakeup; fall through to the stop/scan checks.
            } else if (err < 0) {
                spdlog::error("CondorJobWatcher: error {}", err);
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

inline void CondorJobWatcher::register_scan_jobs(const ScanResult& result)
{
    JobSink sink;
    {
        std::lock_guard<std::mutex> lock(life_mutex_);
        sink = sink_.on_job;
    }
    if (!sink) return;
    for (const auto& job : result.jobs) {
        // The sink is caller-owned code on the polling thread: a throwing
        // consumer must not take the discovery adapter down with it.
        try {
            const UpsertResult registered = sink(job);
            // A facts/metadata failure asks the maintenance pass to look
            // again; a post-commit failure is repaired from record state.
            if (!registered.ok && registered.retry_registration()) {
                requestRescan();
                return;
            }
        } catch (const std::exception& error) {
            spdlog::error("CondorJobWatcher: registration sink failed: {}", error.what());
            requestRescan();
            return;
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
        spdlog::debug("CondorJobWatcher: exec hint {} could not be resolved: {}", pid, error.what());
    }
    if (!discovered) return;   // not a JobLens execution, or it already exited

    if (discovery_job_ready(*discovered)) {
        UpsertResult result;
        try {
            result = sink(*discovered);
        } catch (const std::exception& error) {
            spdlog::error("CondorJobWatcher: registration sink failed: {}", error.what());
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

inline ScanResult CondorJobWatcher::finish_scan(ScanResult result)
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

    spdlog::debug("CondorJobWatcher: scan complete={} candidates={} complete={} incomplete={} skipped={}",
                  result.complete, result.counts.candidates, result.counts.complete,
                  result.counts.incomplete, result.counts.skipped);
    return result;
}

inline std::vector<pid_t> CondorJobWatcher::collect_member_pids(
    const DiscoveryEnvironment& env, std::string& cgroup_path,
    std::vector<std::string>& notes, pid_t starter_pid, bool& membership_known)
{
    membership_known = false;
    std::vector<pid_t> members;
    std::string members_error;
    if (!cgroup_path.empty() && read_cgroup_members(cgroup_path, members, members_error)) {
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
        notes.push_back(fmt::format(
            "cgroup {} is shared with the daemon tree (contains starter parent {}); "
            "members resolved from the starter's children",
            cgroup_path, *starter_parent));
        cgroup_path.clear();
        members.clear();
    }

    // No readable cgroup: fall back to the starter's live children so member
    // PIDs (and the job ad that usually lives on them) survive without cgroup
    // v2. The enumeration cannot prove completeness, so consumers must treat
    // the member list as partial (members_complete == false).
    if (!cgroup_path.empty())
        notes.push_back("cgroup membership unreadable: " + members_error);
    for (pid_t pid : list_proc_pids(env.proc_root)) {
        if (pid == starter_pid) continue;
        const auto ppid = read_proc_ppid(env.proc_root, pid);
        if (ppid && *ppid == starter_pid) members.push_back(pid);
    }
    std::sort(members.begin(), members.end());
    if (!members.empty())
        notes.push_back(fmt::format(
            "cgroup unavailable; {} member pid(s) resolved from the starter's children",
            members.size()));
    return members;
}

inline Job CondorJobWatcher::observe_starter(const DiscoveryEnvironment& env,
                                             const std::string& boot_id,
                                             pid_t starter_pid,
                                             pid_t preferred_member_pid)
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
    job.subtype = JobSubType::Condor;
    execution.read_at_us = scan_clock_now(options_.clock) * 1000;

    std::string cluster_name = env.cluster_name;
    if (cluster_name.empty()) cluster_name = Utils::get_condor_collector_host();
    if (cluster_name.empty())
        reasons.push_back("condor cluster namespace unresolved (COLLECTOR_HOST unavailable)");
    job.cluster_name = cluster_name;

    const auto manager = read_process_identity(env.proc_root, boot_id, starter_pid);
    execution.generation.boot_id = manager.boot_id;
    execution.generation.manager_pid = starter_pid;
    execution.generation.manager_start_ticks = manager.start_ticks;

    std::string cgroup_path =
        read_proc_cgroup_path(env.proc_root, starter_pid, env.cgroup_root);
    if (cgroup_path.empty()) {
        execution.notes.push_back(fmt::format(
            "cgroup path unavailable for starter pid {}", starter_pid));
    }

    // Typed attrs the scan itself owns; the ad fields join once parsed below.
    CondorJobAttr attributes{};
    attributes.auto_update_child = false;
    attributes.starter_pid = starter_pid;
    attributes.collector_host = cluster_name;

    // Members first: the job ad usually lives on a member process, so the
    // membership must be known before the ad resolution can fall back to it.
    bool membership_known = false;
    const auto member_pids =
        collect_member_pids(env, cgroup_path, execution.notes, starter_pid,
                            membership_known);
    // The cgroup may have been discarded as daemon-shared while collecting
    // members; the typed attribute and the generation evidence must agree with
    // whatever survived.
    execution.generation.cgroup_identity = cgroup_path;
    attributes.slots_cgroup_path = cgroup_path;
    execution.members_authoritative = membership_known;
    for (pid_t member_pid : member_pids) {
        if (member_pid == starter_pid) continue;
        const auto member =
            read_process_identity(env.proc_root, boot_id, member_pid);
        if (!member.complete()) execution.members_authoritative = false;
        execution.members.push_back(member);
    }
    if (member_pids.empty() && membership_known)
        execution.notes.push_back("cgroup " + cgroup_path +
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
                execution.notes.push_back(fmt::format(
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
                job.NativeJobID = ad->native_job_id;
                job.clusterTag = ad->scheduler_name;
                attributes.owner = ad->owner;
                attributes.scheduler_host = ad->scheduler_host;
                attributes.scheduler_name = ad->scheduler_name;
            }
        }
    }

    job.sub_attr = attributes;
    finalize_discovery_job(job, std::move(reasons));
    return job;
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
