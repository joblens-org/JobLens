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
#include "job_watcher/condor_job.hpp"
#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include "common/ebpf_common.hpp"
#include "common/procfs.hpp"
#include "common/utils.hpp"
#include "core/job_discovery.hpp"
#include <algorithm>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include "ebpf/trace_condor_starter.h"

// Condor discovery adapter. One construction path for broad discovery and
// legacy auto-add alike:
//
//   * explicit DiscoveryOptions only: no Config access, no eBPF load and no
//     thread in the constructor;
//   * start(DiscoverySink) installs the sink before anything can call back,
//     then loads the event source on demand and starts polling;
//   * enumerateManagers() is the authoritative pass driven by the registry
//     heartbeat, observe(manager) the O(job) read for a manager that pass does
//     not track yet, and observeForHint(pid) the O(job) targeted pass used for
//     exec events.
//
// The poll thread resolves each exec event into a Job and registers it
// synchronously through the sink; policy, storage and /proc work stay on that
// thread, while the heartbeat keeps enumeration, removal and reconciliation.
class CondorJobWatcher final : public IDiscoverySource{
public:
    // Options only: nothing is loaded and nothing polls until start().
    explicit CondorJobWatcher(DiscoveryOptions options,
                              std::string starter_cgroup_subtree = {});

    ~CondorJobWatcher() override { stop(); }

    CondorJobWatcher(const CondorJobWatcher&) = delete;
    CondorJobWatcher& operator=(const CondorJobWatcher&) = delete;

    // ---- IDiscoverySource -------------------------------------------------
    const std::string& name() const override { return name_; }
    bool start(const DiscoverySink& sink) override;
    void stop() override;
    // Authoritative pass: the starters of this pass only, read through the
    // tier-1/tier-2 enumeration, without reading any job ad or member set.
    // Owns the source health of the pass.
    ManagerSet enumerateManagers() override;
    // O(job): reads exactly the manager's own starter (ad, slot cgroup,
    // members). Used for managers the discovery pass does not track yet.
    std::optional<Job> observe(const ProcessIdentity& manager) override;
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Job> observeForHint(pid_t hint_pid, pid_t hint_ppid = 0) override;
    std::optional<Job> observeForHint(pid_t hint_pid, pid_t hint_ppid, bool read_groups);

    // ---- Owner/test observability ------------------------------------------
    bool polling() const;
    bool thread_joinable() const;

    // Legacy spelling kept for existing callers.
    void request_rescan() { requestRescan(); }

    // ---- Watcher-owned discovery semantics ---------------------------------
    // Whether a Condor discovery counts as ready, and which reasons to stamp
    // on an incomplete one, are this adapter's own rules — deliberately NOT
    // an IDiscoverySource virtual (it is not an external dependency). The
    // registry keeps deciding whether a Job may be *registered* (identity
    // resolvable + manager complete); the watcher decides when *its*
    // discovery is finished. Each scheduler holds its own copy so the rules
    // may diverge later.
    static std::optional<Job> finalize_discovery(Job job,
                                                 std::vector<std::string> source_reasons = {},
                                                 std::vector<std::string>* notes = nullptr);

    // Discovery inputs (paths, namespaces) may be re-resolved between passes.
    void set_discovery_environment(const DiscoveryEnvironment& environment);
    DiscoveryEnvironment discovery_environment() const;

private:
    bool start_polling();
    void poll_loop();
    // Records one authoritative pass outcome on SourceHealth (state, success
    // timestamp, failure count/error, joined notes). Called with pass_mutex_
    // held; state_mutex_ guards the fields it writes.
    void finish_pass(ManagerSet& set, const std::vector<std::string>& notes);
    // An exec event is handled on the polling thread: resolve the pid, offer
    // the built Job to this turn's batch, and remember the pid when the
    // scheduler metadata is not ready yet (bounded re-read).
    // `ppid` comes from the event itself; the observation skips the ppid
    // read when it is positive.
    void handle_exec_event(uint32_t pid, uint32_t ppid);
    // One sample ended the turn: libbpf must stop consuming so the tail batch,
    // the due re-reads and the stop check can run.
    bool stop_requested();
    // One bounded re-read pass over the pids whose deadline expired.
    // Samples handled in the current poll call; the turn ends at
    // kSamplesPerTurn so the stop check always gets a chance.
    static constexpr std::size_t kSamplesPerTurn = 32;
    std::size_t samples_this_turn_ = 0;
    std::atomic<bool> yield_requested_{false};

    // Tier-1 starter enumeration: the configured slot cgroup subtree holds one
    // directory per slot whose cgroup.procs lists that slot's processes. Only
    // those pids' cmdlines are read (O(slots x procs) instead of the whole
    // process table). On success `starters` receives the deduplicated starter
    // pids. False = subtree missing/unreadable/empty or no starter found; the
    // caller falls back to the full process-table pass.
    bool enumerate_starters_from_cgroup(const DiscoveryEnvironment& env,
                                        const std::string& subtree_root,
                                        std::vector<pid_t>& starters) const;

    // One starter enumeration pass for the authoritative manager pass: the
    // configured slot cgroup subtree first (tier 1), the full process table
    // otherwise (tier 2). `notes` receives the diagnostics for
    // SourceHealth::last_note.
    std::vector<pid_t> enumerate_starters(const DiscoveryEnvironment& env,
                                          std::vector<std::string>& notes) const;

    // Builds one discovered Job from a condor_starter candidate using the same
    // sources the manager pass reads (starter environ, _CONDOR_JOB_AD file, slot
    // cgroup), through the injectable discovery roots. `preferred_pid`
    // (exec-hint path) is probed for the job ad before the other pids.
    std::optional<Job> observe_starter(const DiscoveryEnvironment& env, const std::string& boot_id,
                        pid_t starter_pid, pid_t preferred_pid = 0,
                        std::vector<std::string>* drop_notes = nullptr, bool read_groups = true);

    // Pids of one starter: cgroup pids when the cgroup is readable,
    // otherwise the starter's live children in /proc (the classic layout where
    // the job ad is exported to the job process instead of the starter).
    // `cgroup_path` may be cleared (cgroup shared with the daemon tree); notes
    // explain whichever fallback was taken.
    std::vector<pid_t> collect_pids(const DiscoveryEnvironment& env,
                                    std::string& cgroup_path,
                                    std::vector<std::string>& notes,
                                    pid_t starter_pid, bool& membership_known);

    bool init_event_source();
    // Idempotent counterpart of init_event_source().
    void deinit_event_source();

    std::string name_{"condor"};
    DiscoveryOptions options_;
    // Relative to the cgroup mount. Empty or unreadable falls back to /proc.
    std::string starter_cgroup_subtree_;
    std::string rb_name = "exec_events";

    // Lifecycle of the poll thread.
    mutable std::mutex life_mutex_;
    bool polling_running_{false};
    std::unique_ptr<std::thread> poll_thread_;
    DiscoverySink sink_;
    // Serializes stop(): shutdown can arrive from the service lifecycle and
    // from the destructor.
    std::mutex stop_mutex_;

    // Health and re-resolvable discovery inputs.
    mutable std::mutex state_mutex_;
    SourceHealth health_;
    // Serializes the authoritative pass with itself (see enumerateManagers()).
    std::mutex pass_mutex_;

    std::vector<bpf_link*> bpf_links_;
    bpf_object* bpf_obj_{nullptr};
    ring_buffer* bpf_rb_{nullptr};
    bool event_source_ready_{false};
    std::string event_source_note_;
};

// ---------------------------------------------------------------------------
// Inline definitions (header-only adapter, no .cpp in the monolithic build)
// ---------------------------------------------------------------------------

inline CondorJobWatcher::CondorJobWatcher(DiscoveryOptions options,
                                           std::string starter_cgroup_subtree)
    : options_(std::move(options)),
      starter_cgroup_subtree_(std::move(starter_cgroup_subtree))
{
    if (!options_.enable_event_source)
        event_source_note_ =
            "eBPF event source disabled by configuration: no exec hints, "
            "discovery runs through the registry pass";
}

// ---------------------------------------------------------------------------
// Watcher-owned discovery semantics
// ---------------------------------------------------------------------------

inline std::optional<Job> CondorJobWatcher::finalize_discovery(
    Job job, std::vector<std::string> source_reasons, std::vector<std::string>* notes)
{
    std::vector<std::string> reasons = std::move(source_reasons);
    if (!canonicalize(selector_of(job))) {
        reasons.push_back(
            "logical identity unresolved: scheduler type, cluster namespace or native job id missing");
    }
    if (!execution_complete(job.execution)) {
        reasons.push_back(
            "execution manager unresolved: manager pid, manager start ticks or boot id missing");
    }
    if (!reasons.empty()) {
        // An incomplete discovery is not published. The registry never sees a
        // half-read job, and the reason stays with the watcher that knows it.
        std::string joined;
        for (const auto& reason : reasons) {
            if (!joined.empty()) joined += "; ";
            joined += reason;
        }
        spdlog::warn("CondorJobWatcher: discovery dropped as incomplete: {}", joined);
        if (notes != nullptr) notes->push_back("discovery dropped as incomplete: " + joined);
        return std::nullopt;
    }
    return job;
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
                         "continuing without exec hints, the registry pass still "
                         "discovers managers",
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

inline ManagerSet CondorJobWatcher::enumerateManagers()
{
    // The authoritative pass: one at a time, because every pass reads the same
    // roots and a concurrent one would only duplicate the work.
    std::lock_guard<std::mutex> pass_lock(pass_mutex_);
    ManagerSet set;
    // The pass owns its health record: it consumes a pending rescan request,
    // records when it started, and finish_pass() records the outcome. Without
    // this the heartbeat would see rescanRequested() forever and force a pass
    // on every tick.
    const std::int64_t started_at_ms = scan_clock_now(options_.clock);
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        health_.last_scan_start_ms = started_at_ms;
        health_.rescan_requested = false;
    }

    const auto env = discovery_environment();
    std::vector<std::string> notes;
    notes.push_back(event_source_ready_
        ? "event source: eBPF ring buffer available (the manager pass uses the process table)"
        : (event_source_note_.empty() ? std::string("event source unavailable")
                                      : event_source_note_));

    std::string error;
    if (!ensure_readable_directory(env.proc_root, error)) {
        set.error = "condor discovery: " + error;
        finish_pass(set, notes);
        return set;
    }

    const std::string boot_id = read_boot_id(env.proc_root);
    if (boot_id.empty())
        notes.push_back("boot id unavailable: no process identity can be complete");

    // Tier 1: enumerate starters from the slot cgroup subtree when one is
    // configured. Tier 2 (fallback): the full process table.
    const std::vector<pid_t> starters = enumerate_starters(env, notes);
    // No ad, no cgroup membership, no member read: identity only. A starter
    // that exited mid-pass keeps its pid with incomplete ticks/boot id; the
    // caller's observe() then reports it as unobservable.
    set.managers.reserve(starters.size());
    for (pid_t starter : starters) {
        set.managers.push_back(read_process_identity(env.proc_root, boot_id, starter));
    }
    // Coverage rule: an I/O/configuration failure is the only thing that makes
    // a pass incomplete; unreadable starters are partial entries, retried by
    // the caller's observe().
    finish_pass(set, notes);
    return set;
}

inline std::optional<Job> CondorJobWatcher::observe(const ProcessIdentity& manager)
{
    if (!manager.complete()) return std::nullopt;
    const auto env = discovery_environment();
    const std::string boot_id = read_boot_id(env.proc_root);
    // A reboot or a different proc root invalidates every saved identity.
    if (boot_id != manager.boot_id) return std::nullopt;

    // Exactly this starter: its own environ, ad and slot cgroup. No process
    // table pass, no other manager's files.
    auto job = observe_starter(env, boot_id, manager.pid, 0, nullptr);
    if (!job) return std::nullopt;
    // The starter that produced the Job must be the identity the caller asked
    // for: a recycled pid (different start ticks) is not this execution.
    if (job->execution.manager != manager) return std::nullopt;
    return job;
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

inline std::optional<Job> CondorJobWatcher::observeForHint(pid_t hint_pid, pid_t hint_ppid)
{
    return observeForHint(hint_pid, hint_ppid, true);
}

inline std::optional<Job> CondorJobWatcher::observeForHint(
    pid_t hint_pid, pid_t hint_ppid, bool read_groups)
{
    if (hint_pid <= 0) return std::nullopt;
    const auto env = discovery_environment();
    // The event carries its own ppid (the starter): use it directly. Events
    // from old BPF objects carry 0 and fall back to reading the hint's stat.
    pid_t ppid = hint_ppid;
    if (ppid <= 0) {
        const auto read = read_proc_ppid(env.proc_root, hint_pid);
        if (!read || *read <= 0) return std::nullopt;
        ppid = *read;
    }
    // The BPF filter guarantees the parent is a starter for real events; the
    // cmdline check keeps a recycled pid from fabricating a discovery.
    std::string cmdline;
    std::string error;
    if (!read_proc_cmdline(env.proc_root, ppid, cmdline, error) ||
        cmdline.find("condor_starter") == std::string::npos)
        return std::nullopt;
    const std::string boot_id = read_boot_id(env.proc_root);
    // The hinted pid is the exec'd job process: probe it for the job ad first,
    // mirroring the old exec-event watcher that read _CONDOR_JOB_AD there.
    return observe_starter(env, boot_id, ppid, hint_pid, nullptr, read_groups);
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

inline bool CondorJobWatcher::start_polling()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    if (poll_thread_ && poll_thread_->joinable()) return true;
    if (!event_source_ready_) {
        // Discovery itself still runs: the registry heartbeat drives
        // enumerateManagers()/observe(). Without an event source there is no
        // asynchronous work, so no thread is started.
        spdlog::info("CondorJobWatcher: start requested without an event source, "
                     "no poll thread (discovery runs through the registry pass)");
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
    while (true) {
        {
            std::lock_guard<std::mutex> lock(life_mutex_);
            if (!polling_running_) return;
        }

        // Block inside libbpf's own epoll: data arrives at kernel-wakeup
        // speed — the timeout never delays an event, it only bounds how long
        // a stop() can go unnoticed.
        samples_this_turn_ = 0;
        const int err = ring_buffer__poll(bpf_rb_, 100);
        if (err == -ECANCELED && yield_requested_.exchange(false)) {
            // Normal turn end requested by the sample callback; the sample
            // itself was already handled before the yield.
        } else if (err == -EINTR) {
            // Plain signal wakeup; fall through to the stop check.
        } else if (err < 0) {
            spdlog::error("CondorJobWatcher: error {}", err);
        }
    }
}

inline bool CondorJobWatcher::stop_requested()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return !polling_running_;
}

inline void CondorJobWatcher::handle_exec_event(uint32_t pid, uint32_t ppid)
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
        discovered = observeForHint(static_cast<pid_t>(pid), static_cast<pid_t>(ppid));
    } catch (const std::exception& error) {
        spdlog::debug("CondorJobWatcher: exec hint {} could not be resolved: {}", pid, error.what());
    }
    if (!discovered) return;   // not a JobLens execution, or it already exited

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
}

inline void CondorJobWatcher::finish_pass(ManagerSet& set,
                                          const std::vector<std::string>& notes)
{
    set.complete = set.error.empty();
    const std::int64_t finished_at_ms = scan_clock_now(options_.clock);

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        health_.last_note = join_source_notes(notes);
        if (set.complete) {
            health_.state = SourceState::Healthy;
            health_.last_scan_success_ms = finished_at_ms;
            health_.consecutive_failures = 0;
            health_.last_error.clear();
        } else {
            health_.state = SourceState::Degraded;
            health_.last_error = set.error;
            ++health_.consecutive_failures;
        }
    }

    spdlog::debug("CondorJobWatcher: authoritative pass complete={} managers={}",
                  set.complete, set.managers.size());
}

inline std::vector<pid_t> CondorJobWatcher::collect_pids(
    const DiscoveryEnvironment& env, std::string& cgroup_path,
    std::vector<std::string>& notes, pid_t starter_pid, bool& membership_known)
{
    membership_known = false;
    std::vector<pid_t> pids;
    std::string pids_error;
    if (!cgroup_path.empty() && read_cgroup_members(cgroup_path, pids, pids_error)) {
        // A job-scoped cgroup contains the starter and its job processes, never
        // the daemon that spawned the starter. Nodes without per-slot cgroups
        // put the starter in the shared service cgroup, where all Condor
        // daemons (and every other job's processes) live; treating that as the
        // job's membership would attribute the daemon tree to the job.
        const auto starter_parent = read_proc_ppid(env.proc_root, starter_pid);
        const bool shared_with_daemons =
            starter_parent && *starter_parent > 0 &&
            std::find(pids.begin(), pids.end(), *starter_parent) != pids.end();
        if (!shared_with_daemons) {
            membership_known = true;  // the read succeeded, an empty list included
            return pids;
        }
        notes.push_back(fmt::format(
            "cgroup {} is shared with the daemon tree (contains starter parent {}); "
            "pids resolved from the starter's children",
            cgroup_path, *starter_parent));
        cgroup_path.clear();
        pids.clear();
    }

    // No readable cgroup: fall back to the starter's own children so pid
    // identities (and the job ad that usually lives on them) survive without
    // cgroup v2. The read is per manager (/proc/<starter>/task/*/children),
    // never a process-table pass: the unified update chain refreshes one record
    // at a time, so a table pass here would be O(records x processes). The
    // enumeration cannot prove completeness, so consumers must treat the pid
    // list as partial (membership_known == false); an empty read stays empty and
    // is never widened into a table walk.
    if (!cgroup_path.empty())
        notes.push_back("cgroup membership unreadable: " + pids_error);
    pids = read_proc_children(env.proc_root, starter_pid);
    pids.erase(std::remove(pids.begin(), pids.end(), starter_pid), pids.end());
    if (!pids.empty())
        notes.push_back(fmt::format(
            "cgroup unavailable; {} pid(s) resolved from the starter's children",
            pids.size()));
    return pids;
}

inline std::vector<pid_t> CondorJobWatcher::enumerate_starters(
    const DiscoveryEnvironment& env, std::vector<std::string>& notes) const
{
    std::vector<pid_t> starters;
    // Tier 1: the slot cgroup subtree, when one is configured.
    if (!starter_cgroup_subtree_.empty()) {
        const std::string subtree_root =
            (std::filesystem::path(env.cgroup_root) / starter_cgroup_subtree_)
                .lexically_normal()
                .string();
        if (enumerate_starters_from_cgroup(env, subtree_root, starters)) {
            notes.push_back(fmt::format(
                "starters enumerated from cgroup subtree {}", subtree_root));
            return starters;
        }
        notes.push_back(fmt::format(
            "cgroup subtree unavailable/empty ({}); fell back to process-table scan",
            subtree_root));
    }

    // Tier 2: the full process table. The cmdline holds the starter name; the
    // unreadable entries are counted but never attributed.
    std::size_t skipped = 0;
    for (pid_t pid : list_proc_pids(env.proc_root)) {
        std::string cmdline;
        std::string cmdline_error;
        if (!read_proc_cmdline(env.proc_root, pid, cmdline, cmdline_error)) {
            ++skipped;
            continue;
        }
        if (cmdline.find("condor_starter") != std::string::npos)
            starters.push_back(pid);
    }
    if (skipped > 0) {
        notes.push_back(fmt::format(
            "{} process entries unreadable (permission denied or exited mid-pass); "
            "they cannot be attributed to condor", skipped));
    }
    return starters;
}

inline bool CondorJobWatcher::enumerate_starters_from_cgroup(
    const DiscoveryEnvironment& env, const std::string& subtree_root,
    std::vector<pid_t>& starters) const
{
    std::string subtree_error;
    if (!ensure_readable_directory(subtree_root, subtree_error)) return false;

    std::error_code ec;
    std::vector<std::filesystem::path> slot_directories;
    for (const auto& entry : std::filesystem::directory_iterator(subtree_root, ec)) {
        std::error_code type_ec;
        if (!entry.is_directory(type_ec) || type_ec) continue;
        slot_directories.push_back(entry.path());
    }
    if (ec || slot_directories.empty()) return false;

    // Directory order is unspecified; sort so a pass reads slots in a stable
    // order and the starter list comes out sorted like the process-table tier.
    std::sort(slot_directories.begin(), slot_directories.end());
    for (const auto& slot : slot_directories) {
        for (pid_t pid : read_cgroup_members(slot.string())) {
            std::string cmdline;
            std::string cmdline_error;
            if (!read_proc_cmdline(env.proc_root, pid, cmdline, cmdline_error)) continue;
            if (cmdline.find("condor_starter") == std::string::npos) continue;
            if (std::find(starters.begin(), starters.end(), pid) == starters.end())
                starters.push_back(pid);
        }
    }
    // A readable subtree with zero starters is not evidence that no starter
    // runs (the layout may predate per-slot cgroups): fall back.
    return !starters.empty();
}

inline std::optional<Job> CondorJobWatcher::observe_starter(const DiscoveryEnvironment& env,
                                             const std::string& boot_id,
                                             pid_t starter_pid,
                                             pid_t preferred_pid,
                                             std::vector<std::string>* drop_notes, bool read_groups)
{
    // The Job is the registration unit: typed facts (filled here, where the
    // sources are parsed) plus the execution evidence group. No second
    // envelope — the registry derives selector/identity on admission.
    Job job;
    ExecutionEvidence& execution = job.execution;
    std::vector<std::string> notes;
    // Assigned before the first read so the store can reject an older pass
    // overwriting newer facts.
    execution.read_order = next_discovery_read_order();
    std::vector<std::string> reasons;
    job.subtype_name = name_;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Condor;

    std::string clusterName = env.clusterName;
    if (clusterName.empty()) clusterName = CondorJob::collector_host();
    if (clusterName.empty())
        reasons.push_back("condor cluster namespace unresolved (COLLECTOR_HOST unavailable)");
    job.clusterName = clusterName;

    const auto manager = read_process_identity(env.proc_root, boot_id, starter_pid);
    execution.manager.boot_id = manager.boot_id;
    execution.manager.pid = starter_pid;
    execution.manager.start_ticks = manager.start_ticks;

    std::string cgroup_path =
        read_proc_cgroup_path(env.proc_root, starter_pid, env.cgroup_root);
    if (cgroup_path.empty()) {
        notes.push_back(fmt::format(
            "cgroup path unavailable for starter pid {}", starter_pid));
    }

    // Typed attrs the manager pass itself owns; the ad fields join once parsed below.
    CondorJobAttr attributes{};
    attributes.auto_update_child = false;
    attributes.starter_pid = starter_pid;
    attributes.collector_host = clusterName;

    // Pids first: the job ad usually lives on a job process, so the
    // membership must be known before the ad resolution can fall back to it.
    bool membership_known = false;
    const auto pids =
        collect_pids(env, cgroup_path, notes, starter_pid,
                     membership_known);
    // The cgroup may have been discarded as daemon-shared while collecting
    // pids; the typed attribute and the manager evidence must agree with
    // whatever survived.
    attributes.slots_cgroup_path = cgroup_path;
    execution.pid_identities_authoritative = membership_known;
    for (pid_t pid : pids) {
        if (pid == starter_pid) continue;
        const auto pid_identity =
            read_process_identity(env.proc_root, boot_id, pid);
        if (!pid_identity.complete()) execution.pid_identities_authoritative = false;
        execution.pid_identities.push_back(pid_identity);
    }
    if (pids.empty() && membership_known)
        notes.push_back("cgroup " + cgroup_path +
                                  " has no job processes");

    // Job ad resolution: the starter's own _CONDOR_JOB_AD first, then the job
    // child processes. HTCondor exports the ad to the job environment, and the
    // previous exec-event watcher read it from there; requiring it on the
    // starter regressed that discovery path.
    std::string ad_path;
    std::string env_error;
    bool ad_from_starter = false;
    pid_t ad_member = 0;
    if (read_proc_env(env.proc_root, starter_pid, "_CONDOR_JOB_AD", ad_path, env_error) &&
        !ad_path.empty()) {
        ad_from_starter = true;
    } else {
        std::vector<pid_t> ad_candidates;
        if (preferred_pid > 0) ad_candidates.push_back(preferred_pid);
        for (pid_t pid : pids) {
            if (pid != preferred_pid && pid != starter_pid)
                ad_candidates.push_back(pid);
        }
        for (pid_t candidate : ad_candidates) {
            std::string candidate_error;
            if (read_proc_env(env.proc_root, candidate, "_CONDOR_JOB_AD", ad_path,
                              candidate_error) &&
                !ad_path.empty()) {
                ad_member = candidate;
                notes.push_back(fmt::format(
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
                job.JobNativeID = ad->job_native_id;
                job.clusterTag = ad->scheduler_name;
                attributes.owner = ad->owner;
                attributes.scheduler_host = ad->scheduler_host;
                attributes.scheduler_name = ad->scheduler_name;
                // The typed pair rides the same parse as the string render;
                // without it typed selectors and the duplicate screen see a
                // zero-initialized identity (JL-04).
                attributes.cluster_id = ad->cluster_id;
                attributes.proc_id = ad->proc_id;
                if (read_groups) {
                    auto& stats = GroupProbeStats::instance();
                    const auto source = GroupProbeStats::Source::CondorAd;
                    stats.record_attempt(source);
                    const auto owner = CondorJob::file_owner_uid(ad_path);
                    const bool trusted = owner != static_cast<uid_t>(-1) &&
                        CondorJob::is_regular_nonsymlink(ad_path) &&
                        (ad_from_starter || owner == 0 ||
                         owner == CondorJob::proc_owner_uid(ad_member, env.proc_root) ||
                         owner == CondorJob::proc_owner_uid(starter_pid, env.proc_root));
                    if (trusted && CondorJob::apply_ad_groups(attributes, ad_content)) {
                        job.group_probed = true;
                        if (attributes.acct_group.empty()) stats.record_empty(source);
                        else stats.record_non_empty(source, attributes.acct_group);
                    } else {
                        job.group_probe_attempts = 1;
                        stats.record_failure(source);
                    }
                }
            }
        }
    }

    job.sub_attr = attributes;
    return finalize_discovery(std::move(job), std::move(reasons), drop_notes);
}

inline bool CondorJobWatcher::init_event_source()
{
    auto path = EbpfCommon::resolve_bpf_obj("trace_condor_starter.bpf.o");
    if (path.empty()) {
        event_source_note_ = "eBPF object trace_condor_starter.bpf.o not found: "
                             "exec hints disabled, discovery runs through the registry pass";
        return false;
    }
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    if (!bpf_obj_) {
        event_source_note_ = "eBPF object trace_condor_starter.bpf.o could not be loaded "
                             "(privilege or kernel support): exec hints disabled, discovery "
                             "runs through the registry pass";
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
        // The event carries its ppid: no /proc stat read is needed on this
        // path (old event objects without the field hand 0 and the
        // observation falls back to reading it).
        ptr->handle_exec_event(event_ptr->pid, event_ptr->ppid);
        // Bound the turn: the sample above is already handled, so yielding now
        // only ends libbpf's consume loop and lets the stop check run.
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
                             "exec hints disabled, discovery runs through the registry pass";
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
