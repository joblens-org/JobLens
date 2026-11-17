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
#include "common/procfs.hpp"
#include "common/utils.hpp"
#include "core/job_discovery.hpp"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include "ebpf/trace_slurm_stepd.h"

// Slurm discovery adapter. Same single construction path as CondorJobWatcher:
// explicit DiscoveryOptions only (no Config, no eBPF, no thread in the
// constructor), start(DiscoverySink) begins operation, enumerateManagers() is
// the authoritative pass driven by the registry heartbeat, observe(manager) the
// O(job) read for a manager that pass does not track yet, and observeForHint(pid)
// the targeted pass for exec events.
//
// Discovery inputs are slurmstepd processes and their children in the process
// table plus the optional slurmd spool, read through an injectable
// DiscoveryEnvironment so an unprivileged host can present a fake tree.
//
// No cgroup-subtree starter enumeration here (`starter_cgroup_subtree` is
// Condor-only): the Slurm cgroup layout nests job_X/step_Y directories under
// the root, and the process table is the stable source for slurmstepd per
// step across supported Slurm versions.
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
    // Authoritative pass: the live slurmstepd processes of this pass only,
    // without reading any step environment or member set. The pass also
    // refreshes the children index observe() uses, so one pass serves every
    // manager it reports. Owns the source health of the pass.
    ManagerSet enumerateManagers() override;
    // O(job): reads exactly this stepd's children and step cgroup. Members come
    // from the index of the pass that reported the manager, or from a targeted
    // /proc/<stepd>/task/*/children read — never a process-table pass.
    std::optional<Job> observe(const ProcessIdentity& manager) override;
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Job> observeForHint(pid_t hint_pid, pid_t hint_ppid = 0) override;

    // ---- Owner/test observability ------------------------------------------
    bool polling() const;
    bool thread_joinable() const;

    // Legacy spelling kept for existing callers.
    void request_rescan() { requestRescan(); }

    // ---- Watcher-owned discovery semantics ---------------------------------
    // Whether a Slurm discovery counts as ready, and which reasons to stamp
    // on an incomplete one, are this adapter's own rules — deliberately NOT
    // an IDiscoverySource virtual (it is not an external dependency). The
    // registry keeps deciding whether a Job may be *registered* (identity
    // resolvable + generation complete); the watcher decides when *its*
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
    // Exec events only hand over the pid; building the discovered Job is the
    // consumer's job (worker thread), never the polling thread's.
    // Handled on the polling thread: resolve, offer to this turn's batch, and
    // remember the pid when the scheduler metadata is not ready yet.
    // `ppid` comes from the event itself; the observation skips the ppid
    // read when it is positive.
    void handle_exec_event(uint32_t pid, uint32_t ppid);
    // One sample ended the turn: libbpf must stop consuming so the tail batch,
    // the due re-reads and the stop check can run.
    bool stop_requested();
    // Samples handled in the current poll call; the turn ends at
    // kSamplesPerTurn so the stop check always gets a chance.
    static constexpr std::size_t kSamplesPerTurn = 32;
    std::size_t samples_this_turn_ = 0;
    std::atomic<bool> yield_requested_{false};

    // Builds the discovered Job for one slurmstepd: manager identity from the
    // stepd, member processes from its children and the step cgroup, logical
    // identity from SLURM_* of the members.
    std::optional<Job> observe_stepd(const DiscoveryEnvironment& env, const std::string& boot_id,
                      pid_t stepd_pid,
                      const std::map<pid_t, std::vector<pid_t>>& children,
                      std::vector<std::string>* drop_notes = nullptr);

    // Direct children of one manager, read from
    // /proc/<pid>/task/<tid>/children. The targeted fallback for observe()
    // when the pass index does not cover this manager (a fresh adapter, or a
    // stepd that appeared after the last pass) — one to a few file reads
    // instead of a process-table pass.
    static std::vector<pid_t> read_manager_children(const std::string& proc_root, pid_t pid);

    bool init_event_source();
    // Idempotent counterpart of init_event_source().
    void deinit_event_source();

    std::string name_{"slurm"};
    DiscoveryOptions options_;
    std::string rb_name = "slurm_exec_events";

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
    // Guards one authoritative pass against another and against observe()'s
    // read of the children index.
    std::mutex pass_mutex_;

    // Children of every live process, captured by the last authoritative
    // enumerateManagers() pass so observe() can build one manager's Job without
    // a second process-table read. Guarded by pass_mutex_;
    // child_index_root_ stamps the proc root it was read from, so a re-resolved
    // environment invalidates it without taking pass_mutex_ there.
    std::map<pid_t, std::vector<pid_t>> child_index_;
    std::string child_index_root_;
    bool child_index_valid_ = false;

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
            "eBPF event source disabled by configuration: no exec hints, "
            "discovery runs through the registry pass";
}

// ---------------------------------------------------------------------------
// Watcher-owned discovery semantics
// ---------------------------------------------------------------------------

inline std::optional<Job> SlurmJobWatcher::finalize_discovery(
    Job job, std::vector<std::string> source_reasons, std::vector<std::string>* notes)
{
    std::vector<std::string> reasons = std::move(source_reasons);
    if (!canonicalize(selector_of(job))) {
        reasons.push_back(
            "logical identity unresolved: scheduler type, cluster namespace or native job id missing");
    }
    if (!execution_complete(job.execution)) {
        reasons.push_back(
            "execution generation unresolved: manager pid, manager start ticks or boot id missing");
    }
    if (!reasons.empty()) {
        // An incomplete discovery is not published. The registry never sees a
        // half-read job, and the reason stays with the watcher that knows it.
        std::string joined;
        for (const auto& reason : reasons) {
            if (!joined.empty()) joined += "; ";
            joined += reason;
        }
        spdlog::warn("SlurmJobWatcher: discovery dropped as incomplete: {}", joined);
        if (notes != nullptr) notes->push_back("discovery dropped as incomplete: " + joined);
        return std::nullopt;
    }
    return job;
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
                         "continuing without exec hints, the registry pass still "
                         "discovers managers",
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

inline ManagerSet SlurmJobWatcher::enumerateManagers()
{
    // The authoritative pass: serialized against itself and against observe()'s
    // read of the children index, because both enumerate from the same roots.
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
        set.error = "slurm discovery: " + error;
        finish_pass(set, notes);
        return set;
    }

    const std::string boot_id = read_boot_id(env.proc_root);
    if (boot_id.empty())
        notes.push_back("boot id unavailable: no process identity can be complete");

    // One process-table pass: stepd candidates and their children together, so
    // the following observe() calls need no process-table read at all.
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
        notes.push_back(fmt::format(
            "{} process entries unreadable (permission denied or exited mid-pass); "
            "they cannot be attributed to slurm", unreadable_entries));
    }

    // The stepds are live managers whether or not the spool source can be
    // consulted: fill them before the spool probe so a spool failure only marks
    // the pass incomplete instead of dropping the processes it found.
    set.managers.reserve(stepds.size());
    for (pid_t stepd : stepds) {
        set.managers.push_back(read_process_identity(env.proc_root, boot_id, stepd));
    }

    // Keep the index even when the spool probe below fails: these children
    // came from a complete process-table pass and are what observe() needs.
    child_index_ = std::move(children);
    child_index_root_ = env.proc_root;
    child_index_valid_ = true;

    // The spool source can only corroborate presence, but it is part of the
    // pass's coverage: an unreadable configured spool root makes the pass
    // incomplete. The entries themselves add no manager evidence, so they are
    // never listed here.
    if (!env.slurm_spool_dir.empty()) {
        std::string spool_error;
        if (!ensure_readable_directory(env.slurm_spool_dir, spool_error)) {
            set.error = "slurm discovery: spool root " + spool_error;
            finish_pass(set, notes);
            return set;
        }
        std::error_code ec;
        std::filesystem::directory_iterator probe(env.slurm_spool_dir, ec);
        if (ec) {
            set.error = "slurm discovery: cannot enumerate spool root '" +
                        env.slurm_spool_dir + "': " + ec.message();
            finish_pass(set, notes);
            return set;
        }
    }

    finish_pass(set, notes);
    return set;
}

inline std::optional<Job> SlurmJobWatcher::observe(const ProcessIdentity& manager)
{
    if (!manager.complete()) return std::nullopt;
    const auto env = discovery_environment();
    const std::string boot_id = read_boot_id(env.proc_root);
    // A reboot or a different proc root invalidates every saved identity.
    if (boot_id != manager.boot_id) return std::nullopt;

    // The pid must still be a slurmstepd: a manager identity can outlive the
    // process by a pass, and a recycled pid must never seed a Job. The triple
    // is re-verified against the built Job below.
    std::string cmdline;
    std::string cmdline_error;
    if (!read_proc_cmdline(env.proc_root, manager.pid, cmdline, cmdline_error) ||
        cmdline.find("slurmstepd") == std::string::npos) {
        return std::nullopt;
    }

    // Members come from the index of the pass that reported this manager; a
    // manager that appeared afterwards falls back to the stepd's own
    // /proc/<pid>/task/*/children, never to a process-table pass.
    std::map<pid_t, std::vector<pid_t>> children;
    bool found_in_index = false;
    {
        std::lock_guard<std::mutex> lock(pass_mutex_);
        if (child_index_valid_ && child_index_root_ == env.proc_root) {
            const auto found = child_index_.find(manager.pid);
            if (found != child_index_.end()) {
                children[manager.pid] = found->second;
                found_in_index = true;
            }
        }
    }
    if (!found_in_index)
        children[manager.pid] = read_manager_children(env.proc_root, manager.pid);

    auto job = observe_stepd(env, boot_id, manager.pid, children);
    if (!job) return std::nullopt;
    if (job->execution.manager != manager) return std::nullopt;
    return job;
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

inline std::optional<Job> SlurmJobWatcher::observeForHint(pid_t hint_pid, pid_t hint_ppid)
{
    if (hint_pid <= 0) return std::nullopt;
    const auto env = discovery_environment();
    // The event carries its own ppid (the stepd): use it directly. Events
    // from old BPF objects carry 0 and fall back to reading the hint's stat.
    pid_t ppid = hint_ppid;
    if (ppid <= 0) {
        const auto read = read_proc_ppid(env.proc_root, hint_pid);
        if (!read || *read <= 0) return std::nullopt;
        ppid = *read;
    }
    // Real events guarantee the parent is a slurmstepd; the cmdline check
    // keeps a recycled pid from fabricating a discovery.
    std::string cmdline;
    std::string error;
    if (!read_proc_cmdline(env.proc_root, ppid, cmdline, error) ||
        cmdline.find("slurmstepd") == std::string::npos)
        return std::nullopt;
    const std::string boot_id = read_boot_id(env.proc_root);
    // The exec'd task seeds the identity derivation; the step cgroup members
    // (merged by observe_stepd) complete the process list.
    std::map<pid_t, std::vector<pid_t>> children;
    children[ppid] = {hint_pid};
    return observe_stepd(env, boot_id, ppid, children);
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

inline bool SlurmJobWatcher::start_polling()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    if (poll_thread_ && poll_thread_->joinable()) return true;
    if (!event_source_ready_) {
        // Discovery itself still runs: the registry heartbeat drives
        // enumerateManagers()/observe(). Without an event source there is no
        // asynchronous work, so no thread is started.
        spdlog::info("SlurmJobWatcher: start requested without an event source, "
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

inline void SlurmJobWatcher::poll_loop()
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
            spdlog::error("SlurmJobWatcher: error {}", err);
        }
    }
}

inline bool SlurmJobWatcher::stop_requested()
{
    std::lock_guard<std::mutex> lock(life_mutex_);
    return !polling_running_;
}

inline void SlurmJobWatcher::handle_exec_event(uint32_t pid, uint32_t ppid)
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
        spdlog::debug("SlurmJobWatcher: exec hint {} could not be resolved: {}", pid, error.what());
    }
    if (!discovered) return;   // not a JobLens execution, or it already exited

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
}

inline void SlurmJobWatcher::finish_pass(ManagerSet& set,
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

    spdlog::debug("SlurmJobWatcher: authoritative pass complete={} managers={}",
                  set.complete, set.managers.size());
}

inline std::optional<Job> SlurmJobWatcher::observe_stepd(const DiscoveryEnvironment& env,
                                          const std::string& boot_id,
                                          pid_t stepd_pid,
                                          const std::map<pid_t, std::vector<pid_t>>& children,
                                          std::vector<std::string>* drop_notes)
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
    job.subtype = JobSubType::Slurm;

    const auto manager = read_process_identity(env.proc_root, boot_id, stepd_pid);
    execution.manager.boot_id = manager.boot_id;
    execution.manager.pid = stepd_pid;
    execution.manager.start_ticks = manager.start_ticks;

    // Typed attrs this observation fills from the members' environment; the
    // identity derivations join once the env pass below agrees.
    SlurmJobAttr attributes{};
    attributes.auto_update_child = false;
    attributes.stepd_pid = stepd_pid;

    std::vector<pid_t> member_pids;
    const auto found = children.find(stepd_pid);
    if (found != children.end()) member_pids = found->second;

    // Logical identity comes from the members' environment — the same keys
    // SlurmJob::parse_job_id()/parse_step_id() decode. One environ read per
    // member covers every key (the hot path used to re-read the whole file per
    // variable).
    static const std::vector<std::string> kEnvKeys = {
        "SLURM_JOB_ID", "SLURM_STEP_ID", "SLURM_JOB_USER",
        "SLURM_JOB_NAME", "SLURM_JOB_PARTITION", "SLURM_CLUSTER_NAME",
    };
    std::optional<std::uint64_t> job_id;
    std::optional<std::uint32_t> step_id;
    for (pid_t member_pid : member_pids) {
        std::map<std::string, std::string> env_values;
        std::string env_error;
        if (!read_proc_env_values(env.proc_root, member_pid, kEnvKeys,
                                  env_values, env_error)) {
            notes.push_back(fmt::format(
                "child pid {} environment unreadable: {}", member_pid, env_error));
            continue;
        }
        const auto job_it = env_values.find("SLURM_JOB_ID");
        const std::string job_env = job_it != env_values.end() ? job_it->second
                                                               : std::string{};
        if (job_env.empty()) continue;  // not a job process

        std::string parse_error;
        const auto candidate_job = SlurmJob::parse_job_id(job_env, parse_error);
        if (!candidate_job) {
            reasons.push_back(fmt::format("child pid {}: {}", member_pid, parse_error));
            continue;
        }
        if (job_id && *job_id != *candidate_job) {
            notes.push_back(fmt::format(
                "slurmstepd {} children disagree on SLURM_JOB_ID; using {}",
                stepd_pid, *job_id));
            continue;
        }
        job_id = candidate_job;

        const auto step_it = env_values.find("SLURM_STEP_ID");
        const std::string step_env = step_it != env_values.end() ? step_it->second
                                                                 : std::string{};
        std::string step_error;
        const auto candidate_step = SlurmJob::parse_step_id(step_env, step_error);
        if (!candidate_step) {
            reasons.push_back(fmt::format("child pid {}: {}", member_pid, step_error));
            continue;
        }
        if (step_id && *step_id != *candidate_step) {
            notes.push_back(fmt::format(
                "slurmstepd {} children disagree on SLURM_STEP_ID; using {}",
                stepd_pid, SlurmJob::step_id_name(*step_id)));
            continue;
        }
        step_id = candidate_step;

        if (attributes.user.empty()) {
            const auto it = env_values.find("SLURM_JOB_USER");
            if (it != env_values.end() && !it->second.empty()) attributes.user = it->second;
        }
        if (attributes.job_name.empty()) {
            const auto it = env_values.find("SLURM_JOB_NAME");
            if (it != env_values.end() && !it->second.empty()) attributes.job_name = it->second;
        }
        if (attributes.partition.empty()) {
            const auto it = env_values.find("SLURM_JOB_PARTITION");
            if (it != env_values.end() && !it->second.empty()) attributes.partition = it->second;
        }
        if (job.cluster_name.empty()) {
            const auto it = env_values.find("SLURM_CLUSTER_NAME");
            if (it != env_values.end()) job.cluster_name = it->second;
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
        attributes.step_id = *step_id;
    }
    if (job.cluster_name.empty())
        job.cluster_name = env.cluster_name;
    if (job.cluster_name.empty())
        reasons.push_back("slurm cluster namespace unresolved (SLURM_CLUSTER_NAME unavailable)");
    attributes.cluster_name = job.cluster_name;

    // Member processes: stepd children plus step-cgroup members, so a child
    // created before registration is still attributed. Only a successfully read
    // step cgroup makes the set authoritative; a failed read is never reported
    // as a confirmed member set.
    std::string cgroup_path =
        read_proc_cgroup_path(env.proc_root, stepd_pid, env.cgroup_root);
    if (cgroup_path.empty() && !member_pids.empty()) {
        cgroup_path =
            read_proc_cgroup_path(env.proc_root, member_pids.front(), env.cgroup_root);
        if (!cgroup_path.empty())
            notes.push_back(fmt::format(
                "stepd cgroup unknown; used member pid {} cgroup", member_pids.front()));
    }
    attributes.cgroup_path = cgroup_path;

    std::vector<pid_t> effective_members = member_pids;
    std::vector<pid_t> cgroup_members;
    std::string cgroup_error;
    // Members start as the stepd's children, which may be incomplete (a child
    // created after this read, or an unreadable stepd). A readable step cgroup
    // answers for the whole scope — an empty list included — and is the only
    // thing that makes the set authoritative; an unreadable cgroup leaves it
    // partial, and an incomplete member identity below clears it again.
    if (!cgroup_path.empty()) {
        if (read_cgroup_members(cgroup_path, cgroup_members, cgroup_error)) {
            execution.members_authoritative = true;
            for (pid_t cgroup_pid : cgroup_members) {
                if (std::find(effective_members.begin(), effective_members.end(),
                              cgroup_pid) == effective_members.end()) {
                    effective_members.push_back(cgroup_pid);
                }
            }
        } else {
            execution.members_authoritative = false;
            notes.push_back("step cgroup membership unreadable: " + cgroup_error);
        }
    }
    for (pid_t member_pid : effective_members) {
        if (member_pid == stepd_pid) continue;
        const auto member = read_process_identity(env.proc_root, boot_id, member_pid);
        if (!member.complete()) execution.members_authoritative = false;
        execution.members.push_back(member);
    }

    job.sub_attr = attributes;
    return finalize_discovery(std::move(job), std::move(reasons), drop_notes);
}

inline std::vector<pid_t> SlurmJobWatcher::read_manager_children(const std::string& proc_root,
                                                                 pid_t pid)
{
    // /proc/<pid>/task/<tid>/children lists the tasks a thread forked; the
    // union over the stepd's threads is its child set. A handful of small
    // files, never a process-table pass.
    std::vector<pid_t> children;
    const std::filesystem::path task_dir =
        std::filesystem::path(proc_root) / std::to_string(pid) / "task";
    std::error_code ec;
    std::filesystem::directory_iterator iterator(task_dir, ec);
    const std::filesystem::directory_iterator end;
    while (!ec && iterator != end) {
        std::ifstream file(iterator->path() / "children");
        if (file) {
            std::string line;
            std::getline(file, line);
            std::istringstream values(line);
            long value = 0;
            while (values >> value) {
                if (value > 0) children.push_back(static_cast<pid_t>(value));
            }
        }
        iterator.increment(ec);
    }
    std::sort(children.begin(), children.end());
    children.erase(std::unique(children.begin(), children.end()), children.end());
    return children;
}

inline bool SlurmJobWatcher::init_event_source()
{
    auto path = EbpfCommon::resolve_bpf_obj("trace_slurm_stepd.bpf.o");
    if (path.empty()) {
        event_source_note_ = "eBPF object trace_slurm_stepd.bpf.o not found: "
                             "exec hints disabled, discovery runs through the registry pass";
        return false;
    }
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    if (!bpf_obj_) {
        event_source_note_ = "eBPF object trace_slurm_stepd.bpf.o could not be loaded "
                             "(privilege or kernel support): exec hints disabled, discovery "
                             "runs through the registry pass";
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
        event_source_note_ = "eBPF ring buffer for trace_slurm_stepd.bpf.o unavailable: "
                             "exec hints disabled, discovery runs through the registry pass";
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
