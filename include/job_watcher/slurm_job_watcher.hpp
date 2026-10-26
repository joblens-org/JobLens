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
#include <fmt/ranges.h>
#include "common/ebpf_common.hpp"
#include "common/utils.hpp"
#include "rule_engine/rules_manager.hpp"
#include "common/config.hpp"
#include "core/job_observation.hpp"
#include <algorithm>
#include <atomic>
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
// section 3.1).
//
// Construction paths, lifecycle, scan() and health() mirror CondorJobWatcher:
//
//   SlurmJobWatcher()                       legacy: Config + eBPF polling from
//                                          the constructor.
//   SlurmJobWatcher(WatcherRuntimeOptions)  discovery adapter: no polling until
//                                          start(sink).
//
// Discovery inputs are the same ones the legacy paths use - slurmstepd
// processes and their children in the process table plus the optional slurmd
// spool - read through an injectable DiscoveryEnvironment so an unprivileged
// host can present a fake tree instead of live schedulers.
class SlurmJobWatcher{
public:
    // Legacy construction: keeps the pre-Task-3 behavior.
    SlurmJobWatcher() : SlurmJobWatcher(WatcherRuntimeOptions{}, true) {}

    // Discovery-adapter construction. Never starts polling.
    explicit SlurmJobWatcher(const WatcherRuntimeOptions& options)
        : SlurmJobWatcher(options, false) {}

    ~SlurmJobWatcher(){
        stop();
    }

    SlurmJobWatcher(const SlurmJobWatcher&) = delete;
    SlurmJobWatcher& operator=(const SlurmJobWatcher&) = delete;

    // Legacy Job sink, kept for the existing JobRegistry call site.
    void register_add_if(std::function<void(Job)> add_if){
        add_if_ = add_if;
    }

    size_t add_all_slurm_job(bool use_rules = true){
        size_t added_count = 0;
        auto slurm_pids = SlurmJob::get_all_slurm_job_pids();
        for(auto pid: slurm_pids){
            auto job_opt = buildSlurmJob(pid);
            if(!job_opt) continue;

            Job job = std::move(job_opt.value());
            job.JobID = 0; // 设置为0，自动生成JobID
            job.CollectorNames = use_collectors;

            if(use_rules && !evaluate_rules(job)){
                spdlog::info("SlurmJobWatcher: job ID {} did not pass rules, skip adding", job.JobID);
                continue;
            }
            add_if_(job);
            added_count++;
        }
        spdlog::info("SlurmJobWatcher: added {} slurm jobs", added_count);
        return added_count;
    }

    // ---- Discovery adapter API ------------------------------------------

    // Installs `sink` and then starts the poll thread, so no callback can ever
    // run before the sink exists. Returns false when the adapter has nothing to
    // run (no event source and no scan interval); the sink stays installed and
    // scan() still works. The sink runs on the poll thread and must not call
    // stop() (that would self-join).
    bool start(ObservationSink sink){
        {
            std::lock_guard<std::mutex> lock(life_mutex_);
            observation_sink_ = std::move(sink);
            observation_mode_.store(true, std::memory_order_release);
        }
        return start_polling();
    }

    // Idempotent: stops and joins the poll thread, releases the eBPF source.
    void stop(){
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

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            health_.polling = false;
        }
        deinit_event_source();
    }

    bool polling() const{
        std::lock_guard<std::mutex> lock(life_mutex_);
        return polling_running_;
    }

    // Observable thread state for shutdown tests.
    bool thread_joinable() const{
        std::lock_guard<std::mutex> lock(life_mutex_);
        return poll_thread_ && poll_thread_->joinable();
    }

    // One synchronous source pass over slurmstepd processes and the optional
    // slurmd spool. Never throws on missing inputs.
    ScanResult scan(){
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

        ObservationAccumulator accumulator;
        std::set<std::string> live_native_job_ids;
        for (pid_t stepd : stepds) {
            auto observation = observe_stepd(env, boot_id, stepd, children);
            if (!observation.selector.native_job_id.empty())
                live_native_job_ids.insert(observation.selector.native_job_id);
            accumulator.add(std::move(observation));
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
                auto observation = observe_spool_entry(env, dir_name);
                if (observation.selector.native_job_id.empty()) {
                    result.counts.skipped++;
                    result.notes.push_back("skipped spool entry '" + dir_name +
                                           "': not a job<id> directory");
                    continue;
                }
                if (live_native_job_ids.count(observation.selector.native_job_id) > 0) continue;
                accumulator.add(std::move(observation));
            }
        }

        result.counts.candidates = stepds.size() + spool_candidates;
        result.observations = accumulator.take();
        return finish_scan(std::move(result));
    }

    SourceHealth health() const{
        std::lock_guard<std::mutex> lock(state_mutex_);
        return health_;
    }

    // The next scan starts immediately instead of waiting for the interval.
    void request_rescan(){
        std::lock_guard<std::mutex> lock(state_mutex_);
        health_.rescan_requested = true;
    }

    // Discovery inputs (paths, namespaces) may be re-resolved between passes.
    void set_discovery_environment(const DiscoveryEnvironment& environment){
        std::lock_guard<std::mutex> lock(state_mutex_);
        options_.discovery = environment;
    }

    DiscoveryEnvironment discovery_environment() const{
        std::lock_guard<std::mutex> lock(state_mutex_);
        return options_.discovery;
    }

private:
    SlurmJobWatcher(const WatcherRuntimeOptions& options, bool auto_start){
        initialize(options, auto_start);
    }

    void initialize(const WatcherRuntimeOptions& options, bool auto_start){
        options_ = options;
        if (options_.load_config) {
            use_collectors = Config::instance().getArray<std::string>("slurm_job_watcher", "auto_add_collectors");
            if (Config::instance().getBool("slurm_job_watcher", "use_rules", false)) {
                rules_manager_ = std::make_unique<RulesManager>(
                    "SlurmJobWatcherRules",
                    Config::instance().getString("job_registry_config", "rules_dir", Utils::JobLensRootDir() + "../config/rules/slurm_jobs"),
                    Config::instance().getString("slurm_job_watcher", "rules_prefix", "slurm_job_")
                );
                spdlog::info("SlurmJobWatcher: use rules engine to filter slurm jobs");
            }
        } else {
            use_collectors = options_.default_collectors;
            if (options_.use_rules) {
                rules_manager_ = std::make_unique<RulesManager>(
                    "SlurmJobWatcherRules", options_.rules_dir, options_.rules_prefix);
                spdlog::info("SlurmJobWatcher: use rules engine to filter slurm jobs");
            }
        }

        if (options_.enable_event_source) {
            if(!init_event_source()){
                spdlog::warn("SlurmJobWatcher: init ebpf error, deinit now");
                deinit_event_source();
            }else{
                spdlog::info("SlurmJobWatcher: inited auto add slurm job");
            }
        } else {
            event_source_note_ =
                "eBPF event source disabled by configuration (scan-only adapter)";
        }

        if (auto_start && event_source_ready_) start_polling();
    }

    bool start_polling(){
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

    void poll_loop(){
        const std::uint32_t slice_ms = options_.scan_interval_ms > 0
            ? std::min<std::uint32_t>(100, options_.scan_interval_ms) : 100;
        auto next_scan = std::chrono::steady_clock::now();
        while (true) {
            {
                std::unique_lock<std::mutex> lock(life_mutex_);
                if (!polling_running_) return;
                poll_cv_.wait_for(lock, std::chrono::milliseconds(slice_ms),
                                  [this](){ return !polling_running_; });
                if (!polling_running_) return;
            }

            if (bpf_rb_) {
                int err = ring_buffer__poll(bpf_rb_, 0);
                if (err == -EINTR){
                    spdlog::error("SlurmJobWatcher: error EINTR");
                    return;
                }
                if (err < 0){
                    spdlog::error("SlurmJobWatcher: error {}", err);
                }
            }

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

    void emit_observations(const ScanResult& result){
        ObservationSink sink;
        {
            std::lock_guard<std::mutex> lock(life_mutex_);
            sink = observation_sink_;
        }
        if (!sink) return;
        for (const auto& observation : result.observations) {
            // The sink is caller-owned code on the polling thread: a throwing
            // consumer must not take the discovery adapter down with it.
            try {
                sink(observation);
            } catch (const std::exception& error) {
                spdlog::error("SlurmJobWatcher: observation sink failed: {}", error.what());
            }
        }
    }

    ScanResult finish_scan(ScanResult result){
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

        spdlog::debug("SlurmJobWatcher: scan complete={} candidates={} complete={} incomplete={} skipped={}",
                      result.complete, result.counts.candidates, result.counts.complete,
                      result.counts.incomplete, result.counts.skipped);
        return result;
    }

    // Builds the observation for one slurmstepd: manager identity from the
    // stepd, member processes from its children and the step cgroup, logical
    // identity from SLURM_* of the members.
    Observation observe_stepd(const DiscoveryEnvironment& env, const std::string& boot_id,
                              pid_t stepd_pid,
                              const std::map<pid_t, std::vector<pid_t>>& children){
        Observation observation;
        std::vector<std::string> reasons;
        observation.scheduler_type = SchedulerType::Slurm;
        observation.selector.scheduler_type = SchedulerType::Slurm;
        observation.source_timestamp_ms = scan_clock_now(options_.clock);

        const auto manager = read_process_identity(env.proc_root, boot_id, stepd_pid);
        observation.generation.boot_id = manager.boot_id;
        observation.generation.manager_pid = stepd_pid;
        observation.generation.manager_start_ticks = manager.start_ticks;

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
                observation.notes.push_back(fmt::format(
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
                observation.notes.push_back(fmt::format(
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
                observation.notes.push_back(fmt::format(
                    "slurmstepd {} children disagree on SLURM_STEP_ID; using {}",
                    stepd_pid, SlurmJob::step_id_name(*step_id)));
                continue;
            }
            step_id = candidate_step;

            if (observation.owner.empty()) {
                std::string value;
                if (read_proc_env(env.proc_root, member_pid, "SLURM_JOB_USER", value, env_error) && !value.empty())
                    observation.owner = value;
            }
            if (observation.job_name.empty()) {
                std::string value;
                if (read_proc_env(env.proc_root, member_pid, "SLURM_JOB_NAME", value, env_error) && !value.empty())
                    observation.job_name = value;
            }
            if (observation.partition.empty()) {
                std::string value;
                if (read_proc_env(env.proc_root, member_pid, "SLURM_JOB_PARTITION", value, env_error) && !value.empty())
                    observation.partition = value;
            }
            if (observation.selector.cluster_name.empty()) {
                std::string value;
                if (read_proc_env(env.proc_root, member_pid, "SLURM_CLUSTER_NAME", value, env_error))
                    observation.selector.cluster_name = value;
            }
        }

        if (!job_id) {
            reasons.push_back(fmt::format(
                "slurmstepd {} has no attributable job members (SLURM_JOB_ID missing)", stepd_pid));
        } else {
            observation.selector.native_job_id = std::to_string(*job_id);
        }
        if (!step_id) {
            if (job_id) reasons.push_back(fmt::format(
                "slurmstepd {} step identity unresolved (SLURM_STEP_ID missing or unparsable)",
                stepd_pid));
        } else {
            observation.selector.step_id = SlurmJob::step_id_name(*step_id);
        }
        if (observation.selector.cluster_name.empty())
            observation.selector.cluster_name = env.slurm_cluster_name;
        if (observation.selector.cluster_name.empty())
            reasons.push_back("slurm cluster namespace unresolved (SLURM_CLUSTER_NAME unavailable)");

        // Member processes: stepd children plus step-cgroup members, so a child
        // created before registration is still attributed.
        observation.cgroup_path =
            read_proc_cgroup_path(env.proc_root, stepd_pid, env.cgroup_root);
        if (observation.cgroup_path.empty() && !member_pids.empty()) {
            observation.cgroup_path =
                read_proc_cgroup_path(env.proc_root, member_pids.front(), env.cgroup_root);
            if (!observation.cgroup_path.empty())
                observation.notes.push_back(fmt::format(
                    "stepd cgroup unknown; used member pid {} cgroup", member_pids.front()));
        }
        observation.generation.cgroup_identity = observation.cgroup_path;

        std::vector<pid_t> effective_members = member_pids;
        for (pid_t cgroup_pid : read_cgroup_members(observation.cgroup_path)) {
            if (std::find(effective_members.begin(), effective_members.end(), cgroup_pid) ==
                effective_members.end()) {
                effective_members.push_back(cgroup_pid);
            }
        }
        for (pid_t member_pid : effective_members) {
            if (member_pid == stepd_pid) continue;
            const auto member = read_process_identity(env.proc_root, boot_id, member_pid);
            if (!member.complete()) observation.members_complete = false;
            observation.members.push_back(member);
        }

        finalize_observation(observation, std::move(reasons));
        return observation;
    }

    // A slurmd spool entry can only corroborate presence: it proves a job
    // directory exists but carries no process or step evidence, so it is always
    // an incomplete observation with an explicit reason.
    Observation observe_spool_entry(const DiscoveryEnvironment& env,
                                    const std::string& dir_name){
        Observation observation;
        std::vector<std::string> reasons;
        observation.scheduler_type = SchedulerType::Slurm;
        observation.selector.scheduler_type = SchedulerType::Slurm;
        observation.source_timestamp_ms = scan_clock_now(options_.clock);
        observation.selector.cluster_name = env.slurm_cluster_name;

        const bool has_prefix = dir_name.size() > 3 && dir_name.compare(0, 3, "job") == 0;
        if (has_prefix) {
            const auto id_text = dir_name.substr(3);
            std::string parse_error;
            if (const auto job_id = SlurmJob::parse_job_id(id_text, parse_error))
                observation.selector.native_job_id = std::to_string(*job_id);
        }
        if (observation.selector.cluster_name.empty())
            reasons.push_back("slurm cluster namespace unresolved (SLURM_CLUSTER_NAME unavailable)");
        reasons.push_back("no live slurmstepd observed for spool directory '" + dir_name +
                          "' in '" + env.slurm_spool_dir + "'");
        observation.notes.push_back(
            "spool entry only proves the job directory exists; no step or process evidence");

        finalize_observation(observation, std::move(reasons));
        return observation;
    }

    std::optional<Job> buildSlurmJob(pid_t pid){
        auto JobID = SlurmJob::getJobID(pid);
        if(!JobID) return std::nullopt;

        Job job;
        job.JobID = JobID;
        job.JobPIDs.push_back(pid);
        job.jobtype = JobType::Job;
        job.subtype = JobSubType::Slurm;

        job.sub_attr = SlurmJobAttr{
            .auto_update_child = true,
            .stepd_pid = SlurmJob::get_ppid_of(pid),
            .job_id = JobID,
            .step_id = SlurmJob::getStepID(pid),
            .cluster_name = SlurmJob::get_cluster_name(pid),
            .cgroup_path = SlurmJob::v2_cgroup_absolute_path(pid),
            .user = SlurmJob::getUser(pid),
            .job_name = SlurmJob::getJobName(pid),
            .partition = SlurmJob::getPartition(pid)
        };
        auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
        job.cluster_name = SlurmJob::get_cluster_name(pid);
        job.clusterTag = SlurmJob::get_cluster_name(pid);
        job.NativeJobID = SlurmJob::get_native_job_id(pid);

        // 发现时探测一次组字段（失败留给 update 路径重试）
        SlurmJob::probe_groups(job);
        spdlog::debug("SlurmJobWatcher: built job from pid {}, JobID {}, step_id {}, user {}, partition {}", 
                    pid, job.JobID, slurm_attr.step_id, slurm_attr.user, slurm_attr.partition);
        return job;
    }

    void buildAndAddJob(pid_t pid, bool use_rules, std::vector<std::string> default_collectors){
        auto job_opt = buildSlurmJob(pid);
        if(!job_opt) return;

        Job job = std::move(job_opt.value());
        // 将规则返回的采集器名称添加到Job对象中
        if(!evaluate_rules(job)){
            spdlog::info("SlurmJobWatcher: job ID {} did not pass rules, skip adding", job.JobID);
            return;
        }
        if (!use_rules) {
            spdlog::info("SlurmJobWatcher: job ID {} passed without rules, adding default collectors", job.JobID);
            job.CollectorNames.insert(job.CollectorNames.end(), default_collectors.begin(), default_collectors.end());
        }
        add_if_(job);
    }

    bool init_event_source(){
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
            if (ptr->observation_mode_.load(std::memory_order_acquire)) {
                // Discovery adapter: enqueue a rescan request and keep the
                // polling thread cheap (plan 5.2). The maintenance controller
                // runs the authoritative pass and produces observations.
                ptr->request_rescan();
                return 0;
            }
            if (!ptr->add_if_) {
                spdlog::warn("SlurmJobWatcher: exec event for pid {} has no discovery sink; ignored",
                             event_ptr->pid);
                return 0;
            }
            bool use_rules = false;
            auto default_collectors = ptr->use_collectors;
            if (ptr->rules_manager_) {
                use_rules = true;
            }
            Job job;
            spdlog::debug("SlurmJobWatcher: ebpf got exec event pid {} ppid {} comm {}",
                        event_ptr->pid, event_ptr->ppid, event_ptr->comm);
            ptr->buildAndAddJob(event_ptr->pid, use_rules, default_collectors);
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

    // Idempotent counterpart of init_event_source().
    void deinit_event_source(){
        EbpfCommon::free_rb(bpf_rb_);
        bpf_rb_ = nullptr;
        EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
        bpf_obj_ = nullptr;
        event_source_ready_ = false;
        spdlog::info("SlurmJobWatcher: deinit_ebpf");
    }

    // 评估规则并返回采集器名称
    // 返回值: std::pair<bool, std::vector<std::string>>
    // 第一个元素: 是否所有规则都通过
    // 第二个元素: 所有规则添加的采集器名称列表
    std::pair<bool, std::vector<std::string>> evaluate_rules_with_collectors(const Job& job){
        if(!rules_manager_) return {true, {}};
        spdlog::debug("SlurmJobWatcher: evaluating rules for job ID {}", job.JobID);
        auto result = rules_manager_->evaluate_all<Job>(job);
        spdlog::debug("SlurmJobWatcher: job ID {} passed rules: {}, collectors: {}", 
                     job.JobID, result.first, fmt::join(result.second, ", "));
        return result;
    }

    bool evaluate_rules(Job& job){
        if(!rules_manager_) return true;
        spdlog::debug("SlurmJobWatcher: evaluating rules for job ID {}", job.JobID);
        auto [passed, collectors] = evaluate_rules_with_collectors(job);
        
        // 将规则返回的采集器名称添加到Job对象中
        if (!collectors.empty()) {
            job.CollectorNames.insert(job.CollectorNames.end(), collectors.begin(), collectors.end());
            // 去重
            std::sort(job.CollectorNames.begin(), job.CollectorNames.end());
            job.CollectorNames.erase(std::unique(job.CollectorNames.begin(), job.CollectorNames.end()), 
                                    job.CollectorNames.end());
        }
        
        spdlog::debug("SlurmJobWatcher: job ID {} passed rules: {}", job.JobID, passed);
        return passed;
    }
    
    WatcherRuntimeOptions options_;
    std::unique_ptr<RulesManager> rules_manager_;
    bool enable_auto_add_slurm_job{false};
    std::vector<std::string> use_collectors;
    std::string rb_name = "slurm_exec_events";
    std::function<void(Job)> add_if_;

    // Lifecycle of the poll thread.
    mutable std::mutex life_mutex_;
    std::condition_variable poll_cv_;
    bool polling_running_{false};
    std::unique_ptr<std::thread> poll_thread_;
    ObservationSink observation_sink_;
    // True only for the discovery-adapter construction path (start(sink) was
    // called). Read from the ring-buffer callback, so it is atomic.
    std::atomic<bool> observation_mode_{false};
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
