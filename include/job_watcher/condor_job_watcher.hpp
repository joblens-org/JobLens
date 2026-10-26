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
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include "ebpf/trace_condor_starter.h"

// Condor discovery adapter (doc/job_inventory_collection_decoupling_plan.md
// section 3.1).
//
// Two construction paths coexist:
//
//   CondorJobWatcher()                      legacy: reads Config and starts eBPF
//                                           polling from the constructor, exactly
//                                           as before the decoupling.
//   CondorJobWatcher(WatcherRuntimeOptions) discovery adapter: no polling until
//                                           start(sink) is called; the caller
//                                           owns config resolution.
//
// start(sink) installs the observation sink before the poll thread exists, so
// "start before callback installation" is unrepresentable in the API. stop()
// joins the thread and is safe to call repeatedly and around start().
class CondorJobWatcher{
public:
    // Legacy construction: keeps the pre-Task-3 behavior (Config-driven,
    // event polling started immediately).
    CondorJobWatcher() : CondorJobWatcher(WatcherRuntimeOptions{}, true) {}

    // Discovery-adapter construction. Never starts polling.
    explicit CondorJobWatcher(const WatcherRuntimeOptions& options)
        : CondorJobWatcher(options, false) {}

    ~CondorJobWatcher(){
        stop();
    }

    CondorJobWatcher(const CondorJobWatcher&) = delete;
    CondorJobWatcher& operator=(const CondorJobWatcher&) = delete;

    // Legacy Job sink, kept for the existing JobRegistry call site.
    void register_add_if(std::function<void(Job)> add_if){
        add_if_ = add_if;
    }

    size_t add_all_condor_job(bool use_rules = true){
        size_t added_count = 0;
        auto condor_pids = CondorJob::get_all_condor_job_pids();
        for(auto pid: condor_pids){
            auto job_opt = buildCondorJob(pid);
            if(!job_opt) continue;

            Job job = std::move(job_opt.value());
            job.JobID = 0; // 设置为0，自动生成JobID
            job.CollectorNames = use_collectors;

            if(use_rules && !evaluate_rules(job)){
                spdlog::info("CondorJobWatcher: job ID {} did not pass rules, skip adding", job.JobID);
                continue;
            }
            add_if_(job);
            added_count++;
        }
        spdlog::info("CondorJobWatcher: added {} condor jobs", added_count);
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

    // Idempotent: stops and joins the poll thread, releases the eBPF event
    // source. Safe to call twice and after a failed start().
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

    // One synchronous source pass. Never throws on missing inputs: unreadable
    // entries become incomplete observations or scan errors.
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

        ObservationAccumulator accumulator;
        for (pid_t starter : starters) {
            accumulator.add(observe_starter(env, boot_id, starter));
        }
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
    CondorJobWatcher(const WatcherRuntimeOptions& options, bool auto_start){
        initialize(options, auto_start);
    }

    void initialize(const WatcherRuntimeOptions& options, bool auto_start){
        options_ = options;
        if (options_.load_config) {
            use_collectors = Config::instance().getArray<std::string>("condor_job_watcher", "auto_add_collectors");
            if (Config::instance().getBool("condor_job_watcher", "use_rules", false)) {
                rules_manager_ = std::make_unique<RulesManager>(
                    "CondorJobWatcherRules",
                    Config::instance().getString("job_registry_config", "rules_dir", Utils::JobLensRootDir() + "../config/rules/condor_jobs"),
                    Config::instance().getString("condor_job_watcher", "rules_prefix", "condor_job_")
                );
                spdlog::info("CondorJobWatcher: use rules engine to filter condor jobs");
            }
        } else {
            use_collectors = options_.default_collectors;
            if (options_.use_rules) {
                rules_manager_ = std::make_unique<RulesManager>(
                    "CondorJobWatcherRules", options_.rules_dir, options_.rules_prefix);
                spdlog::info("CondorJobWatcher: use rules engine to filter condor jobs");
            }
        }

        if (options_.enable_event_source) {
            if(!init_event_source()){
                spdlog::warn("CondorJobWatcher: init ebpf error, deinit now");
                deinit_event_source();
            }else{
                spdlog::info("CondorJobWatcher: inited auto add condor job");
            }
        } else {
            event_source_note_ =
                "eBPF event source disabled by configuration (scan-only adapter)";
        }

        if (auto_start && event_source_ready_) start_polling();
    }

    // Callback installation precedes thread startup in start(), so this may
    // only be reached with the sink already in place (or from the legacy
    // constructor, which keeps its historic sink-less behavior).
    bool start_polling(){
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
                    spdlog::error("CondorJobWatcher: error EINTR");
                    return;
                }
                if (err < 0){
                    spdlog::error("CondorJobWatcher: error {}", err);
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
                spdlog::error("CondorJobWatcher: observation sink failed: {}", error.what());
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

        spdlog::debug("CondorJobWatcher: scan complete={} candidates={} complete={} incomplete={} skipped={}",
                      result.complete, result.counts.candidates, result.counts.complete,
                      result.counts.incomplete, result.counts.skipped);
        return result;
    }

    // Builds one observation from a condor_starter candidate using the same
    // sources the legacy buildCondorJob() reads (starter environ, _CONDOR_JOB_AD
    // file, slot cgroup), but through the injectable discovery roots.
    Observation observe_starter(const DiscoveryEnvironment& env, const std::string& boot_id,
                               pid_t starter_pid){
        Observation observation;
        std::vector<std::string> reasons;
        observation.scheduler_type = SchedulerType::Condor;
        observation.selector.scheduler_type = SchedulerType::Condor;
        observation.source_timestamp_ms = scan_clock_now(options_.clock);

        std::string cluster_name = env.condor_cluster_name;
        if (cluster_name.empty()) cluster_name = Utils::get_condor_collector_host();
        if (cluster_name.empty())
            reasons.push_back("condor cluster namespace unresolved (COLLECTOR_HOST unavailable)");
        observation.selector.cluster_name = cluster_name;

        std::string ad_path;
        std::string env_error;
        if (!read_proc_env(env.proc_root, starter_pid, "_CONDOR_JOB_AD", ad_path, env_error)) {
            reasons.push_back(fmt::format("condor job ad path unreadable for pid {}: {}",
                                          starter_pid, env_error));
        } else if (ad_path.empty()) {
            reasons.push_back(fmt::format(
                "condor job ad path unavailable for pid {}: _CONDOR_JOB_AD missing", starter_pid));
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
        } else {
            const auto cgroup_pids = read_cgroup_members(observation.cgroup_path);
            if (cgroup_pids.empty())
                observation.notes.push_back("cgroup " + observation.cgroup_path +
                                            " has no member processes");
            for (pid_t member_pid : cgroup_pids) {
                if (member_pid == starter_pid) continue;
                const auto member =
                    read_process_identity(env.proc_root, boot_id, member_pid);
                if (!member.complete()) observation.members_complete = false;
                observation.members.push_back(member);
            }
        }

        finalize_observation(observation, std::move(reasons));
        return observation;
    }

    std::optional<Job> buildCondorJob(pid_t pid){
        auto JobID = CondorJob::getJobID(pid);
        if(!JobID) return std::nullopt;

        Job job;
        job.JobID = JobID;
        job.JobPIDs.push_back(pid);
        job.jobtype = JobType::Job;
        job.subtype = JobSubType::Condor;

        auto [cluster_id, proc_id] = CondorJob::get_cluster_and_proc_id(pid);
        job.sub_attr = CondorJobAttr{
            .auto_update_child = true,
            .starter_pid = CondorJob::get_ppid_of(pid),
            .cluster_id = cluster_id,
            .proc_id = proc_id,
            .scheduler_host = CondorJob::get_scheduler_host(pid),
            .scheduler_name = CondorJob::get_scheduler_name(pid),
            .slots_cgroup_path = CondorJob::v2_cgroup_absolute_path(pid),
            .owner = CondorJob::getOwner(pid),
            .job_ad_path = CondorJob::get_env_field(pid, "_CONDOR_JOB_AD"),
            .collector_host = Utils::get_condor_collector_host()
        };
        auto& condor_attr = std::get<CondorJobAttr>(job.sub_attr);
        job.cluster_name = Utils::get_condor_collector_host();
        job.clusterTag = condor_attr.scheduler_name;
        job.NativeJobID = fmt::format("{}.{}", condor_attr.cluster_id, condor_attr.proc_id);

        // 发现时探测一次组字段（starter_pid/JobPIDs 已就绪；失败留给 update 路径重试）
        CondorJob::probe_groups(job);

        spdlog::debug("CondorJobWatcher: built job from pid {}, JobID {}, cluster_id {}, proc_id {}, scheduler_host {}, owner {}", 
                    pid, job.JobID, condor_attr.cluster_id, condor_attr.proc_id, condor_attr.scheduler_host, condor_attr.owner);
        return job;
    }

    void buildAndAddJob(pid_t pid, bool use_rules, std::vector<std::string> default_collectors){
        auto job_opt = buildCondorJob(pid);
        if(!job_opt) return;

        Job job = std::move(job_opt.value());
        // 将规则返回的采集器名称添加到Job对象中
        if(!evaluate_rules(job)){
            spdlog::info("CondorJobWatcher: job ID {} did not pass rules, skip adding", job.JobID);
            return;
        }
        if (!use_rules) {
            spdlog::info("CondorJobWatcher: job ID {} passed without rules, adding default collectors", job.JobID);
            job.CollectorNames.insert(job.CollectorNames.end(), default_collectors.begin(), default_collectors.end());
        }
        add_if_(job);
    }

    bool init_event_source(){
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
            if (ptr->observation_mode_.load(std::memory_order_acquire)) {
                // Discovery adapter: ring-buffer callbacks must stay cheap (plan
                // 5.2). Ask for an authoritative pass instead of building a Job,
                // evaluating rules or touching storage on the polling thread.
                ptr->request_rescan();
                return 0;
            }
            if (!ptr->add_if_) {
                spdlog::warn("CondorJobWatcher: exec event for pid {} has no discovery sink; ignored",
                             event_ptr->pid);
                return 0;
            }
            bool use_rules = false;
            auto default_collectors = ptr->use_collectors;
            if (ptr->rules_manager_) {
                use_rules = true;
            }
            Job job;
            spdlog::debug("CondorJobWatcher: ebpf got exec event pid {} ppid {} comm {}",
                        event_ptr->pid, event_ptr->ppid, event_ptr->comm);
            
            ptr->buildAndAddJob(event_ptr->pid, use_rules, default_collectors);
            
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

    // Idempotent counterpart of init_event_source().
    void deinit_event_source(){
        EbpfCommon::free_rb(bpf_rb_);
        bpf_rb_ = nullptr;
        EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
        bpf_obj_ = nullptr;
        event_source_ready_ = false;
        spdlog::info("CondorJobWatcher: deinit_ebpf");
    }

    // 评估规则并返回采集器名称
    // 返回值: std::pair<bool, std::vector<std::string>>
    // 第一个元素: 是否所有规则都通过
    // 第二个元素: 所有规则添加的采集器名称列表
    std::pair<bool, std::vector<std::string>> evaluate_rules_with_collectors(const Job& job){
        if(!rules_manager_) return {true, {}};
        spdlog::debug("CondorJobWatcher: evaluating rules for job ID {}", job.JobID);
        auto result = rules_manager_->evaluate_all<Job>(job);
        spdlog::debug("CondorJobWatcher: job ID {} passed rules: {}, collectors: {}", 
                     job.JobID, result.first, fmt::join(result.second, ", "));
        return result;
    }

    bool evaluate_rules(Job& job){
        if(!rules_manager_) return true;
        spdlog::debug("CondorJobWatcher: evaluating rules for job ID {}", job.JobID);
        auto [passed, collectors] = evaluate_rules_with_collectors(job);
        
        // 将规则返回的采集器名称添加到Job对象中
        if (!collectors.empty()) {
            job.CollectorNames.insert(job.CollectorNames.end(), collectors.begin(), collectors.end());
            // 去重
            std::sort(job.CollectorNames.begin(), job.CollectorNames.end());
            job.CollectorNames.erase(std::unique(job.CollectorNames.begin(), job.CollectorNames.end()), 
                                    job.CollectorNames.end());
        }
        
        spdlog::debug("CondorJobWatcher: job ID {} passed rules: {}", job.JobID, passed);
        return passed;
    }
    
    WatcherRuntimeOptions options_;
    std::unique_ptr<RulesManager> rules_manager_;
    bool enable_auto_add_condor_job{false};
    std::vector<std::string> use_collectors;
    std::string rb_name = "exec_events";
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
