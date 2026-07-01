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
#include <filesystem>
#include "ebpf/trace_condor_starter.h"

class CondorJobWatcher{
public:
    CondorJobWatcher(){
        auto collectors = Config::instance().getArray<std::string>("condor_job_watcher", "auto_add_collectors");
        auto use_rules = Config::instance().getBool("condor_job_watcher", "use_rules", false);
        if(use_rules){
            rules_manager_ = std::make_unique<RulesManager>(
                "CondorJobWatcherRules",
                Config::instance().getString("job_registry_config", "rules_dir", Utils::JobLensRootDir() + "../config/rules/condor_jobs"),
                Config::instance().getString("condor_job_watcher", "rules_prefix", "condor_job_")
            );
            spdlog::info("CondorJobWatcher: use rules engine to filter condor jobs");
        }
        use_collectors = collectors;
        if(!init_ebpf()){
            spdlog::warn("CondorJobWatcher: init ebpf error, deinit now");
            deinit_ebpf();
        }else{
            spdlog::info("CondorJobWatcher: inited auto add condor job");
        }
    }
    ~CondorJobWatcher(){
        polling_running = false;
        poll_thread->join();
        
        deinit_ebpf();
    }

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

private:
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

    bool init_ebpf(){
        auto path = Utils::JobLensRootDir() + bpf_o_path;
        /* Dev-build fallback: try build directory's bpf_obj/ first */
        if (access(path.c_str(), R_OK) != 0) {
            path = "bpf_obj/trace_condor_starter.bpf.o";
        }
        bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);

        ring_buffer_sample_fn fn  = [](void *ctx, void *data, size_t size){
            auto ptr = static_cast<CondorJobWatcher*>(ctx);
            auto event_ptr = static_cast<struct event*>(data);
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
        
        if(!bpf_obj_ || !bpf_rb_){
            return false;
        }
        polling_running = true;
        poll_thread = std::make_unique<std::thread>([this](){
            while(polling_running){
                int err = ring_buffer__poll(bpf_rb_, 100 /* ms */);
                if (err == -EINTR){
                    spdlog::error("CondorJobWatcher: error EINTR");
                    break;
                }
                if (err < 0){
                    spdlog::error("CondorJobWatcher: error {}", err);
                }
            }
        });

        return true;
    }

    void deinit_ebpf(){
        EbpfCommon::free_rb(bpf_rb_);
        EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
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
    
    std::unique_ptr<RulesManager> rules_manager_;
    bool enable_auto_add_condor_job{false};
    bool polling_running;
    std::unique_ptr<std::thread> poll_thread;
    std::vector<std::string> use_collectors;
    std::string bpf_o_path = JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/trace_condor_starter.bpf.o";
    std::string rb_name = "exec_events";
    std::function<void(Job)> add_if_;
    std::vector<bpf_link*> bpf_links_;
    bpf_object* bpf_obj_;
    ring_buffer* bpf_rb_;
};

