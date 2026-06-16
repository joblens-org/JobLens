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
#include <filesystem>
#include "ebpf/trace_slurm_stepd.h"


class SlurmJobWatcher{
public:
    SlurmJobWatcher(){
        auto collectors = Config::instance().getArray<std::string>("slurm_job_watcher", "auto_add_collectors");
        auto use_rules = Config::instance().getBool("slurm_job_watcher", "use_rules", false);
        if(use_rules){
            rules_manager_ = std::make_unique<RulesManager>(
                "SlurmJobWatcherRules",
                Config::instance().getString("job_registry_config", "rules_dir", Utils::JobLensRootDir() + "../config/rules/slurm_jobs"),
                Config::instance().getString("slurm_job_watcher", "rules_prefix", "slurm_job_")
            );
            spdlog::info("SlurmJobWatcher: use rules engine to filter slurm jobs");
        }
        use_collectors = collectors;
        if(!init_ebpf()){
            spdlog::warn("SlurmJobWatcher: init ebpf error, deinit now");
            deinit_ebpf();
        }else{
            spdlog::info("SlurmJobWatcher: inited auto add slurm job");
        }
    }
    ~SlurmJobWatcher(){
        polling_running = false;
        if(poll_thread && poll_thread->joinable()){
            poll_thread->join();
        }
        
        deinit_ebpf();
    }

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

private:
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

    bool init_ebpf(){
        auto path = Utils::JobLensRootDir() + bpf_o_path;
        bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);

        ring_buffer_sample_fn fn  = [](void *ctx, void *data, size_t size){
            auto ptr = static_cast<SlurmJobWatcher*>(ctx);
            auto event_ptr = static_cast<struct slurm_event*>(data);
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
        
        if(!bpf_obj_ || !bpf_rb_){
            return false;
        }
        polling_running = true;
        poll_thread = std::make_unique<std::thread>([this](){
            while(polling_running){
                int err = ring_buffer__poll(bpf_rb_, 100 /* ms */);
                if (err == -EINTR){
                    spdlog::error("SlurmJobWatcher: error EINTR");
                    break;
                }
                if (err < 0){
                    spdlog::error("SlurmJobWatcher: error {}", err);
                }
            }
        });

        return true;
    }

    void deinit_ebpf(){
        EbpfCommon::free_rb(bpf_rb_);
        EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
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
    
    std::unique_ptr<RulesManager> rules_manager_;
    bool enable_auto_add_slurm_job{false};
    bool polling_running{false};
    std::unique_ptr<std::thread> poll_thread;
    std::vector<std::string> use_collectors;
    std::string bpf_o_path = JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/trace_slurm_stepd.bpf.o";
    std::string rb_name = "slurm_exec_events";
    std::function<void(Job)> add_if_;
    std::vector<bpf_link*> bpf_links_;
    bpf_object* bpf_obj_{nullptr};
    ring_buffer* bpf_rb_{nullptr};
};
