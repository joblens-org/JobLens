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
#include "common/utils.hpp"
#include "rule_engine/rules_manager.hpp"
#include "common/config.hpp"
#include <chrono>
#include <thread>


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
        spdlog::info("SlurmJobWatcher: inited auto add slurm job");
    }
    ~SlurmJobWatcher() = default;

    void register_add_if(std::function<void(Job)> add_if){
        add_if_ = add_if;
    }

    size_t add_all_slurm_job(bool use_rules = true){
        size_t added_count = 0;
        auto slurm_pids = SlurmJob::get_all_slurm_job_pids_from_cgroups();
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

    void on_cgroup_mkdir(const std::string& cgroup_path){
        if (!SlurmJob::looks_like_slurm_cgroup(cgroup_path)) {
            return;
        }
        auto step_path = SlurmJob::normalize_to_slurm_step_cgroup(cgroup_path);
        auto job_opt = buildSlurmJobFromCgroup(step_path);
        if (!job_opt) {
            spdlog::debug("SlurmJobWatcher: no pid in cgroup {} after retry", step_path);
            return;
        }
        addBuiltJob(std::move(job_opt.value()), rules_manager_ != nullptr, use_collectors);
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
        spdlog::debug("SlurmJobWatcher: built job from pid {}, JobID {}, step_id {}, user {}, partition {}", 
                    pid, job.JobID, slurm_attr.step_id, slurm_attr.user, slurm_attr.partition);
        return job;
    }

    std::optional<Job> buildSlurmJobFromCgroup(const std::string& cgroup_path){
        auto pids = get_cgroup_pids_with_retry(cgroup_path);
        if (!pids || pids->empty()) return std::nullopt;

        spdlog::debug("SlurmJobWatcher: building job from cgroup {}, pids={}", cgroup_path, fmt::join(*pids, ", "));
        for (auto pid : *pids) {
            auto job_opt = buildSlurmJob(pid);
            if (!job_opt) continue;
            Job job = std::move(job_opt.value());
            job.JobPIDs = *pids;
            auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
            slurm_attr.cgroup_path = cgroup_path;
            return job;
        }
        spdlog::debug("SlurmJobWatcher: no valid job from any pid in cgroup {}", cgroup_path);
        return std::nullopt;
    }

    void buildAndAddJob(pid_t pid, bool use_rules, std::vector<std::string> default_collectors){
        auto job_opt = buildSlurmJob(pid);
        if(!job_opt) return;

        addBuiltJob(std::move(job_opt.value()), use_rules, std::move(default_collectors));
    }

    void addBuiltJob(Job job, bool use_rules, std::vector<std::string> default_collectors){
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

    std::optional<std::vector<pid_t>> get_cgroup_pids_with_retry(const std::string& cgroup_path){
        for (int attempt = 0; attempt < 3; ++attempt) {
            if (attempt > 0) {
                spdlog::debug("SlurmJobWatcher: retry reading cgroup {} (attempt {})", cgroup_path, attempt + 1);
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            auto result = Utils::get_pids_in_cgroup(cgroup_path);
            if (result.status == Utils::CgroupProcsReadStatus::Success && !result.pids.empty()) {
                spdlog::debug("SlurmJobWatcher: read {} pids from cgroup {} on attempt {}",
                              result.pids.size(), cgroup_path, attempt + 1);
                return result.pids;
            }
            spdlog::debug("SlurmJobWatcher: cgroup {} read attempt {} status={} pids={}",
                          cgroup_path, attempt + 1,
                          Utils::cgroup_procs_status_name(result.status), result.pids.size());
            if (result.status == Utils::CgroupProcsReadStatus::Missing ||
                result.status == Utils::CgroupProcsReadStatus::PermissionDenied) {
                return std::nullopt;
            }
        }
        spdlog::debug("SlurmJobWatcher: failed to read pids from cgroup {} after 3 attempts", cgroup_path);
        return std::nullopt;
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
    std::vector<std::string> use_collectors;
    std::function<void(Job)> add_if_;
};
