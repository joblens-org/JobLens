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
#include "job_watcher/job_trigger_event.hpp"
#include <spdlog/spdlog.h>
#include <fmt/ranges.h>
#include "common/utils.hpp"
#include "rule_engine/rules_manager.hpp"
#include "common/config.hpp"
#include <chrono>
#include <thread>

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
        spdlog::info("CondorJobWatcher: inited auto add condor job");
    }
    ~CondorJobWatcher() = default;

    void register_add_if(std::function<void(Job)> add_if){
        add_if_ = add_if;
    }

    size_t add_all_condor_job(bool use_rules = true){
        size_t added_count = 0;
        auto condor_pids = CondorJob::get_all_condor_job_pids_from_cgroups();
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

    void on_cgroup_mkdir(const std::string& cgroup_path){
        if (!CondorJob::looks_like_condor_cgroup(cgroup_path)) {
            return;
        }
        spdlog::debug("CondorJobWatcher: cgroup mkdir matched path={}, building job", cgroup_path);
        auto job_opt = buildCondorJobFromCgroup(cgroup_path);
        if (!job_opt) {
            spdlog::debug("CondorJobWatcher: no pid in cgroup {} after retry", cgroup_path);
            return;
        }
        auto job = std::move(job_opt.value());
        spdlog::debug("CondorJobWatcher: built cgroup job JobID={} NativeJobID={} cgroup={} pids=[{}]",
                      job.JobID, job.NativeJobID, cgroup_path, fmt::join(job.JobPIDs, ", "));
        addBuiltJob(std::move(job), rules_manager_ != nullptr, use_collectors);
    }

    void on_trigger_event(const TriggerEvent& event)
    {
        std::visit([this](const auto& e) {
            using T = std::decay_t<decltype(e)>;
            if constexpr (std::is_same_v<T, CgroupMkdirTriggerEvent>) {
                on_cgroup_mkdir(e.cgroup_path);
            } else if constexpr (std::is_same_v<T, ExecveTriggerEvent>) {
                on_execve(static_cast<pid_t>(e.pid), static_cast<pid_t>(e.ppid), e.comm);
            }
        }, event);
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

    std::optional<Job> buildCondorJobFromCgroup(const std::string& cgroup_path){
        auto pids = get_cgroup_pids_with_retry(cgroup_path);
        if (!pids || pids->empty()) return std::nullopt;

        spdlog::debug("CondorJobWatcher: building job from cgroup {}, pids={}", cgroup_path, fmt::join(*pids, ", "));
        for (auto pid : *pids) {
            auto job_opt = buildCondorJob(pid);
            if (!job_opt) continue;
            Job job = std::move(job_opt.value());
            job.JobPIDs = *pids;
            auto& condor_attr = std::get<CondorJobAttr>(job.sub_attr);
            condor_attr.slots_cgroup_path = cgroup_path;
            spdlog::debug("CondorJobWatcher: buildCondorJobFromCgroup selected pid={} JobID={} NativeJobID={} starter_pid={} cgroup={} all_pids=[{}]",
                          pid, job.JobID, job.NativeJobID, condor_attr.starter_pid,
                          cgroup_path, fmt::join(job.JobPIDs, ", "));
            return job;
        }
        spdlog::debug("CondorJobWatcher: no valid job from any pid in cgroup {}", cgroup_path);
        return std::nullopt;
    }

    void buildAndAddJob(pid_t pid, bool use_rules, std::vector<std::string> default_collectors){
        auto job_opt = buildCondorJob(pid);
        if(!job_opt) return;

        addBuiltJob(std::move(job_opt.value()), use_rules, std::move(default_collectors));
    }

    void on_execve(pid_t pid, pid_t /*ppid*/, const std::string& /*comm*/)
    {
        auto job_opt = buildCondorJob(pid);
        if (!job_opt) {
            spdlog::debug("CondorJobWatcher: execve pid {} not a valid condor job", pid);
            return;
        }
        Job job = std::move(job_opt.value());
        // 尝试从 proc/cgroup 读取完整 PID 列表（增强模式）
        auto pids = get_cgroup_pids_with_retry(
            std::get<CondorJobAttr>(job.sub_attr).slots_cgroup_path);
        if (pids && !pids->empty()) {
            job.JobPIDs = *pids;
        }
        addBuiltJob(std::move(job), rules_manager_ != nullptr, use_collectors);
    }

    void addBuiltJob(Job job, bool use_rules, std::vector<std::string> default_collectors){
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

    std::optional<std::vector<pid_t>> get_cgroup_pids_with_retry(const std::string& cgroup_path){
        for (int attempt = 0; attempt < 3; ++attempt) {
            if (attempt > 0) {
                spdlog::debug("CondorJobWatcher: retry reading cgroup {} (attempt {})", cgroup_path, attempt + 1);
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            auto result = Utils::get_pids_in_cgroup(cgroup_path);
            if (result.status == Utils::CgroupProcsReadStatus::Success && !result.pids.empty()) {
                spdlog::debug("CondorJobWatcher: read {} pids from cgroup {} on attempt {}",
                              result.pids.size(), cgroup_path, attempt + 1);
                return result.pids;
            }
            spdlog::debug("CondorJobWatcher: cgroup {} read attempt {} status={} pids={}",
                          cgroup_path, attempt + 1,
                          Utils::cgroup_procs_status_name(result.status), result.pids.size());
            if (result.status == Utils::CgroupProcsReadStatus::Missing ||
                result.status == Utils::CgroupProcsReadStatus::PermissionDenied) {
                return std::nullopt;
            }
        }
        spdlog::debug("CondorJobWatcher: failed to read pids from cgroup {} after 3 attempts", cgroup_path);
        return std::nullopt;
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
    std::vector<std::string> use_collectors;
    std::function<void(Job)> add_if_;
};
