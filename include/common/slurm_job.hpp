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
#include "core/collector_type.h"
#include "common/utils.hpp"
#include <unistd.h>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <spdlog/spdlog.h>
#include <unordered_set>
#include <set>
#include <fmt/core.h>
#include <fmt/ranges.h>

namespace SlurmJob
{

// 使用Utils命名空间中的公共函数
using Utils::get_ppid_of;
using Utils::get_cmdline_of;
using Utils::get_env_field;
using Utils::v2_cgroup_absolute_path;
using Utils::get_pids_in_cgroup;

inline bool looks_like_slurm_cgroup(const std::string& path)
{
    std::string lower = path;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return lower.find("slurm") != std::string::npos ||
           lower.find("/step_") != std::string::npos ||
           lower.find("/job_") != std::string::npos;
}

inline std::string normalize_to_slurm_step_cgroup(const std::string& path)
{
    auto pos = path.find("/step_");
    if (pos == std::string::npos) {
        return path;
    }
    auto next = path.find('/', pos + 1);
    if (next == std::string::npos) {
        return path;
    }
    return path.substr(0, next);
}

inline std::vector<pid_t> get_all_slurm_job_pids_from_cgroups()
{
    std::vector<pid_t> pids;
    std::set<std::string> seen_step_paths;
    for (const auto& dir : Utils::scan_cgroup2_dirs()) {
        if (!looks_like_slurm_cgroup(dir)) {
            continue;
        }
        auto step_path = normalize_to_slurm_step_cgroup(dir);
        if (!seen_step_paths.insert(step_path).second) {
            continue;
        }
        auto result = get_pids_in_cgroup(step_path);
        if (result.ok() && !result.pids.empty()) {
            pids.insert(pids.end(), result.pids.begin(), result.pids.end());
        }
    }
    return pids;
}

inline bool refresh_cgroup_metadata(Job& job){
    auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
    pid_t source_pid = slurm_attr.stepd_pid;
    if (source_pid == 0 && !job.JobPIDs.empty()) {
        source_pid = get_ppid_of(job.JobPIDs[0]);
        slurm_attr.stepd_pid = source_pid;
    }
    if (source_pid <= 0) {
        return false;
    }

    auto cgroup_path = v2_cgroup_absolute_path(source_pid);
    if (cgroup_path.empty()) {
        return false;
    }
    slurm_attr.cgroup_path = std::move(cgroup_path);
    return true;
}

inline void update_job_pids(Job& job){
    auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
    spdlog::debug("SlurmJob: update_job_pids begin JobID={} NativeJobID={} stepd_pid={} cgroup_path={} current_pids=[{}]",
                  job.JobID, job.NativeJobID, slurm_attr.stepd_pid,
                  slurm_attr.cgroup_path, fmt::join(job.JobPIDs, ", "));
    if(slurm_attr.stepd_pid == 0){
        if(job.JobPIDs.empty()){
            spdlog::warn("SlurmJob: cannot update pids for job {} without stepd pid or job pids", job.JobID);
            return;
        }
        if(job.JobPIDs.size() > 1){
            spdlog::warn("SlurmJob: multi pids, use first");
        }
        slurm_attr.stepd_pid = get_ppid_of(job.JobPIDs[0]);
        slurm_attr.cgroup_path = v2_cgroup_absolute_path(job.JobPIDs[0]);
        spdlog::debug("SlurmJob: inferred stepd_pid={} cgroup_path={} from pid={} for job {}",
                      slurm_attr.stepd_pid, slurm_attr.cgroup_path,
                      job.JobPIDs[0], job.JobID);
    }
    if (slurm_attr.cgroup_path.empty() && !refresh_cgroup_metadata(job)) {
        spdlog::warn("SlurmJob: cannot refresh cgroup path for job {}", job.JobID);
        return;
    }

    auto pid_cgroup = get_pids_in_cgroup(slurm_attr.cgroup_path);
    spdlog::debug("SlurmJob: cgroup read JobID={} path={} status={} raw_pids=[{}]",
                  job.JobID, slurm_attr.cgroup_path,
                  Utils::cgroup_procs_status_name(pid_cgroup.status),
                  fmt::join(pid_cgroup.pids, ", "));
    if (!pid_cgroup.ok()) {
        spdlog::warn("SlurmJob: failed to read cgroup pids from {}: {}, try refresh",
                     slurm_attr.cgroup_path,
                     Utils::cgroup_procs_status_name(pid_cgroup.status));
        if (!refresh_cgroup_metadata(job)) {
            return;
        }
        spdlog::debug("SlurmJob: refreshed cgroup metadata JobID={} stepd_pid={} cgroup_path={}",
                      job.JobID, slurm_attr.stepd_pid, slurm_attr.cgroup_path);
        pid_cgroup = get_pids_in_cgroup(slurm_attr.cgroup_path);
        spdlog::debug("SlurmJob: refreshed cgroup read JobID={} path={} status={} raw_pids=[{}]",
                      job.JobID, slurm_attr.cgroup_path,
                      Utils::cgroup_procs_status_name(pid_cgroup.status),
                      fmt::join(pid_cgroup.pids, ", "));
        if (!pid_cgroup.ok()) {
            spdlog::warn("SlurmJob: failed to read refreshed cgroup pids from {}: {}",
                         slurm_attr.cgroup_path,
                         Utils::cgroup_procs_status_name(pid_cgroup.status));
            return;
        }
    }
    auto raw_pids = pid_cgroup.pids;
    pid_cgroup.pids.erase(
        std::remove(pid_cgroup.pids.begin(), pid_cgroup.pids.end(), slurm_attr.stepd_pid),
        pid_cgroup.pids.end());
    job.JobPIDs = std::move(pid_cgroup.pids);
    spdlog::debug("SlurmJob: update_job_pids end JobID={} stepd_pid={} raw_pids=[{}] final_pids=[{}]",
                  job.JobID, slurm_attr.stepd_pid,
                  fmt::join(raw_pids, ", "), fmt::join(job.JobPIDs, ", "));
}

/* 获取Slurm Job ID */
inline uint64_t getJobID(pid_t pid){
    std::string job_id_str = get_env_field(pid, "SLURM_JOB_ID");
    spdlog::debug("SlurmJob: pid {} SLURM_JOB_ID {}", pid, job_id_str);
    if (job_id_str.empty()) return 0;

    try {
        return std::stoull(job_id_str);
    } catch (const std::exception& e) {
        spdlog::error("SlurmJob: failed to parse job ID '{}': {}", job_id_str, e.what());
        return 0;
    }
}

/* 获取Slurm Step ID */
inline uint32_t getStepID(pid_t pid){
    std::string step_id_str = get_env_field(pid, "SLURM_STEP_ID");
    spdlog::debug("SlurmJob: pid {} SLURM_STEP_ID {}", pid, step_id_str);
    if (step_id_str.empty()) return 0;

    try {
        return static_cast<uint32_t>(std::stoul(step_id_str));
    } catch (const std::exception& e) {
        spdlog::error("SlurmJob: failed to parse step ID '{}': {}", step_id_str, e.what());
        return 0;
    }
}

/* 获取作业用户 */
inline std::string getUser(pid_t pid){
    return get_env_field(pid, "SLURM_JOB_USER");
}

/* 获取作业名称 */
inline std::string getJobName(pid_t pid){
    return get_env_field(pid, "SLURM_JOB_NAME");
}

/* 获取分区/队列名称 */
inline std::string getPartition(pid_t pid){
    return get_env_field(pid, "SLURM_JOB_PARTITION");
}

/* 获取所有slurmstepd进程PID列表 */
inline std::vector<pid_t> get_all_slurmstepd_pids(){
    std::vector<pid_t> slurm_pids;
    namespace fs = std::filesystem;
    for (const auto& entry : fs::directory_iterator("/proc")) {
        if (!entry.is_directory()) continue;
        std::string filename = entry.path().filename().string();
        if (!std::all_of(filename.begin(), filename.end(), ::isdigit)) continue;
        pid_t pid = static_cast<pid_t>(std::stol(filename));
        std::string cmdline = get_cmdline_of(pid);
        // slurmstepd 命令行通常包含 "slurmstepd"
        if (cmdline.find("slurmstepd") != std::string::npos) {
            slurm_pids.push_back(pid);
        }
    }
    return slurm_pids;
}

/* 获取指定slurmstepd下的所有子进程 */
inline std::vector<pid_t> get_slurm_job_pids(pid_t stepd_pid){
    std::vector<pid_t> job_pids;
    namespace fs = std::filesystem;
    
    for (const auto& entry : fs::directory_iterator("/proc")) {
        if (!entry.is_directory()) continue;
        std::string filename = entry.path().filename().string();
        if (!std::all_of(filename.begin(), filename.end(), ::isdigit)) continue;
        pid_t pid = static_cast<pid_t>(std::stol(filename));
        
        // 检查父进程是否是该stepd
        pid_t ppid = get_ppid_of(pid);
        if (ppid == stepd_pid) {
            // 检查是否有SLURM_JOB_ID环境变量
            if (getJobID(pid) > 0) {
                job_pids.push_back(pid);
            }
        }
    }
    return job_pids;
}

/* 获取所有正在运行的Slurm作业PID（去重） */
inline std::vector<pid_t> get_all_slurm_job_pids(){
    std::vector<pid_t> all_job_pids;
    std::unordered_set<pid_t> seen;
    
    auto stepd_pids = get_all_slurmstepd_pids();
    for (auto stepd_pid : stepd_pids) {
        auto job_pids = get_slurm_job_pids(stepd_pid);
        for (auto pid : job_pids) {
            if (seen.insert(pid).second) {
                all_job_pids.push_back(pid);
            }
        }
    }
    return all_job_pids;
}

inline std::string get_cluster_name(pid_t pid = 0){
    return get_env_field(pid, "SLURM_CLUSTER_NAME");
}

inline std::string get_native_job_id(pid_t pid){
    auto jid = getJobID(pid);
    return fmt::format("{}", jid);
}

inline void update_job_info(Job& job){
    if (job.JobPIDs.empty()) return;

    auto& slurm_attr = std::get<SlurmJobAttr>(job.sub_attr);
    pid_t representative_pid = job.JobPIDs[0]; // 以第一个PID为代表

    // 标识性字段：只在首次未设置时填充
    if (slurm_attr.job_id == 0) {
        slurm_attr.job_id = getJobID(representative_pid);
        slurm_attr.step_id = getStepID(representative_pid);
        job.NativeJobID = fmt::format("{}", slurm_attr.job_id);
    }

    // 描述性字段：每次检查，为空/0则从进程环境变量补全
    if (slurm_attr.user.empty()) {
        slurm_attr.user = getUser(representative_pid);
    }
    if (slurm_attr.job_name.empty()) {
        slurm_attr.job_name = getJobName(representative_pid);
    }
    if (slurm_attr.partition.empty()) {
        slurm_attr.partition = getPartition(representative_pid);
    }
    if (slurm_attr.cluster_name.empty()) {
        slurm_attr.cluster_name = get_cluster_name(representative_pid);
        job.cluster_name = slurm_attr.cluster_name;
        job.clusterTag = slurm_attr.cluster_name;
    }
    // stepd_pid 取 JobPIDs 中最小的 PID (slurmstepd 通常为父进程, PID 最小)
    if (slurm_attr.stepd_pid == 0) {
        slurm_attr.stepd_pid = *std::min_element(job.JobPIDs.begin(), job.JobPIDs.end());
    }
    if (slurm_attr.cgroup_path.empty()) {
        refresh_cgroup_metadata(job);
    }

    return;
}

} // namespace SlurmJob
