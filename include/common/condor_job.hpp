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
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <filesystem>
#include <iostream>
#include <variant>
#include <spdlog/spdlog.h>
#include <fmt/core.h>

namespace fs = std::filesystem;

namespace CondorJob
{

// 使用Utils命名空间中的公共函数
using Utils::get_ppid_of;
using Utils::get_cmdline_of;
using Utils::get_env_field;
using Utils::v2_cgroup_absolute_path;
using Utils::get_pids_in_cgroup;

inline void update_job_pids(Job& job){
    auto& condor_attr = std::get<CondorJobAttr>(job.sub_attr);
    if(condor_attr.starter_pid == 0){
        //初始化相关内容
        if(job.JobPIDs.size()>1){
            spdlog::warn("CondorJob: multi pids, use first");
        }
        condor_attr.starter_pid = get_ppid_of(job.JobPIDs[0]);
        condor_attr.slots_cgroup_path = v2_cgroup_absolute_path(job.JobPIDs[0]);
    }   
    auto pid_slots_cgroup = get_pids_in_cgroup(condor_attr.slots_cgroup_path);
    std::remove(pid_slots_cgroup.begin(), pid_slots_cgroup.end(), condor_attr.starter_pid);
    job.JobPIDs = std::move(pid_slots_cgroup);
}

/* 解析 GlobalJobId */
inline static std::string parse_job_id(const std::string &job_ad)
{
    std::ifstream f(job_ad);
    std::string line;
    while (std::getline(f, line)) {
        auto p = line.find(" = ");
        if (p != std::string::npos && line.substr(0, p) == "GlobalJobId") {
            std::string v = line.substr(p + 3);
            auto sharp = v.find('#');
            return sharp == std::string::npos ? "" : v.substr(sharp + 1);
        }
    }
    return "";
}

inline static uint64_t parse_uint64(std::string_view s)
{
    uint64_t val = 0;
    for (char c : s) {
        if (c == '#') break;
        if (c == '.') continue;
        if (c >= '0' && c <= '9') {
            val = val * 10 + (c - '0');
        } else {
            return 0;  // invalid
        }
    }
    return val;
}

inline std::string get_ad_field(std::string ad_path, const std::string &field){
    std::ifstream f(ad_path);
    std::string line;
    while (std::getline(f, line)) {
        auto p = line.find(" = ");
        if (p != std::string::npos && line.substr(0, p).compare(field) == 0) {
            std::string v = line.substr(p + 3);
            return v;
        }
    }
    return "";
}

inline uint64_t getJobID(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    spdlog::debug("CondorJob: pid {} _CONDOR_JOB_AD {}", pid, ad_path);
    if (ad_path.empty()) return 0;

    std::string condor_jobid = parse_job_id(ad_path);
    spdlog::debug("CondorJob: pid {} condor jobid {}", pid, condor_jobid);
    if (condor_jobid.empty()) return 0;

    return parse_uint64(condor_jobid);
}

inline std::string getOwner(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    if (ad_path.empty()) return "";
    return get_ad_field(ad_path, "Owner");
}

inline std::vector<pid_t> get_all_condor_job_pids(){
    std::vector<pid_t> condor_pids;
    namespace fs = std::filesystem;
    for (const auto& entry : fs::directory_iterator("/proc")) {
        if (!entry.is_directory()) continue;
        std::string filename = entry.path().filename().string();
        if (!std::all_of(filename.begin(), filename.end(), ::isdigit)) continue;
        pid_t pid = static_cast<pid_t>(std::stol(filename));
        std::string cmdline = get_cmdline_of(pid);
        if (cmdline.find("condor_starter") != std::string::npos) {
            condor_pids.push_back(pid);
        }
    }
    return condor_pids;
}

inline std::string get_scheduler_host(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    if (ad_path.empty()) return "";
    auto global_job_id = get_ad_field(ad_path, "GlobalJobId");
    std::string host_part;
    auto at_pos = global_job_id.find('@');
    if (at_pos != std::string::npos) {
        auto sharp_pos = global_job_id.find('#', at_pos);
        if (sharp_pos != std::string::npos) {
            host_part = global_job_id.substr(at_pos + 1, sharp_pos - at_pos - 1);
        }
    }   
    return host_part;
}

inline std::string get_scheduler_name(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    if (ad_path.empty()) return "";
    auto global_job_id = get_ad_field(ad_path, "GlobalJobId");
    std::string scheduler_name;
    // GlobalJobId格式示例："scheduler@schedd07.ihep.ac.cn#76226337.0#1775723895"
    auto sharp_pos = global_job_id.find('#');
    if (sharp_pos != std::string::npos) {
        auto quotation_pos = global_job_id.find('"');
        if (quotation_pos != std::string::npos) {
            scheduler_name = global_job_id.substr(quotation_pos + 1, sharp_pos - quotation_pos - 1);
        }
    }
    return scheduler_name;
}

inline std::pair<size_t, size_t> get_cluster_and_proc_id(pid_t pid){
    std::string ad_path = get_env_field(pid, "_CONDOR_JOB_AD");
    if (ad_path.empty()) return {0, 0};
    auto global_job_id = get_ad_field(ad_path, "GlobalJobId");
    auto first_sharp = global_job_id.find('#');
    if (first_sharp == std::string::npos) return {0, 0};
    auto second_sharp = global_job_id.find('#', first_sharp + 1);
    if (second_sharp == std::string::npos) return {0, 0};
    
    std::string cluster_proc = global_job_id.substr(first_sharp + 1, second_sharp - first_sharp - 1);
    auto dot_pos = cluster_proc.find('.');
    if (dot_pos == std::string::npos) return {0, 0};
    
    size_t cluster_id = parse_uint64(cluster_proc.substr(0, dot_pos));
    size_t proc_id = parse_uint64(cluster_proc.substr(dot_pos + 1));
    
    return {cluster_id, proc_id};
}

inline std::string get_native_job_id(pid_t pid){
    auto [cluster_id, proc_id] = get_cluster_and_proc_id(pid);
    return fmt::format("{}.{}", cluster_id, proc_id);
}

inline void update_job_info(Job& job){
    auto& condor_attr = std::get<CondorJobAttr>(job.sub_attr);

    // 确保 starter_pid 已知 (其他字段依赖它)
    if (condor_attr.starter_pid == 0) {
        condor_attr.starter_pid = get_ppid_of(job.JobPIDs[0]);
    }

    // 逐字段检查，为空则从系统补全
    if (condor_attr.scheduler_host.empty()) {
        condor_attr.scheduler_host = get_scheduler_host(condor_attr.starter_pid);
    }
    if (condor_attr.scheduler_name.empty()) {
        condor_attr.scheduler_name = get_scheduler_name(condor_attr.starter_pid);
    }
    job.clusterTag = condor_attr.scheduler_name;

    if (condor_attr.slots_cgroup_path.empty()) {
        condor_attr.slots_cgroup_path = v2_cgroup_absolute_path(condor_attr.starter_pid);
    }
    if (condor_attr.collector_host.empty()) {
        condor_attr.collector_host = Utils::get_condor_collector_host();
        job.cluster_name = condor_attr.collector_host;
    }
    if (condor_attr.cluster_id == 0 || condor_attr.proc_id == 0) {
        auto [cid, pid_] = get_cluster_and_proc_id(condor_attr.starter_pid);
        if (condor_attr.cluster_id == 0) condor_attr.cluster_id = cid;
        if (condor_attr.proc_id == 0) condor_attr.proc_id = pid_;
        job.NativeJobID = fmt::format("{}.{}", condor_attr.cluster_id, condor_attr.proc_id);
    }

    return;
}
} // namespace CondorJob
