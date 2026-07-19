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
#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <functional>
#include <any>
#include <nlohmann/json.hpp>
#include <variant>
#include "common/struct2ltable.hpp"
#include <spdlog/spdlog.h>

#define COLLECTOR_TYPE_PROC "ProcCollector"

enum class CollectorType {
    ProcCollector,      // 采集 /proc/<pid>/stat
    kStatus,    // 采集 /proc/<pid>/status
    kCmdline,   // 采集 /proc/<pid>/cmdline
    kFd         // 采集 /proc/<pid>/fd 信息
};

//TODO: 其实更好的方案是设计Job描述方法，但是设计和实现都很痛苦，先开摆

enum class JobType{
    Unknown = 0,
    Job,
    Sys
};

inline const char* to_string(JobType t) {
    switch (t) {
        case JobType::Job: return "Job";
        case JobType::Sys: return "Sys";
        default:           return "Unknown";
    }
}

enum class JobSubType{
    Unknown = 0,
    Common,
    Condor,
    Slurm
};

inline const char* to_string(JobSubType t) {
    switch (t) {
        case JobSubType::Common: return "Common";
        case JobSubType::Condor: return "Condor";
        case JobSubType::Slurm:  return "Slurm";
        default:                 return "Unknown";
    }
}

// 处理枚举（转换为字符串）
inline sol::object to_lua(sol::state_view lua, JobType type) {
    return sol::make_object(lua, to_string(type));
}


inline sol::object to_lua(sol::state_view lua, JobSubType subtype) {
    return sol::make_object(lua, to_string(subtype));
}

enum class CollectorScope{
    Undefined = 0,
    Job,
    System
};

struct CondorJobAttr{
    bool auto_update_child{};
    pid_t starter_pid{};
    size_t cluster_id{};   //将会逐步将condor中的jobid切换到这里
    size_t proc_id{};
    std::string scheduler_host{}; 
    std::string scheduler_name{};
    std::string slots_cgroup_path{};
    std::string owner{};
    std::string job_ad_path{};
    std::string collector_host{};  // COLLECTOR_HOST 值，用于查询

    static constexpr auto reflection() {
        return std::make_tuple(
            std::make_pair("auto_update_child", &CondorJobAttr::auto_update_child),
            std::make_pair("starter_pid", &CondorJobAttr::starter_pid),
            std::make_pair("cluster_id", &CondorJobAttr::cluster_id),
            std::make_pair("proc_id", &CondorJobAttr::proc_id),
            std::make_pair("scheduler_host", &CondorJobAttr::scheduler_host),
            std::make_pair("scheduler_name", &CondorJobAttr::scheduler_name),
            std::make_pair("slots_cgroup_path", &CondorJobAttr::slots_cgroup_path),
            std::make_pair("owner", &CondorJobAttr::owner),
            std::make_pair("job_ad_path", &CondorJobAttr::job_ad_path),
            std::make_pair("collector_host", &CondorJobAttr::collector_host)
        );
    }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(
    CondorJobAttr, \
    auto_update_child, \
    starter_pid, \
    cluster_id, \
    proc_id, \
    scheduler_host, \
    slots_cgroup_path, \
    owner, \
    job_ad_path, \
    collector_host
)

struct CommonJobAttr{
    bool auto_update_child{}; //当为true时，每次采集时自动更新有没有子进程
    static constexpr auto reflection() {
        return std::make_tuple(
            std::make_pair("auto_update_child", &CommonJobAttr::auto_update_child)
        );
    }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(CommonJobAttr, auto_update_child)

struct SlurmJobAttr{
    bool auto_update_child{};
    pid_t stepd_pid{};              // slurmstepd进程PID
    size_t job_id{};                // Slurm Job ID
    uint32_t step_id{};             // Slurm Step ID
    std::string cluster_name{};        // 集群名称
    std::string cgroup_path{};      // cgroup路径
    std::string user{};             // 作业用户
    std::string job_name{};         // 作业名称
    std::string partition{};        // 分区/队列名称

    static constexpr auto reflection() {
        return std::make_tuple(
            std::make_pair("auto_update_child", &SlurmJobAttr::auto_update_child),
            std::make_pair("stepd_pid", &SlurmJobAttr::stepd_pid),
            std::make_pair("job_id", &SlurmJobAttr::job_id),
            std::make_pair("step_id", &SlurmJobAttr::step_id),
            std::make_pair("cgroup_path", &SlurmJobAttr::cgroup_path),
            std::make_pair("user", &SlurmJobAttr::user),
            std::make_pair("job_name", &SlurmJobAttr::job_name),
            std::make_pair("partition", &SlurmJobAttr::partition)
        );
    }
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(SlurmJobAttr, \
    auto_update_child, \
    stepd_pid, \
    job_id, \
    step_id, \
    cgroup_path, \
    user, \
    job_name, \
    partition, \
    cluster_name)

using SubAttr = std::variant<CondorJobAttr, CommonJobAttr, SlurmJobAttr>;

// SubAttr 匹配器，用于通过 sub_attr 字段匹配作业
struct SubAttrMatcher {
    // 通用匹配条件
    std::optional<JobSubType> subtype;  // 匹配特定的子类型
    
    // CondorJobAttr 匹配字段
    std::optional<size_t> condor_cluster_id;
    std::optional<size_t> condor_proc_id;
    std::optional<std::string> condor_owner;
    std::optional<pid_t> condor_starter_pid;
    
    // SlurmJobAttr 匹配字段
    std::optional<size_t> slurm_job_id;
    std::optional<uint32_t> slurm_step_id;
    std::optional<std::string> slurm_user;
    std::optional<std::string> slurm_job_name;
    std::optional<pid_t> slurm_stepd_pid;
    
    // CommonJobAttr 匹配字段（如果需要可以添加）
    
    // 检查是否匹配给定的 SubAttr
    bool matches(const SubAttr& attr) const {
        // 首先检查子类型
        if (subtype.has_value()) {
            JobSubType actual_subtype;
            std::visit([&actual_subtype](const auto& a) {
                using T = std::decay_t<decltype(a)>;
                if constexpr (std::is_same_v<T, CondorJobAttr>) actual_subtype = JobSubType::Condor;
                else if constexpr (std::is_same_v<T, SlurmJobAttr>) actual_subtype = JobSubType::Slurm;
                else actual_subtype = JobSubType::Common;
            }, attr);
            if (actual_subtype != subtype.value()) return false;
        }
        
        // 检查具体字段
        return std::visit([this](const auto& a) -> bool {
            using T = std::decay_t<decltype(a)>;
            if constexpr (std::is_same_v<T, CondorJobAttr>) {
                spdlog::debug("SubAttrMatcher: matching CondorJobAttr with cluster_id={}, proc_id={}, owner={}, starter_pid={}", 
                    a.cluster_id, a.proc_id, a.owner, a.starter_pid);
                if (condor_cluster_id.has_value() && a.cluster_id != condor_cluster_id.value()) return false;
                if (condor_proc_id.has_value() && a.proc_id != condor_proc_id.value()) return false;
                if (condor_owner.has_value() && a.owner != condor_owner.value()) return false;
                if (condor_starter_pid.has_value() && a.starter_pid != condor_starter_pid.value()) return false;
                spdlog::debug("SubAttrMatcher: CondorJobAttr matched successfully");
                return true;
            } else if constexpr (std::is_same_v<T, SlurmJobAttr>) {
                spdlog::debug("SubAttrMatcher: matching SlurmJobAttr with job_id={}, step_id={}, user={}, job_name={}, stepd_pid={}", 
                    a.job_id, a.step_id, a.user, a.job_name, a.stepd_pid);
                if (slurm_job_id.has_value() && a.job_id != slurm_job_id.value()) return false;
                if (slurm_step_id.has_value() && a.step_id != slurm_step_id.value()) return false;
                if (slurm_user.has_value() && a.user != slurm_user.value()) return false;
                if (slurm_job_name.has_value() && a.job_name != slurm_job_name.value()) return false;
                if (slurm_stepd_pid.has_value() && a.stepd_pid != slurm_stepd_pid.value()) return false;
                spdlog::debug("SubAttrMatcher: SlurmJobAttr matched successfully");
                return true;
            } else if constexpr (std::is_same_v<T, CommonJobAttr>) {
                // CommonJobAttr 目前没有特殊字段需要匹配
                spdlog::debug("SubAttrMatcher: matching CommonJobAttr is not implemented, returning false");
                return false;
            }
            return false;
        }, attr);
    }
};

struct Job {
    uint64_t                              JobID{};
    std::string                           NativeJobID{};   // 调度器原生作业ID字符串，用于查询（Condor: cluster_id.proc_id, Slurm: job_id.step_id）
    std::string                           clusterTag{};    //Job所在集群的标识
    std::string                           cluster_name{};  // 集群名称（公共属性：Condor=COLLECTOR_HOST, Slurm=SLURM_CLUSTER_NAME）
    JobType                                 jobtype;
    JobSubType                              subtype;
    std::vector<pid_t>                        JobPIDs;
    // std::chrono::system_clock::time_point   JobCreateTime{std::chrono::system_clock::now()};
    // std::unordered_map<std::string, void*>  JobInfo; //目前没有启用
    std::vector<std::string>                CollectorNames;

    SubAttr                                 sub_attr;

    static constexpr auto reflection() {
        return std::make_tuple(
            std::make_pair("JobID", &Job::JobID),
            std::make_pair("NativeJobID", &Job::NativeJobID),
            std::make_pair("clusterTag", &Job::clusterTag),
            std::make_pair("cluster_name", &Job::cluster_name),
            std::make_pair("jobtype", &Job::jobtype),
            std::make_pair("subtype", &Job::subtype),
            std::make_pair("JobPIDs", &Job::JobPIDs),
            std::make_pair("CollectorNames", &Job::CollectorNames),
            std::make_pair("sub_attr", &Job::sub_attr)
        );
    }
};

// // variant 的序列化：visit 到具体类型
// inline void variant_to_json(nlohmann::json& j, const SubAttr& v) {
//     std::visit([&j](const auto& obj) { j = obj; }, v);
// }

inline nlohmann::json dump_job(const Job& job) {
    nlohmann::json j;
    j["JobID"] = job.JobID;
    j["NativeJobID"] = job.NativeJobID;
    j["clusterTag"] = job.clusterTag;
    j["cluster_name"] = job.cluster_name;
    j["jobtype"] = to_string(job.jobtype);
    j["subtype"] = to_string(job.subtype);
    j["JobPIDs"] = nlohmann::json::array();
    for (const auto& pid : job.JobPIDs) {
        j["JobPIDs"].push_back(pid);
    }
    j["CollectorNames"] = nlohmann::json::array();
    for (const auto& name : job.CollectorNames) {
        j["CollectorNames"].push_back(name);
    }
    std::visit([&j](const auto& obj) {
        j["sub_attr"] = nlohmann::json(obj);
    }, job.sub_attr);
    return j;
}

inline nlohmann::json job_to_json(const Job& job) {
    nlohmann::json j = {
        {"JobID",         job.JobID},
        {"NativeJobID",   job.NativeJobID},
        {"clusterTag",    job.clusterTag}, 
        {"cluster_name",  job.cluster_name},
        {"jobtype",       to_string(job.jobtype)},
        {"subtype",       to_string(job.subtype)},
        {"JobPIDs",       job.JobPIDs},              // 无需再包一层 nlohmann::json(...)
    };
    // 处理 sub_attr
    std::visit([&j](const auto& obj) {
        j["sub_attr"] = nlohmann::json(obj); // 显式转换为 nlohmann::json
    }, job.sub_attr);
    return j;
}

using OnFinish = std::function<void(const std::string, const Job&, const std::any, std::chrono::system_clock::time_point)>;

using CollectResult = std::any;

// 统一的可调用签名
using CollectFunc = std::function<CollectResult(const Job&)>;
using CollectInitFunc = std::function<bool(const nlohmann::json& config)>;
using CollectDeinitFunc = std::function<void()>;

using CollectDataParseFunc = std::function<std::any(std::any)>;

// ============================================================================
// V2 Parser Context API — 为 writer parser 提供更丰富的上下文信息
// 替代旧版 std::any 单参数签名，writer parser 可以通过 context 获取
// writer 名称、类型、配置名、collector 名称、Job 信息和时间戳
// ============================================================================

/// Writer 解析上下文 — 传递给 V2 parser 函数的上下文参数
struct WriterParseContext {
    std::string writer_name;          // writer 实例名称（如 "file_writer"）
    std::string writer_type;          // writer 类型标识（如 "file"）
    std::string writer_config_name;   // writer 配置节名称（如 "output"）
    std::string collector_name;       // 触发此 writer 的 collector 名称
    Job job;                          // 当前解析的 Job 对象
    std::chrono::system_clock::time_point timestamp;  // 解析触发时间戳
};

/// V2 解析函数签名 — 接收 WriterParseContext + 解析数据，返回 std::any 结果
using CollectDataParseFuncV2 = std::function<std::any(const WriterParseContext&, std::any)>;

struct CollectorHandle {
    CollectInitFunc        init;   
    CollectFunc            collect;
    CollectDeinitFunc      deinit;
};

using ConfigParam = std::pair<std::string, std::string>; // <param_name, description>
using ConfigParams = std::vector<ConfigParam>;
struct CollectorHelpInfo{
    std::string help_text;
    std::vector<ConfigParam> config_params; //<param_name, description>
};
