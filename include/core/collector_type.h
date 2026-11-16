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
#include <optional>
#include <unordered_map>
#include <functional>
#include <any>
#include <nlohmann/json.hpp>
#include <variant>
#include "common/struct2ltable.hpp"
#include "core/job_identity.hpp"
#include <spdlog/spdlog.h>

// Job is a concrete struct rather than a generic property bag: collectors and
// the store read its fields directly, and the typed SubAttr alternative keeps
// scheduler-specific facts out of shared code. A generic descriptor would move
// the same knowledge into string keys and runtime validation without removing
// any of the coupling.

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

inline const char* to_string(CollectorScope scope) {
    switch (scope) {
        case CollectorScope::Job:    return "job";
        case CollectorScope::System: return "system";
        default:                     return "undefined";
    }
}

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
    // 组归属（上游标准属性，见 doc 与 group_probe_stats）：
    std::string acct_group{};      // AcctGroup：计费组，完整三段（实验.子组.队列等级）
    std::string unix_group{};      // PrimaryUnixGroup：作业主动声明的主 Unix 组；未声明为空

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
            std::make_pair("collector_host", &CondorJobAttr::collector_host),
            std::make_pair("acct_group", &CondorJobAttr::acct_group),
            std::make_pair("unix_group", &CondorJobAttr::unix_group)
        );
    }
};

// 手写容错序列化（取代 NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE）：
// - from_json 全字段 `contains() && !is_null()` 判断后取值，旧记录缺字段 → 默认值，不抛异常
//   （宏生成的 from_json 用 j.at()，新增字段会让旧记录恢复时抛 type_error）。
// - to_json 写全部字段；scheduler_name 自此进入持久化（旧宏漏写，属声明过的行为变化）。
// - 字段清单必须与 reflection() 保持一致（group_fields_test 锁定）。
inline void to_json(nlohmann::json& j, const CondorJobAttr& t) {
    j = nlohmann::json{
        {"auto_update_child", t.auto_update_child},
        {"starter_pid", t.starter_pid},
        {"cluster_id", t.cluster_id},
        {"proc_id", t.proc_id},
        {"scheduler_host", t.scheduler_host},
        {"scheduler_name", t.scheduler_name},
        {"slots_cgroup_path", t.slots_cgroup_path},
        {"owner", t.owner},
        {"job_ad_path", t.job_ad_path},
        {"collector_host", t.collector_host},
        {"acct_group", t.acct_group},
        {"unix_group", t.unix_group},
    };
}
inline void from_json(const nlohmann::json& j, CondorJobAttr& t) {
    auto rd_bool = [&](const char* k, bool& out) { if (j.contains(k) && !j[k].is_null()) j[k].get_to(out); };
    auto rd_i64  = [&](const char* k, auto& out) { if (j.contains(k) && !j[k].is_null() && j[k].is_number_integer()) j[k].get_to(out); };
    auto rd_str  = [&](const char* k, std::string& out) { if (j.contains(k) && !j[k].is_null() && j[k].is_string()) j[k].get_to(out); };
    rd_bool("auto_update_child", t.auto_update_child);
    rd_i64("starter_pid", t.starter_pid);
    rd_i64("cluster_id", t.cluster_id);
    rd_i64("proc_id", t.proc_id);
    rd_str("scheduler_host", t.scheduler_host);
    rd_str("scheduler_name", t.scheduler_name);
    rd_str("slots_cgroup_path", t.slots_cgroup_path);
    rd_str("owner", t.owner);
    rd_str("job_ad_path", t.job_ad_path);
    rd_str("collector_host", t.collector_host);
    rd_str("acct_group", t.acct_group);
    rd_str("unix_group", t.unix_group);
}

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
    // 组归属（上游标准属性，见 doc 与 group_probe_stats）：
    std::string acct_group{};       // SLURM_JOB_ACCOUNT：计费账户（与 Condor AcctGroup 同语义）
    std::string unix_group{};       // gid 经 getgrgid_r 解析的主 Unix 组名；解析失败为空
    int64_t unix_gid{-1};           // SLURM_JOB_GID；-1 = 未知（int64：gid_t 为 uint32，大 gid 不溢出、不与哨兵混淆）

    static constexpr auto reflection() {
        return std::make_tuple(
            std::make_pair("auto_update_child", &SlurmJobAttr::auto_update_child),
            std::make_pair("stepd_pid", &SlurmJobAttr::stepd_pid),
            std::make_pair("job_id", &SlurmJobAttr::job_id),
            std::make_pair("step_id", &SlurmJobAttr::step_id),
            std::make_pair("cluster_name", &SlurmJobAttr::cluster_name),
            std::make_pair("cgroup_path", &SlurmJobAttr::cgroup_path),
            std::make_pair("user", &SlurmJobAttr::user),
            std::make_pair("job_name", &SlurmJobAttr::job_name),
            std::make_pair("partition", &SlurmJobAttr::partition),
            std::make_pair("acct_group", &SlurmJobAttr::acct_group),
            std::make_pair("unix_group", &SlurmJobAttr::unix_group),
            std::make_pair("unix_gid", &SlurmJobAttr::unix_gid)
        );
    }
};

// 手写容错序列化（取代 NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE），理由与字段清单约束同 CondorJobAttr。
inline void to_json(nlohmann::json& j, const SlurmJobAttr& t) {
    j = nlohmann::json{
        {"auto_update_child", t.auto_update_child},
        {"stepd_pid", t.stepd_pid},
        {"job_id", t.job_id},
        {"step_id", t.step_id},
        {"cgroup_path", t.cgroup_path},
        {"user", t.user},
        {"job_name", t.job_name},
        {"partition", t.partition},
        {"cluster_name", t.cluster_name},
        {"acct_group", t.acct_group},
        {"unix_group", t.unix_group},
        {"unix_gid", t.unix_gid},
    };
}
inline void from_json(const nlohmann::json& j, SlurmJobAttr& t) {
    auto rd_bool = [&](const char* k, bool& out) { if (j.contains(k) && !j[k].is_null()) j[k].get_to(out); };
    auto rd_i64  = [&](const char* k, auto& out) { if (j.contains(k) && !j[k].is_null() && j[k].is_number_integer()) j[k].get_to(out); };
    auto rd_str  = [&](const char* k, std::string& out) { if (j.contains(k) && !j[k].is_null() && j[k].is_string()) j[k].get_to(out); };
    rd_bool("auto_update_child", t.auto_update_child);
    rd_i64("stepd_pid", t.stepd_pid);
    rd_i64("job_id", t.job_id);
    rd_i64("step_id", t.step_id);
    rd_str("cgroup_path", t.cgroup_path);
    rd_str("user", t.user);
    rd_str("job_name", t.job_name);
    rd_str("partition", t.partition);
    rd_str("cluster_name", t.cluster_name);
    rd_str("acct_group", t.acct_group);
    rd_str("unix_group", t.unix_group);
    rd_i64("unix_gid", t.unix_gid);
}

using SubAttr = std::variant<CondorJobAttr, CommonJobAttr, SlurmJobAttr, nlohmann::json>;
// The json alternative is the open-world facts channel: a scheduler
// whose adapter registered after the built-ins carries its typed facts as a
// JSON object the adapter owns (schema, keys, evolution). Built-in attrs
// stay strongly typed so existing collectors and matchers are untouched.

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

    // 开放世界匹配（新调度器）：subtype_name 与 Job::subtype_name 比对；
    // json_fields 对 sub_attr 的 JSON 成员做字段级子集匹配。
    std::optional<std::string> subtype_name;
    std::optional<nlohmann::json> json_fields;

    // 检查是否匹配给定的 SubAttr
    bool matches(const SubAttr& attr) const {        // 首先检查子类型
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
                spdlog::debug("SubAttrMatcher: matching CondorJobAttr with cluster_id={}, proc_id={}, owner={}, starter_pid={}, scheduler_name={}, scheduler_host={}", 
                    a.cluster_id, a.proc_id, a.owner, a.starter_pid, a.scheduler_name, a.scheduler_host);
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
            } else {
                // Open-world JSON facts: field-level subset match. Without
                // json_fields the match is by presence (callers combine it
                // with subtype_name at the Job level).
                if (!json_fields.has_value()) return true;
                for (auto it = json_fields->begin(); it != json_fields->end(); ++it) {
                    if (!a.contains(it.key()) || a.at(it.key()) != it.value()) return false;
                }
                return true;
            }
            return false;
        }, attr);
    }
};

// One identity block per Job: logical identity (which scheduled job) plus
// execution identity (which local run). The watcher fills it while parsing;
// the store persists the compatible projection under the frozen row keys.
struct Job {
    uint64_t                              JobID{};
    std::string                           NativeJobID{};   // 调度器原生作业ID字符串，用于查询（Condor: cluster_id.proc_id, Slurm: job_id.step_id）
    // 命名空间公共字段是 typed sub_attr 的渲染投影（ES job_info 查询契约），
    // 不是第二事实源：哪个 attr 字段喂哪个分量由 adapter 拥有
    //（Condor: collector_host/scheduler_name, Slurm: cluster_name）。比较、
    // 判重与注入永远走结构化事实（ISchedulerAdapter 的 namespace 接口）。
    std::string                           clusterTag{};    // 渲染：调度器标签（Condor=schedd 名）
    std::string                           cluster_name{};  // 渲染：集群名（Condor=COLLECTOR_HOST, Slurm=SLURM_CLUSTER_NAME）
    JobType                                 jobtype{JobType::Unknown};
    JobSubType                              subtype{JobSubType::Unknown};
    // Projection of identity.adapter. Persisted next to the numeric subtype
    // for the frozen row layout; identity is the source.
    std::string                             subtype_name;
    std::vector<pid_t>                        JobPIDs;
    // Runtime/ingress projection of the record's selection. Never persisted:
    // the durable source is JobRecord::selection, and the collection runtime
    // fills this when it builds the collector-facing view.
    std::vector<std::string>                CollectorNames;

    SubAttr                                 sub_attr;

    // 通用采集状态：表示"已进入采集管线"——调度器当前实际持有本 Job
    // 的 collector 集合（部分成功保留实际生效子集）。只由采集运行时的
    // attach/detach 完成结果写入，绝不从 CollectorNames 推断，也绝不跨重启
    // 恢复为 true（重启后由 replay 重新 attach 时再置位）。不参与序列化。
    struct CollectionStatus {
        bool in_pipeline{false};
        std::vector<std::string> collectors;   // 实际持有者
    };
    CollectionStatus                        collection;

    // 组字段探测状态（仅内存，不序列化、不进 reflection）：
    // group_probed 仅在“成功读到来源（ad 文件 / env）”后置位——读失败不置位，
    // 由 group_probe_attempts 有界重试（每个进程生命周期内最多 kMaxGroupProbeAttempts 次）。
    // 采集每个周期都会走 update_job → update_job_info，若只按“字段为空就重读”，
    // 组本来就为空的站点会每周期重读 ad 文件（见 collector_scheduler 的 findJob 调用链）。
    bool                                    group_probed{false};
    uint8_t                                 group_probe_attempts{0};
    // restore/更新路径拿到了组值、等待落库一次（write-once + 至多一次回填重写）。
    bool                                    group_need_persist{false};

    // The one identity block. Manual registrations leave the manager empty and
    // bind `members`; discovery observations fill both.
    JobIdentity                             identity;

    static constexpr auto reflection() {
        return std::make_tuple(
            std::make_pair("JobID", &Job::JobID),
            std::make_pair("NativeJobID", &Job::NativeJobID),
            std::make_pair("clusterTag", &Job::clusterTag),
            std::make_pair("cluster_name", &Job::cluster_name),
            std::make_pair("jobtype", &Job::jobtype),
            std::make_pair("subtype", &Job::subtype),
            std::make_pair("JobPIDs", &Job::JobPIDs),
            std::make_pair("sub_attr", &Job::sub_attr)
        );
    }
};

// ---------------------------------------------------------------------------
// Identity projection accessors
//
// The Job is the single source of truth; these accessors are the only way the
// store derives the selector and execution evidence it persists, so no caller
// keeps a second copy of the same evidence.
// ---------------------------------------------------------------------------

inline NativeSelector selector_of(const Job& job) {
    NativeSelector selector;
    selector.adapter = job.identity.adapter.empty() ? job.subtype_name
                                                    : job.identity.adapter;
    if (selector.adapter.empty()) {
        switch (job.subtype) {
            case JobSubType::Condor: selector.adapter = "condor"; break;
            case JobSubType::Slurm:  selector.adapter = "slurm";  break;
            default: break;
        }
    }
    selector.cluster_name = job.cluster_name;
    selector.cluster_tag = job.clusterTag;
    selector.native_job_id = job.NativeJobID;
    selector.step_id = job.identity.step_id;
    return selector;
}

inline const ProcessIdentity& manager_of(const Job& job) {
    return job.identity.manager;
}

inline const std::vector<ProcessIdentity>& members_of(const Job& job) {
    return job.identity.members;
}

// True only when the source read the execution scope successfully: an empty
// member list without this flag is "unknown", not "confirmed empty".
inline bool members_authoritative_of(const Job& job) {
    return job.identity.members_authoritative;
}

inline int64_t read_at_us_of(const Job& job) {
    return job.identity.read_at_us;
}

// Publishes the identity-derived projections on the Job. The identity is the
// source; these fields exist only for the wire/collector contract.
inline void project_identity(Job& job) {
    if (!job.identity.adapter.empty()) job.subtype_name = job.identity.adapter;
    if (!job.identity.native_job_id.empty())
        job.NativeJobID = job.identity.native_job_id;
}

// Job 级匹配：开放世界名字（subtype_name）先于字段匹配——新调度器的作业
// 无需 typed variant 成员即可被 find/del 精确命中。
inline bool matches_job(const SubAttrMatcher& matcher, const Job& job) {
    if (matcher.subtype_name.has_value() && job.subtype_name != *matcher.subtype_name)
        return false;
    return matcher.matches(job.sub_attr);
}
// inline void variant_to_json(nlohmann::json& j, const SubAttr& v) {
//     std::visit([&j](const auto& obj) { j = obj; }, v);
// }

// 组归属的统一只读访问器：组字段只存在于 sub_attr（源样，随调度器取值，
// 对外 JSON/LevelDB 也只出现在 sub_attr 下，不再有顶层 groups 对象）。
// writer/registry 通过本视图取值，不各自 std::visit 按 subtype 分支。
// unix_gid 语义：-1 表示未知，内存/LevelDB/Lua/对外 JSON 一致。
struct JobGroupView {
    std::string acct_group;
    std::string unix_group;
    int64_t     unix_gid{-1};

    bool empty() const {
        return acct_group.empty() && unix_group.empty() && unix_gid < 0;
    }
};

inline JobGroupView getGroups(const Job& job) {
    JobGroupView v;
    std::visit([&v](const auto& attr) {
        using T = std::decay_t<decltype(attr)>;
        if constexpr (std::is_same_v<T, CondorJobAttr>) {
            v.acct_group = attr.acct_group;
            v.unix_group = attr.unix_group;
        } else if constexpr (std::is_same_v<T, SlurmJobAttr>) {
            v.acct_group = attr.acct_group;
            v.unix_group = attr.unix_group;
            v.unix_gid   = attr.unix_gid;
        }
        // CommonJobAttr 没有组字段，保持全空
    }, job.sub_attr);
    return v;
}

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

// ============================================================================
// 原生输出契约（Collector-Controlled Output）
//
// 采集器在 collect() 内通过 OutputContext 提交 0..N 条带类型记录。
// Success 允许零输出；Skipped 表示没有可用样本且不得输出。
// ============================================================================

enum class CollectState { Success, Skipped, Failed };

struct CollectOutcome {
    CollectState state = CollectState::Success;
    std::string error;
};

// ---------------------------------------------------------------------------
// Execution contracts: scope, trigger and output policy are independent.
// ---------------------------------------------------------------------------
enum class CollectorTrigger {
    Periodic,
    Event,
};

enum class OutputPolicy {
    Stream,
    Final,
    Both,
};

inline const char* to_string(CollectorTrigger trigger)
{
    switch (trigger) {
        case CollectorTrigger::Periodic: return "periodic";
        case CollectorTrigger::Event:    return "event";
    }
    return "unknown";
}

inline const char* to_string(OutputPolicy policy)
{
    switch (policy) {
        case OutputPolicy::Stream: return "stream";
        case OutputPolicy::Final:  return "final";
        case OutputPolicy::Both:   return "both";
    }
    return "unknown";
}

struct SourceHandle {
    uint64_t id = 0;
    uint64_t generation = 0;
};

inline bool operator==(const SourceHandle& lhs, const SourceHandle& rhs)
{
    return lhs.id == rhs.id && lhs.generation == rhs.generation;
}

inline bool operator!=(const SourceHandle& lhs, const SourceHandle& rhs)
{
    return !(lhs == rhs);
}

enum class Interest { Readable };

struct ReadySource {
    SourceHandle source;
    uint32_t readiness = 0;
};

using ReadySources = std::vector<ReadySource>;

struct WorkBudget {
    size_t max_events = 0;
    size_t max_bytes = 0;
    size_t max_jobs = 0;
    std::chrono::steady_clock::time_point deadline{};
};

struct DrainOutcome {
    CollectOutcome outcome;
    size_t consumed = 0;
    bool more = false;
};

struct JobKey {
    uint64_t job_id = 0;
    uint64_t monitoring_generation = 1;
};

class JobEventContext;
class SystemEventContext;

// 输出通道与 payload 类型标签。定义在此以便 CollectorHandle 声明通道，
// 具体 payload 容器（Document/Text/MetricBatch）位于 core/output_record.h。
enum class OutputKind { Document, Text, Metrics };

inline const char* to_string(OutputKind kind)
{
    switch (kind) {
        case OutputKind::Document: return "document";
        case OutputKind::Text:     return "text";
        case OutputKind::Metrics:  return "metrics";
    }
    return "unknown";
}

struct OutputChannel {
    std::string name;
    OutputKind kind = OutputKind::Document;
};

class OutputContext;
class OutputRouter;
class CollectorInitContext;

using NativeCollectFunc = std::function<CollectOutcome(const Job&, OutputContext&)>;
using SystemCollectFunc = std::function<CollectOutcome(OutputContext&)>;
using EventJobFunc = std::function<DrainOutcome(const ReadySources&, const WorkBudget&,
                                               JobEventContext&)>;
using EventSystemFunc = std::function<DrainOutcome(const ReadySources&, const WorkBudget&,
                                                  SystemEventContext&)>;

/// Job 生命周期钩子签名 — 由 CollectorScheduler 同步调用，传入调用瞬间的 Job 快照
using JobLifecycleFunc = std::function<void(const Job&)>;

using CollectInitFunc = std::function<bool(const nlohmann::json& config,
                                           CollectorInitContext& context)>;
using CollectDeinitFunc = std::function<void()>;

struct PeriodicJobBinding {
    NativeCollectFunc collect;
};

struct PeriodicSystemBinding {
    SystemCollectFunc collect;
};

struct EventJobBinding {
    EventJobFunc on_ready;
};

struct EventSystemBinding {
    EventSystemFunc on_ready;
};

using CollectorBinding = std::variant<PeriodicJobBinding, PeriodicSystemBinding,
                                      EventJobBinding, EventSystemBinding>;

struct CollectorHandle {
    CollectInitFunc        init;
    CollectDeinitFunc      deinit;
    // Job 生命周期钩子（两种作用域都会绑定，作用域语义见 ICollector::on_job_added 注释）
    // 未提供时为空 std::function，调度器会跳过调用
    JobLifecycleFunc       on_job_added;
    JobLifecycleFunc       on_job_updated;
    JobLifecycleFunc       on_job_removed;
    // Exactly one binding is populated according to scope + trigger.
    CollectorBinding       binding = PeriodicJobBinding{};
    CollectorScope         scope = CollectorScope::Undefined;
    CollectorTrigger       trigger = CollectorTrigger::Periodic;
    bool                   supports_final = false;
    std::vector<OutputChannel> channels;
    std::string            error;

    bool valid() const { return error.empty() && init; }
};

using ConfigParam = std::pair<std::string, std::string>; // <param_name, description>
using ConfigParams = std::vector<ConfigParam>;
struct CollectorHelpInfo{
    std::string help_text;
    std::vector<ConfigParam> config_params; //<param_name, description>
};
