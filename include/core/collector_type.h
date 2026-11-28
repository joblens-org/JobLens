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
#include <algorithm>
#include <tuple>
#include <utility>
#include <string>
#include <string_view>
#include <vector>
#include <array>
#include <chrono>
#include <optional>
#include <unordered_map>
#include <functional>
#include <any>
#include <nlohmann/json.hpp>
#include <variant>
#include "common/struct2ltable.hpp"
#include "core/job_identity.hpp"
#include "core/job_groups.hpp"
#include "core/sub_attr_reflect.hpp"
#include "job_watcher/builtin_job_facts.hpp"
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

struct CommonJobAttr {
    bool auto_update_child{}; //当为true时，每次采集时自动更新有没有子进程

    static constexpr std::array field_names{"auto_update_child"};
    static constexpr std::array<const char*, 0> optional_fields{};
    static constexpr bool is_optional(std::string_view) { return false; }
};

static_assert(joblens::facts::valid_schema<CommonJobAttr>(),
              "CommonJobAttr::field_names must cover every reflected member");

using SubAttr = JobFactStorage<CommonJobAttr>;

// A typed legacy adapter supplies its wire-key/member mapping next to its
// facts. The open-world default uses JSON field matching instead.
template <class Attr>
constexpr auto legacy_matcher_fields(const Attr*) { return std::tuple<>{}; }

struct SubAttrMatcher {
    std::optional<JobSubType> subtype;
    std::optional<std::string> subtype_name;
    std::optional<nlohmann::json> json_fields;

    template <class Attr, class Field>
    void require(Field Attr::* member, Field value) {
        typed_filters_.emplace_back([member, value = std::move(value)](const SubAttr& attr) {
            const auto* facts = std::get_if<Attr>(&attr);
            // Preserve the legacy contract: a field filter applies only to
            // the facts type that owns it; subtype is a separate filter.
            return facts == nullptr || facts->*member == value;
        });
    }

    void addLegacyFields(const nlohmann::json& request) {
        addLegacyFields(request, static_cast<const SubAttr*>(nullptr));
    }

    bool matches(const SubAttr& attr) const {
        return std::visit([this, &attr](const auto& facts) {
            using Attr = std::decay_t<decltype(facts)>;
            constexpr auto tag = static_cast<const Attr*>(nullptr);
            if (subtype && *subtype != legacy_subtype_of(tag)) return false;
            if constexpr (std::is_same_v<Attr, CommonJobAttr>) {
                // A Common job has no native facts to match; address it by JobID.
                return false;
            } else if constexpr (std::tuple_size_v<decltype(legacy_matcher_fields(tag))> != 0) {
                return std::all_of(typed_filters_.begin(), typed_filters_.end(),
                                   [&attr](const auto& filter) { return filter(attr); });
            } else {
                if (!json_fields) return true;
                const nlohmann::json& json = facts;
                for (auto it = json_fields->begin(); it != json_fields->end(); ++it) {
                    if (!json.contains(it.key()) || json.at(it.key()) != it.value()) return false;
                }
                return true;
            }
        }, attr);
    }

private:
    template <class... Attrs>
    void addLegacyFields(const nlohmann::json& request, const std::variant<Attrs...>*) {
        (addLegacyType(request, static_cast<const Attrs*>(nullptr)), ...);
    }

    template <class Attr>
    void addLegacyType(const nlohmann::json& request, const Attr* tag) {
        std::apply([&](const auto&... fields) {
            (requireIfPresent(request, fields.first, fields.second), ...);
        }, legacy_matcher_fields(tag));
    }

    template <class Attr, class Field>
    void requireIfPresent(const nlohmann::json& request, const char* key, Field Attr::* member) {
        const auto it = request.find(key);
        if (it != request.end()) require(member, it->template get<Field>());
    }

    std::vector<std::function<bool(const SubAttr&)>> typed_filters_;
};

struct Job {
    uint64_t                              JobID{};
    std::string                           JobNativeID{};   // Scheduler-native id, preserved verbatim.
    // 命名空间公共字段是 typed sub_attr 的渲染投影（ES job_info 查询契约），
    // 不是第二事实源：哪个 attr 字段喂哪个分量由 adapter 拥有
    // 比较、
    // 判重与注入永远走结构化事实（ISchedulerAdapter 的 namespace 接口）。
    std::string                           clusterTag{};    // Rendered scheduler tag.
    std::string                           clusterName{};   // Rendered cluster label.
    JobType                                 jobtype{JobType::Unknown};
    JobSubType                              subtype{JobSubType::Unknown};
    // Projection of identity.adapter. Persisted next to the numeric subtype
    // for the frozen row layout; identity is the source.
    std::string                             subtype_name;
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

    // Process-local probing state. Only the validated sub_attr fields are durable.
    bool                                    group_probed{false};
    uint8_t                                 group_probe_attempts{0};

    // Execution evidence: which local run produced these facts. Not identity:
    // the facts name the job, and this block only proves the processes.
    ExecutionEvidence                       execution;

    static constexpr auto reflection() {
        return std::make_tuple(
            std::make_pair("JobID", &Job::JobID),
            std::make_pair("JobNativeID", &Job::JobNativeID),
            std::make_pair("clusterTag", &Job::clusterTag),
            std::make_pair("clusterName", &Job::clusterName),
            std::make_pair("jobtype", &Job::jobtype),
            std::make_pair("subtype", &Job::subtype),
            std::make_pair("sub_attr", &Job::sub_attr)
        );
    }
};

// Facts structs are exposed to Lua through their reflected member names, the
// same schema JSON uses. Job keeps its own reflection().
template <typename T, std::enable_if_t<joblens::facts::is_facts_v<T>, int> = 0>
inline sol::object to_lua(sol::state_view lua, const T& value)
{
    sol::table tbl = lua.create_table();
    joblens::facts::each(value, [&](const char* name, const auto& field, std::size_t) {
        tbl[name] = to_lua(lua, field);
    });
    return tbl;
}

// ---------------------------------------------------------------------------
// Identity projection accessors
//
// The Job is the single source of truth; these accessors are the only way the
// store derives the selector and execution evidence it persists, so no caller
// keeps a second copy of the same evidence.
// ---------------------------------------------------------------------------

inline NativeSelector selector_of(const Job& job) {
    NativeSelector selector;
    selector.adapter = job.subtype_name;
    if (selector.adapter.empty()) {
        if (const char* name = adapter_name_for(job.subtype))
            selector.adapter = name;
    }
    selector.clusterName = job.clusterName;
    selector.clusterTag = job.clusterTag;
    selector.job_native_id = job.JobNativeID;
    return selector;
}

inline const ProcessIdentity& manager_of(const Job& job) {
    return job.execution.manager;
}

inline const std::vector<ProcessIdentity>& pid_identities_of(const Job& job) {
    return job.execution.pid_identities;
}

// The collector-facing pid projection of the one process set. There is no
// separate bare pid list on the Job: the evidence is authoritative.
inline std::vector<pid_t> pids_of(const Job& job) {
    std::vector<pid_t> pids;
    pids.reserve(job.execution.pid_identities.size());
    for (const auto& pid_identity : job.execution.pid_identities)
        pids.push_back(pid_identity.pid);
    return pids;
}

// Replaces the process set while keeping the binding (start ticks, boot id) of
// every process that stays. A pid the set did not hold yet is recorded
// unbound: the next observation or the manual binding fills it in.
inline void set_pids(Job& job, std::vector<pid_t> pids) {
    std::vector<ProcessIdentity> kept;
    kept.reserve(pids.size());
    for (const pid_t pid : pids) {
        const auto existing = std::find_if(
            job.execution.pid_identities.begin(), job.execution.pid_identities.end(),
            [pid](const ProcessIdentity& pid_identity) { return pid_identity.pid == pid; });
        if (existing != job.execution.pid_identities.end()) kept.push_back(*existing);
        else kept.push_back(ProcessIdentity{pid, 0, std::string{}});
    }
    std::sort(kept.begin(), kept.end(),
              [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                  return lhs.pid < rhs.pid;
              });
    kept.erase(std::unique(kept.begin(), kept.end(),
                           [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                               return lhs.pid == rhs.pid;
                           }),
               kept.end());
    job.execution.pid_identities = std::move(kept);
}

// True only when the source read the execution scope successfully: an empty
// pid identity list without this flag is "unknown", not "confirmed empty".
inline bool pid_identities_authoritative_of(const Job& job) {
    return job.execution.pid_identities_authoritative;
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

// Adapter overloads project group facts from their own schemas. A facts
// type without such a projection has no supported group fields.
template <class Attr>
JobGroupView scheduler_groups(const Attr&) { return {}; }

template <class Attr>
bool backfill_scheduler_groups(Attr&, const Attr&) { return false; }

inline JobGroupView getGroups(const Job& job) {
    return std::visit([](const auto& facts) { return scheduler_groups(facts); }, job.sub_attr);
}

// Group facts can only fill empty slots; probe bookkeeping is common.
inline bool fill_missing_groups(Job& target, const Job& source)
{
    const bool changed = std::visit([&source](auto& dst) {
        using Attr = std::decay_t<decltype(dst)>;
        const auto* src = std::get_if<Attr>(&source.sub_attr);
        return src && backfill_scheduler_groups(dst, *src);
    }, target.sub_attr);
    target.group_probed |= source.group_probed;
    if (!target.group_probed && source.group_probe_attempts > target.group_probe_attempts)
        target.group_probe_attempts = source.group_probe_attempts;
    return changed;
}

// The Lua view of a Job: the reflected fact fields plus the pid projection,
// which has no member of its own. The field name stays "JobPIDs" because rules
// read it.
inline sol::object to_lua(sol::state_view lua, const Job& job) {
    sol::table tbl = lua.create_table();
    constexpr auto fields = Job::reflection();
    std::apply([&](const auto&... pair) {
        ((tbl[pair.first] = to_lua(lua, job.*(pair.second))), ...);
    }, fields);
    const std::vector<pid_t> pids = pids_of(job);
    tbl["JobPIDs"] = to_lua(lua, pids);
    return tbl;
}

inline nlohmann::json dump_job(const Job& job) {
    nlohmann::json j;
    j["JobID"] = job.JobID;
    j["JobNativeID"] = job.JobNativeID;
    j["clusterTag"] = job.clusterTag;
    j["clusterName"] = job.clusterName;
    j["jobtype"] = to_string(job.jobtype);
    j["subtype"] = to_string(job.subtype);
    j["JobPIDs"] = pids_of(job);
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
        {"JobNativeID",   job.JobNativeID},
        {"clusterTag",    job.clusterTag}, 
        {"clusterName",   job.clusterName},
        {"jobtype",       to_string(job.jobtype)},
        {"subtype",       to_string(job.subtype)},
        {"JobPIDs",       pids_of(job)},
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
