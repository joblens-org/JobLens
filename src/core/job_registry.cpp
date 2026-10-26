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
// job_registry.cpp
#include "core/job_registry.hpp"
#include <spdlog/spdlog.h>
#include "common/streamer_watcher.hpp"
#include <date/date.h>
#include "common/config.hpp"
#include <signal.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <thread>
#include <chrono>
#include <filesystem>
#include <algorithm>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>

#include "core/job_identity.hpp"
#include "core/job_collection_manager.hpp"
#include "core/collector_registry.hpp"
#include "common/utils.hpp"
#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "common/common_job.hpp"
#include "common/local_rpc.hpp"
#include "core/group_probe_stats.hpp"
#include <algorithm>

#include <set>
#include <type_traits>

// Maintenance-side inventory health (source/queue/reconciliation state) is
// published by the maintenance controller through this seam. When no provider
// is registered, the status handler serves the explicit "not_configured"
// marker instead of guessing a source state.
#if __has_include("core/inventory_status_provider.hpp")
#include "core/inventory_status_provider.hpp"
#define JOBLENS_HAS_INVENTORY_STATUS_PROVIDER 1
#endif

std::pair<std::string, std::string>
split_two(std::string s, char sep = '.') {
    auto p = s.find(sep);
    return (p == s.npos)
           ? std::pair{s, ""}
           : std::pair{s.substr(0, p), s.substr(p + 1)};
}

// hex64: uint64 → 16 字符零填充十六进制，保证字典序与数值序一致
static std::string hex64(uint64_t v) {
    return fmt::format("{:016x}", v);
}

static uint64_t unhex64(const std::string& s) {
    return std::stoull(s, nullptr, 16);
}

static const std::string KEY_PREFIX_JOB      = "j:";
static const std::string KEY_PREFIX_HISTORY  = "h:";
static const std::string KEY_PREFIX_COUNTER  = "c:";
static const std::string KEY_PREFIX_META     = "m:";
static const std::string KEY_PREFIX_IDENTITY = "i:";
static const std::string KEY_PREFIX_BINDING  = "b:";

static constexpr uint32_t kInventorySchemaVersion = 2;
static constexpr size_t kMigrationBatchSize = 64;

static std::string key_job(uint64_t jobID) {
    return KEY_PREFIX_JOB + hex64(jobID);
}

static std::string key_history(uint64_t jobID, uint64_t end_time) {
    return KEY_PREFIX_HISTORY + hex64(jobID) + ":" + hex64(end_time);
}

static std::string key_counter(const std::string& name) {
    return KEY_PREFIX_COUNTER + name;
}

static std::string key_meta(const std::string& name) {
    return KEY_PREFIX_META + name;
}

static std::string key_binding(uint64_t jobID) {
    return KEY_PREFIX_BINDING + hex64(jobID);
}

// Unambiguous identity index key. Canonical components are validated
// (scheduler name, digits/dot native id, normalized step), so the only
// free-form part is the cluster namespace and it is length-prefixed.
static std::string identity_index_key(const CanonicalIdentity& identity) {
    std::string key;
    key.reserve(identity.cluster_namespace.size() + identity.canonical_native.size() + 32);
    key += to_string(identity.scheduler_type);
    key += '|';
    key += std::to_string(identity.cluster_namespace.size());
    key += ':';
    key += identity.cluster_namespace;
    key += '|';
    key += identity.canonical_native;
    key += '|';
    key += identity.step_id;
    return key;
}

static std::string key_identity(const std::string& encoded) {
    return KEY_PREFIX_IDENTITY + encoded;
}

static int64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

static std::string generate_db_uuid() {
    std::ifstream input("/proc/sys/kernel/random/uuid");
    std::string value;
    if (input && std::getline(input, value) && value.size() >= 32) {
        return value;
    }
    std::random_device device;
    return fmt::format("{:016x}{:016x}-{:016x}", 
                       (static_cast<uint64_t>(device()) << 32) | device(),
                       (static_cast<uint64_t>(device()) << 32) | device(),
                       (static_cast<uint64_t>(device()) << 32) | device());
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

static nlohmann::json generation_to_json(const ExecutionGeneration& generation) {
    return nlohmann::json{
        {"boot_id", generation.boot_id},
        {"manager_pid", generation.manager_pid},
        {"manager_start_ticks", generation.manager_start_ticks},
        {"cgroup_identity", generation.cgroup_identity},
    };
}

static ExecutionGeneration generation_from_json(const nlohmann::json& in) {
    ExecutionGeneration generation;
    generation.boot_id = in.value("boot_id", std::string{});
    generation.manager_pid = in.value("manager_pid", pid_t{0});
    generation.manager_start_ticks = in.value("manager_start_ticks", uint64_t{0});
    generation.cgroup_identity = in.value("cgroup_identity", std::string{});
    return generation;
}

static nlohmann::json process_to_json(const ProcessIdentity& process) {
    return nlohmann::json{
        {"pid", process.pid},
        {"start_ticks", process.start_ticks},
        {"boot_id", process.boot_id},
    };
}

static ProcessIdentity process_from_json(const nlohmann::json& in) {
    ProcessIdentity process;
    process.pid = in.value("pid", pid_t{0});
    process.start_ticks = in.value("start_ticks", uint64_t{0});
    process.boot_id = in.value("boot_id", std::string{});
    return process;
}

static std::vector<ProcessIdentity> normalized_processes(std::vector<ProcessIdentity> processes) {
    std::sort(processes.begin(), processes.end(),
              [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                  if (lhs.pid != rhs.pid) return lhs.pid < rhs.pid;
                  return lhs.start_ticks < rhs.start_ticks;
              });
    processes.erase(std::unique(processes.begin(), processes.end(),
                                [](const ProcessIdentity& lhs, const ProcessIdentity& rhs) {
                                    return lhs == rhs;
                                }),
                    processes.end());
    return processes;
}

static void write_job_fields(nlohmann::json& out, const Job& job) {
    out["jobtype"]       = static_cast<int>(job.jobtype);
    out["subtype"]       = static_cast<int>(job.subtype);
    out["pids"]          = job.JobPIDs;
    out["collectors"]    = job.CollectorNames;
    out["native_job_id"] = job.NativeJobID;
    out["cluster_name"]  = job.cluster_name;
    out["clusterTag"]    = job.clusterTag;
    std::visit([&out](const auto& obj) {
        out["sub_attr"] = nlohmann::json(obj);
    }, job.sub_attr);
}

static nlohmann::json job_record_json(const Job& job, const InventoryRecord& meta) {
    nlohmann::json out;
    write_job_fields(out, job);
    out["schema_version"] = kInventorySchemaVersion;
    out["revision"]       = meta.facts_revision;
    out["state"]          = meta.state;
    out["quarantined"]    = meta.quarantined;
    out["first_seen_us"]  = meta.first_seen_us;
    out["last_seen_us"]   = meta.last_seen_us;
    if (meta.selector.scheduler_type == SchedulerType::Unknown) {
        out["selector"] = nullptr;
    } else {
        out["selector"] = meta.selector;
    }
    if (meta.identity_complete) out["identity"] = meta.identity;
    else                        out["identity"] = nullptr;
    out["generation"]     = generation_to_json(meta.generation);
    out["processes"]      = nlohmann::json::array();
    for (const auto& process : meta.processes) {
        out["processes"].push_back(process_to_json(process));
    }
    return out;
}

static Job persist_json_to_job(const std::string& json_str) {
    auto j = nlohmann::json::parse(json_str);
    Job job;
    job.jobtype       = static_cast<JobType>(j.value("jobtype", 0));
    job.subtype       = static_cast<JobSubType>(j.value("subtype", 0));
    job.JobPIDs       = j.value("pids", nlohmann::json::array()).get<std::vector<pid_t>>();
    job.CollectorNames = j.value("collectors", nlohmann::json::array()).get<std::vector<std::string>>();
    job.NativeJobID   = j.value("native_job_id", "");
    job.cluster_name  = j.value("cluster_name", "");
    job.clusterTag    = j.value("clusterTag", "");

    if (job.subtype == JobSubType::Condor) {
        if (j.contains("sub_attr")) {
            job.sub_attr = j["sub_attr"].get<CondorJobAttr>();
        } else {
            job.sub_attr = CondorJobAttr{};
        }
    } else if (job.subtype == JobSubType::Slurm) {
        if (j.contains("sub_attr")) {
            job.sub_attr = j["sub_attr"].get<SlurmJobAttr>();
        } else {
            job.sub_attr = SlurmJobAttr{};
        }
    } else {
        job.sub_attr = CommonJobAttr{.auto_update_child = true};
    }

    return job;
}

struct StoredRecord {
    Job job;
    InventoryRecord meta;
};

static StoredRecord parse_job_record(const std::string& raw, uint64_t internal_id) {
    StoredRecord stored;
    stored.job = persist_json_to_job(raw);
    stored.job.JobID = internal_id;

    const auto j = nlohmann::json::parse(raw);
    auto& meta = stored.meta;
    meta.internal_id       = internal_id;
    meta.facts_revision    = j.value("revision", uint64_t{1});
    meta.state             = j.value("state", std::string{"active"});
    meta.quarantined       = j.value("quarantined", false);
    meta.first_seen_us     = j.value("first_seen_us", int64_t{0});
    meta.last_seen_us      = j.value("last_seen_us", int64_t{0});
    if (meta.facts_revision == 0) meta.facts_revision = 1;

    if (j.contains("selector") && j["selector"].is_object() &&
        j["selector"].value("scheduler_type", std::string{"unknown"}) != "unknown") {
        try {
            meta.selector = j["selector"].get<NativeSelector>();
        } catch (const std::exception& error) {
            spdlog::warn("JobRegistry: invalid stored selector for job {}: {}",
                         internal_id, error.what());
        }
    }
    if (j.contains("identity") && j["identity"].is_object()) {
        try {
            meta.identity = j["identity"].get<CanonicalIdentity>();
            meta.identity_complete = true;
        } catch (const std::exception& error) {
            spdlog::warn("JobRegistry: invalid stored identity for job {}: {}",
                         internal_id, error.what());
        }
    }
    if (j.contains("generation") && j["generation"].is_object()) {
        meta.generation = generation_from_json(j["generation"]);
    }
    if (j.contains("processes") && j["processes"].is_array()) {
        for (const auto& process : j["processes"]) {
            if (process.is_object()) meta.processes.push_back(process_from_json(process));
        }
        meta.processes = normalized_processes(std::move(meta.processes));
    }
    return stored;
}

static nlohmann::json binding_to_json(const BindingRecord& binding) {
    return nlohmann::json{
        {"internal_id", binding.internal_id},
        {"revision", binding.revision},
        {"mode", to_string(binding.mode)},
        {"auto_collectors", binding.auto_collectors},
        {"manual_collectors", binding.manual_collectors},
        {"policy_revision", binding.policy_revision},
    };
}

std::optional<CollectionMode> collection_mode_from_string(const std::string& raw)
{
    if (raw == "manual") return CollectionMode::Manual;
    if (raw == "auto") return CollectionMode::Auto;
    return std::nullopt;
}

static BindingRecord binding_from_json(const nlohmann::json& in, uint64_t internal_id) {
    BindingRecord binding;
    binding.internal_id = internal_id;
    binding.revision = in.value("revision", uint64_t{0});
    if (const auto mode = collection_mode_from_string(in.value("mode", std::string{"auto"}))) {
        binding.mode = *mode;
    }
    binding.auto_collectors =
        in.value("auto_collectors", nlohmann::json::array()).get<std::vector<std::string>>();
    binding.manual_collectors =
        in.value("manual_collectors", nlohmann::json::array()).get<std::vector<std::string>>();
    binding.policy_revision = in.value("policy_revision", std::string{});
    return binding;
}

std::vector<std::string> BindingRecord::effectiveCollectors() const
{
    return mode == CollectionMode::Manual ? manual_collectors : auto_collectors;
}

// ---------------------------------------------------------------------------
// Fact merging
// ---------------------------------------------------------------------------

static bool merge_text(std::string& target, const std::string& source) {
    if (source.empty() || target == source) return false;
    target = source;
    return true;
}

template <typename T>
static bool merge_scalar(T& target, T source) {
    if (source == T{} || target == source) return false;
    target = source;
    return true;
}

static bool merge_flag(bool& target, bool source) {
    if (!source || target) return false;
    target = true;
    return true;
}

static bool merge_condor_attr(CondorJobAttr& target, const CondorJobAttr& source) {
    bool changed = false;
    changed |= merge_scalar(target.starter_pid, source.starter_pid);
    changed |= merge_scalar(target.cluster_id, source.cluster_id);
    changed |= merge_scalar(target.proc_id, source.proc_id);
    changed |= merge_text(target.scheduler_host, source.scheduler_host);
    changed |= merge_text(target.scheduler_name, source.scheduler_name);
    changed |= merge_text(target.slots_cgroup_path, source.slots_cgroup_path);
    changed |= merge_text(target.owner, source.owner);
    changed |= merge_text(target.job_ad_path, source.job_ad_path);
    changed |= merge_text(target.collector_host, source.collector_host);
    changed |= merge_flag(target.auto_update_child, source.auto_update_child);
    return changed;
}

static bool merge_slurm_attr(SlurmJobAttr& target, const SlurmJobAttr& source) {
    bool changed = false;
    changed |= merge_scalar(target.stepd_pid, source.stepd_pid);
    changed |= merge_scalar(target.job_id, source.job_id);
    changed |= merge_scalar(target.step_id, source.step_id);
    changed |= merge_text(target.cluster_name, source.cluster_name);
    changed |= merge_text(target.cgroup_path, source.cgroup_path);
    changed |= merge_text(target.user, source.user);
    changed |= merge_text(target.job_name, source.job_name);
    changed |= merge_text(target.partition, source.partition);
    changed |= merge_flag(target.auto_update_child, source.auto_update_child);
    return changed;
}

static bool merge_common_attr(CommonJobAttr& target, const CommonJobAttr& source) {
    return merge_flag(target.auto_update_child, source.auto_update_child);
}

// Merge observation facts into the stored facts. Empty/default observation
// fields never erase known facts; CollectorNames is never taken from an
// observation because selection is intent, not a discovery fact.
static bool merge_job_facts(Job& target, const Job& observed) {
    bool changed = false;

    if (observed.subtype != JobSubType::Unknown && target.subtype != observed.subtype) {
        target.subtype = observed.subtype;
        changed = true;
    }
    if (observed.jobtype != JobType::Unknown && target.jobtype != observed.jobtype) {
        target.jobtype = observed.jobtype;
        changed = true;
    }
    changed |= merge_text(target.NativeJobID, observed.NativeJobID);
    changed |= merge_text(target.cluster_name, observed.cluster_name);
    changed |= merge_text(target.clusterTag, observed.clusterTag);

    if (!observed.JobPIDs.empty()) {
        auto pids = observed.JobPIDs;
        std::sort(pids.begin(), pids.end());
        pids.erase(std::unique(pids.begin(), pids.end()), pids.end());
        if (pids != target.JobPIDs) {
            target.JobPIDs = std::move(pids);
            changed = true;
        }
    }

    std::visit([&changed, &target](const auto& source) {
        using T = std::decay_t<decltype(source)>;
        if constexpr (std::is_same_v<T, CondorJobAttr>) {
            if (auto* current = std::get_if<CondorJobAttr>(&target.sub_attr)) {
                changed |= merge_condor_attr(*current, source);
            } else {
                target.sub_attr = source;
                changed = true;
            }
        } else if constexpr (std::is_same_v<T, SlurmJobAttr>) {
            if (auto* current = std::get_if<SlurmJobAttr>(&target.sub_attr)) {
                changed |= merge_slurm_attr(*current, source);
            } else {
                target.sub_attr = source;
                changed = true;
            }
        } else {
            if (auto* current = std::get_if<CommonJobAttr>(&target.sub_attr)) {
                changed |= merge_common_attr(*current, source);
            } else {
                target.sub_attr = source;
                changed = true;
            }
        }
    }, observed.sub_attr);

    return changed;
}

static nlohmann::json facts_fingerprint(const Job& job, const InventoryRecord& meta) {
    nlohmann::json out;
    out["jobtype"] = static_cast<int>(job.jobtype);
    out["subtype"] = static_cast<int>(job.subtype);
    out["native_job_id"] = job.NativeJobID;
    out["cluster_name"] = job.cluster_name;
    out["clusterTag"] = job.clusterTag;

    auto pids = job.JobPIDs;
    std::sort(pids.begin(), pids.end());
    pids.erase(std::unique(pids.begin(), pids.end()), pids.end());
    out["pids"] = pids;

    nlohmann::json processes = nlohmann::json::array();
    for (const auto& process : normalized_processes(meta.processes)) {
        processes.push_back(process_to_json(process));
    }
    out["processes"] = std::move(processes);

    std::visit([&out](const auto& attr) {
        out["sub_attr"] = nlohmann::json(attr);
    }, job.sub_attr);
    out["generation"] = generation_to_json(meta.generation);
    return out;
}

// ---------------------------------------------------------------------------
// Identity helpers
// ---------------------------------------------------------------------------

// Apply a resolved logical identity to typed facts. The native string and
// namespace are authoritative; scheduler numeric fields are derived from the
// canonical form so they can never disagree with the identity index.
static void apply_identity_to_facts(Job& job, const NativeSelector& selector,
                                    const CanonicalIdentity& identity) {
    job.NativeJobID = identity.native_job_id;
    if (!selector.cluster_name.empty()) job.cluster_name = selector.cluster_name;
    if (!selector.cluster_tag.empty()) job.clusterTag = selector.cluster_tag;
    if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys) {
        job.jobtype = JobType::Job;
    }

    if (identity.scheduler_type == SchedulerType::Condor) {
        job.subtype = JobSubType::Condor;
        if (!std::holds_alternative<CondorJobAttr>(job.sub_attr)) {
            job.sub_attr = CondorJobAttr{};
        }
        auto& attr = std::get<CondorJobAttr>(job.sub_attr);
        const auto dot = identity.canonical_native.find('.');
        if (dot != std::string::npos) {
            try {
                attr.cluster_id = std::stoull(identity.canonical_native.substr(0, dot));
                attr.proc_id = std::stoull(identity.canonical_native.substr(dot + 1));
            } catch (const std::exception&) {
                // Canonical validation already guaranteed decimal parts.
            }
        }
    } else {
        job.subtype = JobSubType::Slurm;
        if (!std::holds_alternative<SlurmJobAttr>(job.sub_attr)) {
            job.sub_attr = SlurmJobAttr{};
        }
        auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
        if (attr.cluster_name.empty()) attr.cluster_name = selector.cluster_name;
        if (!selector.step_id.empty()) {
            if (const auto numeric = slurm_step_value(selector.step_id)) {
                attr.step_id = *numeric;
            }
        }
    }
}

// Best-effort selector reconstruction for legacy (schema < 2) records. A
// missing namespace or native id yields an incomplete selector, never a guess.
static NativeSelector selector_from_legacy_job(const Job& job) {
    NativeSelector selector;
    if (job.subtype == JobSubType::Condor) {
        selector.scheduler_type = SchedulerType::Condor;
        selector.cluster_name = job.cluster_name;
        selector.cluster_tag = job.clusterTag;
        selector.native_job_id = job.NativeJobID;
        if (selector.native_job_id.empty()) {
            if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
                selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
            }
        }
    } else if (job.subtype == JobSubType::Slurm) {
        selector.scheduler_type = SchedulerType::Slurm;
        selector.cluster_name = job.cluster_name;
        selector.native_job_id = job.NativeJobID;
        if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
            if (selector.cluster_name.empty()) selector.cluster_name = attr->cluster_name;
            if (selector.native_job_id.empty()) {
                selector.native_job_id = std::to_string(attr->job_id);
            }
            if (attr->step_id != 0) {
                if (const auto name = slurm_step_name(attr->step_id)) {
                    selector.step_id = *name;
                } else {
                    selector.step_id = std::to_string(attr->step_id);
                }
            }
        }
    }
    return selector;
}

std::string json2JobOpt(const nlohmann::json& obj, Job& job) {
    auto opt = obj["opt"].get<std::string>();

    // Internal IDs parse explicitly as uint64: a JSON number or a decimal
    // string, never a 32-bit default. Absent keeps the legacy auto-allocate
    // sentinel; reserved (0) keeps its historical meaning; anything else is
    // an explicit request error.
    const auto legacy_id = parse_legacy_job_id(
        obj.contains("JobID") ? obj["JobID"] : nlohmann::json(nullptr));
    if (legacy_id.status == LegacyIdStatus::Invalid) {
        throw std::runtime_error(
            "invalid JobID: expected a uint64 number or decimal string");
    }
    job.JobID = legacy_id.value;
    job.JobPIDs = obj.value("JobPIDs", nlohmann::json::array()).get<std::vector<pid_t>>();
    job.CollectorNames = obj.value("Lens", nlohmann::json::array()).get<std::vector<std::string>>();
    if (job.JobPIDs.size() == 0) {
        spdlog::warn("JobRegistry: job ID {} has empty PID list", job.JobID);
    }
    for (auto pid: job.JobPIDs) {
        if (pid < 0) {
            spdlog::warn("JobRegistry: invalid PID {} in job ID {}", pid, job.JobID);
        }
    }

    auto [type, subtype] = split_two(obj["type"].get<std::string>());
    if(type.compare("job") == 0){
        job.jobtype = JobType::Job;
    }else if (type.compare("sys") == 0){
        job.jobtype = JobType::Sys;
    }else {
        job.jobtype = JobType::Unknown;
        throw std::runtime_error("invalid job type: " + type);
    }
    
    if (subtype.compare("condor") == 0){
        job.subtype = JobSubType::Condor;
        auto sub = obj.value("sub_attr", nlohmann::json::object());
        auto cid = sub.value("cluster_id", nlohmann::json(0u)).get<size_t>();
        auto proc = sub.value("proc_id", nlohmann::json(0u)).get<size_t>();
        if (!obj.contains("sub_attr")) {
            spdlog::info("JobRegistry: condor job sub_attr not provided, using defaults cluster_id={}, proc_id={}", cid, proc);
        }
        job.sub_attr = CondorJobAttr{
            .auto_update_child = obj.value("auto_update_child", true),
            .cluster_id = cid,
            .proc_id = proc,
            .owner = sub.value("owner", ""),
        };
        job.NativeJobID = fmt::format("{}.{}", cid, proc);
    }else if (subtype.compare("slurm") == 0){
        job.subtype = JobSubType::Slurm;
        auto sub = obj.value("sub_attr", nlohmann::json::object());
        auto jid = sub.value("job_id", nlohmann::json(0u)).get<size_t>();
        auto sid = sub.value("step_id", nlohmann::json(0u)).get<uint32_t>();
        if (!obj.contains("sub_attr")) {
            spdlog::info("JobRegistry: slurm job sub_attr not provided, using defaults job_id={}, step_id={}", jid, sid);
        }
        job.sub_attr = SlurmJobAttr{
            .auto_update_child = obj.value("auto_update_child", true),
            .job_id = jid,
            .step_id = sid,
            .user = sub.value("user", ""),
        };
        job.NativeJobID = fmt::format("{}", jid);
    }else if (subtype.compare("common") == 0){
        job.subtype = JobSubType::Common;
        job.sub_attr = CommonJobAttr{
            .auto_update_child = obj.value("auto_update_child", true)
        };
    }

    spdlog::info("JobRegistry: json2JobOpt parsed opt={}, type={}, subtype={}, JobID={}, "
                 "PIDs_count={}, Lens_count={}, nativeID={}",
                 opt, type, subtype, job.JobID, job.JobPIDs.size(),
                 job.CollectorNames.size(), job.NativeJobID);
    
    return opt;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

JobRegistry::JobRegistry() : JobRegistry(JobRegistryOptions{}) {}

JobRegistry::JobRegistry(const JobRegistryOptions& options) {
    Job fake_job;
    fake_job.JobID = 0;
    fake_job.JobPIDs = {1};
    fake_job.jobtype = JobType::Sys;
    fake_job.subtype = JobSubType::Common;
    fake_job.sub_attr = CommonJobAttr{
        .auto_update_child = false
    };
    fake_job.CollectorNames = {"Easter egg"}; //实际上这里的添加方式不一样，所以无所谓这里的名字
    jobs_.emplace(0, fake_job); // 直接添加避免拉起回调

    migration_batch_limit_ = options.migration_batch_limit;

    if (init_job_db(options)) {
        spdlog::info("JobRegistry: store ready at {} (schema {}, migration {})",
                     job_db ? "db" : "memory", schema_version_, migration_state_);
    } else {
        db_running = false;
        spdlog::warn("JobRegistry: job db not running");
    }

    if (options.enable_job_watchers) init_job_watcher();
    if (options.enable_pid_tracker) init_pid_tracker();
    if (options.register_rpc) regRPCHandle();
};

void JobRegistry::init_job_watcher() {
    enable_auto_add_condor_job = Config::instance().getBool("job_registry_config", "auto_add_condorjob", false);
    if (enable_auto_add_condor_job) {
        condor_job_watcher_ = std::make_unique<CondorJobWatcher>();
        condor_job_watcher_->register_add_if(
            [this](Job job){
                this->addJob(job);
            }
        );
        spdlog::info("JobRegistry: enabled auto add condor job");
    }

    enable_auto_add_slurm_job = Config::instance().getBool("job_registry_config", "auto_add_slurmjob", false);
    if (enable_auto_add_slurm_job) {
        slurm_job_watcher_ = std::make_unique<SlurmJobWatcher>();
        slurm_job_watcher_->register_add_if(
            [this](Job job){
                this->addJob(job);
            }
        );
        spdlog::info("JobRegistry: enabled auto add slurm job");
    }
}

// 从 Job 取 cgroup 路径: Slurm 用 cgroup_path, Condor 用 slots_cgroup_path。
static std::string job_cgroup_path(const Job& job) {
    try {
        if (job.subtype == JobSubType::Slurm)
            return std::get<SlurmJobAttr>(job.sub_attr).cgroup_path;
        if (job.subtype == JobSubType::Condor)
            return std::get<CondorJobAttr>(job.sub_attr).slots_cgroup_path;
    } catch (const std::bad_variant_access&) {
    }
    return {};
}

// 解析 Job 的 cgroup 路径为 cgroup_id 并缓存到 job.cgroup_id(仅解析一次)。
// 删除作业时直接复用缓存的 id, 不依赖 cgroup 目录仍存在(作业结束后调度器可能已删除该目录)。
static void resolve_and_cache_cgroup_id(Job& job) {
    if (job.cgroup_id != 0) return;  // 已缓存, 跳过重复 stat
    const auto cg = job_cgroup_path(job);
    if (cg.empty()) return;
    if (auto cgid = EbpfCommon::cgroup_path_to_id(cg)) {
        job.cgroup_id = *cgid;
    }
}

void JobRegistry::init_pid_tracker() {
    pid_tracker_ = std::make_unique<JobPidTracker>();
    pid_tracker_->set_fork_cb([this](uint32_t pid, uint64_t job_id) { on_kernel_fork(pid, job_id); });
    pid_tracker_->set_exit_cb([this](uint32_t pid, uint64_t job_id) { on_kernel_exit(pid, job_id); });
    if (!pid_tracker_->start()) {
        spdlog::warn("JobRegistry: pid tracker start failed, kernel pid2job maintenance disabled");
        pid_tracker_.reset();
        return;
    }
    reconcile_running_ = true;
    reconcile_thread_ = std::make_unique<std::thread>([this]() { reconcile_loop(); });
    spdlog::info("JobRegistry: pid tracker enabled");
}

void JobRegistry::stop_pid_tracker() {
    if (reconcile_running_) {
        reconcile_running_ = false;
        if (reconcile_thread_ && reconcile_thread_->joinable()) reconcile_thread_->join();
        reconcile_thread_.reset();
    }
    if (pid_tracker_) {
        pid_tracker_->stop();
        pid_tracker_.reset();
    }
}

// JobEvent::Added 时调用: 向内核登记 cgroup 归属 + 写入根 pid 种子, 内核 fork hook 据此繁衍。
void JobRegistry::sync_job_to_kernel(Job& job) {
    if (!pid_tracker_ || job.JobID == 0) return;
    // 确保 cgroup_id 已缓存(从 db 恢复的旧 job 可能未缓存, 此处兜底解析一次)。
    resolve_and_cache_cgroup_id(job);
    if (job.cgroup_id != 0) {
        if (pid_tracker_->set_cgroup_job(job.cgroup_id, job.JobID)) {
            spdlog::debug("JobRegistry: kernel cgroup2job registered job_id={} cgroup_id={}",
                          job.JobID, job.cgroup_id);
        } else {
            spdlog::warn("JobRegistry: kernel cgroup2job write failed job_id={} cgroup_id={}",
                         job.JobID, job.cgroup_id);
        }
    }
    for (pid_t pid : job.JobPIDs) {
        if (!pid_tracker_->set_pid_job(static_cast<uint32_t>(pid), job.JobID)) {
            spdlog::warn("JobRegistry: kernel pid2job seed write failed job_id={} pid={}", job.JobID, pid);
        }
    }
    spdlog::debug("JobRegistry: synced job to kernel job_id={} seed_pids={} has_cgroup={}",
                  job.JobID, job.JobPIDs.size(), job.cgroup_id != 0);
}

// JobEvent::Removed 时调用: 删 cgroup2job + 清理该 Job 在 pid2job 的所有已知 pid。
void JobRegistry::remove_job_from_kernel(const Job& job) {
    if (!pid_tracker_ || job.JobID == 0) return;
    if (job.cgroup_id != 0) {
        pid_tracker_->del_cgroup_job(job.cgroup_id);
        spdlog::debug("JobRegistry: kernel cgroup2job removed job_id={} cgroup_id={}",
                      job.JobID, job.cgroup_id);
    }
    for (pid_t pid : job.JobPIDs) {
        pid_tracker_->del_pid_job(static_cast<uint32_t>(pid));
    }
    spdlog::debug("JobRegistry: removed job from kernel job_id={} cleared_pids={} has_cgroup={}",
                  job.JobID, job.JobPIDs.size(), job.cgroup_id != 0);
}

// ringbuf FORK: 内核已将 child 写入 pid2job, 这里同步更新用户态 Job.JobPIDs。
void JobRegistry::on_kernel_fork(uint32_t pid, uint64_t job_id) {
    std::unique_lock lg(mtx_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        spdlog::debug("JobRegistry: kernel FORK event for unknown job_id={} pid={}, ignored "
                      "(job may have just been removed)", job_id, pid);
        return;
    }
    auto& pids = it->second.JobPIDs;
    if (std::find(pids.begin(), pids.end(), static_cast<pid_t>(pid)) == pids.end()) {
        pids.push_back(static_cast<pid_t>(pid));
        spdlog::trace("JobRegistry: kernel FORK added pid={} to job_id={} (now {} pids)",
                      pid, job_id, pids.size());
    }
}

// ringbuf EXIT: 内核已从 pid2job 删除, 这里同步移除用户态 Job.JobPIDs。
void JobRegistry::on_kernel_exit(uint32_t pid, uint64_t job_id) {
    std::unique_lock lg(mtx_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        spdlog::debug("JobRegistry: kernel EXIT event for unknown job_id={} pid={}, ignored", job_id, pid);
        return;
    }
    auto& pids = it->second.JobPIDs;
    auto before = pids.size();
    pids.erase(std::remove(pids.begin(), pids.end(), static_cast<pid_t>(pid)), pids.end());
    if (pids.size() != before) {
        spdlog::trace("JobRegistry: kernel EXIT removed pid={} from job_id={} (now {} pids)",
                      pid, job_id, pids.size());
    }
}

// 低频对账: 周期性把当前所有 Job 的归属重新写回内核, 校正 ringbuf 丢事件导致的漂移。
void JobRegistry::reconcile_loop() {
    while (reconcile_running_) {
        for (int i = 0; i < 300 && reconcile_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!reconcile_running_) break;
        auto jobs = snapshot();
        for (auto& job : jobs) {
            sync_job_to_kernel(job);
        }
        spdlog::debug("JobRegistry: pid tracker reconcile pass done, resynced {} jobs to kernel", jobs.size());

        // ringbuf 溢出是静默的: 一旦发生, JobPIDs 会悄悄失真。按对账周期上报增量,
        // 让"归属信息是否可信"有据可查(可用 JobRegistry/pid_tracker_stats 查累计值)。
        if (pid_tracker_) {
            const uint64_t dropped = pid_tracker_->dropped_events();
            if (dropped > last_dropped_events_) {
                spdlog::warn("JobRegistry: job_event ringbuf overflow, {} new pid attribution events dropped "
                             "(cumulative {}); JobPIDs may be incomplete until the next cgroup resync",
                             dropped - last_dropped_events_, dropped);
                last_dropped_events_ = dropped;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Section 9 RPC surface (doc/job_inventory_collection_decoupling_plan.md).
//
// Selector resolution belongs to the RPC layer: JobCollectionManager only
// accepts already-resolved internal ids. Everything below reads state through
// the public JobRegistry store API, so no registry lock is held while a
// mutation or provider call runs.
// ---------------------------------------------------------------------------
namespace {

nlohmann::json error_payload(const std::string& code, const std::string& message)
{
    nlohmann::json out;
    out["status"] = "error";
    out["code"] = code;
    out["message"] = message;
    return out;
}

nlohmann::json error_payload(const std::string& code, const std::string& message,
                             const std::vector<std::string>& candidates)
{
    auto out = error_payload(code, message);
    out["candidates"] = candidates;
    return out;
}

constexpr char kBase64UrlAlphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

// Opaque paging cursor: the wire form must not invite callers to parse it, so
// it is base64url(JSON) rather than a readable offset.
std::string base64url_encode(const std::string& raw)
{
    std::string out;
    out.reserve((raw.size() + 2) / 3 * 4);
    size_t index = 0;
    while (index + 2 < raw.size()) {
        const uint32_t block = (static_cast<uint8_t>(raw[index]) << 16) |
                               (static_cast<uint8_t>(raw[index + 1]) << 8) |
                               static_cast<uint8_t>(raw[index + 2]);
        out.push_back(kBase64UrlAlphabet[(block >> 18) & 0x3f]);
        out.push_back(kBase64UrlAlphabet[(block >> 12) & 0x3f]);
        out.push_back(kBase64UrlAlphabet[(block >> 6) & 0x3f]);
        out.push_back(kBase64UrlAlphabet[block & 0x3f]);
        index += 3;
    }
    const size_t rest = raw.size() - index;
    if (rest == 1) {
        const uint32_t block = static_cast<uint8_t>(raw[index]) << 16;
        out.push_back(kBase64UrlAlphabet[(block >> 18) & 0x3f]);
        out.push_back(kBase64UrlAlphabet[(block >> 12) & 0x3f]);
    } else if (rest == 2) {
        const uint32_t block = (static_cast<uint8_t>(raw[index]) << 16) |
                               (static_cast<uint8_t>(raw[index + 1]) << 8);
        out.push_back(kBase64UrlAlphabet[(block >> 18) & 0x3f]);
        out.push_back(kBase64UrlAlphabet[(block >> 12) & 0x3f]);
        out.push_back(kBase64UrlAlphabet[(block >> 6) & 0x3f]);
    }
    return out;
}

std::optional<std::string> base64url_decode(const std::string& text)
{
    std::string out;
    out.reserve(text.size() / 4 * 3);
    uint32_t buffer = 0;
    int bits = 0;
    for (const char c : text) {
        int value = -1;
        if (c >= 'A' && c <= 'Z') value = c - 'A';
        else if (c >= 'a' && c <= 'z') value = c - 'a' + 26;
        else if (c >= '0' && c <= '9') value = c - '0' + 52;
        else if (c == '-') value = 62;
        else if (c == '_') value = 63;
        else if (c == '=') continue;
        else return std::nullopt;
        buffer = (buffer << 6) | static_cast<uint32_t>(value);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((buffer >> bits) & 0xff));
        }
    }
    return out;
}

// Snapshot revision for a bounded page stream. A cursor captured on page one is
// only valid while the same record set (ids, facts revisions, lifecycle state)
// is observed; anything else is a changed snapshot, never a silent skip.
uint64_t inventory_snapshot_revision(const std::vector<InventoryRecord>& records)
{
    std::vector<const InventoryRecord*> ordered;
    ordered.reserve(records.size());
    for (const auto& record : records) ordered.push_back(&record);
    std::sort(ordered.begin(), ordered.end(),
              [](const InventoryRecord* lhs, const InventoryRecord* rhs) {
                  return lhs->internal_id < rhs->internal_id;
              });

    uint64_t hash = 0xcbf29ce484222325ULL;
    auto mix = [&hash](uint64_t value) {
        for (int byte = 0; byte < 8; ++byte) {
            hash ^= (value >> (byte * 8)) & 0xffULL;
            hash *= 0x100000001b3ULL;
        }
    };
    for (const auto* record : ordered) {
        mix(record->internal_id);
        mix(record->facts_revision);
        mix(record->quarantined ? 1u : 0u);
        mix(record->identity_complete ? 2u : 0u);
        mix(record->state == "closing" ? 3u : (record->state == "suspect" ? 4u : 5u));
    }
    // Stay inside the signed 63-bit range so JavaScript consumers keep the
    // exact value; the revision is an opaque token, not a counter.
    return hash & 0x7fffffffffffffffULL;
}

std::string inventory_source(const InventoryRecord& record)
{
    switch (record.selector.scheduler_type) {
        case SchedulerType::Condor: return "condor";
        case SchedulerType::Slurm:  return "slurm";
        case SchedulerType::Unknown: break;
    }
    return "manual";
}

nlohmann::json inventory_record_json(JobRegistry& registry, const InventoryRecord& record)
{
    nlohmann::json out;
    out["instance_id"] = record.instance_id;
    // New APIs carry internal ids as decimal strings (JavaScript uint64 safety).
    out["internal_job_id"] = format_internal_id(record.internal_id);
    out["schema_version"] = kInventorySchemaVersion;
    out["facts_revision"] = record.facts_revision;
    out["state"] = record.state;
    out["source"] = inventory_source(record);
    out["quarantined"] = record.quarantined;
    out["identity_complete"] = record.identity_complete;
    out["scheduler_type"] = to_string(record.selector.scheduler_type);
    out["cluster_name"] = record.selector.cluster_name;
    out["cluster_tag"] = record.selector.cluster_tag;
    out["native_job_id"] = record.selector.native_job_id;
    out["step_id"] = record.selector.step_id;
    out["first_seen_us"] = record.first_seen_us;
    out["last_seen_us"] = record.last_seen_us;
    out["generation"] = generation_to_json(record.generation);
    out["processes"] = nlohmann::json::array();
    for (const auto& process : record.processes) {
        out["processes"].push_back(process_to_json(process));
    }
    if (const auto job = registry.findJob(record.internal_id)) {
        out["attributes"] = std::visit(
            [](const auto& attr) { return nlohmann::json(attr); }, job->sub_attr);
    } else {
        out["attributes"] = nullptr;
    }
    return out;
}

std::vector<std::string> instance_ids_of(const std::vector<InventoryRecord>& records)
{
    std::vector<std::string> ids;
    ids.reserve(records.size());
    for (const auto& record : records) ids.push_back(record.instance_id);
    return ids;
}

void sort_records(std::vector<InventoryRecord>& records)
{
    std::sort(records.begin(), records.end(),
              [](const InventoryRecord& lhs, const InventoryRecord& rhs) {
                  return lhs.internal_id < rhs.internal_id;
              });
}

// Resolution outcome of section 9.2 selectors. Empty `code` means success;
// on ambiguity `records` carries the candidate instances.
struct TargetResolution {
    std::vector<InventoryRecord> records;
    std::string code;
    std::string message;

    bool ok() const { return code.empty(); }
};

std::string optional_string(const nlohmann::json& node, const char* key)
{
    if (!node.is_object() || !node.contains(key) || node[key].is_null()) return {};
    const auto& value = node[key];
    if (value.is_string()) return value.get<std::string>();
    if (value.is_number_integer() || value.is_number_unsigned()) return value.dump();
    throw std::runtime_error(std::string(key) + " must be a string");
}

// Condor namespaces separate schedds. A selector without a cluster_tag cannot
// pick between them, so it matches the whole cluster and then reports the
// ambiguity instead of guessing one schedd.
bool namespace_matches(const CanonicalIdentity& stored, const NativeSelector& selector,
                       const CanonicalIdentity& requested)
{
    if (selector.scheduler_type != SchedulerType::Condor)
        return stored.cluster_namespace == requested.cluster_namespace;
    if (!selector.cluster_tag.empty())
        return stored.cluster_namespace == requested.cluster_namespace;
    if (stored.cluster_namespace == requested.cluster_namespace) return true;
    return stored.cluster_namespace.rfind(requested.cluster_namespace + "@", 0) == 0;
}

TargetResolution resolve_native_target(JobRegistry& registry, const NativeSelector& selector,
                                       const std::string& scope, bool ensure_registered)
{
    TargetResolution result;
    const auto identity = canonicalize(selector);
    if (!identity) {
        result.code = "invalid_request";
        result.message =
            "incomplete native selector: scheduler_type, cluster_name and native_job_id "
            "must name one scheduler identity (Condor native ids are '<ClusterId>.<ProcId>')";
        return result;
    }

    std::vector<InventoryRecord> matches;
    for (auto& record : registry.inventoryRecords()) {
        if (is_reserved_internal_id(record.internal_id)) continue;
        if (!record.identity_complete) continue;
        if (record.identity.scheduler_type != identity->scheduler_type) continue;
        if (record.identity.canonical_native != identity->canonical_native) continue;
        if (!namespace_matches(record.identity, selector, *identity)) continue;
        if (!identity->step_id.empty() && record.identity.step_id != identity->step_id) continue;
        matches.push_back(std::move(record));
    }
    sort_records(matches);

    if (matches.empty()) {
        result.code = "not_found";
        result.message = "no verified current instance for native job '" +
                         selector.native_job_id + "' in cluster '" +
                         selector.cluster_name + "'";
        if (ensure_registered) {
            // No caller-supplied PID or native number authorizes an invented
            // live record; the maintenance controller owns real discovery.
            result.message +=
                "; ensure_registered requires a live discovery source, which no local "
                "source confirmed for this request";
        }
        return result;
    }

    if (scope == "instance" && matches.size() > 1) {
        result.code = "ambiguous_selector";
        result.message = fmt::format(
            "native job '{}' matches {} current local instances in cluster '{}'; "
            "add step_id or use scope=job", selector.native_job_id, matches.size(),
            selector.cluster_name);
        result.records = std::move(matches);
        return result;
    }

    for (const auto& record : matches) {
        if (!registry.findJob(record.internal_id).has_value()) {
            result.code = "generation_closing";
            result.message = "instance " + record.instance_id +
                             " is closing and rejects new binding mutations";
            result.records.clear();
            return result;
        }
    }
    result.records = std::move(matches);
    return result;
}

TargetResolution resolve_instance_handle(JobRegistry& registry, const std::string& instance_id)
{
    TargetResolution result;
    for (auto& record : registry.inventoryRecords()) {
        if (record.instance_id != instance_id) continue;
        if (!registry.findJob(record.internal_id).has_value()) {
            result.code = "generation_closing";
            result.message = "instance " + instance_id +
                             " is closing and rejects new binding mutations";
            return result;
        }
        result.records.push_back(std::move(record));
        return result;
    }
    result.code = "not_found";
    result.message = "no verified current instance with instance_id '" + instance_id + "'";
    return result;
}

// Desired intent is authoritative; applied state comes from the collection
// runtime that actually owns the collector lanes (plan section 4.2). A core
// with no runtime reports the intent as accepted-but-not-applied, never as
// "collecting".
nlohmann::json collection_target_json(JobRegistry& registry, const InventoryRecord& record,
                                      const BindingRecord& binding, bool changed)
{
    const auto desired = binding.effectiveCollectors();
    const AppliedCollection applied = registry.appliedCollection(record.internal_id);

    nlohmann::json out;
    out["instance_id"] = record.instance_id;
    out["internal_job_id"] = format_internal_id(record.internal_id);
    out["native_job_id"] = record.selector.native_job_id;
    out["binding_revision"] = binding.revision;
    out["mode"] = to_string(binding.mode);
    out["desired_collectors"] = desired;
    out["applied_collectors"] = applied.applied ? applied.collectors
                                                : std::vector<std::string>{};
    out["applied_revision"] = applied.applied ? applied.revision : uint64_t{0};
    out["errors"] = nlohmann::json::array();
    if (!applied.applied && !desired.empty()) {
        out["errors"].push_back(nlohmann::json{
            {"code", "apply_failed"},
            {"message", applied.error.empty() ? "collection runtime is unavailable"
                                              : applied.error},
        });
    }
    out["changed"] = changed;
    return out;
}

// Collector names are collector-instance names from
// collectors_config.collectors[].name (what CollectorNames historically meant)
// plus registered collector types. Unknown names are rejected before mutation.
std::set<std::string> known_collector_names()
{
    std::set<std::string> names;
    for (const auto& name : CollectorRegistry::instance().list()) names.insert(name);
    try {
        const auto collectors = Config::instance().getRawNode("collectors_config")["collectors"];
        if (collectors && collectors.IsSequence()) {
            for (const auto& item : collectors) {
                if (item["name"] && item["name"].IsScalar())
                    names.insert(item["name"].as<std::string>());
            }
        }
    } catch (const std::exception& error) {
        spdlog::warn("JobRegistry: could not read configured collector names: {}", error.what());
    }
    return names;
}

// Legacy add identity (§9.4). The supplied JobID is never consulted; only the
// typed sub_attr plus the scheduler namespace the discovery sources use.
std::optional<NativeSelector> legacy_add_selector(const Job& job, const nlohmann::json& req)
{
    NativeSelector selector;
    const auto sub = req.contains("sub_attr") && req["sub_attr"].is_object()
                         ? req["sub_attr"] : nlohmann::json::object();
    if (job.subtype == JobSubType::Condor) {
        selector.scheduler_type = SchedulerType::Condor;
        selector.native_job_id = job.NativeJobID;
        if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr))
            selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
        selector.cluster_tag = sub.value("scheduler_name", std::string{});
        selector.cluster_name = sub.value("collector_host", std::string{});
        if (selector.cluster_name.empty()) selector.cluster_name = job.cluster_name;
        if (selector.cluster_name.empty())
            selector.cluster_name = Utils::get_condor_collector_host();
    } else if (job.subtype == JobSubType::Slurm) {
        selector.scheduler_type = SchedulerType::Slurm;
        selector.native_job_id = job.NativeJobID;
        selector.cluster_name = sub.value("cluster_name", std::string{});
        if (selector.cluster_name.empty()) selector.cluster_name = SlurmJob::get_cluster_name();
        if (const auto* attr = std::get_if<SlurmJobAttr>(&job.sub_attr)) {
            if (attr->step_id != 0) {
                if (const auto name = slurm_step_name(attr->step_id)) selector.step_id = *name;
                else selector.step_id = std::to_string(attr->step_id);
            }
        }
    } else {
        // Generic/manual registration has no scheduler identity to resolve.
        return std::nullopt;
    }
    if (selector.cluster_name.empty() || selector.native_job_id.empty()) return std::nullopt;
    return selector;
}

// Returns a final response when the legacy add resolved to an inventoried
// instance, or nullopt when the request must fall back to legacy registration.
std::optional<nlohmann::json> try_legacy_add_by_identity(JobRegistry& registry,
                                                         JobCollectionManager& manager,
                                                         const Job& job,
                                                         const nlohmann::json& req)
{
    const auto selector = legacy_add_selector(job, req);
    if (!selector || job.CollectorNames.empty()) return std::nullopt;

    const auto resolved =
        resolve_native_target(registry, *selector, /*scope=*/"instance",
                             /*ensure_registered=*/false);
    if (!resolved.ok()) {
        if (resolved.code == "not_found") return std::nullopt;
        if (resolved.code == "ambiguous_selector")
            return error_payload(resolved.code, resolved.message,
                                 instance_ids_of(resolved.records));
        return error_payload(resolved.code, resolved.message);
    }

    const auto& record = resolved.records.front();
    CollectionRequest request;
    request.targets = {record.internal_id};
    request.collectors = job.CollectorNames;
    const auto outcome = manager.setCollectors(request);
    if (!outcome.ok) return error_payload(to_string(outcome.error), outcome.message);

    const auto& target = outcome.targets.front();
    nlohmann::json out;
    out["status"] = "success";
    // Legacy numeric field for old consumers; the value is the internal id of
    // the existing instance, never a fresh allocation from the JobID hint.
    out["JobID"] = target.internal_id;
    out["internal_job_id"] = format_internal_id(target.internal_id);
    out["instance_id"] = target.instance_id;
    out["binding_revision"] = target.binding.revision;
    out["mode"] = to_string(target.binding.mode);
    out["desired_collectors"] = target.binding.effectiveCollectors();
    // Accepted versus applied stays explicit for legacy callers too: the request
    // is accepted when the intent is durable, but collection only starts if the
    // runtime confirmed it.
    out["applied_collectors"] = target.applied.applied ? target.applied.collectors
                                                       : std::vector<std::string>{};
    out["applied_revision"] = target.applied.applied ? target.applied.revision : uint64_t{0};
    out["errors"] = nlohmann::json::array();
    if (!target.applied.applied && !out["desired_collectors"].empty()) {
        out["errors"].push_back(nlohmann::json{
            {"code", "apply_failed"},
            {"message", target.applied.error.empty() ? "collection runtime is unavailable"
                                                     : target.applied.error},
        });
    }
    out["duplicate_observation"] = !target.changed;

    const auto hint = parse_legacy_job_id(
        req.contains("JobID") ? req["JobID"] : nlohmann::json(nullptr));
    if (hint.status == LegacyIdStatus::Valid && hint.value != target.internal_id) {
        out["ignored_job_id_hint"] = format_internal_id(hint.value);
        spdlog::warn("JobRegistry: legacy add for native job '{}' carried JobID {} as a "
                     "compatibility hint; resolved to inventory instance {} (internal {})",
                     record.selector.native_job_id, hint.value, target.instance_id,
                     target.internal_id);
    }
    spdlog::info("JobRegistry: legacy add resolved native job '{}' to inventory instance {} "
                 "(internal {}), binding revision {}",
                 record.selector.native_job_id, target.instance_id, target.internal_id,
                 target.binding.revision);
    return out;
}

SubAttrMatcher parse_legacy_matcher(const nlohmann::json& matcher_json)
{
    SubAttrMatcher matcher;
    if (matcher_json.contains("subtype")) {
        const auto subtype = matcher_json["subtype"].get<std::string>();
        if (subtype == "Condor") matcher.subtype = JobSubType::Condor;
        else if (subtype == "Slurm") matcher.subtype = JobSubType::Slurm;
        else if (subtype == "Common") matcher.subtype = JobSubType::Common;
    }
    if (matcher_json.contains("condor_cluster_id"))
        matcher.condor_cluster_id = matcher_json["condor_cluster_id"].get<size_t>();
    if (matcher_json.contains("condor_proc_id"))
        matcher.condor_proc_id = matcher_json["condor_proc_id"].get<size_t>();
    if (matcher_json.contains("condor_owner"))
        matcher.condor_owner = matcher_json["condor_owner"].get<std::string>();
    if (matcher_json.contains("condor_starter_pid"))
        matcher.condor_starter_pid = matcher_json["condor_starter_pid"].get<pid_t>();
    if (matcher_json.contains("slurm_job_id"))
        matcher.slurm_job_id = matcher_json["slurm_job_id"].get<size_t>();
    if (matcher_json.contains("slurm_step_id"))
        matcher.slurm_step_id = matcher_json["slurm_step_id"].get<uint32_t>();
    if (matcher_json.contains("slurm_user"))
        matcher.slurm_user = matcher_json["slurm_user"].get<std::string>();
    if (matcher_json.contains("slurm_job_name"))
        matcher.slurm_job_name = matcher_json["slurm_job_name"].get<std::string>();
    if (matcher_json.contains("slurm_stepd_pid"))
        matcher.slurm_stepd_pid = matcher_json["slurm_stepd_pid"].get<pid_t>();
    return matcher;
}

// §9.4 legacy remove: stop collection, keep the inventory fact. Only records
// outside the inventory (pure legacy registrations) are archived as before.
nlohmann::json legacy_remove(JobRegistry& registry, JobCollectionManager& manager,
                             const nlohmann::json& req)
{
    std::vector<uint64_t> targets;
    if (req.contains("JobID") && !req["JobID"].is_null()) {
        const auto parsed = parse_legacy_job_id(req["JobID"]);
        if (parsed.status == LegacyIdStatus::Invalid)
            return error_payload("invalid_request",
                                 "invalid JobID: expected a uint64 number or decimal string");
        if (parsed.status == LegacyIdStatus::Valid) targets.push_back(parsed.value);
    } else if (req.contains("matcher") && req["matcher"].is_object()) {
        for (const auto& job : registry.findJob(parse_legacy_matcher(req["matcher"]))) {
            targets.push_back(job.JobID);
        }
    } else {
        spdlog::warn("JobRegistry: legacy remove request carries neither 'JobID' nor 'matcher'");
    }

    std::vector<std::string> stopped_instances;
    nlohmann::json apply_errors = nlohmann::json::array();
    size_t stopped = 0;
    size_t retired = 0;
    for (const auto id : targets) {
        if (id == kSystemSentinelJobId) continue;
        if (!registry.inventoryRecord(id).has_value()) {
            if (registry.retireInstance(id)) ++retired;
            continue;
        }
        if (!registry.findJob(id).has_value()) {
            // Already draining/retired: stopping again is idempotent.
            ++stopped;
            continue;
        }
        CollectionRequest stop;
        stop.targets = {id};
        stop.collectors = std::vector<std::string>{};
        const auto outcome = manager.setCollectors(stop);
        if (!outcome.ok) {
            spdlog::error("JobRegistry: legacy remove could not stop collection for JobID {}: {}",
                          id, outcome.message);
            return error_payload(to_string(outcome.error), outcome.message);
        }
        ++stopped;
        stopped_instances.push_back(outcome.targets.front().instance_id);
        if (!outcome.targets.front().applied.applied) {
            apply_errors.push_back(nlohmann::json{
                {"instance_id", outcome.targets.front().instance_id},
                {"code", "apply_failed"},
                {"message", outcome.targets.front().applied.error.empty()
                                ? "collection runtime is unavailable"
                                : outcome.targets.front().applied.error},
            });
        }
    }

    if (!targets.empty()) {
        spdlog::warn("JobRegistry: legacy remove is deprecated and now stops collection for "
                     "{} instance(s); inventory records are retained "
                     "(doc/job_inventory_collection_decoupling_plan.md section 9.4)",
                     stopped);
    }

    nlohmann::json out;
    out["status"] = "success";
    out["deleted_count"] = retired;
    out["stopped_count"] = stopped;
    out["stopped_instances"] = stopped_instances;
    out["apply_errors"] = std::move(apply_errors);
    out["inventory_retained"] = true;
    out["deprecated"] = true;
    out["msg"] = "legacy remove now stops collection; inventory records are retained";
    return out;
}

nlohmann::json provider_payload(const nlohmann::json& payload)
{
    return payload;
}

template <typename T>
nlohmann::json provider_payload(const T& provider)
{
    if constexpr (std::is_same_v<std::decay_t<T>, nlohmann::json>) {
        return provider;
    } else if constexpr (std::is_constructible_v<bool, const T&>) {
        // Empty function/optional: no provider registered (calling an empty
        // std::function would throw bad_function_call).
        if (!static_cast<bool>(provider)) return nlohmann::json();
        if constexpr (std::is_invocable_v<const T&>) {
            return provider_payload(provider());
        } else {
            return provider_payload(*provider);
        }
    } else if constexpr (std::is_invocable_v<const T&>) {
        return provider_payload(provider());
    } else {
        return nlohmann::json();
    }
}

// Empty when no maintenance provider is registered.
nlohmann::json current_inventory_status_provider()
{
#ifdef JOBLENS_HAS_INVENTORY_STATUS_PROVIDER
    auto provider = get_inventory_status_provider();
    return provider_payload(provider);
#else
    return nlohmann::json();
#endif
}

// Readiness gate for the inventory/control surface. An unfinished migration or
// a closed store must not look like "no jobs" or "no such instance".
std::optional<nlohmann::json> store_not_ready_payload(const StoreHealth& health)
{
    if (health.db_running && health.migration_complete) return std::nullopt;
    if (!health.db_running)
        return error_payload("store_unavailable",
                             "inventory store is not running on this core");
    return error_payload("store_unavailable",
                         "inventory store is initializing (migration " +
                             (health.migration_state.empty() ? std::string{"unknown"}
                                                             : health.migration_state) + ")");
}

nlohmann::json store_health_json(const StoreHealth& health){
    return nlohmann::json{
        {"running", health.db_running},
        {"migration_complete", health.migration_complete},
        {"migration_state", health.migration_state},
        {"degraded", health.degraded},
        {"write_failures", health.write_failures},
        {"last_write_error", health.last_write_error},
        {"corrupt_records", health.corrupt_records},
        {"db_uuid", health.db_uuid},
        {"persistence", nlohmann::json{
            {"state", health.degraded ? "degraded"
                                      : (health.db_running ? "ok" : "unavailable")},
            {"schema_version", health.schema_version},
        }},
    };
}

nlohmann::json inventory_status_payload(JobRegistry& registry)
{
    const auto health = registry.storeHealth();

    nlohmann::json body;
    body["status"] = !health.db_running
                         ? "disabled"
                         : (!health.migration_complete
                                ? "initializing"
                                : (health.degraded ? "degraded" : "healthy"));
    body["inventory_enabled"] = health.db_running;
    body["sources"] = nlohmann::json{{"state", "not_configured"}};

    uint64_t active = 0;
    uint64_t suspect = 0;
    uint64_t closing = 0;
    for (const auto& record : registry.inventoryRecords()) {
        if (is_reserved_internal_id(record.internal_id)) continue;
        if (record.state == "suspect") ++suspect;
        else if (record.state == "closing") ++closing;
        else ++active;
    }
    body["instances"] = nlohmann::json{
        {"active", active},
        {"suspect", suspect},
        {"closing", closing},
        {"incomplete_identity", health.incomplete_identities},
        {"quarantined", health.quarantined_identities},
    };
    // Contract field name shared with the trigger Pydantic model and Spectra.
    body["db_schema_version"] = health.schema_version;

    // Maintenance-controller keys (sources/queue/reconciliation) win over the
    // defaults above; store facts always come from the store itself.
    const auto provider = current_inventory_status_provider();
    if (provider.is_object()) body.update(provider);
    body["store"] = store_health_json(health);
    body["persistence"] = body["store"]["persistence"];
    body["db_schema_version"] = health.schema_version;
    return body;
}

} // namespace

void JobRegistry::regRPCHandle() {
    // 注册 RPC 方法
    // 请求体格式示例：
    // {
    //     "opt": "add",
    //     "type": "job.condor",
    //     "JobID": 1,
    //     "JobPIDs": [1],
    //     "Lens": ["cpumem_collector"],
    //      "auto_update_child": true
    // }
    // One control manager per registry. Intent mutations are serialized inside
    // it and committed through JobRegistry::commitBindings.
    auto manager = std::make_shared<JobCollectionManager>(*this);
    {
        const auto known = known_collector_names();
        manager->setCollectorValidator(
            [known](const std::string& name) { return known.count(name) != 0; });
    }

    RPCServer::instance().register_method("JobRegistry/job_opt",
        [this, manager](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            try {
                Job job;
                std::string opt = json2JobOpt(req, job);
                if(opt.compare("add") == 0){
                    // §9.4: identity resolution first. A legacy JobID is only a
                    // compatibility hint, so it never allocates or overrides the
                    // internal id of an existing inventoried instance.
                    if (auto resolved = try_legacy_add_by_identity(*this, *manager, job, req)) {
                        return *resolved;
                    }
                    if (addJob(job)) {
                        j["status"] = "success";
                    } else {
                        j["status"] = "error";
                        j["msg"] = fmt::format("failed to add job {} (type={}): empty collectors, duplicate sub_attr, "
                                               "duplicate JobID, or a rejected durable write — check server logs",
                                               job.JobID, job.NativeJobID);
                    }
                }else if(opt.compare("remove") == 0){
                    // §9.4: remove stops collection for resolved live jobs and
                    // keeps the inventory fact instead of forgetting the
                    // execution.
                    j = legacy_remove(*this, *manager, req);
                }else {
                    j["status"] = "error";
                    j["code"] = "invalid_request";
                    j["msg"] = "invalid operation, expected 'add' or 'remove'";
                }
            } catch (const std::exception& e) {
                j["status"] = "error";
                j["code"] = "invalid_request";
                j["msg"] = fmt::format("failed to add job: {}", e.what());
            }
            return j;
        }
    );

    RPCServer::instance().register_method("JobRegistry/get_job_count",
        [this](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            nlohmann::json j;
            j["status"] = "ok";
            std::shared_lock lg(mtx_);
            j["job_count"] = jobs_.size();
            return j;
        }
    );

    // 列出所有 Job
    RPCServer::instance().register_method("JobRegistry/list_jobs",
        [this](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            nlohmann::json res = nlohmann::json::object();
            res["status"] = "ok";
            res["jobs"] = nlohmann::json::array();
            std::shared_lock lg(mtx_);
            for (const auto& job : jobs_) {
                if(job.second.JobID == 0) continue; //跳过彩蛋任务
                res["jobs"].push_back(dump_job(job.second));
            }
            return res;
        }
    );
    // 获取指定 JobID 的 Job 信息
    RPCServer::instance().register_method("JobRegistry/get_job",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            if (!req.contains("JobID")) {
                j["error"] = "missing JobID in request";
                return j;
            }
            uint64_t jobID = req["JobID"].get<uint64_t>();
            auto job = this->findJob(jobID);
            if (!job.has_value()) {
                j["error"] = "job not found with JobID " + std::to_string(jobID);
                return j;
            }
            j = dump_job(job.value());
            return j;
        }
    );

    // pid 归属追踪的运行状态与 ringbuf 入队/丢弃统计。
    // dropped_events 持续增长说明 job_event ringbuf 溢出、JobPIDs 可能失真。
    RPCServer::instance().register_method("JobRegistry/pid_tracker_stats",
        [this](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            nlohmann::json j;
            j["status"] = "ok";
            j["running"] = static_cast<bool>(pid_tracker_);
            j["pushed_events"] = pid_tracker_ ? pid_tracker_->pushed_events() : 0;
            j["dropped_events"] = pid_tracker_ ? pid_tracker_->dropped_events() : 0;
            return j;
        }
    );

    // 组字段探测统计（只读诊断）：attempts/failures/empty/non_empty/skipped、
    // truncated、acct_group 段数分布。只能发现“格式”异常；站点显示组的语义
    // 漂移（如 AcctGroup 第 2 段 ≠ 显示组）core 检测不了，责任在消费方。
    RPCServer::instance().register_method("JobRegistry/group_probe_stats",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            j["status"] = "ok";
            j["stats"] = GroupProbeStats::instance().to_json();
            return j;
        }
    );

    // -----------------------------------------------------------------------
    // §9.1 / §9.3 inventory read surface
    // -----------------------------------------------------------------------
    RPCServer::instance().register_method("JobRegistry/inventory_list",
        [this](const nlohmann::json& req) -> nlohmann::json {
            if (auto not_ready = store_not_ready_payload(this->storeHealth()))
                return *not_ready;
            const json& params = req.is_object() ? req : nlohmann::json::object();

            uint64_t limit = 100;
            if (params.contains("limit") && !params["limit"].is_null()) {
                const auto& node = params["limit"];
                if (!node.is_number_unsigned() && !node.is_number_integer()) {
                    return error_payload("invalid_request", "limit must be an integer between 1 and 1000");
                }
                const auto raw = node.get<int64_t>();
                if (raw < 1 || raw > 1000) {
                    return error_payload("invalid_request", "limit must be between 1 and 1000");
                }
                limit = static_cast<uint64_t>(raw);
            }

            std::string scheduler_filter;
            if (params.contains("scheduler_type") && !params["scheduler_type"].is_null()) {
                if (!params["scheduler_type"].is_string())
                    return error_payload("invalid_request", "scheduler_type must be a string");
                scheduler_filter = params["scheduler_type"].get<std::string>();
                if (!scheduler_type_from_string(scheduler_filter))
                    return error_payload("invalid_request",
                                         "unknown scheduler_type '" + scheduler_filter + "'");
                // Normalize to the canonical lowercase wire name below.
                scheduler_filter = to_string(*scheduler_type_from_string(scheduler_filter));
            }
            std::string native_filter;
            if (params.contains("native_job_id") && !params["native_job_id"].is_null()) {
                if (!params["native_job_id"].is_string())
                    return error_payload("invalid_request", "native_job_id must be a string");
                native_filter = params["native_job_id"].get<std::string>();
            }
            std::string state_filter;
            if (params.contains("state") && !params["state"].is_null()) {
                if (!params["state"].is_string())
                    return error_payload("invalid_request", "state must be a string");
                state_filter = params["state"].get<std::string>();
                if (state_filter != "active" && state_filter != "suspect" &&
                    state_filter != "closing")
                    return error_payload("invalid_request",
                                         "state must be one of active, suspect, closing");
            }
            std::string cursor;
            if (params.contains("cursor") && !params["cursor"].is_null()) {
                if (!params["cursor"].is_string())
                    return error_payload("invalid_request", "cursor must be a string");
                cursor = params["cursor"].get<std::string>();
            }

            std::vector<InventoryRecord> matching;
            for (auto& record : this->inventoryRecords()) {
                if (is_reserved_internal_id(record.internal_id)) continue;
                if (!scheduler_filter.empty() &&
                    to_string(record.selector.scheduler_type) != scheduler_filter)
                    continue;
                if (!native_filter.empty() && record.selector.native_job_id != native_filter)
                    continue;
                if (!state_filter.empty() && record.state != state_filter) continue;
                matching.push_back(std::move(record));
            }
            sort_records(matching);

            const uint64_t snapshot_revision = inventory_snapshot_revision(matching);
            const std::string filter_fingerprint =
                scheduler_filter + "|" + native_filter + "|" + state_filter;

            uint64_t after = 0;
            if (!cursor.empty()) {
                const auto decoded = base64url_decode(cursor);
                if (!decoded) return error_payload("invalid_request", "cursor is not a valid page token");
                nlohmann::json token;
                try {
                    token = nlohmann::json::parse(*decoded);
                } catch (const std::exception&) {
                    return error_payload("invalid_request", "cursor is not a valid page token");
                }
                const auto cursor_revision = token.value("r", uint64_t{0});
                if (token.value("f", std::string{}) != filter_fingerprint ||
                    cursor_revision != snapshot_revision) {
                    return error_payload(
                        "cursor_invalidated",
                        fmt::format("inventory snapshot {} no longer matches cursor snapshot {}; "
                                    "restart from the first page",
                                    snapshot_revision, cursor_revision));
                }
                after = token.value("k", uint64_t{0});
            }

            nlohmann::json records = nlohmann::json::array();
            uint64_t emitted = 0;
            uint64_t last_emitted = after;
            bool more = false;
            for (const auto& record : matching) {
                if (record.internal_id <= after) continue;
                if (emitted == limit) {
                    more = true;
                    break;
                }
                records.push_back(inventory_record_json(*this, record));
                last_emitted = record.internal_id;
                ++emitted;
            }

            nlohmann::json out;
            out["status"] = "ok";
            out["snapshot_revision"] = snapshot_revision;
            out["records"] = std::move(records);
            if (more) {
                nlohmann::json token{
                    {"v", 1},
                    {"r", snapshot_revision},
                    {"k", last_emitted},
                    {"f", filter_fingerprint},
                };
                out["next_cursor"] = base64url_encode(token.dump());
            } else {
                out["next_cursor"] = nullptr;
            }
            return out;
        }
    );

    RPCServer::instance().register_method("JobRegistry/inventory_status",
        [this](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            return inventory_status_payload(*this);
        }
    );

    RPCServer::instance().register_method("JobRegistry/capabilities",
        [this](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            const auto health = this->storeHealth();
            nlohmann::json out;
            out["status"] = "ok";
            out["inventory_version"] = 1;
            out["collection_control_version"] = 1;
            out["native_identity_selectors"] = true;
            out["job_scope"] = "current_local_instances";
            // Contract field name shared with the trigger Pydantic model.
            out["db_schema_version"] = health.schema_version;
            out["inventory_ready"] = health.db_running && health.migration_complete;
            return out;
        }
    );

    // -----------------------------------------------------------------------
    // §9.1 / §9.2 collection control surface
    // -----------------------------------------------------------------------
    RPCServer::instance().register_method("JobCollection/set",
        [this, manager](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request", "request body must be a JSON object");
            if (auto not_ready = store_not_ready_payload(this->storeHealth()))
                return *not_ready;

            const auto selector_error = [](const std::string& message) {
                return error_payload("invalid_request", message);
            };

            std::string scope = "instance";
            if (req.contains("scope") && !req["scope"].is_null()) {
                if (!req["scope"].is_string())
                    return selector_error("scope must be 'instance' or 'job'");
                scope = req["scope"].get<std::string>();
                if (scope != "instance" && scope != "job")
                    return selector_error("scope must be 'instance' or 'job'");
            }

            std::string operation;
            if (!req.contains("operation") || !req["operation"].is_string())
                return selector_error("operation must be 'set' or 'reset_auto'");
            operation = req["operation"].get<std::string>();
            if (operation != "set" && operation != "reset_auto")
                return selector_error("operation must be 'set' or 'reset_auto'");

            bool ensure_registered = false;
            if (req.contains("ensure_registered") && !req["ensure_registered"].is_null()) {
                if (!req["ensure_registered"].is_boolean())
                    return selector_error("ensure_registered must be a boolean");
                ensure_registered = req["ensure_registered"].get<bool>();
            }

            if (!req.contains("selector") || !req["selector"].is_object())
                return selector_error("selector is required");
            const auto& selector = req["selector"];

            const bool has_instance =
                selector.contains("instance_id") && !selector["instance_id"].is_null();
            std::vector<std::string> native_fields;
            for (const char* name : {"scheduler_type", "cluster_name", "cluster_tag",
                                     "native_job_id", "step_id"}) {
                if (selector.contains(name) && !selector[name].is_null())
                    native_fields.emplace_back(name);
            }
            if (has_instance && !native_fields.empty())
                return selector_error("selector must use exactly one form: instance_id or "
                                      "native identity fields, not both");
            if (!has_instance && native_fields.empty())
                return selector_error("selector is required: instance_id or native identity fields");
            if (has_instance && scope != "instance")
                return selector_error("instance_id selector only supports scope=instance");

            NativeSelector native_selector;
            std::string instance_handle;
            try {
                instance_handle = has_instance ? optional_string(selector, "instance_id") : "";
                if (!has_instance) {
                    native_selector.cluster_tag = optional_string(selector, "cluster_tag");
                    native_selector.step_id = optional_string(selector, "step_id");
                    native_selector.native_job_id = optional_string(selector, "native_job_id");
                    native_selector.cluster_name = optional_string(selector, "cluster_name");
                    const auto type = optional_string(selector, "scheduler_type");
                    const auto scheduler_type = scheduler_type_from_string(type);
                    if (!scheduler_type)
                        return selector_error("unknown scheduler_type '" + type + "'");
                    native_selector.scheduler_type = *scheduler_type;
                }
            } catch (const std::exception& error) {
                return selector_error(error.what());
            }
            if (has_instance && instance_handle.empty())
                return selector_error("instance_id must not be empty");

            std::optional<std::vector<std::string>> collectors;
            if (operation == "set") {
                if (!req.contains("collectors") || req["collectors"].is_null())
                    return selector_error(
                        "operation=set requires an explicit collectors array; [] stops collection");
                if (!req["collectors"].is_array())
                    return selector_error("collectors must be an array of collector names");
                std::vector<std::string> names;
                for (const auto& item : req["collectors"]) {
                    if (!item.is_string())
                        return selector_error("collectors must be an array of collector names");
                    const auto name = item.get<std::string>();
                    if (name.empty())
                        return selector_error("collectors must not contain empty names");
                    names.push_back(name);
                }
                collectors = names;
            } else if (req.contains("collectors") && !req["collectors"].is_null()) {
                return selector_error("operation=reset_auto must omit collectors");
            }

            std::optional<uint64_t> expected_revision;
            if (req.contains("expected_revision") && !req["expected_revision"].is_null()) {
                if (scope != "instance")
                    return selector_error(
                        "expected_revision is only accepted for a single-instance request "
                        "(scope=instance)");
                const auto& node = req["expected_revision"];
                if (!node.is_number_unsigned() && !node.is_number_integer())
                    return selector_error("expected_revision must be a non-negative integer");
                const auto raw = node.get<int64_t>();
                if (raw < 0)
                    return selector_error("expected_revision must be a non-negative integer");
                expected_revision = static_cast<uint64_t>(raw);
            }

            const auto resolution =
                has_instance ? resolve_instance_handle(*this, instance_handle)
                             : resolve_native_target(*this, native_selector, scope,
                                                     ensure_registered);
            if (!resolution.ok()) {
                if (resolution.code == "ambiguous_selector")
                    return error_payload(resolution.code, resolution.message,
                                         instance_ids_of(resolution.records));
                return error_payload(resolution.code, resolution.message);
            }

            CollectionRequest request;
            request.collectors = collectors;
            request.expected_revision = expected_revision;
            for (const auto& record : resolution.records)
                request.targets.push_back(record.internal_id);

            const auto outcome = operation == "set" ? manager->setCollectors(request)
                                                    : manager->resetAuto(request);
            if (!outcome.ok) {
                return error_payload(to_string(outcome.error), outcome.message);
            }

            nlohmann::json targets = nlohmann::json::array();
            for (const auto& target : outcome.targets) {
                const auto record = this->inventoryRecord(target.internal_id);
                if (!record) continue;
                targets.push_back(
                    collection_target_json(*this, *record, target.binding, target.changed));
            }
            nlohmann::json out;
            out["status"] = "accepted";
            out["operation"] = operation;
            out["scope"] = scope;
            out["targets"] = std::move(targets);
            return out;
        }
    );

    RPCServer::instance().register_method("JobCollection/get",
        [this](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request", "request body must be a JSON object");
            if (auto not_ready = store_not_ready_payload(this->storeHealth()))
                return *not_ready;
            std::string instance_id;
            try {
                instance_id = optional_string(req, "instance_id");
            } catch (const std::exception& error) {
                return error_payload("invalid_request", error.what());
            }
            if (instance_id.empty())
                return error_payload("invalid_request", "instance_id is required");

            const auto resolution = resolve_instance_handle(*this, instance_id);
            if (!resolution.ok())
                return error_payload(resolution.code, resolution.message);

            const auto& record = resolution.records.front();
            const auto binding = this->bindingRecord(record.internal_id);
            auto target = collection_target_json(*this, record, binding, false);

            nlohmann::json out;
            out["status"] = "ok";
            out["instance_id"] = target["instance_id"];
            out["internal_job_id"] = target["internal_job_id"];
            out["native_job_id"] = target["native_job_id"];
            out["scheduler_type"] = to_string(record.selector.scheduler_type);
            out["cluster_name"] = record.selector.cluster_name;
            out["cluster_tag"] = record.selector.cluster_tag;
            out["step_id"] = record.selector.step_id;
            out["state"] = record.state;
            out["quarantined"] = record.quarantined;
            out["mode"] = target["mode"];
            out["binding_revision"] = target["binding_revision"];
            out["desired_collectors"] = target["desired_collectors"];
            out["applied_collectors"] = target["applied_collectors"];
            out["applied_revision"] = target["applied_revision"];
            out["auto_collectors"] = binding.auto_collectors;
            out["manual_collectors"] = binding.manual_collectors;
            out["policy_revision"] = binding.policy_revision;
            out["errors"] = nlohmann::json::array();
            return out;
        }
    );
}

JobRegistry& JobRegistry::instance() {
    static JobRegistry reg;
    return reg;
}


bool JobRegistry::addJob(const Job& job) {
    spdlog::info("JobRegistry: addJob entry JobID={}, native_id={}, collectors={}, PIDs={}",
                 job.JobID, job.NativeJobID, job.CollectorNames.size(), job.JobPIDs.size());
    // 错误检查
    if (job.CollectorNames.empty()) {
        spdlog::warn("JobRegistry: job with JobID {} has empty CollectorNames", job.JobID);
        return false;
    }
    // 自动生成JobID
    Job job_copy = job; // 先拷贝一份，避免修改外部对象
    if (job_copy.JobID == 0) {
        job_copy.JobID = getNextJobID();
        if (job_copy.JobID == 0) {
            spdlog::error("JobRegistry: durable JobID allocation failed, refusing to register the job");
            return false;
        }
        spdlog::info("JobRegistry: assigned JobID {} to new job", job_copy.JobID);
    } 
    return addJob(job_copy, false);
}

bool JobRegistry::addJob(const Job& job, bool from_db) {
    // 锁外解析 cgroup_id(stat 可能较慢), 缓存到待存储副本, 删除作业时直接复用。
    Job job_with_cgroup = job;
    resolve_and_cache_cgroup_id(job_with_cgroup);
    {
        std::lock_guard lg(mtx_);
        SubAttrMatcher matcher;
        matcher.subtype = job.subtype;
        if (std::holds_alternative<CondorJobAttr>(job.sub_attr)) {
            const auto& attr = std::get<CondorJobAttr>(job.sub_attr);
            matcher.subtype = JobSubType::Condor;
            matcher.condor_cluster_id = attr.cluster_id;
            matcher.condor_proc_id = attr.proc_id;
        } else if (std::holds_alternative<SlurmJobAttr>(job.sub_attr)) {
            const auto& attr = std::get<SlurmJobAttr>(job.sub_attr);
            matcher.subtype = JobSubType::Slurm;
            matcher.slurm_job_id = attr.job_id;
            matcher.slurm_step_id = attr.step_id;
        }
        for (const auto& [id, existing_job] : jobs_) {
            (void)id;
            if (matcher.matches(existing_job.sub_attr)) {
                spdlog::warn("JobRegistry: job with matching sub_attr already exists (JobID {}), ignored", existing_job.JobID);
                return false;
            }
        }
        
        if (jobs_.count(job.JobID) || closing_jobs_.count(job.JobID)) {
            spdlog::warn("JobRegistry: duplicate jobID {}, ignored", job.JobID);
            return false;
        }
        jobs_.emplace(job.JobID, job_with_cgroup);
    }

    if (from_db) {
        spdlog::info("JobRegistry: restored job with JobID {} from db", job.JobID);
        return true;
    }

    // Control-plane publication happens only after the durable write succeeds.
    if (!persist_new_job(job_with_cgroup)) {
        std::lock_guard lg(mtx_);
        jobs_.erase(job.JobID);
        spdlog::error("JobRegistry: durable registration failed, JobID {} rejected", job.JobID);
        return false;
    }

    Job job_for_cb;
    {
        std::shared_lock lg(mtx_);
        auto it = jobs_.find(job.JobID);
        if (it == jobs_.end()) return false;
        job_for_cb = it->second;
    }

    const auto callbacks = lifecycle_callbacks();
    for (const auto& cb : callbacks) cb(JobEvent::Added, job_for_cb);
    // The Added broadcast is the runtime application for a registration, so the
    // applied state is recorded from it instead of asking the runtime twice.
    if (!job_for_cb.CollectorNames.empty()) {
        if (callbacks.empty()) {
            AppliedCollection applied;
            applied.error = "no_collection_runtime";
            std::lock_guard lg(mtx_);
            applied_collections_[job.JobID] = applied;
        } else {
            mark_applied(job.JobID, bindingRecord(job.JobID).revision,
                         job_for_cb.CollectorNames);
        }
    }
    sync_job_to_kernel(job_for_cb);
    spdlog::info("JobRegistry: add job with JobID {}", job.JobID);
    return true;
}

bool JobRegistry::retireInstance(uint64_t jobID) {
    Job removed;
    bool has_callbacks = false;
    {
        std::lock_guard lg(mtx_);
        auto it = jobs_.find(jobID);
        if (it == jobs_.end()) {
            // A previously failed archive may still be pending. With active
            // subscribers a repeated removal must not skip the drain ack.
            if (!cbs_.empty()) return false;
            auto closing = closing_jobs_.find(jobID);
            if (closing == closing_jobs_.end()) return false;
            removed = closing->second;
        } else {
            removed = it->second;
            has_callbacks = !cbs_.empty();
            if (has_callbacks) {
                closing_jobs_[jobID] = std::move(it->second);
                jobs_.erase(it);
            }
        }
        applied_collections_.erase(jobID);
    }

    if (has_callbacks) {
        // Collectors retain the snapshot and acknowledge after their accepted
        // tail has drained; kernel attribution and history wait for that ack.
        broadcast_lifecycle(JobEvent::Removed, removed);
        return true;
    }

    remove_job_from_kernel(removed);
    if (!end_job_in_db(jobID)) {
        spdlog::error("JobRegistry: durable archive failed for JobID {}, instance stays active",
                      jobID);
        return false;
    }

    std::lock_guard lg(mtx_);
    jobs_.erase(jobID);
    closing_jobs_.erase(jobID);
    spdlog::info("JobRegistry: retire instance with JobID {}", jobID);
    return true;
}

void JobRegistry::delJob(uint64_t jobID) {
    retireInstance(jobID);
}

bool JobRegistry::acknowledgeJobRemoval(uint64_t jobID) {
    Job removed;
    {
        std::lock_guard lg(mtx_);
        auto it = closing_jobs_.find(jobID);
        if (it == closing_jobs_.end()) return false;
        removed = it->second;
    }
    remove_job_from_kernel(removed);
    if (!end_job_in_db(jobID)) {
        spdlog::error("JobRegistry: durable archive failed for closing JobID {}, keeping it pending",
                      jobID);
        return false;
    }
    std::lock_guard lg(mtx_);
    closing_jobs_.erase(jobID);
    return true;
}

size_t JobRegistry::closingJobCount() const {
    std::shared_lock lg(mtx_);
    return closing_jobs_.size();
}

void JobRegistry::cleanupClosingJobs() {
    std::vector<Job> closing;
    {
        std::shared_lock lg(mtx_);
        closing.reserve(closing_jobs_.size());
        for (const auto& entry : closing_jobs_) closing.push_back(entry.second);
    }
    for (const auto& job : closing) {
        remove_job_from_kernel(job);
        if (end_job_in_db(job.JobID)) {
            std::lock_guard lg(mtx_);
            closing_jobs_.erase(job.JobID);
        } else {
            spdlog::error("JobRegistry: shutdown could not archive closing JobID {}; "
                          "the incomplete closing state is persisted for recovery",
                          job.JobID);
        }
    }
}

bool JobRegistry::finishJobRemoval(const Job& job) {
    remove_job_from_kernel(job);
    const bool archived = end_job_in_db(job.JobID);
    if (!archived) {
        spdlog::error("JobRegistry: failed to archive removed JobID {}", job.JobID);
        return false;
    }
    spdlog::info("JobRegistry: remove job with JobID {}", job.JobID);
    return true;
}

size_t JobRegistry::delJob(const SubAttrMatcher& matcher) {
    std::vector<Job> removed_jobs;
    bool has_callbacks = false;

    {
        std::lock_guard lg(mtx_);
        for (auto it = jobs_.begin(); it != jobs_.end(); ) {
            if (matcher.matches(it->second.sub_attr)) {
                removed_jobs.push_back(std::move(it->second));
                it = jobs_.erase(it);
            } else {
                ++it;
            }
        }
        for (const auto& job : removed_jobs) closing_jobs_[job.JobID] = job;
        has_callbacks = !cbs_.empty();
    }

    for (const auto& job : removed_jobs) {
        if (has_callbacks) {
            broadcast_lifecycle(JobEvent::Removed, job);
        } else if (finishJobRemoval(job)) {
            std::lock_guard lg(mtx_);
            closing_jobs_.erase(job.JobID);
            spdlog::info("JobRegistry: remove job with JobID {} by sub_attr matcher", job.JobID);
        }
    }

    spdlog::info("JobRegistry: removed {} jobs by sub_attr matcher", removed_jobs.size());
    return removed_jobs.size();
}

size_t JobRegistry::delJobByRequest(const nlohmann::json& req) {
    // 优先检查 JobID，如果存在则按 JobID 删除
    if (req.contains("JobID")) {
        uint64_t jobID = req["JobID"].get<uint64_t>();
        if (findJob(jobID).has_value()) {
            delJob(jobID);
            return 1;
        }
        return 0;
    }
    
    // 检查 matcher 字段，解析 sub_attr 匹配条件
    if (req.contains("matcher")) {
        const auto& matcher_json = req["matcher"];
        SubAttrMatcher matcher;
        
        // 解析 subtype
        if (matcher_json.contains("subtype")) {
            std::string subtype_str = matcher_json["subtype"].get<std::string>();
            if (subtype_str == "Condor") {
                matcher.subtype = JobSubType::Condor;
            } else if (subtype_str == "Slurm") {
                matcher.subtype = JobSubType::Slurm;
            } else if (subtype_str == "Common") {
                matcher.subtype = JobSubType::Common;
            }
        }
        
        // 解析 Condor 匹配字段
        if (matcher_json.contains("condor_cluster_id")) {
            matcher.condor_cluster_id = matcher_json["condor_cluster_id"].get<size_t>();
        }
        if (matcher_json.contains("condor_proc_id")) {
            matcher.condor_proc_id = matcher_json["condor_proc_id"].get<size_t>();
        }
        if (matcher_json.contains("condor_owner")) {
            matcher.condor_owner = matcher_json["condor_owner"].get<std::string>();
        }
        if (matcher_json.contains("condor_starter_pid")) {
            matcher.condor_starter_pid = matcher_json["condor_starter_pid"].get<pid_t>();
        }
        
        // 解析 Slurm 匹配字段
        if (matcher_json.contains("slurm_job_id")) {
            matcher.slurm_job_id = matcher_json["slurm_job_id"].get<size_t>();
        }
        if (matcher_json.contains("slurm_step_id")) {
            matcher.slurm_step_id = matcher_json["slurm_step_id"].get<uint32_t>();
        }
        if (matcher_json.contains("slurm_user")) {
            matcher.slurm_user = matcher_json["slurm_user"].get<std::string>();
        }
        if (matcher_json.contains("slurm_job_name")) {
            matcher.slurm_job_name = matcher_json["slurm_job_name"].get<std::string>();
        }
        if (matcher_json.contains("slurm_stepd_pid")) {
            matcher.slurm_stepd_pid = matcher_json["slurm_stepd_pid"].get<pid_t>();
        }
        
        return delJob(matcher);
    }
    
    // 既没有 JobID 也没有 matcher，返回 0
    spdlog::warn("JobRegistry::delJobByRequest: request must contain 'JobID' or 'matcher' field");
    return 0;
}

inline bool is_process_running(pid_t pid) {
    // EPERM 表示进程存在但当前用户无权限发送信号（例如 pid 1）。
    return kill(pid, 0) == 0 || errno == EPERM;
}

// /proc/<pid>/stat field 22 (starttime), used to tell a live manager process
// from a recycled PID on restore.
static std::optional<uint64_t> read_proc_start_ticks(pid_t pid) {
    std::ifstream stat("/proc/" + std::to_string(pid) + "/stat");
    if (!stat) return std::nullopt;
    std::string line;
    if (!std::getline(stat, line)) return std::nullopt;
    const auto rparen = line.rfind(')');
    if (rparen == std::string::npos) return std::nullopt;
    std::string rest = line.substr(rparen + 1);
    std::istringstream fields(rest);
    std::string token;
    // fields after comm: state(3) ... starttime(22)
    for (int field = 3; field <= 22; ++field) {
        if (!(fields >> token)) return std::nullopt;
        if (field == 22) {
            try {
                return static_cast<uint64_t>(std::stoull(token));
            } catch (const std::exception&) {
                return std::nullopt;
            }
        }
    }
    return std::nullopt;
}

static std::string read_boot_id() {
    std::ifstream input("/proc/sys/kernel/random/boot_id");
    std::string value;
    if (input && std::getline(input, value)) return value;
    return {};
}

// A complete generation whose boot differs, whose manager start time does not
// match (PID reuse) or whose manager is gone cannot be replayed. Without exact
// evidence the record is only marked suspect; it is never deleted here.
static bool generation_is_stale(const ExecutionGeneration& generation,
                                const std::string& current_boot_id) {
    if (!generation.complete()) return false;
    if (!current_boot_id.empty() && generation.boot_id != current_boot_id) return true;
    const auto ticks = read_proc_start_ticks(generation.manager_pid);
    if (ticks.has_value()) return *ticks != generation.manager_start_ticks;
    return !is_process_running(generation.manager_pid);
}

inline void JobRegistry::update_job(Job& job){
    if(job.subtype == JobSubType::Condor){
        CondorJob::update_job_info(job);
        if (!CondorJob::update_job_pids(job)) {
            CommonJob::update_job_child_process(job);
        }
    }
    if(job.subtype == JobSubType::Slurm){
        SlurmJob::update_job_info(job);
        CommonJob::update_job_child_process(job);
    }
    if(job.subtype == JobSubType::Common){
        if (std::get<CommonJobAttr>(job.sub_attr).auto_update_child){
            auto old_pids = job.JobPIDs; 
            CommonJob::update_job_child_process(job);
            if (old_pids != job.JobPIDs){
                update_job_info(job);
            }
        }
    }

    // 组回填落库：write-once + 至多一次回填重写。
    // group_need_persist 由 restore/探测路径在拿到值时置位；这里首次看到就落库并清除。
    // （update_job_info 本身对任意 subtype 都执行 Put；现状只是上方调用方只对 Common 触发。）
    if (job.group_need_persist && !getGroups(job).empty()) {
        update_job_info(job);
        job.group_need_persist = false;
        spdlog::debug("JobRegistry: backfilled groups for JobID {} to db", job.JobID);
    }

}

std::optional<Job> JobRegistry::findJob(uint64_t jobID) {
    // Read-only snapshot: maintenance owns PID/attribute refresh and
    // retirement, so reads never mutate the registry (plan section 3.2).
    std::shared_lock lg(mtx_);
    auto it = jobs_.find(jobID);
    if (it == jobs_.end()) return std::nullopt;
    return it->second;
}

std::vector<Job> JobRegistry::findJob(const SubAttrMatcher& matcher) const {
    std::vector<Job> matched_jobs;
    
    std::shared_lock lg(mtx_);
    for (const auto& [id, job] : jobs_) {
        (void)id;
        if (matcher.matches(job.sub_attr)) {
            matched_jobs.push_back(job);
        }
    }
    
    spdlog::debug("JobRegistry: found {} jobs matching the criteria", matched_jobs.size());
    return matched_jobs;
}

// ---------------------------------------------------------------------------
// Store open, migration and restore
// ---------------------------------------------------------------------------

bool JobRegistry::init_job_db(const JobRegistryOptions& options){
    std::string db_path = options.db_path;
    if (db_path.empty()) {
        db_path = Config::instance().getString("job_registry_config", "job_db_path");
    }

    std::filesystem::path db_dir(db_path);
    if (db_dir.has_parent_path()) {
        std::error_code ec;
        std::filesystem::create_directories(db_dir.parent_path(), ec);
    }

    leveldb::Options level_options;
    level_options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(level_options, db_path, &job_db);
    if (!status.ok()) {
        spdlog::error("JobRegistry: failed to open LevelDB at {}: {}", db_path, status.ToString());
        job_db = nullptr;
        return false;
    }

    spdlog::info("JobRegistry: init_job_db at {}", db_path);
    db_running = true;

    // Metadata first: schema version decides whether a legacy migration runs.
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        std::string value;
        auto s = job_db->Get(leveldb::ReadOptions(), key_meta("schema_version"), &value);
        if (s.ok()) {
            try {
                schema_version_ = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                schema_version_ = 0;
            }
        }
    }

    if (schema_version_ > kInventorySchemaVersion) {
        spdlog::error("JobRegistry: db schema {} is newer than this binary ({}), refusing to open",
                      schema_version_, kInventorySchemaVersion);
        delete job_db;
        job_db = nullptr;
        db_running = false;
        return false;
    }

    migration_complete_ = schema_version_ == kInventorySchemaVersion;
    if (!migration_complete_) {
        if (!run_migration()) {
            spdlog::warn("JobRegistry: inventory migration interrupted ({}); "
                         "inventory is not advertised until it completes", migration_state_);
            return true;   // db is open so a restart/retry can resume, but not ready
        }
        schema_version_ = kInventorySchemaVersion;
        migration_complete_ = true;
        migration_state_ = "complete";
    } else {
        migration_state_ = "complete";
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        std::string value;
        if (job_db->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value).ok()) {
            db_uuid_ = value;
        }
    }

    if (db_uuid_.empty()) {
        if (!load_or_create_db_uuid()) {
            spdlog::error("JobRegistry: could not persist the Agent DB UUID");
        }
    }

    try{
        restore_records_from_db();
    }
    catch(const std::exception& e){
        spdlog::warn("JobRegistry: failed to restore jobs from db: {}", e.what());
    }

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        init_job_id_counter();
    }
    
    return true;
}

bool JobRegistry::load_or_create_db_uuid() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!job_db) return false;

    std::string value;
    auto s = job_db->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value);
    if (s.ok() && !value.empty()) {
        db_uuid_ = value;
        return true;
    }

    db_uuid_ = generate_db_uuid();
    leveldb::WriteBatch batch;
    batch.Put(key_meta("db_uuid"), db_uuid_);
    auto status = write_batch_locked(batch, "create db uuid");
    if (!status.ok()) {
        spdlog::error("JobRegistry: failed to persist db uuid: {}", status.ToString());
        return false;
    }
    return true;
}

bool JobRegistry::resume_migration_state(std::string& stage, std::string& cursor) {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    std::string value;
    auto s = job_db->Get(leveldb::ReadOptions(), key_meta("migration"), &value);
    if (!s.ok()) return false;
    try {
        const auto state = nlohmann::json::parse(value);
        if (state.value("state", std::string{}) != "in_progress") return false;
        stage = state.value("stage", std::string{"allocator"});
        cursor = state.value("cursor", std::string{});
        return true;
    } catch (const std::exception& error) {
        spdlog::warn("JobRegistry: unreadable migration state, restarting migration: {}",
                     error.what());
        return false;
    }
}

bool JobRegistry::migrate_allocator() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!job_db) return false;

    uint64_t max_id = 0;
    {
        leveldb::Iterator* it = job_db->NewIterator(leveldb::ReadOptions());
        it->Seek(KEY_PREFIX_JOB);
        if (it->Valid() && it->key().starts_with(KEY_PREFIX_JOB)) {
            it->SeekToLast();
            while (it->Valid() && !it->key().starts_with(KEY_PREFIX_JOB)) it->Prev();
            if (it->Valid()) {
                max_id = std::max(max_id, unhex64(it->key().ToString().substr(2, 16)));
            }
        }
        it->Seek(KEY_PREFIX_HISTORY);
        if (it->Valid() && it->key().starts_with(KEY_PREFIX_HISTORY)) {
            it->SeekToLast();
            while (it->Valid() && !it->key().starts_with(KEY_PREFIX_HISTORY)) it->Prev();
            if (it->Valid()) {
                max_id = std::max(max_id, unhex64(it->key().ToString().substr(2, 16)));
            }
        }
        const auto scan_status = it->status();
        delete it;
        if (!scan_status.ok()) {
            spdlog::error("JobRegistry: migration allocator scan failed: {}", scan_status.ToString());
            return false;
        }
    }

    uint64_t counter = 0;
    {
        std::string value;
        auto s = job_db->Get(leveldb::ReadOptions(), key_counter("job_id"), &value);
        if (s.ok()) {
            try {
                counter = std::stoull(value);
            } catch (const std::exception&) {
                counter = 0;
            }
        }
    }

    uint64_t high_water = std::max<uint64_t>(counter, 1);
    if (max_id != 0 && max_id < std::numeric_limits<uint64_t>::max()) {
        high_water = std::max(high_water, max_id + 1);
    }

    std::string uuid;
    {
        std::string value;
        if (job_db->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value).ok()) {
            uuid = value;
        }
    }

    leveldb::WriteBatch batch;
    batch.Put(key_counter("job_id"), std::to_string(high_water));
    if (uuid.empty()) {
        uuid = generate_db_uuid();
        batch.Put(key_meta("db_uuid"), uuid);
    }
    const auto state = nlohmann::json{
        {"state", "in_progress"},
        {"stage", "index"},
        {"cursor", ""},
        {"started_at_us", now_us()},
    };
    batch.Put(key_meta("migration"), state.dump());

    auto status = write_batch_locked(batch, "migration allocator");
    if (!status.ok()) {
        spdlog::error("JobRegistry: migration could not raise the allocator high-water mark: {}",
                      status.ToString());
        return false;
    }

    db_uuid_ = uuid;
    migration_state_ = "in_progress:index";
    spdlog::info("JobRegistry: migration allocator high-water mark = {}", high_water);
    return true;
}

bool JobRegistry::migrate_index(std::string& cursor, size_t& batches, size_t batch_limit) {
    leveldb::WriteBatch batch;
    size_t pending = 0;
    size_t processed = 0;
    uint64_t quarantined = 0;
    uint64_t incomplete = 0;

    auto flush = [&](const std::string& last_key) -> bool {
        const auto state = nlohmann::json{
            {"state", "in_progress"},
            {"stage", "index"},
            {"cursor", last_key},
            {"started_at_us", now_us()},
            {"records", processed},
        };
        batch.Put(key_meta("migration"), state.dump());
        auto status = write_batch_locked(batch, "migration batch");
        if (!status.ok()) {
            spdlog::error("JobRegistry: migration batch failed: {}", status.ToString());
            return false;
        }
        ++batches;
        spdlog::info("JobRegistry: migration indexed {} records so far", processed);
        return true;
    };

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        if (!job_db) return false;

        // Identity ownership must be visible to later records in the same
        // batch, so keep an in-memory view seeded from the committed index.
        std::unordered_map<std::string, uint64_t> index_owners;
        {
            leveldb::Iterator* index = job_db->NewIterator(leveldb::ReadOptions());
            for (index->Seek(KEY_PREFIX_IDENTITY);
                 index->Valid() && index->key().starts_with(KEY_PREFIX_IDENTITY);
                 index->Next()) {
                try {
                    const auto value = nlohmann::json::parse(index->value().ToString());
                    index_owners[index->key().ToString()] = value.value("internal_id", uint64_t{0});
                } catch (const std::exception&) {
                    // unreadable index entries are rebuilt from records below
                }
            }
            delete index;
        }

        leveldb::Iterator* it = job_db->NewIterator(leveldb::ReadOptions());
        it->Seek(KEY_PREFIX_JOB);
        if (!cursor.empty() && it->Valid() && it->key().ToString() <= cursor) {
            it->Seek(cursor);
            if (it->Valid() && it->key().ToString() == cursor) it->Next();
        }

        for (; it->Valid() && it->key().starts_with(KEY_PREFIX_JOB); it->Next()) {
            const std::string key = it->key().ToString();
            const uint64_t internal_id = unhex64(key.substr(KEY_PREFIX_JOB.size()));
            if (internal_id == kSystemSentinelJobId) {
                cursor = key;
                continue;
            }

            StoredRecord stored;
            try {
                stored = parse_job_record(it->value().ToString(), internal_id);
            } catch (const std::exception& error) {
                spdlog::error("JobRegistry: corrupt record {} during migration: {}", key, error.what());
                ++corrupt_records_;
                cursor = key;
                continue;
            }

            auto& meta = stored.meta;
            meta.selector = selector_from_legacy_job(stored.job);
            meta.facts_revision = 1;
            meta.first_seen_us = meta.first_seen_us != 0 ? meta.first_seen_us : now_us();
            meta.last_seen_us = meta.first_seen_us;
            meta.state = "active";
            meta.processes.clear();
            for (const auto pid : stored.job.JobPIDs) {
                meta.processes.push_back(ProcessIdentity{pid, 0, ""});
            }

            const auto identity = canonicalize(meta.selector);
            meta.identity_complete = identity.has_value();
            if (identity) {
                meta.identity = *identity;
                const std::string index_key = key_identity(identity_index_key(*identity));
                const auto owner_it = index_owners.find(index_key);
                if (owner_it != index_owners.end() && owner_it->second != 0 &&
                    owner_it->second != internal_id) {
                    meta.quarantined = true;
                    ++quarantined;
                    spdlog::warn("JobRegistry: conflicting live identity for JobID {} "
                                 "(already owned by JobID {}), quarantined instead of merged",
                                 internal_id, owner_it->second);
                } else if (owner_it == index_owners.end()) {
                    batch.Put(index_key, nlohmann::json{
                        {"internal_id", internal_id},
                        {"scheduler_type", to_string(identity->scheduler_type)},
                        {"cluster_namespace", identity->cluster_namespace},
                        {"native_job_id", identity->native_job_id},
                        {"canonical_native", identity->canonical_native},
                        {"step_id", identity->step_id},
                    }.dump());
                    index_owners[index_key] = internal_id;
                }
            } else {
                ++incomplete;
            }

            // Legacy non-empty CollectorNames become manual intent: the
            // original policy origin is unknown and must not be guessed.
            if (!stored.job.CollectorNames.empty()) {
                BindingRecord binding;
                binding.internal_id = internal_id;
                binding.revision = 1;
                binding.mode = CollectionMode::Manual;
                binding.manual_collectors = stored.job.CollectorNames;
                binding.policy_revision = "legacy";
                batch.Put(key_binding(internal_id), binding_to_json(binding).dump());
            }

            batch.Put(key, job_record_json(stored.job, meta).dump());
            cursor = key;
            ++processed;
            ++pending;
            if (pending >= kMigrationBatchSize) {
                if (!flush(cursor)) {
                    delete it;
                    return false;
                }
                batch.Clear();
                pending = 0;
                if (batch_limit != 0 && batches >= batch_limit) {
                    spdlog::warn("JobRegistry: migration stopped after {} batch(es) for resume testing",
                                 batches);
                    delete it;
                    return false;
                }
            }
        }

        const auto status = it->status();
        delete it;
        if (!status.ok()) {
            spdlog::error("JobRegistry: migration iterator failed: {}", status.ToString());
            return false;
        }

        if (pending > 0) {
            if (!flush(cursor)) return false;
            batch.Clear();
        }
        if (batch_limit != 0 && batches >= batch_limit) {
            spdlog::warn("JobRegistry: migration stopped after {} batch(es) for resume testing",
                         batches);
            return false;
        }
    }

    migration_state_ = "in_progress:finalize";
    spdlog::info("JobRegistry: migration indexed {} records ({} quarantined, {} incomplete)",
                 processed, quarantined, incomplete);
    return true;
}

bool JobRegistry::migrate_finalize() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!job_db) return false;

    leveldb::WriteBatch batch;
    batch.Put(key_meta("schema_version"), std::to_string(kInventorySchemaVersion));
    batch.Put(key_meta("migration"), nlohmann::json{
        {"state", "complete"},
        {"stage", "done"},
        {"completed_at_us", now_us()},
    }.dump());

    auto status = write_batch_locked(batch, "migration finalize");
    if (!status.ok()) {
        spdlog::error("JobRegistry: migration finalize failed: {}", status.ToString());
        return false;
    }

    {
        std::string value;
        if (job_db->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value).ok() && !value.empty()) {
            db_uuid_ = value;
        }
    }
    migration_state_ = "complete";
    spdlog::info("JobRegistry: inventory migration complete (schema {})", kInventorySchemaVersion);
    return true;
}

bool JobRegistry::run_migration() {
    std::string stage = "allocator";
    std::string cursor;
    if (resume_migration_state(stage, cursor)) {
        spdlog::info("JobRegistry: resuming interrupted migration at stage {} (cursor {})",
                     stage, cursor);
    } else {
        stage = "allocator";
        cursor.clear();
    }

    if (stage == "allocator") {
        if (!migrate_allocator()) return false;
        stage = "index";
        cursor.clear();
    }
    if (stage == "index") {
        size_t batches = 0;
        if (!migrate_index(cursor, batches, migration_batch_limit_)) return false;
        stage = "finalize";
    }
    if (stage == "finalize") {
        return migrate_finalize();
    }
    return false;
}

void JobRegistry::restore_records_from_db() {
    const std::string boot_id = read_boot_id();
    std::vector<StoredRecord> restored;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        if (!job_db) return;
        leveldb::Iterator* it = job_db->NewIterator(leveldb::ReadOptions());
        for (it->Seek(KEY_PREFIX_JOB); it->Valid() && it->key().starts_with(KEY_PREFIX_JOB); it->Next()) {
            const std::string key = it->key().ToString();
            uint64_t jobID = 0;
            try {
                jobID = unhex64(key.substr(KEY_PREFIX_JOB.size()));
            } catch (const std::exception&) {
                ++corrupt_records_;
                continue;
            }
            if (jobID == kSystemSentinelJobId) continue;
            try {
                auto stored = parse_job_record(it->value().ToString(), jobID);
                // Validate restored executions before publishing them: a
                // stale generation is marked suspect (never replayed), dead
                // PIDs are dropped and never replayed into kernel maps.
                if (generation_is_stale(stored.meta.generation, boot_id)) {
                    stored.meta.state = "suspect";
                    stored.job.JobPIDs.clear();
                    stored.meta.processes.clear();
                    spdlog::warn("JobRegistry: restored JobID {} has a stale execution "
                                 "generation; marked suspect", jobID);
                } else {
                    std::vector<pid_t> live;
                    for (const auto pid : stored.job.JobPIDs) {
                        if (is_process_running(pid)) live.push_back(pid);
                    }
                    if (live.size() != stored.job.JobPIDs.size()) {
                        stored.job.JobPIDs = std::move(live);
                        if (stored.job.JobPIDs.empty()) stored.meta.state = "suspect";
                        std::vector<ProcessIdentity> processes;
                        for (const auto pid : stored.job.JobPIDs) {
                            processes.push_back(ProcessIdentity{pid, 0, ""});
                        }
                        if (!processes.empty()) stored.meta.processes = std::move(processes);
                    }
                }
                restored.push_back(std::move(stored));
            } catch (const std::exception& error) {
                ++corrupt_records_;
                spdlog::warn("JobRegistry: cannot restore job {}: {}", jobID, error.what());
            }
        }
        const auto status = it->status();
        delete it;
        if (!status.ok()) {
            spdlog::warn("JobRegistry: iterator error during restore: {}", status.ToString());
        }
    }

    {
        std::unique_lock lg(mtx_);
        for (auto& stored : restored) {
            stored.meta.instance_id = derive_instance_id(db_uuid_, stored.meta.internal_id);
            if (stored.meta.identity_complete && !stored.meta.quarantined) {
                const auto index_key = identity_index_key(stored.meta.identity);
                const auto existing = identity_ids_.find(index_key);
                if (existing != identity_ids_.end() &&
                    existing->second != stored.meta.internal_id) {
                    stored.meta.quarantined = true;
                    spdlog::warn("JobRegistry: duplicate live identity for JobID {} "
                                 "(already restored as JobID {}), quarantined",
                                 stored.meta.internal_id, existing->second);
                } else {
                    identity_ids_[index_key] = stored.meta.internal_id;
                }
            }
            jobs_[stored.meta.internal_id] = stored.job;
            records_[stored.meta.internal_id] = stored.meta;
        }
    }

    size_t incomplete = 0, quarantined = 0;
    for (const auto& stored : restored) {
        incomplete += stored.meta.identity_complete ? 0 : 1;
        quarantined += stored.meta.quarantined ? 1 : 0;
    }
    spdlog::info("JobRegistry: restored {} inventory records ({} incomplete, {} quarantined)",
                 restored.size(), incomplete, quarantined);
}

void JobRegistry::deinit_job_db(){
    delete job_db;
    job_db = nullptr;
    db_running = false;
    spdlog::info("JobRegistry: deinit_job_db");
}

bool JobRegistry::store_ready() const {
    return db_running && job_db != nullptr && migration_complete_;
}

leveldb::Status JobRegistry::write_batch_locked(leveldb::WriteBatch& batch,
                                                const char* operation) {
    if (!job_db) return leveldb::Status::IOError("job db is not open");

    StoreFaultMode injected = StoreFaultMode::None;
    if (fault_count_ > 0) {
        if (fault_skip_ > 0) {
            --fault_skip_;
        } else {
            --fault_count_;
            injected = fault_mode_;
            if (injected == StoreFaultMode::FailBeforeWrite) {
                record_store_failure(fmt::format("injected write failure before commit ({})",
                                                 operation));
                return leveldb::Status::IOError("injected write failure before commit");
            }
        }
    }

    leveldb::WriteOptions write_options;
    write_options.sync = true;
    auto status = job_db->Write(write_options, &batch);

    if (injected == StoreFaultMode::FailAfterWrite) {
        record_store_failure(fmt::format("injected write failure after commit ({})", operation));
        return leveldb::Status::IOError("injected write failure after commit");
    }
    if (!status.ok()) {
        record_store_failure(fmt::format("LevelDB write failed ({}): {}",
                                         operation, status.ToString()));
        return status;
    }

    record_store_success();
    return status;
}

void JobRegistry::record_store_failure(const std::string& message) {
    store_write_failures_.fetch_add(1);
    store_degraded_.store(true);
    last_store_error_ = message;
    spdlog::error("JobRegistry: {}", message);
}

void JobRegistry::record_store_success() {
    store_degraded_.store(false);
    last_store_error_.clear();
}

void JobRegistry::injectStoreFaultForTest(StoreFaultMode mode, size_t skip, size_t count) {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    fault_mode_ = mode;
    fault_skip_ = skip;
    fault_count_ = count;
}

void JobRegistry::clearStoreFaultsForTest() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    fault_mode_ = StoreFaultMode::None;
    fault_skip_ = 0;
    fault_count_ = 0;
}

void JobRegistry::init_job_id_counter() {
    if (!job_db) {
        spdlog::warn("JobRegistry: db not running, using default next_job_id_ = 1");
        return;
    }

    try {
        uint64_t counter_value = 1;

        std::string counter_json;
        leveldb::Status s = job_db->Get(leveldb::ReadOptions(), key_counter("job_id"), &counter_json);
        if (s.ok()) {
            counter_value = std::stoull(counter_json);
        }

        uint64_t max_job_id = 0;

        // 扫描 jobs 前缀取最大 JobID
        leveldb::Iterator* it = job_db->NewIterator(leveldb::ReadOptions());
        it->Seek(KEY_PREFIX_JOB);
        if (it->Valid() && it->key().starts_with(KEY_PREFIX_JOB)) {
            // 跳到最后一个 j: key
            it->SeekToLast();
            // 回退确保在 j: 范围内
            while (it->Valid() && !it->key().starts_with(KEY_PREFIX_JOB)) {
                it->Prev();
            }
            if (it->Valid()) {
                uint64_t id = unhex64(it->key().ToString().substr(2));
                if (id > max_job_id) max_job_id = id;
            }
        }

        // 扫描 history 前缀
        it->Seek(KEY_PREFIX_HISTORY);
        if (it->Valid() && it->key().starts_with(KEY_PREFIX_HISTORY)) {
            it->SeekToLast();
            while (it->Valid() && !it->key().starts_with(KEY_PREFIX_HISTORY)) {
                it->Prev();
            }
            if (it->Valid()) {
                // history key: "h:" + hex64(JobID) + ":" + hex64(timestamp)
                std::string hex_part = it->key().ToString().substr(2, 16);
                uint64_t id = unhex64(hex_part);
                if (id > max_job_id) max_job_id = id;
            }
        }
        delete it;

        uint64_t high_water = counter_value;
        if (max_job_id == std::numeric_limits<uint64_t>::max()) {
            high_water = max_job_id;
            job_id_exhausted_ = true;
        } else {
            high_water = std::max(counter_value, max_job_id + 1);
        }
        if (high_water == 0) high_water = 1;
        next_job_id_ = high_water;

        // 持久化当前值 (durable high-water reservation)
        leveldb::WriteBatch batch;
        batch.Put(key_counter("job_id"), std::to_string(next_job_id_));
        auto status = write_batch_locked(batch, "init job id counter");
        if (!status.ok()) {
            spdlog::error("JobRegistry: failed to persist job_id_counter: {}", status.ToString());
        }

        spdlog::info("JobRegistry: initialized job_id_counter, next_job_id_ = {}", next_job_id_);
    } catch (const std::exception& e) {
        spdlog::error("JobRegistry: failed to init job_id_counter: {}", e.what());
        next_job_id_ = 1;
    }
}

uint64_t JobRegistry::allocate_internal_id(std::string& error) {
    std::lock_guard<std::mutex> id_lock(job_id_mtx_);
    if (job_id_exhausted_ || next_job_id_ == 0) {
        error = "id_exhausted";
        spdlog::error("JobRegistry: internal id space is exhausted, refusing allocation");
        return 0;
    }

    const uint64_t id = next_job_id_;
    if (id == std::numeric_limits<uint64_t>::max()) {
        job_id_exhausted_ = true;
        next_job_id_ = 0;
    } else {
        next_job_id_ = id + 1;
    }

    // Reserve the id durably before it is handed out. An ambiguous outcome
    // still advances the in-memory high-water mark: ids are never reused.
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    leveldb::WriteBatch batch;
    batch.Put(key_counter("job_id"),
              std::to_string(next_job_id_ == 0 ? id : next_job_id_));
    auto status = write_batch_locked(batch, "allocate internal id");
    if (!status.ok()) {
        error = "store_write_failed";
        spdlog::error("JobRegistry: durable id allocation failed for id {}, id is burned",
                      id);
        return 0;
    }
    return id;
}

uint64_t JobRegistry::getNextJobID() {
    if (!store_ready()) {
        spdlog::warn("JobRegistry: refusing JobID allocation before the inventory store is ready");
        return 0;
    }
    std::string error;
    const uint64_t job_id = allocate_internal_id(error);
    if (job_id == 0) {
        spdlog::error("JobRegistry: JobID allocation failed: {}", error);
        return 0;
    }
    spdlog::debug("JobRegistry: allocated new JobID {}", job_id);
    return job_id;
}

// ---------------------------------------------------------------------------
// Persistence helpers
// ---------------------------------------------------------------------------

bool JobRegistry::persist_new_job(const Job& job)
{
    if (!store_ready()) return false;

    InventoryRecord meta;
    meta.internal_id = job.JobID;
    meta.facts_revision = 1;
    meta.state = "active";
    meta.selector = selector_from_legacy_job(job);
    if (const auto identity = canonicalize(meta.selector)) {
        meta.identity = *identity;
        meta.identity_complete = true;
    }
    meta.processes.clear();
    for (const auto pid : job.JobPIDs) {
        meta.processes.push_back(ProcessIdentity{pid, 0, ""});
    }
    meta.first_seen_us = now_us();
    meta.last_seen_us = meta.first_seen_us;
    meta.instance_id = derive_instance_id(db_uuid_, job.JobID);

    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    leveldb::WriteBatch batch;

    // A legacy register is a manual selection: the caller explicitly asked
    // for these collectors, so it cannot be reported as an automatic origin.
    if (!job.CollectorNames.empty()) {
        BindingRecord binding;
        binding.internal_id = job.JobID;
        binding.revision = 1;
        binding.mode = CollectionMode::Manual;
        binding.manual_collectors = job.CollectorNames;
        binding.policy_revision = "legacy_add";
        batch.Put(key_binding(job.JobID), binding_to_json(binding).dump());
    }

    if (meta.identity_complete) {
        const std::string index_key = key_identity(identity_index_key(meta.identity));
        std::string existing;
        auto s = job_db->Get(leveldb::ReadOptions(), index_key, &existing);
        if (!s.ok() && !s.IsNotFound()) {
            record_store_failure("LevelDB read failed (identity index): " + s.ToString());
            return false;
        }
        if (s.IsNotFound()) {
            batch.Put(index_key, nlohmann::json{
                {"internal_id", job.JobID},
                {"scheduler_type", to_string(meta.identity.scheduler_type)},
                {"cluster_namespace", meta.identity.cluster_namespace},
                {"native_job_id", meta.identity.native_job_id},
                {"canonical_native", meta.identity.canonical_native},
                {"step_id", meta.identity.step_id},
            }.dump());
        } else {
            uint64_t owner = 0;
            try {
                owner = nlohmann::json::parse(existing).value("internal_id", uint64_t{0});
            } catch (const std::exception&) {
                owner = 0;
            }
            if (owner != 0 && owner != job.JobID) {
                spdlog::error("JobRegistry: identity for JobID {} is already registered as "
                              "JobID {}, refusing a duplicate inventory record",
                              job.JobID, owner);
                return false;
            }
            batch.Put(index_key, nlohmann::json{
                {"internal_id", job.JobID},
                {"scheduler_type", to_string(meta.identity.scheduler_type)},
                {"cluster_namespace", meta.identity.cluster_namespace},
                {"native_job_id", meta.identity.native_job_id},
                {"canonical_native", meta.identity.canonical_native},
                {"step_id", meta.identity.step_id},
            }.dump());
        }
    }

    batch.Put(key_job(job.JobID), job_record_json(job, meta).dump());
    auto status = write_batch_locked(batch, "register job");
    if (!status.ok()) {
        return false;
    }

    note_record_in_memory(meta, job);
    spdlog::info("JobRegistry: persist new job with JobID {} to db", job.JobID);
    return true;
}

void JobRegistry::note_record_in_memory(const InventoryRecord& meta, const Job& job) {
    std::unique_lock lg(mtx_);
    jobs_[meta.internal_id] = job;
    records_[meta.internal_id] = meta;
    if (meta.identity_complete && !meta.quarantined) {
        identity_ids_[identity_index_key(meta.identity)] = meta.internal_id;
    }
}


bool JobRegistry::update_job_info(const Job& job){
    if(!db_running) return false;
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);

    InventoryRecord meta;
    {
        std::shared_lock lg(mtx_);
        auto it = records_.find(job.JobID);
        if (it != records_.end()) meta = it->second;
    }
    if (meta.internal_id == 0) {
        meta.internal_id = job.JobID;
        meta.facts_revision = 1;
        meta.first_seen_us = now_us();
        meta.selector = selector_from_legacy_job(job);
        if (const auto identity = canonicalize(meta.selector)) {
            meta.identity = *identity;
            meta.identity_complete = true;
        }
    }
    meta.last_seen_us = now_us();

    leveldb::WriteBatch batch;
    batch.Put(key_job(job.JobID), job_record_json(job, meta).dump());
    auto status = write_batch_locked(batch, "update job facts");
    if (!status.ok()) {
        return false;
    }
    std::unique_lock lg(mtx_);
    records_[job.JobID] = meta;
    jobs_[job.JobID] = job;
    spdlog::info("JobRegistry: update job info with JobID {} in db", job.JobID);
    return true;
}
    

bool JobRegistry::end_job_in_db(uint64_t jobID){
    if(!db_running || job_db == nullptr) return true;   // nothing durable to clean

    // Archive must not race an upsert of the same instance, otherwise the
    // record could be recreated right after the atomic delete.
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);

    std::string key = key_job(jobID);
    std::string json;
    leveldb::Status s = job_db->Get(leveldb::ReadOptions(), key, &json);
    if (s.IsNotFound()) {
        spdlog::warn("JobRegistry: job {} not found in db when ending", jobID);
        std::unique_lock lg(mtx_);
        records_.erase(jobID);
        applied_collections_.erase(jobID);
        return true;
    }
    if (!s.ok()) {
        record_store_failure("LevelDB read failed (archive job): " + s.ToString());
        return false;
    }

    StoredRecord stored;
    try {
        stored = parse_job_record(json, jobID);
    } catch (const std::exception& error) {
        spdlog::error("JobRegistry: cannot parse job {} for archival: {}", jobID, error.what());
        record_store_failure("corrupt job record during archive");
        return false;
    }

    uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    auto history = job_record_json(stored.job, stored.meta);
    history["state"] = "closing";
    history["end_time_us"] = now;

    leveldb::WriteBatch batch;
    batch.Delete(key);
    batch.Delete(key_binding(jobID));

    // Final binding summary for auditing.
    {
        std::string binding_json;
        if (job_db->Get(leveldb::ReadOptions(), key_binding(jobID), &binding_json).ok()) {
            try {
                history["binding"] = nlohmann::json::parse(binding_json);
            } catch (const std::exception&) {
                // ignore unreadable binding summary
            }
        }
    }

    // Never let stale cleanup remove a newer execution's identity index.
    if (stored.meta.identity_complete) {
        const std::string index_key = key_identity(identity_index_key(stored.meta.identity));
        std::string value;
        auto index_status = job_db->Get(leveldb::ReadOptions(), index_key, &value);
        if (index_status.ok()) {
            uint64_t owner = 0;
            try {
                owner = nlohmann::json::parse(value).value("internal_id", uint64_t{0});
            } catch (const std::exception&) {
                owner = 0;
            }
            if (owner == jobID) batch.Delete(index_key);
        } else if (!index_status.IsNotFound()) {
            record_store_failure("LevelDB read failed (identity index archive): " +
                                 index_status.ToString());
            return false;
        }
    }

    batch.Put(key_history(jobID, now), history.dump());

    s = write_batch_locked(batch, "archive job");
    if (!s.ok()) {
        return false;
    }

    {
        std::unique_lock lg(mtx_);
        records_.erase(jobID);
        applied_collections_.erase(jobID);
        if (stored.meta.identity_complete) {
            const auto index_key = identity_index_key(stored.meta.identity);
            auto it = identity_ids_.find(index_key);
            if (it != identity_ids_.end() && it->second == jobID) identity_ids_.erase(it);
        }
    }

    spdlog::info("JobRegistry: moved ended job {} from jobs to jobs_history", jobID);
    return true;
}

// ---------------------------------------------------------------------------
// Inventory store API
// ---------------------------------------------------------------------------

UpsertResult JobRegistry::upsertObservedJob(const ObservedExecution& observation) {
    UpsertResult result;

    if (!store_ready()) {
        result.error = migration_complete_ ? "store_unavailable" : "store_initializing";
        result.message = "inventory store is not ready";
        return result;
    }

    const auto identity = canonicalize(observation.selector);
    if (!identity) {
        result.error = "incomplete_identity";
        result.message = "scheduler type, cluster namespace and native job id are required";
        spdlog::debug("JobRegistry: rejected incomplete observation (native='{}', cluster='{}')",
                      observation.selector.native_job_id, observation.selector.cluster_name);
        return result;
    }

    std::unique_lock<std::mutex> admission(upsert_mtx_);
    const std::string index_key = key_identity(identity_index_key(*identity));

    const int64_t observed_at = observation.observed_at_us != 0
                                    ? observation.observed_at_us : now_us();

    uint64_t internal_id = 0;
    bool indexed = false;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        std::string value;
        auto s = job_db->Get(leveldb::ReadOptions(), index_key, &value);
        if (s.ok()) {
            try {
                internal_id = nlohmann::json::parse(value).value("internal_id", uint64_t{0});
                indexed = internal_id != 0;
            } catch (const std::exception& error) {
                spdlog::warn("JobRegistry: unreadable identity index: {}", error.what());
            }
        } else if (!s.IsNotFound()) {
            result.error = "store_error";
            result.message = s.ToString();
            record_store_failure("LevelDB read failed (identity lookup): " + s.ToString());
            return result;
        }
    }

    if (indexed) {
        InventoryRecord meta;
        Job stored;
        bool have_record = false;
        {
            std::lock_guard<std::mutex> db_lock(db_mtx_);
            std::string raw;
            auto s = job_db->Get(leveldb::ReadOptions(), key_job(internal_id), &raw);
            if (s.ok()) {
                try {
                    auto parsed = parse_job_record(raw, internal_id);
                    meta = parsed.meta;
                    stored = parsed.job;
                    have_record = true;
                } catch (const std::exception& error) {
                    spdlog::warn("JobRegistry: unreadable record {}: {}", internal_id, error.what());
                }
            } else if (!s.IsNotFound()) {
                result.error = "store_error";
                result.message = s.ToString();
                record_store_failure("LevelDB read failed (record lookup): " + s.ToString());
                return result;
            }
        }

        if (!have_record) {
            // Interrupted archive left a stale index entry: drop it and create
            // a fresh record instead of resolving to a missing instance.
            spdlog::warn("JobRegistry: stale identity index for JobID {}, repairing", internal_id);
            std::lock_guard<std::mutex> db_lock(db_mtx_);
            leveldb::WriteBatch batch;
            batch.Delete(index_key);
            auto status = write_batch_locked(batch, "repair stale identity index");
            if (!status.ok()) {
                result.error = "store_write_failed";
                result.message = status.ToString();
                return result;
            }
            indexed = false;
            internal_id = 0;
        } else {
            if (meta.quarantined) {
                result.disposition = UpsertDisposition::Conflict;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = meta.facts_revision;
                result.error = "identity_quarantined";
                result.message = "record is quarantined after an identity conflict";
                return result;
            }
            if (meta.identity_complete && meta.identity != *identity) {
                result.disposition = UpsertDisposition::Conflict;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = meta.facts_revision;
                result.error = "identity_index_mismatch";
                result.message = "the identity index and the stored record disagree";
                spdlog::error("JobRegistry: identity index for JobID {} does not match the "
                              "stored record; refusing to merge", internal_id);
                return result;
            }
            if (meta.generation.complete() && observation.generation.complete() &&
                meta.generation != observation.generation) {
                result.disposition = UpsertDisposition::Conflict;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = meta.facts_revision;
                result.error = "execution_generation_conflict";
                result.message = "a different live execution generation owns this identity";
                spdlog::warn("JobRegistry: observation for '{}' carries a different live "
                             "generation than JobID {}; not merged",
                             identity->native_job_id, internal_id);
                return result;
            }

            // Merge facts idempotently.
            Job observed_job = observation.facts;
            observed_job.JobID = internal_id;
            observed_job.CollectorNames = stored.CollectorNames;
            apply_identity_to_facts(observed_job, observation.selector, *identity);
            if (!observation.processes.empty()) {
                observed_job.JobPIDs.clear();
                for (const auto& process : observation.processes) {
                    observed_job.JobPIDs.push_back(process.pid);
                }
            }

            Job merged = stored;
            merge_job_facts(merged, observed_job);
            merged.JobID = internal_id;
            merged.CollectorNames = stored.CollectorNames;

            InventoryRecord updated = meta;
            updated.selector = observation.selector;
            updated.last_seen_us = observed_at;
            // last_seen is memory-first: it is persisted with the next real
            // fact change rather than on every observation (task 3 checkpoints
            // heartbeats at reconciliation boundaries).
            if (observation.generation.complete()) updated.generation = observation.generation;
            if (!observation.processes.empty()) {
                updated.processes = normalized_processes(observation.processes);
            } else if (!observed_job.JobPIDs.empty()) {
                updated.processes.clear();
                for (const auto pid : observed_job.JobPIDs) {
                    updated.processes.push_back(ProcessIdentity{pid, 0, ""});
                }
            }

            const bool recovered = meta.state == "suspect";
            if (recovered) updated.state = "active";
            const bool changed = recovered ||
                facts_fingerprint(merged, updated) != facts_fingerprint(stored, meta);
            if (!changed) {
                updated.facts_revision = meta.facts_revision;
                std::unique_lock lg(mtx_);
                records_[internal_id] = updated;
                jobs_[internal_id] = merged;
                result.ok = true;
                result.disposition = UpsertDisposition::Unchanged;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = meta.facts_revision;
                return result;
            }

            updated.facts_revision = meta.facts_revision + 1;
            const std::string record = job_record_json(merged, updated).dump();
            {
                std::lock_guard<std::mutex> db_lock(db_mtx_);
                leveldb::WriteBatch batch;
                batch.Put(key_job(internal_id), record);
                auto status = write_batch_locked(batch, "update inventory facts");
                if (!status.ok()) {
                    result.error = "store_write_failed";
                    result.message = status.ToString();
                    return result;
                }
            }

            note_record_in_memory(updated, merged);
            result.ok = true;
            result.disposition = UpsertDisposition::Updated;
            result.internal_id = internal_id;
            result.instance_id = instanceId(internal_id);
            result.facts_revision = updated.facts_revision;
            spdlog::debug("JobRegistry: inventory facts updated for JobID {} (revision {})",
                          internal_id, updated.facts_revision);

            // Fact changes reach already bound consumers (fresh PIDs/attributes)
            // and the collection runtime outside every registry lock.
            admission.unlock();
            const BindingRecord binding = bindingRecord(internal_id);
            AppliedCollection applied;
            apply_collection_intent(merged, binding, applied);
            return result;
        }
    }

    // Create a new inventory record. Nothing is activated and no caller
    // binding is accepted here.
    std::string allocation_error;
    const uint64_t allocated = allocate_internal_id(allocation_error);
    if (allocated == 0) {
        result.error = allocation_error.empty() ? "store_write_failed" : allocation_error;
        result.message = "durable internal id allocation failed";
        return result;
    }

    Job job = observation.facts;
    job.JobID = allocated;
    job.CollectorNames.clear();
    apply_identity_to_facts(job, observation.selector, *identity);
    job.JobPIDs.clear();
    if (!observation.processes.empty()) {
        for (const auto& process : observation.processes) job.JobPIDs.push_back(process.pid);
    }
    std::sort(job.JobPIDs.begin(), job.JobPIDs.end());
    job.JobPIDs.erase(std::unique(job.JobPIDs.begin(), job.JobPIDs.end()), job.JobPIDs.end());

    InventoryRecord meta;
    meta.internal_id = allocated;
    meta.instance_id = instanceId(allocated);
    meta.selector = observation.selector;
    meta.identity = *identity;
    meta.identity_complete = true;
    meta.generation = observation.generation;
    meta.facts_revision = 1;
    meta.state = "active";
    meta.first_seen_us = observed_at;
    meta.last_seen_us = observed_at;
    if (!observation.processes.empty()) {
        meta.processes = normalized_processes(observation.processes);
    } else {
        for (const auto pid : job.JobPIDs) {
            meta.processes.push_back(ProcessIdentity{pid, 0, ""});
        }
    }

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        leveldb::WriteBatch batch;
        batch.Put(key_job(allocated), job_record_json(job, meta).dump());
        batch.Put(index_key, nlohmann::json{
            {"internal_id", allocated},
            {"scheduler_type", to_string(identity->scheduler_type)},
            {"cluster_namespace", identity->cluster_namespace},
            {"native_job_id", identity->native_job_id},
            {"canonical_native", identity->canonical_native},
            {"step_id", identity->step_id},
        }.dump());
        auto status = write_batch_locked(batch, "create inventory record");
        if (!status.ok()) {
            result.error = "store_write_failed";
            result.message = status.ToString();
            return result;
        }
    }

    note_record_in_memory(meta, job);
    result.ok = true;
    result.disposition = UpsertDisposition::Created;
    result.internal_id = allocated;
    result.instance_id = meta.instance_id;
    result.facts_revision = meta.facts_revision;
    spdlog::info("JobRegistry: created inventory instance {} for {} '{}'",
                 allocated, to_string(identity->scheduler_type), identity->native_job_id);

    // Facts only: an unbound instance creates no collector work, but System
    // collectors must still learn the job exists (plan section 6.3).
    admission.unlock();
    broadcast_lifecycle(JobEvent::Added, job);
    return result;
}

std::optional<InventoryRecord> JobRegistry::inventoryRecord(uint64_t jobID) const {
    std::shared_lock lg(mtx_);
    auto it = records_.find(jobID);
    if (it == records_.end()) return std::nullopt;
    auto record = it->second;
    record.instance_id = derive_instance_id(db_uuid_, jobID);
    return record;
}

std::optional<InventoryRecord> JobRegistry::inventoryRecordByIdentity(
    const CanonicalIdentity& identity) const {
    const std::string key = identity_index_key(identity);
    std::shared_lock lg(mtx_);
    auto it = identity_ids_.find(key);
    if (it == identity_ids_.end()) return std::nullopt;
    auto record = records_.find(it->second);
    if (record == records_.end()) return std::nullopt;
    auto value = record->second;
    value.instance_id = derive_instance_id(db_uuid_, it->second);
    return value;
}

std::vector<InventoryRecord> JobRegistry::inventoryRecords() const {
    std::shared_lock lg(mtx_);
    std::vector<InventoryRecord> out;
    out.reserve(records_.size());
    for (const auto& entry : records_) {
        auto record = entry.second;
        record.instance_id = derive_instance_id(db_uuid_, entry.first);
        out.push_back(std::move(record));
    }
    return out;
}

BindingRecord JobRegistry::bindingRecord(uint64_t jobID) const {
    BindingRecord binding;
    binding.internal_id = jobID;
    if (!db_running || job_db == nullptr) return binding;

    std::lock_guard<std::mutex> db_lock(db_mtx_);
    std::string value;
    auto status = job_db->Get(leveldb::ReadOptions(), key_binding(jobID), &value);
    if (!status.ok()) return binding;
    try {
        binding = binding_from_json(nlohmann::json::parse(value), jobID);
    } catch (const std::exception& error) {
        spdlog::warn("JobRegistry: unreadable binding for JobID {}: {}", jobID, error.what());
    }
    return binding;
}

bool JobRegistry::commitBindings(const std::vector<BindingRecord>& bindings,
                                 std::string& error) {
    if (!store_ready()) {
        error = migration_complete_ ? "store_unavailable" : "store_initializing";
        return false;
    }
    if (bindings.empty()) {
        error = "no_targets";
        return false;
    }

    // Serialize the binding read-modify-write with observation upserts so the
    // derived CollectorNames snapshot cannot be lost by a racing fact update.
    // The runtime application happens outside this admission window: collector
    // hooks must never run while the store transaction is in progress.
    std::vector<std::pair<uint64_t, std::vector<std::string>>> collector_updates;
    {
        std::lock_guard<std::mutex> admission(upsert_mtx_);
        {
            std::shared_lock lg(mtx_);
            for (const auto& binding : bindings) {
                if (closing_jobs_.count(binding.internal_id) != 0) {
                    error = "instance_closing";
                    return false;
                }
            }
        }

        std::lock_guard<std::mutex> db_lock(db_mtx_);
        leveldb::WriteBatch batch;
        for (const auto& binding : bindings) {
            std::string raw;
            auto status = job_db->Get(leveldb::ReadOptions(),
                                      key_job(binding.internal_id), &raw);
            if (!status.ok()) {
                error = status.IsNotFound() ? "not_found" : "store_error";
                return false;
            }
            StoredRecord stored;
            try {
                stored = parse_job_record(raw, binding.internal_id);
            } catch (const std::exception&) {
                error = "store_error";
                return false;
            }
            if (stored.meta.quarantined) {
                error = "identity_quarantined";
                return false;
            }
            stored.job.CollectorNames = binding.effectiveCollectors();
            batch.Put(key_binding(binding.internal_id), binding_to_json(binding).dump());
            batch.Put(key_job(binding.internal_id),
                      job_record_json(stored.job, stored.meta).dump());
            collector_updates.emplace_back(binding.internal_id,
                                           stored.job.CollectorNames);
        }

        auto status = write_batch_locked(batch, "commit bindings");
        if (!status.ok()) {
            error = "store_write_failed";
            return false;
        }
    }

    std::vector<uint64_t> apply_ids;
    {
        std::unique_lock lg(mtx_);
        for (const auto& [internal_id, collectors] : collector_updates) {
            auto it = jobs_.find(internal_id);
            if (it == jobs_.end()) continue;
            if (it->second.CollectorNames == collectors) continue;
            it->second.CollectorNames = collectors;
            apply_ids.push_back(internal_id);
        }
    }

    // Publish to the collection runtime only after the durable commit succeeded.
    // A runtime failure is recorded per instance and reported by the API; the
    // intent itself stays accepted (plan section 6.1). Auto bookkeeping can move
    // without changing the effective selection (manual override), so only real
    // membership changes reach the runtime.
    for (const auto& binding : bindings) {
        if (std::find(apply_ids.begin(), apply_ids.end(), binding.internal_id) == apply_ids.end())
            continue;
        Job job;
        {
            std::shared_lock lg(mtx_);
            const auto it = jobs_.find(binding.internal_id);
            if (it == jobs_.end()) continue;  // retired meanwhile: nothing to apply
            job = it->second;
        }
        AppliedCollection applied;
        apply_collection_intent(job, binding, applied);
    }
    return true;
}

bool JobRegistry::hasInstance(uint64_t jobID) const {
    std::shared_lock lg(mtx_);
    return jobs_.count(jobID) != 0;
}

std::string JobRegistry::instanceId(uint64_t jobID) const {
    if (jobID == kSystemSentinelJobId) return "";
    return derive_instance_id(db_uuid_, jobID);
}

// ---------------------------------------------------------------------------
// Lifecycle fan-out and collection runtime seam
// ---------------------------------------------------------------------------

std::vector<JobLifecycleCb> JobRegistry::lifecycle_callbacks() const {
    std::shared_lock lg(mtx_);
    return cbs_;
}

void JobRegistry::broadcast_lifecycle(JobEvent event, const Job& job) {
    for (const auto& cb : lifecycle_callbacks()) cb(event, job);
}

void JobRegistry::setCollectionApplier(CollectionApplier applier) {
    std::lock_guard<std::mutex> lock(applier_mtx_);
    collection_applier_ = std::move(applier);
}

void JobRegistry::apply_collection_intent(const Job& job, const BindingRecord& binding,
                                          AppliedCollection& applied) {
    CollectionApplier applier;
    {
        std::lock_guard<std::mutex> lock(applier_mtx_);
        applier = collection_applier_;
    }

    applied = AppliedCollection{};
    applied.revision = binding.revision;
    if (!applier) {
        // No collection runtime on this Agent (inventory-only core, tests).
        // The intent is durable, but nothing was applied: say so.
        applied.error = "no_collection_runtime";
    } else {
        std::vector<std::string> collectors;
        std::string error;
        bool ok = false;
        try {
            ok = applier(job, collectors, error);
        } catch (const std::exception& failure) {
            error = std::string("collection runtime threw: ") + failure.what();
        }
        applied.applied = ok;
        applied.collectors = ok ? std::move(collectors) : std::vector<std::string>{};
        if (!ok && error.empty()) error = "collection runtime rejected the binding";
        applied.error = error;
        if (!ok)
            spdlog::warn("JobRegistry: collection intent for JobID {} not applied: {}",
                         job.JobID, error);
    }

    std::lock_guard lg(mtx_);
    if (jobs_.count(job.JobID) == 0) {
        // The instance was retired while the runtime was applying the intent;
        // its applied state no longer has an owner.
        return;
    }
    applied_collections_[job.JobID] = applied;
}

AppliedCollection JobRegistry::appliedCollection(uint64_t jobID) const {
    std::shared_lock lg(mtx_);
    const auto it = applied_collections_.find(jobID);
    if (it != applied_collections_.end()) return it->second;
    return AppliedCollection{};
}

void JobRegistry::mark_applied(uint64_t jobID, uint64_t revision,
                               std::vector<std::string> collectors) {
    std::sort(collectors.begin(), collectors.end());
    collectors.erase(std::unique(collectors.begin(), collectors.end()), collectors.end());
    AppliedCollection applied;
    applied.applied = true;
    applied.revision = revision;
    applied.collectors = std::move(collectors);
    std::lock_guard lg(mtx_);
    if (jobs_.count(jobID) == 0) return;  // retired while the replay ran
    applied_collections_[jobID] = std::move(applied);
}

StoreHealth JobRegistry::storeHealth() const {
    StoreHealth health;
    health.db_running = db_running && job_db != nullptr;
    health.migration_complete = migration_complete_;
    health.migration_state = migration_state_;
    health.schema_version = schema_version_;
    health.db_uuid = db_uuid_;
    health.degraded = store_degraded_.load();
    health.write_failures = store_write_failures_.load();
    health.corrupt_records = corrupt_records_;

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        health.last_write_error = last_store_error_;
    }

    {
        std::shared_lock lg(mtx_);
        health.active_instances = records_.size();
        for (const auto& entry : records_) {
            if (!entry.second.identity_complete) ++health.incomplete_identities;
            if (entry.second.quarantined) ++health.quarantined_identities;
        }
    }
    return health;
}

// 暂时弃用，等待加入bpf特性改为共享指针加作业粒度锁
// const Job* JobRegistry::findJob(int jobID)
// {
//     std::vector<int> toDelete;          
//     const Job* ret = nullptr;

//     {
//         std::shared_lock lg(mtx_);
//         auto it = jobs_.find(jobID);
//         if (it == jobs_.end()) return nullptr;
//         // 其实这里设计的目的是为了惰性加载，在符合实时性的前提下减少调用次数
//         // 更好的方法是用bpf监听相关内容，使用shared_point，使用更加细粒度的锁
//         // 目前这里选择
//         update_job(jobs_[jobID]);
//         // 过滤 PID
//         jobs_[jobID].JobPIDs.erase(
//             std::remove_if(jobs_[jobID].JobPIDs.begin(),
//                            jobs_[jobID].JobPIDs.end(),
//                            [](pid_t pid){ return !is_process_running(pid); }),
//             jobs_[jobID].JobPIDs.end());

//         if (jobs_[jobID].JobPIDs.empty()) {
//             toDelete.push_back(jobID);
//             spdlog::info("JobRegistry: job {} has no running process, delete it", jobs_[jobID].JobID);
//         }
        
//         ret = &jobs_[jobID];
//     }

//     for (int id : toDelete)
//         const_cast<JobRegistry*>(this)->delJob(id);   // 非 const 调用

    
//     return toDelete.empty() ? ret : nullptr;   // 如果删了，就返回空
// }


std::vector<Job> JobRegistry::snapshot() const {
    std::shared_lock lg(mtx_);
    std::vector<Job> out;
    out.reserve(jobs_.size());
    for (const auto& [id, job] : jobs_) {
        (void)id;
        out.push_back(job);
    }
    return out;
}

void JobRegistry::addLifecycleCb(JobLifecycleCb cb) {
    std::vector<Job> replay;
    {
        std::unique_lock lg(mtx_);
        cbs_.push_back(cb);
        // Restored records are already published; replay them once to this
        // subscriber instead of re-adding them (which used to reject
        // empty-binding inventory records).
        replay.reserve(jobs_.size());
        for (const auto& [id, job] : jobs_) {
            if (id == kSystemSentinelJobId) continue;
            replay.push_back(job);
        }
    }

    for (const auto& job : replay) {
        cb(JobEvent::Added, job);
    }

    // The replay is how persisted intent is applied after a restart, so record
    // the same applied state a fresh registration would report.
    for (const auto& job : replay) {
        if (job.CollectorNames.empty()) continue;
        mark_applied(job.JobID, bindingRecord(job.JobID).revision, job.CollectorNames);
    }
}
