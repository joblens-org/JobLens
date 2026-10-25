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
#include "common/utils.hpp"
#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "common/common_job.hpp"
#include "common/local_rpc.hpp"
#include "core/group_probe_stats.hpp"
#include <algorithm>

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
    RPCServer::instance().register_method("JobRegistry/job_opt",
        [this](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            try {
                Job job;
                std::string opt = json2JobOpt(req, job);
                if(opt.compare("add") == 0){
                    if (addJob(job)) {
                        j["status"] = "success";
                    } else {
                        j["status"] = "error";
                        j["msg"] = fmt::format("failed to add job {} (type={}): empty collectors, duplicate sub_attr, "
                                               "duplicate JobID, or a rejected durable write — check server logs",
                                               job.JobID, job.NativeJobID);
                    }
                }else if(opt.compare("remove") == 0){
                    size_t deleted = delJobByRequest(req);
                    j["status"] = "success";
                    j["deleted_count"] = deleted;
                }else {
                    j["status"] = "error";
                    j["msg"] = "invalid operation, expected 'add' or 'remove'";
                }
            } catch (const std::exception& e) {
                j["status"] = "error";
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

    for (const auto& cb : cbs_) cb(JobEvent::Added, job_for_cb);
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
    }

    if (has_callbacks) {
        // Collectors retain the snapshot and acknowledge after their accepted
        // tail has drained; kernel attribution and history wait for that ack.
        for (const auto& cb : cbs_) cb(JobEvent::Removed, removed);
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
            for (const auto& cb : cbs_) cb(JobEvent::Removed, job);
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

    std::lock_guard<std::mutex> admission(upsert_mtx_);
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

    std::vector<std::pair<uint64_t, std::vector<std::string>>> collector_updates;
    {
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

    {
        std::unique_lock lg(mtx_);
        for (const auto& [internal_id, collectors] : collector_updates) {
            auto it = jobs_.find(internal_id);
            if (it != jobs_.end()) it->second.CollectorNames = collectors;
        }
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
}
