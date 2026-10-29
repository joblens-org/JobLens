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
// JobStore implementation — see include/core/job_store.hpp.
//
// Scheduler-specific fact decoding (Condor/Slurm attribute variants) is still
// parked here in decode_facts/selector_from_legacy_job/apply_identity_to_facts
// and moves into the scheduler adapters in v2 stage S4.

#include "core/job_store.hpp"

#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>

#include "common/config.hpp"
#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "common/common_job.hpp"

namespace {

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

const std::string KEY_PREFIX_JOB      = "j:";
const std::string KEY_PREFIX_HISTORY  = "h:";
const std::string KEY_PREFIX_COUNTER  = "c:";
const std::string KEY_PREFIX_META     = "m:";
const std::string KEY_PREFIX_IDENTITY = "i:";
const std::string KEY_PREFIX_BINDING  = "b:";

constexpr uint32_t kInventorySchemaVersion = 2;
constexpr size_t kMigrationBatchSize = 64;

std::string hex64(uint64_t v) {
    return fmt::format("{:016x}", v);
}

uint64_t unhex64(const std::string& s) {
    return std::stoull(s, nullptr, 16);
}

std::string key_job(uint64_t jobID) {
    return KEY_PREFIX_JOB + hex64(jobID);
}

std::string key_history(uint64_t jobID, uint64_t end_time) {
    return KEY_PREFIX_HISTORY + hex64(jobID) + ":" + hex64(end_time);
}

std::string key_counter(const std::string& name) {
    return KEY_PREFIX_COUNTER + name;
}

std::string key_meta(const std::string& name) {
    return KEY_PREFIX_META + name;
}

std::string key_binding(uint64_t jobID) {
    return KEY_PREFIX_BINDING + hex64(jobID);
}

// Unambiguous identity index key. Canonical components are validated
// (scheduler name, digits/dot native id, normalized step), so the only
// free-form part is the cluster namespace and it is length-prefixed.
std::string identity_index_key(const CanonicalIdentity& identity) {
    std::string key;
    key.reserve(identity.cluster_namespace.size() + identity.canonical_native.size() + 32);
    key += identity.adapter;
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

std::string key_identity(const std::string& encoded) {
    return KEY_PREFIX_IDENTITY + encoded;
}

int64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string generate_db_uuid() {
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
// Process liveness helpers (restore-time validation)
// ---------------------------------------------------------------------------

bool is_process_running(pid_t pid) {
    // EPERM 表示进程存在但当前用户无权限发送信号（例如 pid 1）。
    return kill(pid, 0) == 0 || errno == EPERM;
}

// /proc/<pid>/stat field 22 (starttime), used to tell a live manager process
// from a recycled PID on restore.
std::optional<uint64_t> read_proc_start_ticks(pid_t pid) {
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

std::string read_boot_id() {
    std::ifstream input("/proc/sys/kernel/random/boot_id");
    std::string value;
    if (input && std::getline(input, value)) return value;
    return {};
}

// A complete generation whose boot differs, whose manager start time does not
// match (PID reuse) or whose manager is gone cannot be replayed. Without exact
// evidence the record is only marked suspect; it is never deleted here.
bool generation_is_stale(const ExecutionGeneration& generation,
                         const std::string& current_boot_id) {
    if (!generation.complete()) return false;
    if (!current_boot_id.empty() && generation.boot_id != current_boot_id) return true;
    const auto ticks = read_proc_start_ticks(generation.manager_pid);
    if (ticks.has_value()) return *ticks != generation.manager_start_ticks;
    return !is_process_running(generation.manager_pid);
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

nlohmann::json generation_to_json(const ExecutionGeneration& generation) {
    return nlohmann::json{
        {"boot_id", generation.boot_id},
        {"manager_pid", generation.manager_pid},
        {"manager_start_ticks", generation.manager_start_ticks},
        {"cgroup_identity", generation.cgroup_identity},
    };
}

ExecutionGeneration generation_from_json(const nlohmann::json& in) {
    ExecutionGeneration generation;
    generation.boot_id = in.value("boot_id", std::string{});
    generation.manager_pid = in.value("manager_pid", pid_t{0});
    generation.manager_start_ticks = in.value("manager_start_ticks", uint64_t{0});
    generation.cgroup_identity = in.value("cgroup_identity", std::string{});
    return generation;
}

nlohmann::json process_to_json(const ProcessIdentity& process) {
    return nlohmann::json{
        {"pid", process.pid},
        {"start_ticks", process.start_ticks},
        {"boot_id", process.boot_id},
    };
}

ProcessIdentity process_from_json(const nlohmann::json& in) {
    ProcessIdentity process;
    process.pid = in.value("pid", pid_t{0});
    process.start_ticks = in.value("start_ticks", uint64_t{0});
    process.boot_id = in.value("boot_id", std::string{});
    return process;
}

std::vector<ProcessIdentity> normalized_processes(std::vector<ProcessIdentity> processes) {
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

void write_job_fields(nlohmann::json& out, const Job& job) {
    out["jobtype"]       = static_cast<int>(job.jobtype);
    out["subtype"]       = static_cast<int>(job.subtype);
    if (!job.subtype_name.empty()) out["subtype_name"] = job.subtype_name;
    out["pids"]          = job.JobPIDs;
    out["collectors"]    = job.CollectorNames;
    out["native_job_id"] = job.NativeJobID;
    out["cluster_name"]  = job.cluster_name;
    out["clusterTag"]    = job.clusterTag;
    std::visit([&out](const auto& obj) {
        out["sub_attr"] = nlohmann::json(obj);
    }, job.sub_attr);
}

nlohmann::json record_json(const JobRecord& record) {
    nlohmann::json out;
    write_job_fields(out, record.job);
    out["schema_version"] = kInventorySchemaVersion;
    out["revision"]       = record.facts_revision;
    out["state"]          = record.state;
    out["quarantined"]    = record.quarantined;
    out["first_seen_us"]  = record.first_seen_us;
    out["last_seen_us"]   = record.last_seen_us;
    out["members_authoritative"] = record.members_authoritative;
    if (!record.adapter.empty()) out["adapter"] = record.adapter;
    if (record.selector.adapter.empty()) {
        out["selector"] = nullptr;
    } else {
        out["selector"] = record.selector;
    }
    if (record.identity_complete) out["identity"] = record.identity;
    else                          out["identity"] = nullptr;
    out["generation"]     = generation_to_json(record.generation);
    out["members"]        = nlohmann::json::array();
    for (const auto& member : record.members) {
        out["members"].push_back(process_to_json(member));
    }
    out["applied"] = nlohmann::json{
        {"applied", record.applied.applied},
        {"revision", record.applied.revision},
        {"collectors", record.applied.collectors},
        {"error", record.applied.error},
    };
    return out;
}

Job decode_legacy_job(const std::string& json_str) {
    auto j = nlohmann::json::parse(json_str);
    Job job;
    job.jobtype       = static_cast<JobType>(j.value("jobtype", 0));
    job.subtype       = static_cast<JobSubType>(j.value("subtype", 0));
    job.subtype_name  = j.value("subtype_name", "");
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
    } else if (!job.subtype_name.empty()) {
        // Open-world scheduler: the adapter owns the facts schema; the JSON
        // object is carried verbatim (the adapter's decodeFacts is the only
        // interpreter).
        job.sub_attr = j.contains("sub_attr") && j["sub_attr"].is_object()
                           ? j["sub_attr"]
                           : nlohmann::json::object();
    } else {
        job.sub_attr = CommonJobAttr{.auto_update_child = true};
    }

    return job;
}

JobRecord parse_record(const std::string& raw, uint64_t internal_id) {
    JobRecord record;
    record.job = decode_legacy_job(raw);
    record.job.JobID = internal_id;
    record.internal_id = internal_id;

    const auto j = nlohmann::json::parse(raw);
    record.facts_revision = j.value("revision", uint64_t{1});
    record.state          = j.value("state", std::string{kStateActive});
    record.quarantined    = j.value("quarantined", false);
    record.first_seen_us  = j.value("first_seen_us", int64_t{0});
    record.last_seen_us   = j.value("last_seen_us", int64_t{0});
    record.members_authoritative = j.value("members_authoritative", false);
    record.adapter        = j.value("adapter", std::string{});
    if (record.facts_revision == 0) record.facts_revision = 1;

    if (j.contains("selector") && j["selector"].is_object() &&
        j["selector"].value("scheduler_type", std::string{"unknown"}) != "unknown") {
        // Legacy rows carry "scheduler_type": "unknown" for "no selector";
        // the selector round trip itself accepts either key (adapter wins).
        try {
            record.selector = j["selector"].get<NativeSelector>();
        } catch (const std::exception& error) {
            spdlog::warn("JobStore: invalid stored selector for job {}: {}",
                         internal_id, error.what());
        }
    }
    if (j.contains("identity") && j["identity"].is_object()) {
        try {
            record.identity = j["identity"].get<CanonicalIdentity>();
            record.identity_complete = true;
        } catch (const std::exception& error) {
            spdlog::warn("JobStore: invalid stored identity for job {}: {}",
                         internal_id, error.what());
        }
    }
    if (j.contains("generation") && j["generation"].is_object()) {
        record.generation = generation_from_json(j["generation"]);
    }
    const auto members_it = j.contains("members") ? j.find("members") : j.find("processes");
    if (members_it != j.end() && members_it->is_array()) {
        for (const auto& process : *members_it) {
            if (process.is_object()) record.members.push_back(process_from_json(process));
        }
        record.members = normalized_processes(std::move(record.members));
    }
    if (j.contains("applied") && j["applied"].is_object()) {
        const auto& applied = j["applied"];
        record.applied.applied = applied.value("applied", false);
        record.applied.revision = applied.value("revision", uint64_t{0});
        record.applied.collectors =
            applied.value("collectors", nlohmann::json::array()).get<std::vector<std::string>>();
        record.applied.error = applied.value("error", std::string{});
    }
    return record;
}

nlohmann::json binding_to_json(const BindingRecord& binding) {
    return nlohmann::json{
        {"internal_id", binding.internal_id},
        {"revision", binding.revision},
        {"mode", to_string(binding.mode)},
        {"auto_collectors", binding.auto_collectors},
        {"manual_collectors", binding.manual_collectors},
        {"policy_revision", binding.policy_revision},
    };
}

BindingRecord binding_from_json(const nlohmann::json& in, uint64_t internal_id) {
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

// ---------------------------------------------------------------------------
// Fact merging
// ---------------------------------------------------------------------------

bool merge_text(std::string& target, const std::string& source) {
    if (source.empty() || target == source) return false;
    target = source;
    return true;
}

template <typename T>
bool merge_scalar(T& target, T source) {
    if (source == T{} || target == source) return false;
    target = source;
    return true;
}

bool merge_flag(bool& target, bool source) {
    if (!source || target) return false;
    target = true;
    return true;
}

bool merge_condor_attr(CondorJobAttr& target, const CondorJobAttr& source) {
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

bool merge_slurm_attr(SlurmJobAttr& target, const SlurmJobAttr& source) {
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

bool merge_common_attr(CommonJobAttr& target, const CommonJobAttr& source) {
    return merge_flag(target.auto_update_child, source.auto_update_child);
}

// Merge observation facts into the stored facts. Empty/default observation
// fields never erase known facts; CollectorNames is never taken from an
// observation because selection is intent, not a discovery fact.
//
// `replace_members` upgrades the PID list to an authoritative replacement: an
// empty list then means "the source read the membership and found none", which
// must clear stale PIDs instead of silently keeping them. Without the flag
// (membership only partially known) an empty list is ignored.
bool merge_job_facts(Job& target, const Job& observed, bool replace_members) {
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

    std::vector<pid_t> observed_pids = observed.JobPIDs;
    std::sort(observed_pids.begin(), observed_pids.end());
    observed_pids.erase(std::unique(observed_pids.begin(), observed_pids.end()),
                        observed_pids.end());
    // Authoritative replacement (possibly empty), or a non-empty merge list;
    // a partial member set never erases stored members.
    if (replace_members || !observed_pids.empty()) {
        if (observed_pids != target.JobPIDs) {
            target.JobPIDs = std::move(observed_pids);
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

nlohmann::json facts_fingerprint(const Job& job, const JobRecord& record) {
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

    nlohmann::json members = nlohmann::json::array();
    for (const auto& member : normalized_processes(record.members)) {
        members.push_back(process_to_json(member));
    }
    out["members"] = std::move(members);

    std::visit([&out](const auto& attr) {
        out["sub_attr"] = nlohmann::json(attr);
    }, job.sub_attr);
    out["generation"] = generation_to_json(record.generation);
    return out;
}

// ---------------------------------------------------------------------------
// Scheduler-specific fact helpers (move into adapters in S4)
// ---------------------------------------------------------------------------

// Best-effort selector reconstruction for legacy (schema < 2) records. A
// missing namespace or native id yields an incomplete selector, never a guess.
NativeSelector selector_from_legacy_job(const Job& job) {
    NativeSelector selector;
    if (job.subtype == JobSubType::Condor) {
        selector.adapter = kAdapterCondor;
        selector.cluster_name = job.cluster_name;
        selector.cluster_tag = job.clusterTag;
        selector.native_job_id = job.NativeJobID;
        if (selector.native_job_id.empty()) {
            if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr)) {
                selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
            }
        }
    } else if (job.subtype == JobSubType::Slurm) {
        selector.adapter = kAdapterSlurm;
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

// Apply a resolved logical identity to typed facts through the registry's
// injected applier (adapter-owned derivation). The generic parts — the
// authoritative native string and namespace — apply here; the scheduler
// typed-attr derivation lives in the adapters. Without an applier (store-only
// tests) the observed facts are stored verbatim: no branch, no misfiling of
// an open-world scheduler into a built-in schema.
void apply_identity_via_applier(
    const std::function<void(Job&, const NativeSelector&, const CanonicalIdentity&)>& applier,
    Job& job, const NativeSelector& selector, const CanonicalIdentity& identity) {
    job.NativeJobID = identity.native_job_id;
    if (!selector.cluster_name.empty()) job.cluster_name = selector.cluster_name;
    if (!selector.cluster_tag.empty()) job.clusterTag = selector.cluster_tag;
    if (job.jobtype != JobType::Job && job.jobtype != JobType::Sys) {
        job.jobtype = JobType::Job;
    }
    if (applier) applier(job, selector, identity);
}

}  // namespace

// ---------------------------------------------------------------------------
// Intent value types
// ---------------------------------------------------------------------------

std::vector<std::string> BindingRecord::effectiveCollectors() const
{
    return mode == CollectionMode::Manual ? manual_collectors : auto_collectors;
}

std::optional<CollectionMode> collection_mode_from_string(const std::string& raw)
{
    if (raw == "manual") return CollectionMode::Manual;
    if (raw == "auto") return CollectionMode::Auto;
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Construction / open / migration
// ---------------------------------------------------------------------------

JobStore::JobStore(const Options& options) : options_(options) {
    migration_batch_limit_ = options.migration_batch_limit;
    if (!init_db(options)) {
        db_running_ = false;
        spdlog::warn("JobStore: job db not running");
    }
}

JobStore::~JobStore()
{
    close_db();
}

bool JobStore::init_db(const Options& options) {
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
    leveldb::Status status = leveldb::DB::Open(level_options, db_path, &db_);
    if (!status.ok()) {
        spdlog::error("JobStore: failed to open LevelDB at {}: {}", db_path, status.ToString());
        db_ = nullptr;
        return false;
    }

    spdlog::info("JobStore: opened {}", db_path);
    db_running_ = true;

    // Metadata first: schema version decides whether a legacy migration runs.
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        std::string value;
        auto s = db_->Get(leveldb::ReadOptions(), key_meta("schema_version"), &value);
        if (s.ok()) {
            try {
                schema_version_ = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                schema_version_ = 0;
            }
        }
    }

    if (schema_version_ > kInventorySchemaVersion) {
        spdlog::error("JobStore: db schema {} is newer than this binary ({}), refusing to open",
                      schema_version_, kInventorySchemaVersion);
        delete db_;
        db_ = nullptr;
        db_running_ = false;
        return false;
    }

    migration_complete_ = schema_version_ == kInventorySchemaVersion;
    if (!migration_complete_) {
        if (!run_migration()) {
            spdlog::warn("JobStore: inventory migration interrupted ({}); "
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
        if (db_->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value).ok()) {
            db_uuid_ = value;
        }
    }

    if (db_uuid_.empty()) {
        if (!load_or_create_db_uuid()) {
            spdlog::error("JobStore: could not persist the Agent DB UUID");
        }
    }

    try {
        restore();
    } catch (const std::exception& e) {
        spdlog::warn("JobStore: failed to restore records from db: {}", e.what());
    }

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        init_id_counter();
    }

    return true;
}

void JobStore::close_db() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    delete db_;
    db_ = nullptr;
    db_running_ = false;
    spdlog::info("JobStore: closed");
}

bool JobStore::load_or_create_db_uuid() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return false;

    std::string value;
    auto s = db_->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value);
    if (s.ok() && !value.empty()) {
        db_uuid_ = value;
        return true;
    }

    db_uuid_ = generate_db_uuid();
    leveldb::WriteBatch batch;
    batch.Put(key_meta("db_uuid"), db_uuid_);
    auto status = write_batch_locked(batch, "create db uuid");
    if (!status.ok()) {
        spdlog::error("JobStore: failed to persist db uuid: {}", status.ToString());
        return false;
    }
    return true;
}

bool JobStore::resume_migration_state(std::string& stage, std::string& cursor) {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    std::string value;
    auto s = db_->Get(leveldb::ReadOptions(), key_meta("migration"), &value);
    if (!s.ok()) return false;
    try {
        const auto state = nlohmann::json::parse(value);
        if (state.value("state", std::string{}) != "in_progress") return false;
        stage = state.value("stage", std::string{"allocator"});
        cursor = state.value("cursor", std::string{});
        return true;
    } catch (const std::exception& error) {
        spdlog::warn("JobStore: unreadable migration state, restarting migration: {}",
                     error.what());
        return false;
    }
}

bool JobStore::migrate_allocator() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return false;

    uint64_t max_id = 0;
    {
        leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
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
            spdlog::error("JobStore: migration allocator scan failed: {}", scan_status.ToString());
            return false;
        }
    }

    uint64_t counter = 0;
    {
        std::string value;
        auto s = db_->Get(leveldb::ReadOptions(), key_counter("job_id"), &value);
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
        if (db_->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value).ok()) {
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
        spdlog::error("JobStore: migration could not raise the allocator high-water mark: {}",
                      status.ToString());
        return false;
    }

    db_uuid_ = uuid;
    migration_state_ = "in_progress:index";
    spdlog::info("JobStore: migration allocator high-water mark = {}", high_water);
    return true;
}

bool JobStore::migrate_index(std::string& cursor, size_t& batches, size_t batch_limit) {
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
            spdlog::error("JobStore: migration batch failed: {}", status.ToString());
            return false;
        }
        ++batches;
        spdlog::info("JobStore: migration indexed {} records so far", processed);
        return true;
    };

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        if (!db_) return false;

        // Identity ownership must be visible to later records in the same
        // batch, so keep an in-memory view seeded from the committed index.
        std::unordered_map<std::string, uint64_t> index_owners;
        {
            leveldb::Iterator* index = db_->NewIterator(leveldb::ReadOptions());
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

        leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
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

            JobRecord record;
            try {
                record = parse_record(it->value().ToString(), internal_id);
            } catch (const std::exception& error) {
                spdlog::error("JobStore: corrupt record {} during migration: {}", key, error.what());
                ++corrupt_records_;
                cursor = key;
                continue;
            }

            record.selector = selector_from_legacy_job(record.job);
            record.facts_revision = 1;
            record.first_seen_us = record.first_seen_us != 0 ? record.first_seen_us : now_us();
            record.last_seen_us = record.first_seen_us;
            record.state = kStateActive;
            record.members.clear();
            for (const auto pid : record.job.JobPIDs) {
                record.members.push_back(ProcessIdentity{pid, 0, ""});
            }

            const auto identity = canonicalize(record.selector);
            record.identity_complete = identity.has_value();
            if (identity) {
                record.identity = *identity;
                const std::string index_key = key_identity(identity_index_key(*identity));
                const auto owner_it = index_owners.find(index_key);
                if (owner_it != index_owners.end() && owner_it->second != 0 &&
                    owner_it->second != internal_id) {
                    record.quarantined = true;
                    ++quarantined;
                    spdlog::warn("JobStore: conflicting live identity for JobID {} "
                                 "(already owned by JobID {}), quarantined instead of merged",
                                 internal_id, owner_it->second);
                } else if (owner_it == index_owners.end()) {
                    batch.Put(index_key, nlohmann::json{
                        {"internal_id", internal_id},
                        {"scheduler_type", identity->adapter},
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
            if (!record.job.CollectorNames.empty()) {
                BindingRecord binding;
                binding.internal_id = internal_id;
                binding.revision = 1;
                binding.mode = CollectionMode::Manual;
                binding.manual_collectors = record.job.CollectorNames;
                binding.policy_revision = "legacy";
                batch.Put(key_binding(internal_id), binding_to_json(binding).dump());
            }

            batch.Put(key, record_json(record).dump());
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
                    spdlog::warn("JobStore: migration stopped after {} batch(es) for resume testing",
                                 batches);
                    delete it;
                    return false;
                }
            }
        }

        const auto status = it->status();
        delete it;
        if (!status.ok()) {
            spdlog::error("JobStore: migration iterator failed: {}", status.ToString());
            return false;
        }

        if (pending > 0) {
            if (!flush(cursor)) return false;
            batch.Clear();
        }
        if (batch_limit != 0 && batches >= batch_limit) {
            spdlog::warn("JobStore: migration stopped after {} batch(es) for resume testing",
                         batches);
            return false;
        }
    }

    migration_state_ = "in_progress:finalize";
    spdlog::info("JobStore: migration indexed {} records ({} quarantined, {} incomplete)",
                 processed, quarantined, incomplete);
    return true;
}

bool JobStore::migrate_finalize() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return false;

    leveldb::WriteBatch batch;
    batch.Put(key_meta("schema_version"), std::to_string(kInventorySchemaVersion));
    batch.Put(key_meta("migration"), nlohmann::json{
        {"state", "complete"},
        {"stage", "done"},
        {"completed_at_us", now_us()},
    }.dump());

    auto status = write_batch_locked(batch, "migration finalize");
    if (!status.ok()) {
        spdlog::error("JobStore: migration finalize failed: {}", status.ToString());
        return false;
    }

    {
        std::string value;
        if (db_->Get(leveldb::ReadOptions(), key_meta("db_uuid"), &value).ok() && !value.empty()) {
            db_uuid_ = value;
        }
    }
    migration_state_ = "complete";
    spdlog::info("JobStore: inventory migration complete (schema {})", kInventorySchemaVersion);
    return true;
}

bool JobStore::run_migration() {
    std::string stage = "allocator";
    std::string cursor;
    if (resume_migration_state(stage, cursor)) {
        spdlog::info("JobStore: resuming interrupted migration at stage {} (cursor {})",
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

std::vector<JobRecord> JobStore::restore() {
    const std::string boot_id = read_boot_id();
    std::vector<JobRecord> restored;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        if (!db_) return restored;
        leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
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
                auto record = parse_record(it->value().ToString(), jobID);
                if (record.state == kStateClosing) {
                    // An interrupted retirement: the drain owns the record, it
                    // must not come back as an active instance. It stays out of
                    // the active set and is archived by the closing cleanup.
                    restored.push_back(std::move(record));
                    continue;
                }
                // Validate restored executions before publishing them: a
                // stale generation is marked suspect (never replayed), dead
                // PIDs are dropped and never replayed into kernel maps.
                if (generation_is_stale(record.generation, boot_id)) {
                    record.state = kStateSuspect;
                    record.job.JobPIDs.clear();
                    record.members.clear();
                    spdlog::warn("JobStore: restored JobID {} has a stale execution "
                                 "generation; marked suspect", jobID);
                } else {
                    std::vector<pid_t> live;
                    for (const auto pid : record.job.JobPIDs) {
                        if (is_process_running(pid)) live.push_back(pid);
                    }
                    if (live.size() != record.job.JobPIDs.size()) {
                        record.job.JobPIDs = std::move(live);
                        if (record.job.JobPIDs.empty()) record.state = kStateSuspect;
                        std::vector<ProcessIdentity> members;
                        for (const auto pid : record.job.JobPIDs) {
                            members.push_back(ProcessIdentity{pid, 0, ""});
                        }
                        if (!members.empty()) record.members = std::move(members);
                    }
                }
                restored.push_back(std::move(record));
            } catch (const std::exception& error) {
                ++corrupt_records_;
                spdlog::warn("JobStore: cannot restore job {}: {}", jobID, error.what());
            }
        }
        const auto status = it->status();
        delete it;
        if (!status.ok()) {
            spdlog::warn("JobStore: iterator error during restore: {}", status.ToString());
        }
    }

    size_t closing_restored = 0;
    {
        std::unique_lock lg(mtx_);
        for (auto& record : restored) {
            record.instance_id = derive_instance_id(db_uuid_, record.internal_id);
            if (record.state == kStateClosing) {
                ++closing_restored;
            }
            if (record.identity_complete && !record.quarantined) {
                identity_index_[identity_index_key(record.identity)] = record.internal_id;
            }
            records_[record.internal_id] = record;
        }
    }
    if (closing_restored > 0) {
        spdlog::warn("JobStore: restored {} interrupted closing record(s); they will be "
                     "archived by the closing cleanup", closing_restored);
    }

    size_t incomplete = 0, quarantined = 0;
    for (const auto& record : restored) {
        incomplete += record.identity_complete ? 0 : 1;
        quarantined += record.quarantined ? 1 : 0;
    }
    spdlog::info("JobStore: restored {} inventory records ({} incomplete, {} quarantined)",
                 restored.size(), incomplete, quarantined);
    return restored;
}

// ---------------------------------------------------------------------------
// Faults, health, ids
// ---------------------------------------------------------------------------

bool JobStore::store_ready() const {
    return db_running_ && db_ != nullptr && migration_complete_;
}

bool JobStore::ready() const {
    return store_ready();
}

leveldb::Status JobStore::write_batch_locked(leveldb::WriteBatch& batch,
                                             const char* operation) {
    if (!db_) return leveldb::Status::IOError("job db is not open");

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
    auto status = db_->Write(write_options, &batch);

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

void JobStore::record_store_failure(const std::string& message) {
    store_write_failures_.fetch_add(1);
    store_degraded_.store(true);
    last_store_error_ = message;
    spdlog::error("JobStore: {}", message);
}

void JobStore::record_store_success() {
    store_degraded_.store(false);
    last_store_error_.clear();
}

void JobStore::injectFaultForTest(StoreFaultMode mode, size_t skip, size_t count) {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    fault_mode_ = mode;
    fault_skip_ = skip;
    fault_count_ = count;
}

void JobStore::clearFaultsForTest() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    fault_mode_ = StoreFaultMode::None;
    fault_skip_ = 0;
    fault_count_ = 0;
}

void JobStore::init_id_counter() {
    if (!db_) {
        spdlog::warn("JobStore: db not running, using default next_id_ = 1");
        return;
    }

    try {
        uint64_t counter_value = 1;

        std::string counter_json;
        leveldb::Status s = db_->Get(leveldb::ReadOptions(), key_counter("job_id"), &counter_json);
        if (s.ok()) {
            counter_value = std::stoull(counter_json);
        }

        uint64_t max_job_id = 0;

        // 扫描 jobs 前缀取最大 JobID
        leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
        it->Seek(KEY_PREFIX_JOB);
        if (it->Valid() && it->key().starts_with(KEY_PREFIX_JOB)) {
            it->SeekToLast();
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
            id_exhausted_ = true;
        } else {
            high_water = std::max(counter_value, max_job_id + 1);
        }
        if (high_water == 0) high_water = 1;
        next_id_ = high_water;

        // 持久化当前值 (durable high-water reservation)
        leveldb::WriteBatch batch;
        batch.Put(key_counter("job_id"), std::to_string(next_id_));
        auto status = write_batch_locked(batch, "init job id counter");
        if (!status.ok()) {
            spdlog::error("JobStore: failed to persist job_id_counter: {}", status.ToString());
        }

        spdlog::info("JobStore: initialized job_id counter, next_id_ = {}", next_id_);
    } catch (const std::exception& e) {
        spdlog::error("JobStore: failed to init job_id counter: {}", e.what());
        next_id_ = 1;
    }
}

uint64_t JobStore::allocateId(std::string& error) {
    std::lock_guard<std::mutex> id_lock(id_mtx_);
    if (id_exhausted_ || next_id_ == 0) {
        error = "id_exhausted";
        spdlog::error("JobStore: internal id space is exhausted, refusing allocation");
        return 0;
    }

    const uint64_t id = next_id_;
    if (id == std::numeric_limits<uint64_t>::max()) {
        id_exhausted_ = true;
        next_id_ = 0;
    } else {
        next_id_ = id + 1;
    }

    // Reserve the id durably before it is handed out. An ambiguous outcome
    // still advances the in-memory high-water mark: ids are never reused.
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    leveldb::WriteBatch batch;
    batch.Put(key_counter("job_id"),
              std::to_string(next_id_ == 0 ? id : next_id_));
    auto status = write_batch_locked(batch, "allocate internal id");
    if (!status.ok()) {
        error = "store_write_failed";
        spdlog::error("JobStore: durable id allocation failed for id {}, id is burned", id);
        return 0;
    }
    return id;
}

uint64_t JobStore::nextJobId() {
    if (!store_ready()) {
        spdlog::warn("JobStore: refusing JobID allocation before the inventory store is ready");
        return 0;
    }
    std::string error;
    const uint64_t job_id = allocateId(error);
    if (job_id == 0) {
        spdlog::error("JobStore: JobID allocation failed: {}", error);
        return 0;
    }
    return job_id;
}

// ---------------------------------------------------------------------------
// Manual registration (legacy addJob persistence)
// ---------------------------------------------------------------------------

bool JobStore::insertManual(JobRecord record) {
    if (!store_ready()) return false;

    record.facts_revision = std::max<uint64_t>(record.facts_revision, 1);
    if (record.state.empty()) record.state = kStateActive;
    if (record.first_seen_us == 0) record.first_seen_us = now_us();
    if (record.last_seen_us == 0) record.last_seen_us = record.first_seen_us;
    record.instance_id = derive_instance_id(db_uuid_, record.internal_id);
    if (record.selector.adapter.empty()) {
        // Legacy registrations carry their identity only inside the typed
        // facts; reconstruct the selector, never a guess.
        record.selector = selector_from_legacy_job(record.job);
        record.adapter = record.selector.adapter;
        if (const auto identity = canonicalize(record.selector)) {
            record.identity = *identity;
            record.identity_complete = true;
        }
    }
    record.members.clear();
    for (const auto pid : record.job.JobPIDs) {
        record.members.push_back(ProcessIdentity{pid, 0, ""});
    }

    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    leveldb::WriteBatch batch;

    // A legacy register is a manual selection: the caller explicitly asked
    // for these collectors, so it cannot be reported as an automatic origin.
    if (!record.job.CollectorNames.empty()) {
        BindingRecord binding;
        binding.internal_id = record.internal_id;
        binding.revision = 1;
        binding.mode = CollectionMode::Manual;
        binding.manual_collectors = record.job.CollectorNames;
        binding.policy_revision = "legacy_add";
        record.intent = binding;
        batch.Put(key_binding(record.internal_id), binding_to_json(binding).dump());
    }

    if (record.identity_complete) {
        const std::string index_key = key_identity(identity_index_key(record.identity));
        std::string existing;
        auto s = db_->Get(leveldb::ReadOptions(), index_key, &existing);
        if (!s.ok() && !s.IsNotFound()) {
            record_store_failure("LevelDB read failed (identity index): " + s.ToString());
            return false;
        }
        if (s.IsNotFound()) {
            batch.Put(index_key, nlohmann::json{
                {"internal_id", record.internal_id},
                {"scheduler_type", record.identity.adapter},
                {"cluster_namespace", record.identity.cluster_namespace},
                {"native_job_id", record.identity.native_job_id},
                {"canonical_native", record.identity.canonical_native},
                {"step_id", record.identity.step_id},
            }.dump());
        } else {
            uint64_t owner = 0;
            try {
                owner = nlohmann::json::parse(existing).value("internal_id", uint64_t{0});
            } catch (const std::exception&) {
                owner = 0;
            }
            if (owner != 0 && owner != record.internal_id) {
                spdlog::error("JobStore: identity for JobID {} is already registered as "
                              "JobID {}, refusing a duplicate inventory record",
                              record.internal_id, owner);
                return false;
            }
            batch.Put(index_key, nlohmann::json{
                {"internal_id", record.internal_id},
                {"scheduler_type", record.identity.adapter},
                {"cluster_namespace", record.identity.cluster_namespace},
                {"native_job_id", record.identity.native_job_id},
                {"canonical_native", record.identity.canonical_native},
                {"step_id", record.identity.step_id},
            }.dump());
        }
    }

    batch.Put(key_job(record.internal_id), record_json(record).dump());
    auto status = write_batch_locked(batch, "register job");
    if (!status.ok()) {
        return false;
    }

    const uint64_t registered_id = record.internal_id;
    note_record(std::move(record));
    spdlog::info("JobStore: persisted manual registration of JobID {}", registered_id);
    return true;
}

// ---------------------------------------------------------------------------
// Upsert (the single identity-resolving admission path)
// ---------------------------------------------------------------------------

UpsertResult JobStore::upsert(const ObservedExecution& observation, JobRecord& out) {
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
        spdlog::debug("JobStore: rejected incomplete observation (native='{}', cluster='{}')",
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
        auto s = db_->Get(leveldb::ReadOptions(), index_key, &value);
        if (s.ok()) {
            try {
                internal_id = nlohmann::json::parse(value).value("internal_id", uint64_t{0});
                indexed = internal_id != 0;
            } catch (const std::exception& error) {
                spdlog::warn("JobStore: unreadable identity index: {}", error.what());
            }
        } else if (!s.IsNotFound()) {
            result.error = "store_error";
            result.message = s.ToString();
            record_store_failure("LevelDB read failed (identity lookup): " + s.ToString());
            return result;
        }
    }

    if (indexed) {
        JobRecord stored;
        bool have_record = false;
        {
            std::lock_guard<std::mutex> db_lock(db_mtx_);
            std::string raw;
            auto s = db_->Get(leveldb::ReadOptions(), key_job(internal_id), &raw);
            if (s.ok()) {
                try {
                    stored = parse_record(raw, internal_id);
                    have_record = true;
                } catch (const std::exception& error) {
                    spdlog::warn("JobStore: unreadable record {}: {}", internal_id, error.what());
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
            spdlog::warn("JobStore: stale identity index for JobID {}, repairing", internal_id);
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
            if (stored.quarantined) {
                result.disposition = UpsertDisposition::Conflict;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = stored.facts_revision;
                result.error = "identity_quarantined";
                result.message = "record is quarantined after an identity conflict";
                return result;
            }
            if (stored.state == kStateClosing) {
                // A retiring instance must never be resurrected into the
                // active set by a late observation: the drain owns it until it
                // is archived, and a new execution registers as a new instance.
                result.disposition = UpsertDisposition::Conflict;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = stored.facts_revision;
                result.error = "instance_closing";
                result.message = "the instance is retiring and no longer accepts observations";
                return result;
            }
            if (stored.identity_complete && stored.identity != *identity) {
                result.disposition = UpsertDisposition::Conflict;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = stored.facts_revision;
                result.error = "identity_index_mismatch";
                result.message = "the identity index and the stored record disagree";
                spdlog::error("JobStore: identity index for JobID {} does not match the "
                              "stored record; refusing to merge", internal_id);
                return result;
            }
            if (stored.generation.complete() && observation.generation.complete() &&
                stored.generation != observation.generation) {
                result.disposition = UpsertDisposition::Conflict;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = stored.facts_revision;
                result.error = "execution_generation_conflict";
                result.message = "a different live execution generation owns this identity";
                spdlog::warn("JobStore: observation for '{}' carries a different live "
                             "generation than JobID {}; not merged",
                             identity->native_job_id, internal_id);
                return result;
            }

            // Merge facts idempotently. The member list is authoritative only
            // when the source says so: an empty list from a partially-known
            // membership must never erase stored members.
            Job observed_job = observation.facts;
            observed_job.JobID = internal_id;
            observed_job.CollectorNames = stored.job.CollectorNames;
            apply_identity_via_applier(identity_applier_, observed_job, observation.selector, *identity);
            const bool authoritative_members = observation.members_complete;
            if (!observation.processes.empty()) {
                observed_job.JobPIDs.clear();
                for (const auto& process : observation.processes) {
                    observed_job.JobPIDs.push_back(process.pid);
                }
            } else if (observation.members_complete) {
                // Read succeeded, no members: an authoritative empty set.
                observed_job.JobPIDs.clear();
            }

            Job merged = stored.job;
            merge_job_facts(merged, observed_job, authoritative_members);
            merged.JobID = internal_id;
            merged.CollectorNames = stored.job.CollectorNames;

            JobRecord updated = stored;
            updated.job = merged;   // the merged facts are the stored projection
            updated.selector = observation.selector;
            updated.last_seen_us = observed_at;
            const bool generation_became_complete =
                !stored.generation.complete() && observation.generation.complete();
            if (observation.generation.complete()) updated.generation = observation.generation;
            if (!observation.processes.empty()) {
                updated.members = normalized_processes(observation.processes);
            } else if (observation.members_complete) {
                // Confirmed empty membership replaces the stored evidence.
                updated.members.clear();
            } else if (!observed_job.JobPIDs.empty()) {
                updated.members.clear();
                for (const auto pid : observed_job.JobPIDs) {
                    updated.members.push_back(ProcessIdentity{pid, 0, ""});
                }
            }

            const bool recovered = stored.state == kStateSuspect;
            if (recovered) updated.state = kStateActive;
            const bool changed = recovered ||
                facts_fingerprint(merged, updated) != facts_fingerprint(stored.job, stored);
            if (!changed) {
                updated.facts_revision = stored.facts_revision;
                std::unique_lock lg(mtx_);
                records_[internal_id] = updated;
                result.ok = true;
                result.disposition = UpsertDisposition::Unchanged;
                result.internal_id = internal_id;
                result.instance_id = instanceId(internal_id);
                result.facts_revision = stored.facts_revision;
                out = std::move(updated);
                return result;
            }

            updated.facts_revision = stored.facts_revision + 1;
            const std::string record = record_json(updated).dump();
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

            note_record(updated);
            result.ok = true;
            result.disposition = UpsertDisposition::Updated;
            result.internal_id = internal_id;
            result.instance_id = instanceId(internal_id);
            result.facts_revision = updated.facts_revision;
            result.generation_became_complete = generation_became_complete;
            spdlog::debug("JobStore: inventory facts updated for JobID {} (revision {})",
                          internal_id, updated.facts_revision);
            out = std::move(updated);
            return result;
        }
    }

    // Create a new inventory record. Nothing is activated and no caller
    // binding is accepted here.
    std::string allocation_error;
    const uint64_t allocated = allocateId(allocation_error);
    if (allocated == 0) {
        result.error = allocation_error.empty() ? "store_write_failed" : allocation_error;
        result.message = "durable internal id allocation failed";
        return result;
    }

    JobRecord record;
    record.job = observation.facts;
    record.job.JobID = allocated;
    record.job.CollectorNames.clear();
    apply_identity_via_applier(identity_applier_, record.job, observation.selector, *identity);
    record.job.JobPIDs.clear();
    if (!observation.processes.empty()) {
        for (const auto& process : observation.processes) record.job.JobPIDs.push_back(process.pid);
    }
    std::sort(record.job.JobPIDs.begin(), record.job.JobPIDs.end());
    record.job.JobPIDs.erase(std::unique(record.job.JobPIDs.begin(), record.job.JobPIDs.end()),
                             record.job.JobPIDs.end());

    record.internal_id = allocated;
    record.instance_id = instanceId(allocated);
    record.adapter = observation.selector.adapter;
    record.selector = observation.selector;
    record.identity = *identity;
    record.identity_complete = true;
    record.generation = observation.generation;
    record.members_authoritative = observation.members_complete;
    record.facts_revision = 1;
    record.state = kStateActive;
    record.first_seen_us = observed_at;
    record.last_seen_us = observed_at;
    if (!observation.processes.empty()) {
        record.members = normalized_processes(observation.processes);
    } else {
        for (const auto pid : record.job.JobPIDs) {
            record.members.push_back(ProcessIdentity{pid, 0, ""});
        }
    }

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        leveldb::WriteBatch batch;
        batch.Put(key_job(allocated), record_json(record).dump());
        batch.Put(index_key, nlohmann::json{
            {"internal_id", allocated},
            {"scheduler_type", identity->adapter},
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

    note_record(record);
    result.ok = true;
    result.disposition = UpsertDisposition::Created;
    result.internal_id = allocated;
    result.instance_id = record.instance_id;
    result.facts_revision = record.facts_revision;
    spdlog::info("JobStore: created inventory instance {} for {} '{}'",
                 allocated, identity->adapter, identity->native_job_id);
    out = std::move(record);
    return result;
}

// ---------------------------------------------------------------------------
// Record mutations
// ---------------------------------------------------------------------------

bool JobStore::mutateRecord(uint64_t id, const std::function<void(JobRecord&)>& mutate) {
    std::unique_lock lg(mtx_);
    auto it = records_.find(id);
    if (it == records_.end()) return false;
    mutate(it->second);
    return true;
}

bool JobStore::updateRecord(uint64_t id, const std::function<void(JobRecord&)>& mutate,
                            const char* operation) {
    if (!store_ready()) return false;
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);

    std::string raw;
    auto status = db_->Get(leveldb::ReadOptions(), key_job(id), &raw);
    if (status.IsNotFound()) return false;
    if (!status.ok()) {
        record_store_failure(std::string("LevelDB read failed (") + operation + "): " +
                             status.ToString());
        return false;
    }

    JobRecord record;
    try {
        record = parse_record(raw, id);
    } catch (const std::exception& error) {
        record_store_failure(std::string("corrupt job record during ") + operation);
        return false;
    }
    mutate(record);

    leveldb::WriteBatch batch;
    batch.Put(key_job(id), record_json(record).dump());
    status = write_batch_locked(batch, operation);
    if (!status.ok()) return false;

    std::unique_lock lg(mtx_);
    records_[id] = record;
    return true;
}

bool JobStore::persistState(uint64_t id, const std::string& state) {
    if (!db_running_ || db_ == nullptr) return false;

    // Serialize with observation upserts: the record read-modify-write must
    // not lose a concurrent fact update's fields.
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);

    std::string raw;
    auto status = db_->Get(leveldb::ReadOptions(), key_job(id), &raw);
    if (status.IsNotFound()) return true;   // nothing durable left to mark
    if (!status.ok()) {
        record_store_failure("LevelDB read failed (record state): " + status.ToString());
        return false;
    }

    JobRecord record;
    try {
        record = parse_record(raw, id);
    } catch (const std::exception&) {
        record_store_failure("corrupt job record during state persist");
        return false;
    }
    if (record.state == state) return true;

    record.state = state;
    leveldb::WriteBatch batch;
    batch.Put(key_job(id), record_json(record).dump());
    status = write_batch_locked(batch, "persist record state");
    if (!status.ok()) return false;

    std::unique_lock lg(mtx_);
    auto it = records_.find(id);
    if (it != records_.end()) it->second.state = state;
    return true;
}

bool JobStore::refreshFacts(uint64_t id, const std::function<void(Job&)>& refresh) {
    Job job;
    {
        std::shared_lock lg(mtx_);
        const auto it = records_.find(id);
        if (it == records_.end()) return false;
        job = it->second.job;
    }

    // /proc and scheduler metadata I/O strictly outside every store lock.
    refresh(job);
    if (!update_job_info(id, job)) return false;

    std::unique_lock lg(mtx_);
    const auto it = records_.find(id);
    if (it != records_.end()) {
        // Records without a validated execution generation keep their member
        // list as the only liveness evidence. Only replace it while the
        // refresh still sees processes: an empty result is itself the
        // termination evidence the retirement pass has to evaluate.
        if (!it->second.generation.complete() && !job.JobPIDs.empty()) {
            it->second.members.clear();
            for (const auto pid : job.JobPIDs)
                it->second.members.push_back(ProcessIdentity{pid, 0, ""});
        }
        it->second.last_seen_us = now_us();
    }
    return true;
}

bool JobStore::update_job_info(uint64_t id, const Job& job) {
    if (!db_running_) return false;
    return updateRecord(id, [&job, id](JobRecord& record) {
        record.job = job;
        record.job.JobID = id;   // CollectorNames stay intent-owned, untouched
        record.last_seen_us = now_us();
        if (record.facts_revision == 0) {
            record.facts_revision = 1;
            record.first_seen_us = now_us();
            record.selector = selector_from_legacy_job(record.job);
            if (const auto identity = canonicalize(record.selector)) {
                record.identity = *identity;
                record.identity_complete = true;
            }
        }
    }, "update job facts");
}

bool JobStore::archive(uint64_t id) {
    if (!db_running_ || db_ == nullptr) return true;   // nothing durable to clean

    // Archive must not race an upsert of the same instance, otherwise the
    // record could be recreated right after the atomic delete.
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);

    std::string key = key_job(id);
    std::string json;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &json);
    if (s.IsNotFound()) {
        spdlog::warn("JobStore: job {} not found in db when ending", id);
        std::unique_lock lg(mtx_);
        records_.erase(id);
        return true;
    }
    if (!s.ok()) {
        record_store_failure("LevelDB read failed (archive job): " + s.ToString());
        return false;
    }

    JobRecord stored;
    try {
        stored = parse_record(json, id);
    } catch (const std::exception& error) {
        spdlog::error("JobStore: cannot parse job {} for archival: {}", id, error.what());
        record_store_failure("corrupt job record during archive");
        return false;
    }

    const int64_t now = now_us();

    auto history = record_json(stored);
    history["state"] = kStateClosing;
    history["end_time_us"] = now;

    leveldb::WriteBatch batch;
    batch.Delete(key);
    batch.Delete(key_binding(id));

    // Final binding summary for auditing.
    {
        std::string binding_json;
        if (db_->Get(leveldb::ReadOptions(), key_binding(id), &binding_json).ok()) {
            try {
                history["binding"] = nlohmann::json::parse(binding_json);
            } catch (const std::exception&) {
                // ignore unreadable binding summary
            }
        }
    }

    // Never let stale cleanup remove a newer execution's identity index.
    if (stored.identity_complete) {
        const std::string index_key = key_identity(identity_index_key(stored.identity));
        std::string value;
        auto index_status = db_->Get(leveldb::ReadOptions(), index_key, &value);
        if (index_status.ok()) {
            uint64_t owner = 0;
            try {
                owner = nlohmann::json::parse(value).value("internal_id", uint64_t{0});
            } catch (const std::exception&) {
                owner = 0;
            }
            if (owner == id) batch.Delete(index_key);
        } else if (!index_status.IsNotFound()) {
            record_store_failure("LevelDB read failed (identity index archive): " +
                                 index_status.ToString());
            return false;
        }
    }

    batch.Put(key_history(id, now), history.dump());

    s = write_batch_locked(batch, "archive job");
    if (!s.ok()) {
        return false;
    }

    {
        std::unique_lock lg(mtx_);
        records_.erase(id);
        if (stored.identity_complete) {
            identity_index_.erase(identity_index_key(stored.identity));
        }
    }

    spdlog::info("JobStore: archived job {} to history", id);
    return true;
}

// ---------------------------------------------------------------------------
// Intent + applied
// ---------------------------------------------------------------------------

BindingRecord JobStore::binding(uint64_t id) const {
    BindingRecord binding;
    binding.internal_id = id;
    if (!db_running_ || db_ == nullptr) return binding;

    std::lock_guard<std::mutex> db_lock(db_mtx_);
    std::string value;
    auto status = db_->Get(leveldb::ReadOptions(), key_binding(id), &value);
    if (!status.ok()) return binding;
    try {
        binding = binding_from_json(nlohmann::json::parse(value), id);
    } catch (const std::exception& error) {
        spdlog::warn("JobStore: unreadable binding for JobID {}: {}", id, error.what());
    }
    return binding;
}

bool JobStore::commitBindings(const std::vector<BindingRecord>& bindings,
                              std::vector<std::pair<JobRecord, bool>>& updated,
                              std::string& error) {
    updated.clear();
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
    {
        std::lock_guard<std::mutex> admission(upsert_mtx_);
        {
            std::shared_lock lg(mtx_);
            for (const auto& binding : bindings) {
                const auto it = records_.find(binding.internal_id);
                if (it == records_.end()) {
                    // fall through to the durable check below for the exact error
                    continue;
                }
                if (it->second.state == kStateClosing) {
                    error = "instance_closing";
                    return false;
                }
            }
        }

        std::lock_guard<std::mutex> db_lock(db_mtx_);
        leveldb::WriteBatch batch;
        std::vector<std::pair<JobRecord, bool>> committed;
        for (const auto& binding : bindings) {
            std::string raw;
            auto status = db_->Get(leveldb::ReadOptions(),
                                   key_job(binding.internal_id), &raw);
            if (!status.ok()) {
                error = status.IsNotFound() ? "not_found" : "store_error";
                return false;
            }
            JobRecord record;
            try {
                record = parse_record(raw, binding.internal_id);
            } catch (const std::exception&) {
                error = "store_error";
                return false;
            }
            if (record.quarantined) {
                error = "identity_quarantined";
                return false;
            }
            const bool changed = record.job.CollectorNames != binding.effectiveCollectors();
            record.job.CollectorNames = binding.effectiveCollectors();
            batch.Put(key_binding(binding.internal_id), binding_to_json(binding).dump());
            batch.Put(key_job(binding.internal_id), record_json(record).dump());
            committed.emplace_back(std::move(record), changed);
        }

        auto status = write_batch_locked(batch, "commit bindings");
        if (!status.ok()) {
            error = "store_write_failed";
            return false;
        }

        if (!committed.empty()) {
            std::unique_lock lg(mtx_);
            for (auto& [record, changed] : committed) {
                for (const auto& b : bindings) {
                    if (b.internal_id == record.internal_id) record.intent = b;
                }
                records_[record.internal_id] = record;
                updated.emplace_back(record, changed);
            }
        }
    }
    return true;
}

AppliedCollection JobStore::applied(uint64_t id) const {
    std::shared_lock lg(mtx_);
    const auto it = records_.find(id);
    if (it != records_.end()) return it->second.applied;
    return AppliedCollection{};
}

bool JobStore::recordApplied(uint64_t id, const AppliedCollection& applied) {
    return updateRecord(id, [&applied](JobRecord& record) {
        record.applied = applied;
    }, "record applied state");
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

void JobStore::note_record(JobRecord record) {
    const uint64_t internal_id = record.internal_id;
    const bool index_it = record.identity_complete && !record.quarantined;
    const std::string index_key = index_it ? identity_index_key(record.identity) : std::string{};
    std::unique_lock lg(mtx_);
    records_[internal_id] = std::move(record);
    if (index_it) {
        identity_index_[index_key] = internal_id;
    }
}

bool JobStore::hasActive(uint64_t id) const {
    std::shared_lock lg(mtx_);
    const auto it = records_.find(id);
    return it != records_.end() && it->second.state != kStateClosing;
}

bool JobStore::isClosing(uint64_t id) const {
    std::shared_lock lg(mtx_);
    const auto it = records_.find(id);
    return it != records_.end() && it->second.state == kStateClosing;
}

size_t JobStore::closingCount() const {
    std::shared_lock lg(mtx_);
    size_t count = 0;
    for (const auto& entry : records_) {
        if (entry.second.state == kStateClosing) ++count;
    }
    return count;
}

size_t JobStore::activeCount() const {
    std::shared_lock lg(mtx_);
    size_t count = 0;
    for (const auto& entry : records_) {
        if (entry.second.state != kStateClosing) ++count;
    }
    return count;
}

std::optional<JobRecord> JobStore::record(uint64_t id) const {
    std::shared_lock lg(mtx_);
    const auto it = records_.find(id);
    if (it == records_.end()) return std::nullopt;
    auto record = it->second;
    record.instance_id = derive_instance_id(db_uuid_, id);
    return record;
}

std::optional<JobRecord> JobStore::recordByIdentity(const CanonicalIdentity& identity) const {
    std::shared_lock lg(mtx_);
    const auto it = identity_index_.find(identity_index_key(identity));
    if (it == identity_index_.end()) return std::nullopt;
    const auto record = records_.find(it->second);
    if (record == records_.end()) return std::nullopt;
    auto value = record->second;
    value.instance_id = derive_instance_id(db_uuid_, it->second);
    return value;
}

std::vector<JobRecord> JobStore::records() const {
    std::shared_lock lg(mtx_);
    std::vector<JobRecord> out;
    out.reserve(records_.size());
    for (const auto& entry : records_) {
        auto record = entry.second;
        record.instance_id = derive_instance_id(db_uuid_, entry.first);
        out.push_back(std::move(record));
    }
    return out;
}

std::vector<JobRecord> JobStore::findActive(
    const std::function<bool(const Job&)>& pred) const {
    std::shared_lock lg(mtx_);
    std::vector<JobRecord> out;
    for (const auto& entry : records_) {
        if (entry.second.state == kStateClosing) continue;
        if (pred(entry.second.job)) {
            auto record = entry.second;
            record.instance_id = derive_instance_id(db_uuid_, entry.first);
            out.push_back(std::move(record));
        }
    }
    return out;
}

std::string JobStore::instanceId(uint64_t id) const {
    if (id == kSystemSentinelJobId) return "";
    return derive_instance_id(db_uuid_, id);
}

StoreHealth JobStore::health() const {
    StoreHealth store_health;
    store_health.db_running = db_running_ && db_ != nullptr;
    store_health.migration_complete = migration_complete_;
    store_health.migration_state = migration_state_;
    store_health.schema_version = schema_version_;
    store_health.db_uuid = db_uuid_;
    store_health.degraded = store_degraded_.load();
    store_health.write_failures = store_write_failures_.load();
    store_health.corrupt_records = corrupt_records_;

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        store_health.last_write_error = last_store_error_;
    }

    {
        std::shared_lock lg(mtx_);
        store_health.active_instances = records_.size();
        for (const auto& entry : records_) {
            if (!entry.second.identity_complete) ++store_health.incomplete_identities;
            if (entry.second.quarantined) ++store_health.quarantined_identities;
        }
    }
    return store_health;
}
