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
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>

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

constexpr uint32_t kInventorySchemaVersion = 4;

// The schema marker is a canonical decimal: ASCII digits only, no sign, no
// whitespace, no leading zero, no trailing junk, and it must fit the field.
// std::stoul would accept "3garbage" (prefix parse) and 4294967299 (fits
// unsigned long, truncates on narrowing), both of which would make an
// unsupported store look like schema 3.
std::optional<uint32_t> parse_schema_marker(const std::string& raw)
{
    if (raw.empty() || raw.size() > 10) return std::nullopt;
    if (raw.size() > 1 && raw.front() == '0') return std::nullopt;
    uint64_t value = 0;
    for (const char digit : raw) {
        if (digit < '0' || digit > '9') return std::nullopt;
        value = value * 10 + static_cast<uint64_t>(digit - '0');
        if (value > std::numeric_limits<uint32_t>::max()) return std::nullopt;
    }
    return static_cast<uint32_t>(value);
}

std::string hex64(uint64_t v) {
    return fmt::format("{:016x}", v);
}

uint64_t unhex64(const std::string& s) {
    return std::stoull(s, nullptr, 16);
}

std::string key_job(uint64_t jobID) {
    return KEY_PREFIX_JOB + hex64(jobID);
}

std::string key_history_prefix(uint64_t jobID) {
    return KEY_PREFIX_HISTORY + hex64(jobID) + ":";
}

std::string key_history(uint64_t jobID, uint64_t end_time) {
    return key_history_prefix(jobID) + hex64(end_time);
}

std::string key_counter(const std::string& name) {
    return KEY_PREFIX_COUNTER + name;
}

std::string key_meta(const std::string& name) {
    return KEY_PREFIX_META + name;
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

// /proc/<pid>/stat field 22 (starttime) comes from the shared helper so the
// store, the registry and the probes agree on what "the same process" means.

// read_boot_id() lives in core/liveness so the store, the probes and the
// admission boundary cannot disagree about the current boot.

// A generation is only replayable on POSITIVE evidence: the same boot and the
// same manager start time read back from the current process.  A populated
// generation proves nothing by itself (a numeric PID is recycled), and a
// readable-but-different start time is positive proof of reuse.
//
// The third answer matters: when /proc/<pid>/stat cannot be read the process
// may well be alive, so the record is neither stale (never delete evidence on
// "unknown") nor verified (never claim a complete execution we did not check).
//
// Provenance decides what "nothing to verify" means.  A manual/legacy row owns
// no generation and its bound members are the evidence; a discovery row that
// never produced a complete manager identity has an UNVERIFIED execution, not a
// verified one — an incomplete discovery generation must never be replayed as
// an owned instance.
enum class ExecutionVerdict {
    Verified,
    Stale,
    Unknown,
};

ExecutionVerdict verify_execution(const ExecutionGeneration& generation,
                                  bool execution_observed,
                                  const std::string& current_boot_id,
                                  const ProcessLivenessProbe& probe)
{
    if (!generation.complete()) {
        return execution_observed ? ExecutionVerdict::Unknown
                                  : ExecutionVerdict::Verified;   // manual: nothing to verify
    }
    if (!current_boot_id.empty() && generation.boot_id != current_boot_id)
        return ExecutionVerdict::Stale;   // another boot cannot host this execution
    const ProcessIdentity manager{generation.manager_pid,
                                  generation.manager_start_ticks,
                                  generation.boot_id};
    const LivenessResult result =
        verify_process_identity(manager, current_boot_id, probe);
    switch (result.state) {
        case LivenessState::Alive: return ExecutionVerdict::Verified;
        case LivenessState::Dead: return ExecutionVerdict::Stale;
        case LivenessState::Unknown: break;
    }
    return ExecutionVerdict::Unknown;
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

nlohmann::json selection_to_json(const CollectionSelection& selection);
CollectionSelection selection_from_json(const nlohmann::json& in);

nlohmann::json record_json(const JobRecord& record) {
    nlohmann::json out;
    write_job_fields(out, record.job);
    out["schema_version"] = kInventorySchemaVersion;
    // The desired selection lives in the record that owns the instance and is
    // ordered by the record's single revision.
    out["selection"] = selection_to_json(record.selection);
    out["revision"]       = record.revision;
    out["state"]          = record.state;
    out["quarantined"]    = record.quarantined;
    out["first_seen_us"]  = record.first_seen_us;
    out["last_seen_us"]   = record.last_seen_us;
    out["members_authoritative"] = record.members_authoritative;
    out["execution_observed"] = record.execution_observed;
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
    record.revision       = j.value("revision", uint64_t{1});
    record.state          = j.value("state", std::string{kStateActive});
    record.quarantined    = j.value("quarantined", false);
    record.first_seen_us  = j.value("first_seen_us", int64_t{0});
    record.last_seen_us   = j.value("last_seen_us", int64_t{0});
    record.members_authoritative = j.value("members_authoritative", false);
    record.adapter        = j.value("adapter", std::string{});
    if (record.revision == 0) record.revision = 1;

    // Schema 4 stores the desired selection in the same row, ordered by the
    // record revision. A record without a selection object is a corrupt row,
    // not an implicit auto selection: never silently invent intent for an
    // existing instance.
    if (!j.contains("selection") || !j["selection"].is_object()) {
        throw std::runtime_error("record has no selection object");
    }
    record.selection = selection_from_json(j["selection"]);

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
    // Provenance is stored explicitly.  A row written before the field existed
    // is classified from its own evidence: any generation field means a
    // discovery observation produced it, a completely empty one is the manual
    // shape (manual registration never invents a generation).
    const bool has_execution_flag = j.contains("execution_observed");
    record.execution_observed = has_execution_flag
                                    ? j.value("execution_observed", false)
                                    : !(record.generation.boot_id.empty() &&
                                        record.generation.manager_pid == 0 &&
                                        record.generation.manager_start_ticks == 0);
    const auto members_it = j.contains("members") ? j.find("members") : j.find("processes");
    if (members_it != j.end() && members_it->is_array()) {
        for (const auto& process : *members_it) {
            if (process.is_object()) record.members.push_back(process_from_json(process));
        }
        record.members = normalized_processes(std::move(record.members));
    }
    // Execution evidence is rebuilt from its stored projection instead of a
    // new JSON group: the durable layout stays compatible (plan §4.3) and the
    // in-memory Job remains the single source of truth for the store.
    {
        JobExecution execution;
        execution.generation = record.generation;
        execution.members = record.members;
        execution.members_authoritative = record.members_authoritative;
        if (!record.selector.step_id.empty())
            execution.step_id = record.selector.step_id;
        record.job.execution = std::move(execution);
    }
    return record;
}

nlohmann::json selection_to_json(const CollectionSelection& selection) {
    nlohmann::json out;
    out["automatic"] = selection.automatic.has_value()
                           ? nlohmann::json(*selection.automatic)
                           : nlohmann::json(nullptr);
    out["automatic_source"] = selection.automatic_source;
    out["manual"] = selection.manual.has_value() ? nlohmann::json(*selection.manual)
                                                 : nlohmann::json(nullptr);
    return out;
}

CollectionSelection selection_from_json(const nlohmann::json& in) {
    CollectionSelection selection;
    if (in.contains("automatic") && in["automatic"].is_array()) {
        selection.automatic =
            in["automatic"].get<std::vector<std::string>>();
    }
    selection.automatic_source = in.value("automatic_source", std::string{});
    if (in.contains("manual") && in["manual"].is_array()) {
        selection.manual = in["manual"].get<std::vector<std::string>>();
    }
    return selection;
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
// Selection value types
// ---------------------------------------------------------------------------

std::vector<std::string> CollectionSelection::effectiveCollectors() const
{
    if (manual.has_value()) return *manual;
    if (automatic.has_value()) return *automatic;
    return {};
}

std::optional<CollectionMode> collection_mode_from_string(const std::string& raw)
{
    if (raw == "manual") return CollectionMode::Manual;
    if (raw == "auto") return CollectionMode::Auto;
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Construction / open / schema admission
// ---------------------------------------------------------------------------

JobStore::JobStore(const Options& options)
    : options_(options),
      identity_probe_(options.identity_probe ? options.identity_probe
                                             : make_proc_liveness_probe()),
      boot_id_(read_boot_id()) {
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

    // Schema admission is strict: this build reads schema 3 and initializes a
    // genuinely empty store. Any other on-disk layout is rejected loudly; it
    // is never migrated or deleted in place.
    bool schema_present = false;
    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        std::string value;
        auto s = db_->Get(leveldb::ReadOptions(), key_meta("schema_version"), &value);
        if (s.ok()) {
            schema_present = true;
            const auto parsed = parse_schema_marker(value);
            if (!parsed) {
                // Not a marker this build wrote: no UUID, id counter or record
                // is initialized from it, and no byte of the store is touched.
                spdlog::error("JobStore: schema marker '{}' at {} is not canonical; "
                              "refusing to open an unreadable layout", value, db_path);
                reject_schema("malformed_schema_marker", 0);
                return false;
            }
            schema_version_ = *parsed;
        }
    }

    if (!schema_present) {
        if (!db_is_empty()) {
            spdlog::error("JobStore: database at {} has data but no schema marker; "
                          "refusing to migrate or delete an old-schema store "
                          "(explicit conversion is required)", db_path);
            reject_unsupported_schema(/*found=*/0);
            return false;
        }
        if (!initialize_schema()) {
            spdlog::error("JobStore: could not initialize a new schema-{} store", kInventorySchemaVersion);
            reject_unsupported_schema(/*found=*/0);
            return false;
        }
    } else if (schema_version_ != kInventorySchemaVersion) {
        spdlog::error("JobStore: db schema {} is not supported by this binary (schema {}); "
                      "refusing to migrate or delete it", schema_version_,
                      kInventorySchemaVersion);
        reject_unsupported_schema(schema_version_);
        return false;
    }

    migration_complete_ = true;
    migration_state_ = "complete";
    {
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

bool JobStore::db_is_empty() const {
    if (!db_) return true;
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    it->SeekToFirst();
    const bool empty = !it->Valid();
    const auto status = it->status();
    delete it;
    if (!status.ok()) {
        spdlog::error("JobStore: cannot inspect the database during schema admission: {}",
                      status.ToString());
        return false;   // fail closed: never treat an unreadable store as empty
    }
    return empty;
}

bool JobStore::initialize_schema() {
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    if (!db_) return false;
    if (db_uuid_.empty()) db_uuid_ = generate_db_uuid();
    leveldb::WriteBatch batch;
    batch.Put(key_meta("schema_version"), std::to_string(kInventorySchemaVersion));
    batch.Put(key_meta("db_uuid"), db_uuid_);
    const auto status = write_batch_locked(batch, "initialize schema");
    if (!status.ok()) {
        spdlog::error("JobStore: failed to initialize schema {}: {}", kInventorySchemaVersion,
                      status.ToString());
        return false;
    }
    schema_version_ = kInventorySchemaVersion;
    return true;
}

void JobStore::reject_unsupported_schema(uint32_t found) {
    reject_schema("unsupported_schema:" + std::to_string(found), found);
}

void JobStore::reject_schema(const std::string& state, uint32_t found) {
    schema_version_ = found;
    migration_complete_ = false;
    migration_state_ = state;
    // Rejection is read-only: the database handle is closed without a
    // checkpoint, so every original key and value stays exactly as it was.
    if (db_) {
        delete db_;
        db_ = nullptr;
    }
    db_running_ = false;
}

void JobStore::close_db() {
    // Last chance: anything written without a fence becomes durable here, or it
    // is reported instead of silently dropped.
    if (!flushDirty("shutdown checkpoint")) {
        spdlog::error("JobStore: the shutdown checkpoint could not be written; the last "
                      "unsynced registration window is lost");
    }
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

std::vector<JobRecord> JobStore::restore() {
    // The boot id is fixed for the lifetime of this process (a reboot ends it),
    // and it was read when the store was constructed.
    const std::string& boot_id = boot_id_;
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
                // Validate restored executions before publishing them.  Only
                // positive evidence counts:
                //   * Verified -> keep it, and keep only members whose SAVED
                //                 identity still matches the process holding
                //                 that number (a recycled PID is dropped);
                //   * Stale    -> unverified liveness, no members, never
                //                 replayed;
                //   * Unknown  -> unverified liveness as well: an unreadable
                //                 process (or an incomplete DISCOVERY generation)
                //                 is neither death evidence nor proof of a live
                //                 execution, so the row must not be published as
                //                 verified.  The saved member/PID facts are
                //                 RETAINED: a live child is what stops the
                //                 absence rule from retiring this instance later,
                //                 and the unverified verdict (not evidence
                //                 deletion) blocks admission.  The next
                //                 observation restores it.
                switch (verify_execution(record.generation, record.execution_observed,
                                         boot_id, identity_probe_)) {
                    case ExecutionVerdict::Stale:
                        record.execution_verified = false;
                        record.job.JobPIDs.clear();
                        record.members.clear();
                        spdlog::warn("JobStore: restored JobID {} has a stale execution "
                                     "generation; liveness unknown", jobID);
                        break;
                    case ExecutionVerdict::Unknown:
                        record.execution_verified = false;
                        spdlog::warn("JobStore: restored JobID {} has an execution "
                                     "generation that cannot be verified (unreadable "
                                     "process or incomplete discovery identity); liveness "
                                     "unknown, {} member(s) kept as evidence and never "
                                     "replayed", jobID, record.members.size());
                        break;
                    case ExecutionVerdict::Verified: {
                        // Members carrying a saved identity are re-verified; a
                        // member without one (legacy/manual row) is kept as an
                        // unverified fact and is never fabricated into a
                        // `{pid, 0, ""}` identity the kernel path would trust.
                        std::vector<ProcessIdentity> kept;
                        kept.reserve(record.members.size());
                        size_t dropped = 0;
                        for (const auto& member : record.members) {
                            if (!member.complete()) {
                                kept.push_back(member);
                                continue;
                            }
                            const LivenessResult state = verify_process_identity(
                                member, boot_id, identity_probe_);
                            if (state.state == LivenessState::Dead) {
                                ++dropped;
                                continue;
                            }
                            kept.push_back(member);
                        }
                        if (dropped != 0) {
                            record.members = std::move(kept);
                            spdlog::warn("JobStore: restored JobID {} dropped {} "
                                         "reused/gone member process identity(ies)",
                                         jobID, dropped);
                        }
                        if (!record.members.empty()) {
                            record.job.JobPIDs.clear();
                            for (const auto& member : record.members)
                                record.job.JobPIDs.push_back(member.pid);
                        }
                        if (record.members.empty() && dropped != 0)
                            record.execution_verified = false;
                        break;
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

void JobStore::mark_record_dirty(uint64_t internal_id) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dirty_records_.insert(internal_id);
}

bool JobStore::flushDirty(const char* reason) {
    // One fsynced batch covers both the rows re-written here and every earlier
    // unsynced write LevelDB already buffered, so the whole state up to this
    // point becomes durable in a single fdatasync.
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    // A store that never opened (rejected schema, failed open) has no unsynced
    // registration window to lose. A pending recovery does not block the rows
    // it does not name; flush_dirty_locked filters them.
    if (!db_) return true;
    return flush_dirty_locked(reason);
}

bool JobStore::flush_dirty_locked(const char* reason) {
    std::vector<uint64_t> records;
    std::vector<JobRecord> snapshots;
    {
        std::unique_lock<std::shared_mutex> lock(mtx_);
        if (dirty_records_.empty()) return true;
        records.assign(dirty_records_.begin(), dirty_records_.end());
        if (recovery_required_.load()) {
            // These rows may already carry a write memory never published:
            // rewriting the memory snapshot here would revert the durable
            // image. Shutdown and the destructor must never do that.
            for (const auto id : records) {
                if (recovery_ids_.count(id) != 0) {
                    spdlog::error("JobStore: checkpoint '{}' refused: JobID {} has an "
                                  "unknown durable image (last operation '{}')",
                                  reason, id, recovery_operation_);
                    return false;
                }
            }
        }
        for (const auto id : records) {
            const auto it = records_.find(id);
            if (it != records_.end()) snapshots.push_back(it->second);
        }
    }

    leveldb::WriteBatch batch;
    for (auto& snapshot : snapshots)
        batch.Put(key_job(snapshot.internal_id), record_json(snapshot).dump());

    const auto status = write_batch_locked(batch, reason, /*durable=*/true);
    {
        std::unique_lock<std::shared_mutex> lock(mtx_);
        if (status.ok()) {
            dirty_records_.clear();
        } else {
            dirty_records_.insert(records.begin(), records.end());
        }
    }
    if (!status.ok()) {
        // Memory already holds these rows: the recovery direction is "write
        // them again", never "read them back".
        latch_recovery_locked(reason, RecoveryKind::RewriteMemory, {});
        return false;
    }
    spdlog::debug("JobStore: checkpoint flushed {} record(s) ({})",
                  snapshots.size(), reason);
    return true;
}

leveldb::Status JobStore::write_batch_locked(leveldb::WriteBatch& batch,
                                             const char* operation, bool durable,
                                             const std::vector<uint64_t>* touched) {
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
                if (touched && !touched->empty())
                    latch_recovery_locked(operation, RecoveryKind::AdoptDurable, *touched);
                return leveldb::Status::IOError("injected write failure before commit");
            }
        }
    }

    leveldb::WriteOptions write_options;
    write_options.sync = durable;
    auto status = db_->Write(write_options, &batch);

    if (injected == StoreFaultMode::FailAfterWrite) {
        record_store_failure(fmt::format("injected write failure after commit ({})", operation));
        if (touched && !touched->empty())
            latch_recovery_locked(operation, RecoveryKind::AdoptDurable, *touched);
        return leveldb::Status::IOError("injected write failure after commit");
    }
    if (!status.ok()) {
        record_store_failure(fmt::format("LevelDB write failed ({}): {}",
                                         operation, status.ToString()));
        if (touched && !touched->empty())
            latch_recovery_locked(operation, RecoveryKind::AdoptDurable, *touched);
        return status;
    }

    durable_commits_.fetch_add(1);
    if (durable) sync_commits_.fetch_add(1);
    record_store_success();
    return status;
}

void JobStore::latch_recovery_locked(const char* operation, RecoveryKind kind,
                                     const std::vector<uint64_t>& ids)
{
    // A durable-row uncertainty is never downgraded to a checkpoint retry:
    // adopting disk is the safe direction and it covers every nameable row.
    if (recovery_kind_ != RecoveryKind::AdoptDurable) recovery_kind_ = kind;
    recovery_ids_.insert(ids.begin(), ids.end());
    recovery_operation_ = operation ? operation : "unknown write";
    recovery_required_.store(true);
    spdlog::error("JobStore: write outcome for '{}' is unknown; recovery required before "
                  "any mutation, runtime application or checkpoint", recovery_operation_);
}

bool JobStore::recoverPending(std::string& error) {
    error.clear();
    if (!recovery_required_.load()) return true;
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    return recover_pending_locked(error);
}

bool JobStore::recover_pending_locked(std::string& error) {
    if (!recovery_required_.load()) return true;
    if (!db_) {
        error = "store_unavailable";
        return false;
    }

    std::vector<uint64_t> ids(recovery_ids_.begin(), recovery_ids_.end());
    // Archive images read back from `h:`. An ambiguous archive can land and
    // still report failure; memory then has no row to rewrite, so the durable
    // history row is the only authoritative image and it has to be fenced.
    std::vector<ArchiveFence> archives;
    for (const auto id : ids) {
        std::optional<ArchiveFence> archive;
        std::string failure;
        if (!adopt_durable_locked(id, archive, failure)) {
            // Adoption is partial: keep the fenced-write obligation visible for
            // every recovered row that this process already holds, so a retry
            // cannot forget a row that is still only in an unsynced write.
            {
                std::unique_lock<std::shared_mutex> lock(mtx_);
                for (const auto pending : ids) {
                    if (records_.find(pending) != records_.end())
                        dirty_records_.insert(pending);
                }
            }
            error = failure.empty() ? "recovery_read_failed" : failure;
            spdlog::error("JobStore: recovery of JobID {} failed ({}); the store stays "
                          "latched and no write is attempted", id, error);
            return false;
        }
        if (archive) archives.push_back(std::move(*archive));
    }

    // The recovered rows are once again authoritative memory state, but the
    // write that landed may itself have been unsynced (the hot selection path
    // writes durable=false). Adopting the row and erasing its dirty marker is
    // not a durability fence: rewrite every adopted row together with the
    // remaining unsynced rows in ONE fenced batch. Only a successful fence may
    // clear the latch or the dirty obligation.
    std::set<uint64_t> fence_ids(recovery_ids_);
    std::vector<JobRecord> snapshots;
    {
        std::unique_lock<std::shared_mutex> lock(mtx_);
        // Adoption erased each recovered row's dirty marker; restore the
        // obligation until the fence succeeds. A failed fence must leave both
        // the latch and this duty visible to the next retry.
        for (const auto id : recovery_ids_) {
            if (records_.find(id) != records_.end()) dirty_records_.insert(id);
        }
        fence_ids.insert(dirty_records_.begin(), dirty_records_.end());
        for (const auto id : fence_ids) {
            const auto it = records_.find(id);
            if (it != records_.end()) snapshots.push_back(it->second);
        }
    }

    if (snapshots.empty() && archives.empty()) {
        // Nothing authoritative to re-assert: the latch names rows this process
        // can neither adopt nor rewrite. Reporting completion here would claim a
        // durable state nobody proved.
        error = "recovery_image_unavailable";
        spdlog::error("JobStore: recovery has no authoritative image for {} latched "
                      "instance(s); the store stays latched", ids.size());
        return false;
    }

    {
        leveldb::WriteBatch batch;
        uint64_t fence_entries = 0;
        for (const auto& snapshot : snapshots) {
            batch.Put(key_job(snapshot.internal_id), record_json(snapshot).dump());
            ++fence_entries;
        }
        for (const auto& archive : archives) {
            // Verbatim: the key carries the archive timestamp and the value the
            // final summary. Rebuilding either from memory would write a second
            // history row under a different key.
            batch.Put(archive.history_key, archive.history_value);
            // The active row the archive deleted: re-deleting is the idempotent
            // half of the same image, and it makes the fence non-empty even
            // when the instance had no other active row left.
            batch.Delete(key_job(archive.internal_id));
            fence_entries += 2;
            if (!archive.identity_key.empty()) {
                batch.Delete(archive.identity_key);
                ++fence_entries;
            }
        }
        recovery_fence_entries_ = fence_entries;
        const auto status = write_batch_locked(batch, "recovery checkpoint",
                                               /*durable=*/true);
        if (!status.ok()) {
            // The latch stays set and the dirty sets keep their obligation:
            // the next retry must fence these same rows again.
            error = "recovery_checkpoint_failed";
            return false;
        }
        std::unique_lock<std::shared_mutex> lock(mtx_);
        for (const auto id : fence_ids) {
            dirty_records_.erase(id);
        }
    }

    recovery_ids_.clear();
    recovery_kind_ = RecoveryKind::None;
    recovery_operation_.clear();
    recovery_required_.store(false);
    record_store_success();
    spdlog::info("JobStore: recovery complete; {} instance(s) adopted, {} row(s) fenced "
                 "from the durable state and {} archive image(s) re-asserted",
                 ids.size(), snapshots.size(), archives.size());
    return true;
}

bool JobStore::read_archive_fence_locked(uint64_t internal_id, ArchiveFence& fence,
                                         std::string& error) {
    const std::string prefix = key_history_prefix(internal_id);
    std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(leveldb::ReadOptions()));
    size_t found = 0;
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        if (++found > 1) {
            // Two history rows for one internal id: no image can be called
            // authoritative, so nothing may be re-asserted or dropped.
            error = "recovery_history_ambiguous";
            spdlog::error("JobStore: JobID {} has more than one history row; recovery "
                          "cannot choose an archive image", internal_id);
            return false;
        }
        fence.history_key = it->key().ToString();
        fence.history_value = it->value().ToString();
    }
    const auto status = it->status();
    if (!status.ok()) {
        error = "recovery_read_failed";
        record_store_failure("LevelDB read failed (recovery history): " + status.ToString());
        return false;
    }
    if (found == 0) {
        // The active row is gone but no archive exists: the landed batch is not
        // the archive this process assumed. Guessing either way would lose or
        // resurrect an instance.
        error = "recovery_history_missing";
        spdlog::error("JobStore: JobID {} has no active row and no history row; "
                      "recovery cannot prove what landed", internal_id);
        return false;
    }

    fence.internal_id = internal_id;
    JobRecord archived;
    try {
        archived = parse_record(fence.history_value, internal_id);
    } catch (const std::exception& exc) {
        error = "recovery_parse_failed";
        spdlog::error("JobStore: cannot parse the history row of JobID {} during "
                      "recovery: {}", internal_id, exc.what());
        return false;
    }
    if (!archived.identity_complete) return true;

    // The archive released the identity index only while this instance owned
    // it. Read the owner back instead of deleting blind: a newer execution may
    // own the same identity by now.
    const std::string index_key = key_identity(identity_index_key(archived.identity));
    std::string value;
    const auto index_status = db_->Get(leveldb::ReadOptions(), index_key, &value);
    if (!index_status.ok() && !index_status.IsNotFound()) {
        error = "recovery_read_failed";
        record_store_failure("LevelDB read failed (recovery identity): " +
                             index_status.ToString());
        return false;
    }
    if (index_status.ok()) {
        uint64_t owner = 0;
        try {
            owner = nlohmann::json::parse(value).value("internal_id", uint64_t{0});
        } catch (const std::exception&) {
            owner = 0;
        }
        if (owner == internal_id) fence.identity_key = index_key;
    }
    return true;
}

bool JobStore::adopt_durable_locked(uint64_t internal_id, std::optional<ArchiveFence>& archive,
                                    std::string& error) {
    archive.reset();
    std::string raw;
    const auto status = db_->Get(leveldb::ReadOptions(), key_job(internal_id), &raw);

    // The identity of the row being replaced, needed to repair the index when
    // the durable row is gone (the archive may have landed).
    CanonicalIdentity previous_identity;
    bool had_identity = false;
    {
        std::shared_lock lg(mtx_);
        const auto it = records_.find(internal_id);
        if (it != records_.end() && it->second.identity_complete) {
            previous_identity = it->second.identity;
            had_identity = true;
        }
    }

    if (status.IsNotFound()) {
        // The write that lost certainty deleted the active row. Only the
        // archive deletes `j:`; the image it left in `h:` is read back before
        // memory is touched, so an unprovable archive keeps the latch instead
        // of erasing the instance.
        ArchiveFence fence;
        if (!read_archive_fence_locked(internal_id, fence, error)) return false;

        std::unique_lock lg(mtx_);
        records_.erase(internal_id);
        dirty_records_.erase(internal_id);
        if (had_identity) {
            const auto key = identity_index_key(previous_identity);
            const auto it = identity_index_.find(key);
            if (it != identity_index_.end() && it->second == internal_id)
                identity_index_.erase(it);
        }
        archive = std::move(fence);
        return true;
    }
    if (!status.ok()) {
        error = "recovery_read_failed";
        record_store_failure("LevelDB read failed (recovery): " + status.ToString());
        return false;
    }

    JobRecord record;
    try {
        record = parse_record(raw, internal_id);
    } catch (const std::exception& exc) {
        error = "recovery_parse_failed";
        spdlog::error("JobStore: cannot parse JobID {} during recovery: {}",
                      internal_id, exc.what());
        return false;
    }

    {
        std::unique_lock lg(mtx_);
        records_[internal_id] = record;
        dirty_records_.erase(internal_id);

        // The durable row is authoritative for identity ownership too.
        if (had_identity) {
            const auto key = identity_index_key(previous_identity);
            const auto it = identity_index_.find(key);
            if (it != identity_index_.end() && it->second == internal_id)
                identity_index_.erase(it);
        }
        if (record.identity_complete && !record.quarantined) {
            const auto key = identity_index_key(record.identity);
            std::string value;
            const auto index_status = db_->Get(leveldb::ReadOptions(), key_identity(key), &value);
            if (!index_status.ok() && !index_status.IsNotFound()) {
                error = "recovery_read_failed";
                record_store_failure("LevelDB read failed (recovery index): " +
                                     index_status.ToString());
                return false;
            }
            uint64_t owner = 0;
            if (index_status.ok()) {
                try {
                    owner = nlohmann::json::parse(value).value("internal_id", uint64_t{0});
                } catch (const std::exception&) {
                    owner = 0;
                }
            }
            if (owner == internal_id) {
                identity_index_[key] = internal_id;
            } else if (owner != 0) {
                identity_index_[key] = owner;
            }
        }
    }
    return true;
}

void JobStore::record_store_failure(const std::string& message) {
    store_write_failures_.fetch_add(1);
    store_degraded_.store(true);
    last_store_error_ = message;
    spdlog::error("JobStore: {}", message);
}

void JobStore::record_store_success() {
    // A successful write says nothing about the row whose outcome already lost
    // certainty: it must not release the recovery latch.
    store_degraded_.store(recovery_required_.load());
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
        bool counter_malformed = false;

        std::string counter_json;
        leveldb::Status s = db_->Get(leveldb::ReadOptions(), key_counter("job_id"), &counter_json);
        if (s.ok()) {
            try {
                counter_value = std::stoull(counter_json);
            } catch (const std::exception&) {
                counter_malformed = true;
                spdlog::error("JobStore: job_id counter is not a number ('{}'); refusing to "
                              "reset the allocator", counter_json);
            }
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

        // The durable exhaustion marker (m:job_id_exhausted) decides whether
        // MAX is still available. The legacy layout stored only the counter,
        // where MAX is ambiguous: it may mean "MAX is the next free value" or
        // "MAX was already handed out". Without a marker the conservative
        // choice is to stop issuing IDs: reissuing a value that was already
        // published is worse than burning the last one.
        std::string marker_json;
        bool marker_present = false;
        bool marker_true = false;
        if (db_->Get(leveldb::ReadOptions(), key_meta("job_id_exhausted"), &marker_json).ok()) {
            marker_present = true;
            marker_true = marker_json == "true";
        }

        bool persist_marker = false;
        if (counter_malformed) {
            id_exhausted_ = true;
            next_id_ = 0;
            persist_marker = true;
        } else if (marker_present && marker_true) {
            id_exhausted_ = true;
            next_id_ = 0;
        } else if (marker_present && max_job_id == std::numeric_limits<uint64_t>::max()) {
            // A false marker cannot override the evidence of an existing MAX
            // record: the value was issued, so the counter is exhausted.
            spdlog::error("JobStore: job_id_exhausted=false contradicts an existing "
                          "maximum JobID; keeping the allocator exhausted");
            id_exhausted_ = true;
            next_id_ = 0;
            persist_marker = true;
        } else if (!marker_present &&
                   (counter_value == std::numeric_limits<uint64_t>::max() || id_exhausted_)) {
            spdlog::warn("JobStore: legacy job_id counter {} carries no exhaustion marker; "
                         "treating the allocator as exhausted instead of reissuing MAX",
                         counter_value);
            id_exhausted_ = true;
            next_id_ = 0;
            persist_marker = true;
        }

        // 持久化当前值 (durable high-water reservation)
        leveldb::WriteBatch batch;
        batch.Put(key_counter("job_id"), std::to_string(next_id_));
        if (persist_marker) batch.Put(key_meta("job_id_exhausted"), "true");
        auto status = write_batch_locked(batch, "init job id counter");
        if (!status.ok()) {
            spdlog::error("JobStore: failed to persist job_id_counter: {}", status.ToString());
        }

        spdlog::info("JobStore: initialized job_id counter, next_id_ = {} (exhausted: {})",
                     next_id_, id_exhausted_);
    } catch (const std::exception& e) {
        spdlog::error("JobStore: failed to init job_id counter: {}", e.what());
        next_id_ = 1;
    }
}

uint64_t JobStore::allocateId(std::string& error) {
    // Every allocator consumer shares the same admission order as the inventory
    // registration path (plan §5.3): a standalone reservation can never land
    // between a batch's id reservation and that batch's commit, which would let
    // the batch write an older counter back and reissue a published id.
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> id_lock(id_mtx_);
    if (id_exhausted_ || next_id_ == 0) {
        error = "id_exhausted";
        spdlog::error("JobStore: internal id space is exhausted, refusing allocation");
        return 0;
    }

    const uint64_t id = next_id_;
    bool exhaust_now = false;
    if (id == std::numeric_limits<uint64_t>::max()) {
        id_exhausted_ = true;
        next_id_ = 0;
        exhaust_now = true;
    } else {
        next_id_ = id + 1;
    }

    // Reserve the id durably before it is handed out. An ambiguous outcome
    // still advances the in-memory high-water mark: ids are never reused. The
    // exhaustion marker rides in the same transaction so a restart cannot
    // conclude that MAX is still free.
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    leveldb::WriteBatch batch;
    batch.Put(key_counter("job_id"),
              std::to_string(next_id_ == 0 ? id : next_id_));
    if (exhaust_now) batch.Put(key_meta("job_id_exhausted"), "true");
    auto status = write_batch_locked(batch, "allocate internal id");
    if (!status.ok()) {
        error = "store_write_failed";
        spdlog::error("JobStore: durable id allocation failed for id {}, id is burned", id);
        return 0;
    }
    return id;
}

bool JobStore::reserveIdInMemory(uint64_t& id, uint64_t& durable_counter,
                                 bool& durable_exhausted, std::string& error) {
    std::lock_guard<std::mutex> id_lock(id_mtx_);
    if (id_exhausted_ || next_id_ == 0) {
        error = "id_exhausted";
        spdlog::error("JobStore: internal id space is exhausted, refusing allocation");
        return false;
    }
    durable_exhausted = false;
    id = next_id_;
    if (id == std::numeric_limits<uint64_t>::max()) {
        id_exhausted_ = true;
        next_id_ = 0;
        durable_counter = id;
        durable_exhausted = true;
    } else {
        next_id_ = id + 1;
        durable_counter = next_id_;
    }
    // The reservation becomes durable together with the record that consumes
    // it (one WriteBatch == one fsync). An ambiguous outcome still advances the
    // in-memory high-water mark: a burned id is never handed out twice.
    return true;
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
    if (recovery_required_.load()) {
        spdlog::error("JobStore: refusing a manual registration while recovery is required");
        return false;
    }

    record.revision = std::max<uint64_t>(record.revision, 1);
    // A manual registration owns no discovery execution: its bound members are
    // the evidence, and an empty generation is expected here.
    record.execution_observed = false;
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
    // Bind each numeric PID to the process that holds it NOW.  A manual
    // registration has no scheduler-side identity to copy, and a bare number is
    // not an execution identity: recording the current start ticks makes the
    // kernel path verify the same process later instead of trusting a number
    // that may be recycled.  An unreadable stat leaves the identity incomplete,
    // which is reported as incomplete ownership rather than guessed.
    record.members.clear();
    for (const auto pid : record.job.JobPIDs) {
        const auto ticks = read_process_start_ticks(pid);
        record.members.push_back(ProcessIdentity{
            pid, ticks.value_or(0), ticks.has_value() ? boot_id_ : std::string{}});
    }

    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);
    leveldb::WriteBatch batch;

    // A legacy register is a manual selection: the caller explicitly asked
    // for these collectors, so it cannot be reported as an automatic origin.
    if (!record.job.CollectorNames.empty()) {
        record.selection.manual = record.job.CollectorNames;
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

// One observation against an already-loaded record: pure decision, no durable
// write and no in-memory publish. The register/merge semantics of a repeated
// identity live here, so every caller gets the same result.
UpsertResult JobStore::merge_existing(const Job& job, const NativeSelector& selector,
                                      const CanonicalIdentity& identity,
                                      uint64_t internal_id, const JobRecord& stored,
                                      int64_t observed_at, JobRecord& updated_out) {
    UpsertResult result;
    result.internal_id = internal_id;
    result.instance_id = instanceId(internal_id);
    result.revision = stored.revision;

    const ExecutionGeneration generation = generation_of(job);
    const std::vector<ProcessIdentity>& processes = members_of(job);
    const bool members_complete = members_authoritative_of(job);

    if (stored.quarantined) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::IdentityQuarantined;
        result.error = "identity_quarantined";
        result.message = "record is quarantined after an identity conflict";
        return result;
    }
    if (stored.state == kStateClosing) {
        // A retiring instance must never be resurrected into the active set by
        // a late observation: the drain owns it until it is archived.
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::InstanceClosing;
        result.error = "instance_closing";
        result.message = "the instance is retiring and no longer accepts observations";
        return result;
    }
    if (stored.identity_complete && stored.identity != identity) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::IdentityIndexMismatch;
        result.error = "identity_index_mismatch";
        result.message = "the identity index and the stored record disagree";
        spdlog::error("JobStore: identity index for JobID {} does not match the stored "
                      "record; refusing to merge", internal_id);
        return result;
    }
    // Out-of-order commits: an older source read must never overwrite newer
    // facts. read_order is process-monotonic and assigned before reading, so a
    // late scan loses to an event that was committed first (plan §4.1, A09).
    if (stored.job.execution && job.execution &&
        stored.job.execution->read_order != 0 && job.execution->read_order != 0 &&
        job.execution->read_order < stored.job.execution->read_order) {
        result.ok = true;
        result.disposition = UpsertDisposition::Unchanged;
        result.message = "older source read order; newer facts kept";
        updated_out = stored;
        return result;
    }

    if (stored.generation.complete() && generation.complete() &&
        stored.generation != generation) {
        result.disposition = UpsertDisposition::Conflict;
        result.code = RegistrationError::GenerationConflict;
        result.error = "execution_generation_conflict";
        result.message = "a different live execution generation owns this identity";
        spdlog::warn("JobStore: observation for '{}' carries a different live generation "
                     "than JobID {}; not merged", identity.native_job_id, internal_id);
        return result;
    }

    // Merge facts idempotently. The member list is authoritative only when the
    // source says so: an empty list from a partially-known membership must
    // never erase stored members.
    Job observed_job = job;
    observed_job.JobID = internal_id;
    observed_job.CollectorNames = stored.job.CollectorNames;
    apply_identity_via_applier(identity_applier_, observed_job, selector, identity);
    const bool authoritative_members = members_complete;
    if (!processes.empty()) {
        observed_job.JobPIDs.clear();
        for (const auto& process : processes) observed_job.JobPIDs.push_back(process.pid);
    } else if (members_complete) {
        // Read succeeded, no members: an authoritative empty set.
        observed_job.JobPIDs.clear();
    }

    Job merged = stored.job;
    merge_job_facts(merged, observed_job, authoritative_members);
    merged.JobID = internal_id;
    merged.CollectorNames = stored.job.CollectorNames;

    JobRecord updated = stored;
    // This record now carries a discovery observation, so its execution facts
    // are owned by the source: an incomplete generation is an unverified
    // execution from here on, never a manual row that merely lacks one.
    updated.execution_observed = true;
    updated.job = merged;
    if (job.execution) updated.job.execution = job.execution;
    updated.selector = selector;
    updated.last_seen_us = observed_at;
    const bool generation_became_complete =
        !stored.generation.complete() && generation.complete();
    if (generation.complete()) updated.generation = generation;
    if (!processes.empty()) {
        updated.members = normalized_processes(processes);
    } else if (members_complete) {
        updated.members.clear();
    } else if (!observed_job.JobPIDs.empty()) {
        updated.members.clear();
        for (const auto pid : observed_job.JobPIDs)
            updated.members.push_back(ProcessIdentity{pid, 0, ""});
    }

    // A record whose liveness could not be verified is re-admitted by positive
    // observation evidence. Unverified is not a lifecycle state.
    const bool recovered = !stored.execution_verified;
    if (recovered) updated.execution_verified = true;
    // Provenance is a durable fact of its own: a manual row that first meets a
    // discovery observation stays "manual" on disk when every other fact is
    // identical, and a restart would replay its incomplete generation as
    // Verified.  The delta rides the same write and revision as any fact change.
    const bool provenance_changed =
        updated.execution_observed != stored.execution_observed;
    const bool changed = recovered || provenance_changed ||
        facts_fingerprint(merged, updated) != facts_fingerprint(stored.job, stored);
    if (!changed) {
        updated.revision = stored.revision;
        result.ok = true;
        result.disposition = UpsertDisposition::Unchanged;
        result.revision = stored.revision;
        updated_out = std::move(updated);
        return result;
    }

    updated.revision = stored.revision + 1;
    result.ok = true;
    result.disposition = UpsertDisposition::Updated;
    result.revision = updated.revision;
    result.generation_became_complete = generation_became_complete;
    updated_out = std::move(updated);
    (void)observed_at;
    return result;
}

UpsertResult JobStore::upsert(const Job& job, JobRecord& out) {
    UpsertResult result;

    if (!store_ready()) {
        result.code = RegistrationError::StoreUnavailable;
        result.error = to_string(result.code);
        result.message = migration_complete_ ? "inventory store is not ready"
                                             : "inventory store is still initializing";
        return result;
    }
    if (recovery_required_.load()) {
        result.code = RegistrationError::StoreUnavailable;
        result.error = "recovery_required";
        result.message = "the last durable write outcome is unknown; recovery is required";
        return result;
    }

    const auto selector = selector_of(job);
    const auto identity = canonicalize(selector);
    if (!identity) {
        result.code = RegistrationError::IncompleteIdentity;
        result.error = "incomplete_identity";
        result.message = "scheduler type, cluster namespace and native job id are required";
        spdlog::debug("JobStore: rejected incomplete observation (native='{}', cluster='{}')",
                      selector.native_job_id, selector.cluster_name);
        return result;
    }

    std::unique_lock<std::mutex> admission(upsert_mtx_);
    const std::string index_key = key_identity(identity_index_key(*identity));

    const ExecutionGeneration generation = generation_of(job);
    const std::vector<ProcessIdentity>& processes = members_of(job);
    const bool members_complete = members_authoritative_of(job);
    const int64_t observed_at = read_at_us_of(job) != 0 ? read_at_us_of(job) : now_us();

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
            result.code = RegistrationError::StoreReadFailed;
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
                result.code = RegistrationError::StoreReadFailed;
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
                result.code = RegistrationError::StoreWriteFailed;
                result.error = "store_write_failed";
                result.message = status.ToString();
                return result;
            }
            indexed = false;
            internal_id = 0;
        } else {
            JobRecord updated;
            result = merge_existing(job, selector, *identity, internal_id, stored, observed_at,
                                    updated);
            if (!result.ok) return result;
            if (result.disposition == UpsertDisposition::Unchanged) {
                std::unique_lock lg(mtx_);
                records_[internal_id] = updated;
                out = std::move(updated);
                return result;
            }
            {
                std::lock_guard<std::mutex> db_lock(db_mtx_);
                leveldb::WriteBatch batch;
                batch.Put(key_job(internal_id), record_json(updated).dump());
                const std::vector<uint64_t> touched{internal_id};
                auto status = write_batch_locked(batch, "update inventory facts",
                                                 /*durable=*/false, &touched);
                if (status.ok()) mark_record_dirty(internal_id);
                if (!status.ok()) {
                    result.ok = false;
                    result.code = RegistrationError::StoreWriteFailed;
                    result.error = "store_write_failed";
                    result.message = status.ToString();
                    return result;
                }
            }
            note_record(updated);
            out = std::move(updated);
            return result;
        }
    }

    // Create a new inventory record. Nothing is activated and no caller
    // binding is accepted here. The id reservation rides along in the same
    // batch as the record and its index entry: one durable commit, not two.
    std::string allocation_error;
    uint64_t durable_counter = 0;
    bool durable_exhausted = false;
    uint64_t allocated = 0;
    if (!reserveIdInMemory(allocated, durable_counter, durable_exhausted, allocation_error)) {
        result.code = allocation_error == "id_exhausted" ? RegistrationError::IdExhausted
                                                         : RegistrationError::StoreWriteFailed;
        result.error = result.code == RegistrationError::IdExhausted
                           ? allocation_error
                           : to_string(result.code);
        result.message = "durable internal id allocation failed";
        return result;
    }

    JobRecord record;
    record.job = job;
    record.job.JobID = allocated;
    record.job.CollectorNames.clear();
    apply_identity_via_applier(identity_applier_, record.job, selector, *identity);
    record.job.JobPIDs.clear();
    if (!processes.empty()) {
        for (const auto& process : processes) record.job.JobPIDs.push_back(process.pid);
    }
    std::sort(record.job.JobPIDs.begin(), record.job.JobPIDs.end());
    record.job.JobPIDs.erase(std::unique(record.job.JobPIDs.begin(), record.job.JobPIDs.end()),
                             record.job.JobPIDs.end());

    record.internal_id = allocated;
    record.instance_id = instanceId(allocated);
    record.adapter = selector.adapter;
    record.selector = selector;
    record.identity = *identity;
    record.identity_complete = true;
    record.execution_observed = true;   // created by a discovery observation
    record.generation = generation;
    record.members_authoritative = members_complete;
    record.revision = 1;
    record.state = kStateActive;
    record.first_seen_us = observed_at;
    record.last_seen_us = observed_at;
    if (!processes.empty()) {
        record.members = normalized_processes(processes);
    } else {
        for (const auto pid : record.job.JobPIDs) {
            record.members.push_back(ProcessIdentity{pid, 0, ""});
        }
    }

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        leveldb::WriteBatch batch;
        batch.Put(key_counter("job_id"), std::to_string(durable_counter));
        // MAX is handed out at most once, ever: the marker is committed with
        // the record that consumes it.
        if (durable_exhausted) batch.Put(key_meta("job_id_exhausted"), "true");
        batch.Put(key_job(allocated), record_json(record).dump());
        batch.Put(index_key, nlohmann::json{
            {"internal_id", allocated},
            {"scheduler_type", identity->adapter},
            {"cluster_namespace", identity->cluster_namespace},
            {"native_job_id", identity->native_job_id},
            {"canonical_native", identity->canonical_native},
            {"step_id", identity->step_id},
        }.dump());
        // A create has no published memory image to revert: identity lookup
        // repairs an ambiguous create, so it does not latch recovery.
        auto status = write_batch_locked(batch, "create inventory record",
                                         /*durable=*/false);
        if (status.ok()) mark_record_dirty(allocated);
        if (!status.ok()) {
            result.code = RegistrationError::StoreWriteFailed;
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
    result.revision = record.revision;
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
    if (recovery_required_.load()) {
        spdlog::error("JobStore: refusing '{}' while recovery is required", operation);
        return false;
    }
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
    // A durable record mutation advances the one record revision. This is the
    // same counter the selection CAS compares.
    ++record.revision;

    leveldb::WriteBatch batch;
    batch.Put(key_job(id), record_json(record).dump());
    const std::vector<uint64_t> touched{id};
    status = write_batch_locked(batch, operation, /*durable=*/true, &touched);
    if (!status.ok()) return false;

    std::unique_lock lg(mtx_);
    records_[id] = record;
    return true;
}

bool JobStore::persistState(uint64_t id, const std::string& state) {
    if (!db_running_ || db_ == nullptr) return false;
    if (recovery_required_.load()) {
        spdlog::error("JobStore: refusing a state write for JobID {} while recovery is "
                      "required", id);
        return false;
    }

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
    ++record.revision;
    leveldb::WriteBatch batch;
    batch.Put(key_job(id), record_json(record).dump());
    const std::vector<uint64_t> touched{id};
    status = write_batch_locked(batch, "persist record state", /*durable=*/true, &touched);
    if (!status.ok()) return false;

    std::unique_lock lg(mtx_);
    auto it = records_.find(id);
    if (it != records_.end()) {
        // The ONE revision orders memory and disk alike: publishing only the
        // state here would leave a memory image that names an older revision
        // than the durable row the same commit wrote.
        it->second.state = state;
        it->second.revision = record.revision;
    }
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
        if (record.revision == 0) {
            record.revision = 1;
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
    if (recovery_required_.load()) {
        spdlog::error("JobStore: refusing to archive JobID {} while recovery is required",
                      id);
        return false;
    }

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

    // The archived record already carries the final selection; `selection` is
    // kept as an explicit audit field for readers of the history row.

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

    const std::vector<uint64_t> touched{id};
    s = write_batch_locked(batch, "archive job", /*durable=*/true, &touched);
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
// Desired selection
// ---------------------------------------------------------------------------

CollectionSelection JobStore::selection(uint64_t id) const {
    // The in-memory record is the published projection of the single `j:` row;
    // restore() loads the selection from that row, so this is durable truth
    // after a restart as well.
    std::shared_lock lg(mtx_);
    const auto it = records_.find(id);
    if (it != records_.end()) return it->second.selection;
    return CollectionSelection{};
}

bool JobStore::mutateSelections(const std::vector<SelectionMutation>& mutations,
                                std::vector<SelectionMutationResult>& out,
                                std::string& error) {
    out.clear();
    error.clear();
    if (!store_ready()) {
        error = "store_unavailable";
        return false;
    }
    if (recovery_required_.load()) {
        // The stored row may already carry a write memory did not publish.
        // Refusing is the only answer that cannot diverge further.
        error = "recovery_required";
        return false;
    }
    if (mutations.empty()) {
        error = "no_targets";
        return false;
    }

    // Serialize with observation upserts and hold the db transaction lock for
    // the whole read-transform-write: the revision guard below is evaluated
    // against the same image that is written, so no caller can pass a stale
    // revision between a check and a commit.
    std::lock_guard<std::mutex> admission(upsert_mtx_);
    std::lock_guard<std::mutex> db_lock(db_mtx_);

    struct PendingWrite {
        JobRecord record;
    };
    std::vector<SelectionMutationResult> planned;
    std::vector<PendingWrite> writes;
    std::set<uint64_t> seen;
    planned.reserve(mutations.size());

    for (const auto& mutation : mutations) {
        if (!seen.insert(mutation.internal_id).second) {
            error = "duplicate_target";
            return false;
        }

        std::string raw;
        const auto status = db_->Get(leveldb::ReadOptions(),
                                     key_job(mutation.internal_id), &raw);
        if (!status.ok()) {
            error = status.IsNotFound() ? "not_found" : "store_error";
            return false;
        }
        JobRecord record;
        try {
            record = parse_record(raw, mutation.internal_id);
        } catch (const std::exception&) {
            error = "store_error";
            return false;
        }
        if (record.quarantined) {
            error = "identity_quarantined";
            return false;
        }
        if (record.state == kStateClosing) {
            error = "instance_closing";
            return false;
        }
        if (mutation.expected_revision.has_value() &&
            record.revision != *mutation.expected_revision) {
            error = "stale_revision";
            return false;
        }

        const CollectionSelection current = record.selection;
        const auto before = current.effectiveCollectors();
        CollectionSelection next = current;
        bool intent_changed = false;
        bool stored_changed = false;
        bool skipped_manual_override = false;

        switch (mutation.operation) {
            case SelectionOperation::SetManual:
                if (!current.manual.has_value() ||
                    *current.manual != mutation.collectors) {
                    next.manual = mutation.collectors;
                    intent_changed = true;
                    stored_changed = true;
                }
                break;
            case SelectionOperation::ResetAuto:
                if (current.manual.has_value()) {
                    next.manual.reset();
                    intent_changed = true;
                    stored_changed = true;
                }
                break;
            case SelectionOperation::SetAuto:
                if (current.automatic != mutation.collectors ||
                    current.automatic_source != mutation.automatic_source) {
                    next.automatic = mutation.collectors;
                    next.automatic_source = mutation.automatic_source;
                    stored_changed = true;
                    if (current.manual.has_value()) {
                        // Manual precedence, including an empty manual stop:
                        // record the decision for a later reset_auto without
                        // moving the client-visible intent or the runtime.
                        skipped_manual_override = true;
                    } else {
                        intent_changed = true;
                    }
                }
                break;
        }

        SelectionMutationResult result;
        result.internal_id = mutation.internal_id;
        result.intent_changed = intent_changed;
        result.stored_changed = stored_changed;
        result.skipped_manual_override = skipped_manual_override;
        result.runtime_changed = before != next.effectiveCollectors();
        // ONE revision orders every record mutation, bookkeeping included: an
        // automatic decision recorded under a manual override still advances
        // the record. It does not detach anything (runtime_changed stays false).
        result.revision = stored_changed ? record.revision + 1 : record.revision;
        if (stored_changed) {
            record.selection = next;
            record.revision = result.revision;
            record.job.CollectorNames = next.effectiveCollectors();
            writes.push_back(PendingWrite{std::move(record)});
            result.selection = std::move(next);
        } else {
            result.selection = current;
        }
        planned.push_back(std::move(result));
    }

    if (!writes.empty()) {
        leveldb::WriteBatch batch;
        std::vector<uint64_t> touched;
        touched.reserve(writes.size());
        for (const auto& write : writes) {
            batch.Put(key_job(write.record.internal_id),
                      record_json(write.record).dump());
            touched.push_back(write.record.internal_id);
        }
        // Intent is hot path: unsynced, checkpointed by maintenance/shutdown.
        const auto status = write_batch_locked(batch, "mutate selection",
                                               /*durable=*/false, &touched);
        if (!status.ok()) {
            // A reported failure publishes nothing: the in-memory records and
            // the caller-visible outcome stay at their previous values.
            error = "store_write_failed";
            return false;
        }
    }

    if (!writes.empty()) {
        std::unique_lock lg(mtx_);
        for (const auto& write : writes) {
            const auto id = write.record.internal_id;
            const auto it = records_.find(id);
            if (it != records_.end()) {
                it->second.selection = write.record.selection;
                it->second.revision = write.record.revision;
                it->second.job.CollectorNames = write.record.job.CollectorNames;
            } else {
                records_[id] = write.record;
            }
            dirty_records_.insert(id);
        }
    }

    out = std::move(planned);
    return true;
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
    store_health.recovery_required = recovery_required_.load();
    store_health.durable_commits = durable_commits_.load();
    store_health.sync_commits = sync_commits_.load();
    store_health.write_failures = store_write_failures_.load();
    store_health.corrupt_records = corrupt_records_;

    {
        std::lock_guard<std::mutex> db_lock(db_mtx_);
        store_health.last_write_error = last_store_error_;
        store_health.recovery_state = recovery_operation_;
        store_health.recovery_fence_entries = recovery_fence_entries_;
    }

    {
        std::shared_lock lg(mtx_);
        store_health.active_instances = records_.size();
        store_health.dirty_records = dirty_records_.size();
        for (const auto& entry : records_) {
            if (!entry.second.identity_complete) ++store_health.incomplete_identities;
            if (entry.second.quarantined) ++store_health.quarantined_identities;
        }
    }
    return store_health;
}
