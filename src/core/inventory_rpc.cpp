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
// Inventory/collection RPC surface, split out of the registry facade.
// Everything here reads state through the public JobRegistry API, so no
// registry lock is held while a mutation or provider call runs.

#include "core/inventory_rpc.hpp"

#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <set>
#include <type_traits>

#include "common/config.hpp"
#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "common/utils.hpp"
#include "core/collector_registry.hpp"
#include "core/scheduler_adapter.hpp"
#include "core/inventory_config.hpp"
#include "core/inventory_status_provider.hpp"
#include "common/local_rpc.hpp"

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
    }else{
        // 开放世界：任何其它 subtype 名字即 adapter 名，typed facts 以 JSON
        // 原样入库（该 adapter 的 encodeFacts/decodeFacts 是唯一解释者）。
        if (!normalize_adapter_name(subtype).empty()) {
            job.subtype_name = normalize_adapter_name(subtype);
            job.sub_attr = obj.value("sub_attr", nlohmann::json::object());
            job.NativeJobID = obj.value("NativeJobID", "");
        } else {
            throw std::runtime_error("invalid job subtype: " + subtype);
        }
    }

    spdlog::info("JobRegistry: json2JobOpt parsed opt={}, type={}, subtype={}, JobID={}, "
                 "PIDs_count={}, Lens_count={}, nativeID={}",
                 opt, type, subtype, job.JobID, job.JobPIDs.size(),
                 job.CollectorNames.size(), job.NativeJobID);
    
    return opt;
}


namespace {

constexpr uint32_t kRpcSchemaVersion = 2;

// ---------------------------------------------------------------------------
// Local serialization helpers (the JobRecord wire form lives in job_store;
// the RPC only needs generation/process projections for inventory views)
// ---------------------------------------------------------------------------

nlohmann::json generation_to_json(const ExecutionGeneration& generation) {
    return nlohmann::json{
        {"boot_id", generation.boot_id},
        {"manager_pid", generation.manager_pid},
        {"manager_start_ticks", generation.manager_start_ticks},
        {"cgroup_identity", generation.cgroup_identity},
    };
}

nlohmann::json process_to_json(const ProcessIdentity& process) {
    return nlohmann::json{
        {"pid", process.pid},
        {"start_ticks", process.start_ticks},
        {"boot_id", process.boot_id},
    };
}



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
uint64_t inventory_snapshot_revision(const std::vector<JobRecord>& records)
{
    std::vector<const JobRecord*> ordered;
    ordered.reserve(records.size());
    for (const auto& record : records) ordered.push_back(&record);
    std::sort(ordered.begin(), ordered.end(),
              [](const JobRecord* lhs, const JobRecord* rhs) {
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
        mix(record->revision);
        mix(record->quarantined ? 1u : 0u);
        mix(record->identity_complete ? 2u : 0u);
        mix(record->state == "closing" ? 3u : (record->execution_verified ? 5u : 4u));
    }
    // Stay inside the signed 63-bit range so JavaScript consumers keep the
    // exact value; the revision is an opaque token, not a counter.
    return hash & 0x7fffffffffffffffULL;
}

std::string inventory_source(const JobRecord& record)
{
    return record.job.subtype_name.empty() ? "manual" : record.job.subtype_name;
}

// Step claim as the RPC surface reports it: the value or empty, never a
// bare nullopt (the optional itself is store-internal, 0-vs-missing rule).
std::string step_id_text(const JobRecord& record)
{
    const auto& execution = record.job.execution;
    return execution && execution->step_id ? *execution->step_id : std::string{};
}

nlohmann::json inventory_record_json(JobRegistry& registry, const JobRecord& record)
{
    nlohmann::json out;
    // Deprecated key kept for old readers; the value is the internal id as a
    // decimal string (same as internal_job_id).
    out["instance_id"] = format_internal_id(record.internal_id);
    // New APIs carry internal ids as decimal strings (JavaScript uint64 safety).
    out["internal_job_id"] = format_internal_id(record.internal_id);
    out["schema_version"] = kRpcSchemaVersion;
    out["record_revision"] = record.revision;
    out["state"] = record.state;
    out["source"] = inventory_source(record);
    out["quarantined"] = record.quarantined;
    out["identity_complete"] = record.identity_complete;
    // Selector fields are projections of the Job: the typed facts are the
    // single source of truth, no second copy rides the record.
    out["scheduler_type"] = record.job.subtype_name;
    out["cluster_name"] = record.job.cluster_name;
    out["cluster_tag"] = record.job.clusterTag;
    out["native_job_id"] = record.job.NativeJobID;
    out["step_id"] = step_id_text(record);
    out["first_seen_us"] = record.first_seen_us;
    out["last_seen_us"] = record.last_seen_us;
    out["generation"] = generation_to_json(generation_of(record.job));
    out["processes"] = nlohmann::json::array();
    for (const auto& process : members_of(record.job)) {
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

std::vector<std::string> instance_ids_of(const std::vector<JobRecord>& records)
{
    std::vector<std::string> ids;
    ids.reserve(records.size());
    // Deprecated key spelling: the value is the internal id, decimal string.
    for (const auto& record : records) ids.push_back(format_internal_id(record.internal_id));
    return ids;
}

void sort_records(std::vector<JobRecord>& records)
{
    std::sort(records.begin(), records.end(),
              [](const JobRecord& lhs, const JobRecord& rhs) {
                  return lhs.internal_id < rhs.internal_id;
              });
}

// Resolution outcome of collection selectors. Empty `code` means success;
// on ambiguity `records` carries the candidate instances.
struct TargetResolution {
    std::vector<JobRecord> records;
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
    if (selector.adapter != kAdapterCondor)
        return stored.cluster_namespace == requested.cluster_namespace;
    if (!selector.cluster_tag.empty())
        return stored.cluster_namespace == requested.cluster_namespace;
    if (stored.cluster_namespace == requested.cluster_namespace) return true;
    return stored.cluster_namespace.rfind(requested.cluster_namespace + "@", 0) == 0;
}

// One on-demand authoritative pass through the registry heartbeat. Returns
// false when no controller is running, so callers keep an explicit "not found"
// instead of inventing a record. Never allocates ids or guesses an identity.
bool refresh_inventory_once()
{
#ifdef JOBLENS_HAS_INVENTORY_STATUS_PROVIDER
    auto provider = get_inventory_refresh_provider();
    if (!provider) return false;
    try {
        return provider();
    } catch (const std::exception& error) {
        spdlog::warn("JobRegistry: on-demand inventory refresh failed: {}", error.what());
        return false;
    }
#else
    return false;
#endif
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

    std::vector<JobRecord> matches;
    for (auto& record : registry.inventoryRecords()) {
        if (is_reserved_internal_id(record.internal_id)) continue;
        if (!record.identity_complete) continue;
        if (record.identity.adapter != identity->adapter) continue;
        if (record.identity.canonical_native != identity->canonical_native) continue;
        if (!namespace_matches(record.identity, selector, *identity)) continue;
        if (!identity->step_id.empty() && record.identity.step_id != identity->step_id) continue;
        matches.push_back(std::move(record));
    }
    sort_records(matches);

    if (matches.empty()) {
        if (ensure_registered) {
            // Refresh discovery before declaring the target missing.
            // Only a real source pass may create the record; no caller-supplied
            // PID or native number authorizes an invented live instance.
            if (refresh_inventory_once()) {
                return resolve_native_target(registry, selector, scope,
                                             /*ensure_registered=*/false);
            }
            result.message =
                "no discovery source is running on this core, so ensure_registered "
                "cannot confirm a live instance for this request";
        } else {
            result.message = "no verified current instance for native job '" +
                             selector.native_job_id + "' in cluster '" +
                             selector.cluster_name + "'";
        }
        result.code = "not_found";
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
            result.message = "instance " + format_internal_id(record.internal_id) +
                             " is closing and rejects new binding mutations";
            result.records.clear();
            return result;
        }
    }
    result.records = std::move(matches);
    return result;
}

// Instance addressing by internal_job_id: the durable id resolves in O(1)
// through the registry's record map. Addressability: an active record only
// (findJob), a closing instance rejects with generation_closing, and the
// reserved system sentinel is never addressable.
TargetResolution resolve_instance_handle_by_id(JobRegistry& registry, uint64_t internal_id)
{
    TargetResolution result;
    auto record = is_reserved_internal_id(internal_id)
                      ? std::nullopt
                      : registry.inventoryRecord(internal_id);
    if (!record) {
        result.code = "not_found";
        result.message = "no verified current instance with internal_job_id '" +
                         format_internal_id(internal_id) + "'";
        return result;
    }
    if (!registry.findJob(record->internal_id).has_value()) {
        result.code = "generation_closing";
        result.message = "instance " + format_internal_id(record->internal_id) +
                         " is closing and rejects new binding mutations";
        return result;
    }
    result.records.push_back(std::move(*record));
    return result;
}

// The durable record is authoritative for what the operator asked for; the
// collection runtime is the only source of what is actually attached. The two
// are reported separately so "accepted" can never be read as "collecting".
nlohmann::json collection_target_json(JobRegistry& registry, const JobRecord& record,
                                      const CollectionSelection& selection,
                                      uint64_t revision, bool changed,
                                      const ActualCollectionState& actual)
{
    const auto desired = selection.effectiveCollectors();

    nlohmann::json out;
    // Deprecated key kept for old readers; same value as internal_job_id.
    out["instance_id"] = format_internal_id(record.internal_id);
    out["internal_job_id"] = format_internal_id(record.internal_id);
    out["native_job_id"] = record.job.NativeJobID;
    // Accepted: the durable record revision the command committed.
    out["record_revision"] = revision;
    out["mode"] = to_string(selection.mode());
    out["desired_collectors"] = desired;
    // Actual: the runtime's own state, sampled after the commit. It may still
    // show the previous attachment; that is the honest answer.
    out["applied_collectors"] = actual.collectors;
    out["applied_attach_token"] = actual.attach_token;
    out["draining"] = actual.draining;
    out["errors"] = nlohmann::json::array();
    if (!actual.error.empty()) {
        out["errors"].push_back(nlohmann::json{
            {"code", "apply_failed"},
            {"message", actual.error},
        });
    } else if (!desired.empty() && actual.collectors.size() != desired.size()) {
        out["errors"].push_back(nlohmann::json{
            {"code", "not_converged"},
            {"message", "the collection runtime has not applied the desired set yet"},
        });
    }
    out["in_pipeline"] = !actual.collectors.empty();
    out["changed"] = changed;
    return out;
}

// Legacy add identity. The supplied JobID is never consulted; only the
// typed sub_attr plus the scheduler namespace the discovery sources use.
std::optional<NativeSelector> legacy_add_selector(const Job& job, const nlohmann::json& req)
{
    NativeSelector selector;
    const auto sub = req.contains("sub_attr") && req["sub_attr"].is_object()
                         ? req["sub_attr"] : nlohmann::json::object();
    if (job.subtype == JobSubType::Condor) {
        selector.adapter = kAdapterCondor;
        selector.native_job_id = job.NativeJobID;
        if (const auto* attr = std::get_if<CondorJobAttr>(&job.sub_attr))
            selector.native_job_id = fmt::format("{}.{}", attr->cluster_id, attr->proc_id);
        selector.cluster_tag = sub.value("scheduler_name", std::string{});
        selector.cluster_name = sub.value("collector_host", std::string{});
        if (selector.cluster_name.empty()) selector.cluster_name = job.cluster_name;
        if (selector.cluster_name.empty())
            selector.cluster_name = Utils::get_condor_collector_host();
    } else if (job.subtype == JobSubType::Slurm) {
        selector.adapter = kAdapterSlurm;
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

SubAttrMatcher parse_legacy_matcher(const nlohmann::json& matcher_json)
{
    SubAttrMatcher matcher;
    if (matcher_json.contains("subtype")) {
        const auto subtype = matcher_json["subtype"].get<std::string>();
        if (subtype == "Condor") matcher.subtype = JobSubType::Condor;
        else if (subtype == "Slurm") matcher.subtype = JobSubType::Slurm;
        else if (subtype == "Common") matcher.subtype = JobSubType::Common;
        else {
            // 开放世界：任意 adapter 名字按名字匹配（Job::subtype_name）。
            matcher.subtype_name = normalize_adapter_name(subtype);
        }
    }
    if (matcher_json.contains("facts_fields") && matcher_json["facts_fields"].is_object())
        matcher.json_fields = matcher_json["facts_fields"];
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

// Legacy remove: stop collection, keep the inventory fact. Only records
// outside the inventory (pure legacy registrations) are archived as before.
// With "erase": true the command becomes the real-delete tier — every target
// is retired (closing -> archive for collected instances, durable delete for
// never-collected ones) instead of stopped.
nlohmann::json legacy_remove(JobRegistry& registry, const nlohmann::json& req)
{
    bool erase = false;
    if (req.contains("erase") && !req["erase"].is_null()) {
        if (!req["erase"].is_boolean()) {
            return error_payload("invalid_request",
                                 "erase must be a boolean (true retires the instance for "
                                 "real, the default keeps the stop semantics)");
        }
        erase = req["erase"].get<bool>();
    }

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

    if (erase) {
        // The erase tier: a real retirement. The closing fact is durable
        // before anything destructive starts; collected instances end as a
        // history row, never-collected ones as a durable delete.
        std::vector<std::string> erased_instances;
        size_t erased = 0;
        for (const auto id : targets) {
            if (id == kSystemSentinelJobId) continue;
            if (!registry.retireInstance(id)) {
                return error_payload(
                    "remove_failed",
                    fmt::format("could not retire instance {}; the record stays "
                                "active and the command is retryable",
                                format_internal_id(id)));
            }
            ++erased;
            erased_instances.push_back(format_internal_id(id));
        }
        nlohmann::json out;
        out["status"] = "success";
        out["mode"] = "erase";
        out["deleted_count"] = erased;
        out["erased_instances"] = std::move(erased_instances);
        out["inventory_retained"] = false;
        out["msg"] = "erase retired the instances (collected ones archived, "
                     "never-collected ones deleted)";
        return out;
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
        const auto outcome = registry.setCollectors(stop);
        if (!outcome.ok) {
            spdlog::error("JobRegistry: legacy remove could not stop collection for JobID {}: {}",
                          id, outcome.message);
            return error_payload(to_string(outcome.error), outcome.message);
        }
        ++stopped;
        stopped_instances.push_back(outcome.targets.front().instance_id);
        const auto& target = outcome.targets.front();
        if (!target.actual.error.empty()) {
            apply_errors.push_back(nlohmann::json{
                {"instance_id", target.instance_id},
                {"code", "apply_failed"},
                {"message", target.actual.error},
            });
        }
    }

    if (!targets.empty()) {
        spdlog::warn("JobRegistry: legacy remove is deprecated and now stops collection for "
                     "{} instance(s); inventory records are retained",
                     stopped);
    }

    nlohmann::json out;
    out["status"] = "success";
    out["mode"] = "stop";
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

// Readiness gate for the inventory/control surface. A rejected schema or a
// closed store must not look like "no jobs" or "no such instance".
std::optional<nlohmann::json> store_not_ready_payload(const StoreHealth& health)
{
    if (health.db_running && health.migration_complete) return std::nullopt;
    if (!health.db_running) {
        if (health.migration_state == "malformed_schema_marker") {
            return error_payload("store_unavailable",
                                 "inventory store has a schema marker this build did not "
                                 "write; the database was left untouched (no rewrite, no "
                                 "deletion)");
        }
        if (health.migration_state.rfind("unsupported_schema", 0) == 0) {
            return error_payload("store_unavailable",
                                 "inventory store schema is not supported by this build; "
                                 "it was left untouched (no migration, no deletion): " +
                                     health.migration_state);
        }
        return error_payload("store_unavailable",
                             "inventory store is not running on this core");
    }
    return error_payload("store_unavailable",
                         "inventory store is not ready (" +
                             (health.migration_state.empty() ? std::string{"unknown"}
                                                             : health.migration_state) + ")");
}

nlohmann::json store_health_json(const StoreHealth& health){
    return nlohmann::json{
        {"running", health.db_running},
        {"migration_complete", health.migration_complete},
        {"migration_state", health.migration_state},
        {"degraded", health.degraded},
        {"recovery_required", health.recovery_required},
        {"recovery_state", health.recovery_state},
        {"durable_commits", health.durable_commits},
        // fsynced subset: the registration hot path must not move this
        {"sync_commits", health.sync_commits},
        {"dirty_records", health.dirty_records},
        {"write_failures", health.write_failures},
        {"last_write_error", health.last_write_error},
        {"corrupt_records", health.corrupt_records},
        {"db_uuid", health.db_uuid},
        {"persistence", nlohmann::json{
            {"state", health.recovery_required
                          ? "recovery_required"
                          : (health.degraded ? "degraded"
                                             : (health.db_running ? "ok" : "unavailable"))},
            {"schema_version", health.schema_version},
            {"last_operation", health.recovery_state},
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
                                : ((health.degraded || health.recovery_required)
                                       ? "degraded"
                                       : "healthy"));
    body["inventory_enabled"] = health.db_running;
    body["sources"] = nlohmann::json{{"state", "not_configured"}};

    uint64_t active = 0;
    uint64_t suspect = 0;
    uint64_t closing = 0;
    for (const auto& record : registry.inventoryRecords()) {
        if (is_reserved_internal_id(record.internal_id)) continue;
        if (!record.execution_verified) ++suspect;
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


}  // namespace

void registerInventoryRpc(JobRegistry& registry) {
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
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            try {
                Job job;
                std::string opt = json2JobOpt(req, job);
                if(opt.compare("add") == 0){
                    // The manual add and update routes are one upsert.
                    // The identity selector (kept here as the compatibility
                    // resolution) routes a request naming a live inventoried
                    // instance to UPDATE; a legacy JobID is only a hint and
                    // never allocates or overrides the internal id.
                    j = registry.upsertJob(job, req, legacy_add_selector(job, req));
                }else if(opt.compare("remove") == 0){
                    // Remove stops collection for resolved live jobs and
                    // keeps the inventory fact instead of forgetting the
                    // execution.
                    j = legacy_remove(registry, req);
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
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            nlohmann::json j;
            j["status"] = "ok";
            j["job_count"] = registry.snapshot().size();
            return j;
        }
    );

    // 列出所有 Job
    RPCServer::instance().register_method("JobRegistry/list_jobs",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            nlohmann::json res = nlohmann::json::object();
            res["status"] = "ok";
            res["jobs"] = nlohmann::json::array();
            for (const auto& job : registry.snapshot()) {
                if (job.JobID == 0) continue; //跳过彩蛋任务
                res["jobs"].push_back(dump_job(job));
            }
            return res;
        }
    );
    // 获取指定 JobID 的 Job 信息
    RPCServer::instance().register_method("JobRegistry/get_job",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            if (!req.contains("JobID")) {
                j["error"] = "missing JobID in request";
                return j;
            }
            uint64_t jobID = req["JobID"].get<uint64_t>();
            auto job = registry.findJob(jobID);
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
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            nlohmann::json j;
            j["status"] = "ok";
            j["running"] = registry.attributionRunning();
            j["pushed_events"] = registry.attributionPushedEvents();
            j["dropped_events"] = registry.attributionDroppedEvents();
            return j;
        }
    );

    // -----------------------------------------------------------------------
    // Inventory read surface
    // -----------------------------------------------------------------------
    RPCServer::instance().register_method("JobRegistry/inventory_list",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            if (auto not_ready = store_not_ready_payload(registry.storeHealth()))
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
                scheduler_filter = normalize_adapter_name(
                    params["scheduler_type"].get<std::string>());
                // Open world, validated against the registered adapters: an
                // unknown name is a caller error, not an empty result.
                if (!SchedulerAdapterRegistry::instance().known(scheduler_filter))
                    return error_payload(
                        "invalid_request",
                        fmt::format("unknown scheduler_type '{}' (registered: {})",
                                    scheduler_filter,
                                    fmt::join(SchedulerAdapterRegistry::instance().names(),
                                              ", ")));
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

            std::vector<JobRecord> matching;
            for (auto& record : registry.inventoryRecords()) {
                if (is_reserved_internal_id(record.internal_id)) continue;
                if (!scheduler_filter.empty() &&
                    record.job.subtype_name != scheduler_filter)
                    continue;
                if (!native_filter.empty() && record.job.NativeJobID != native_filter)
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
                records.push_back(inventory_record_json(registry, record));
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
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            return inventory_status_payload(registry);
        }
    );

    RPCServer::instance().register_method("JobRegistry/capabilities",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            const auto health = registry.storeHealth();
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
    // Collection control surface
    // -----------------------------------------------------------------------
    RPCServer::instance().register_method("JobCollection/set",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request", "request body must be a JSON object");
            if (auto not_ready = store_not_ready_payload(registry.storeHealth()))
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

            const bool has_internal_id =
                selector.contains("internal_job_id") &&
                !selector["internal_job_id"].is_null();
            std::vector<std::string> native_fields;
            for (const char* name : {"scheduler_type", "adapter", "cluster_name",
                                     "cluster_tag", "native_job_id", "step_id"}) {
                if (selector.contains(name) && !selector[name].is_null())
                    native_fields.emplace_back(name);
            }
            if (selector.contains("instance_id") && !selector["instance_id"].is_null())
                return selector_error(
                    "instance_id is retired; address the instance by "
                    "internal_job_id (decimal string) or native identity fields");
            if (has_internal_id && !native_fields.empty())
                return selector_error("selector must use exactly one form: an instance "
                                      "handle (internal_job_id) or native "
                                      "identity fields, not both");
            if (!has_internal_id && native_fields.empty())
                return selector_error("selector is required: an instance handle "
                                      "(internal_job_id) or native identity "
                                      "fields");
            if (has_internal_id && scope != "instance")
                return selector_error("instance handle selectors only support "
                                      "scope=instance");

            NativeSelector native_selector;
            std::optional<uint64_t> internal_handle;
            try {
                if (has_internal_id) {
                    const auto raw = optional_string(selector, "internal_job_id");
                    internal_handle = parse_internal_id(raw);
                    if (!internal_handle)
                        return selector_error(
                            "internal_job_id must be a decimal internal id string (got '" +
                            raw + "')");
                }
                if (!has_internal_id) {
                    native_selector.cluster_tag = optional_string(selector, "cluster_tag");
                    native_selector.step_id = optional_string(selector, "step_id");
                    native_selector.native_job_id = optional_string(selector, "native_job_id");
                    native_selector.cluster_name = optional_string(selector, "cluster_name");
                    // "adapter" is the preferred key; the legacy
                    // "scheduler_type" spelling keeps old callers working.
                    std::string adapter_name;
                    if (selector.contains("adapter") && !selector["adapter"].is_null())
                        adapter_name = optional_string(selector, "adapter");
                    else
                        adapter_name = optional_string(selector, "scheduler_type");
                    adapter_name = normalize_adapter_name(adapter_name);
                    if (!SchedulerAdapterRegistry::instance().known(adapter_name))
                        return selector_error(
                            fmt::format("unknown scheduler_type '{}' (registered: {})",
                                        adapter_name,
                                        fmt::join(SchedulerAdapterRegistry::instance()
                                                      .names(),
                                                  ", ")));
                    native_selector.adapter = adapter_name;
                }
            } catch (const std::exception& error) {
                return selector_error(error.what());
            }

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
                has_internal_id
                    ? resolve_instance_handle_by_id(registry, *internal_handle)
                    : resolve_native_target(registry, native_selector, scope,
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

            const auto outcome = operation == "set" ? registry.setCollectors(request)
                                                    : registry.resetAuto(request);
            if (!outcome.ok) {
                return error_payload(to_string(outcome.error), outcome.message);
            }

            nlohmann::json targets = nlohmann::json::array();
            for (const auto& target : outcome.targets) {
                const auto record = registry.inventoryRecord(target.internal_id);
                if (!record) continue;
                targets.push_back(collection_target_json(
                    registry, *record, target.selection, target.revision, target.changed,
                    target.actual));
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
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request", "request body must be a JSON object");
            if (auto not_ready = store_not_ready_payload(registry.storeHealth()))
                return *not_ready;
            // internal_job_id is the addressing field; instance_id is the
            // deprecated spelling kept for readers echoing an old response —
            // both carry the same decimal-string value now.
            std::string internal_job_id;
            try {
                if (req.contains("internal_job_id") && !req["internal_job_id"].is_null())
                    internal_job_id = optional_string(req, "internal_job_id");
                else
                    internal_job_id = optional_string(req, "instance_id");
            } catch (const std::exception& error) {
                return error_payload("invalid_request", error.what());
            }
            if (internal_job_id.empty())
                return error_payload("invalid_request", "internal_job_id is required");
            const auto internal_id = parse_internal_id(internal_job_id);
            if (!internal_id)
                return error_payload(
                    "invalid_request",
                    "internal_job_id must be a decimal internal id string (got '" +
                        internal_job_id + "')");

            const auto resolution = resolve_instance_handle_by_id(registry, *internal_id);
            if (!resolution.ok())
                return error_payload(resolution.code, resolution.message);

            const auto& record = resolution.records.front();
            const auto selection = registry.selectionRecord(record.internal_id);
            auto target = collection_target_json(registry, record, selection,
                                                 record.revision, false,
                                                 registry.actualCollection(record.internal_id));

            nlohmann::json out;
            out["status"] = "ok";
            out["instance_id"] = target["instance_id"];
            out["internal_job_id"] = target["internal_job_id"];
            out["native_job_id"] = target["native_job_id"];
            out["scheduler_type"] = record.job.subtype_name;
            out["cluster_name"] = record.job.cluster_name;
            out["cluster_tag"] = record.job.clusterTag;
            out["step_id"] = step_id_text(record);
            out["state"] = record.state;
            out["quarantined"] = record.quarantined;
            out["mode"] = target["mode"];
            out["record_revision"] = target["record_revision"];
            out["desired_collectors"] = target["desired_collectors"];
            out["applied_collectors"] = target["applied_collectors"];
            out["applied_attach_token"] = target["applied_attach_token"];
            out["draining"] = target["draining"];
            out["in_pipeline"] = target["in_pipeline"];
            out["automatic_collectors"] = selection.automatic.has_value()
                                              ? nlohmann::json(*selection.automatic)
                                              : nlohmann::json(nullptr);
            out["automatic_source"] = selection.automatic_source;
            out["manual_collectors"] = selection.manual.has_value()
                                           ? nlohmann::json(*selection.manual)
                                           : nlohmann::json(nullptr);
            out["errors"] = nlohmann::json::array();
            return out;
        }
    );
}

