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
// Section 9 inventory/collection RPC surface, split out of the registry facade
// (v2 stage S2). Everything here reads state through the public JobRegistry
// API, so no registry lock is held while a mutation or provider call runs.

#include "core/inventory_rpc.hpp"

#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <set>
#include <type_traits>

#include "common/config.hpp"
#include "common/condor_job.hpp"
#include "common/slurm_job.hpp"
#include "common/utils.hpp"
#include "core/collector_registry.hpp"
#include "core/job_collection_manager.hpp"
#include "core/job_inventory_maintenance.hpp"
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
        mix(record->facts_revision);
        mix(record->quarantined ? 1u : 0u);
        mix(record->identity_complete ? 2u : 0u);
        mix(record->state == "closing" ? 3u : (record->state == "suspect" ? 4u : 5u));
    }
    // Stay inside the signed 63-bit range so JavaScript consumers keep the
    // exact value; the revision is an opaque token, not a counter.
    return hash & 0x7fffffffffffffffULL;
}

std::string inventory_source(const JobRecord& record)
{
    switch (record.selector.scheduler_type) {
        case SchedulerType::Condor: return "condor";
        case SchedulerType::Slurm:  return "slurm";
        case SchedulerType::Unknown: break;
    }
    return "manual";
}

nlohmann::json inventory_record_json(JobRegistry& registry, const JobRecord& record)
{
    nlohmann::json out;
    out["instance_id"] = record.instance_id;
    // New APIs carry internal ids as decimal strings (JavaScript uint64 safety).
    out["internal_job_id"] = format_internal_id(record.internal_id);
    out["schema_version"] = kRpcSchemaVersion;
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
    for (const auto& process : record.members) {
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
    for (const auto& record : records) ids.push_back(record.instance_id);
    return ids;
}

void sort_records(std::vector<JobRecord>& records)
{
    std::sort(records.begin(), records.end(),
              [](const JobRecord& lhs, const JobRecord& rhs) {
                  return lhs.internal_id < rhs.internal_id;
              });
}

// Resolution outcome of section 9.2 selectors. Empty `code` means success;
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
    if (selector.scheduler_type != SchedulerType::Condor)
        return stored.cluster_namespace == requested.cluster_namespace;
    if (!selector.cluster_tag.empty())
        return stored.cluster_namespace == requested.cluster_namespace;
    if (stored.cluster_namespace == requested.cluster_namespace) return true;
    return stored.cluster_namespace.rfind(requested.cluster_namespace + "@", 0) == 0;
}

// One on-demand authoritative pass through the maintenance controller. Returns
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
        if (record.identity.scheduler_type != identity->scheduler_type) continue;
        if (record.identity.canonical_native != identity->canonical_native) continue;
        if (!namespace_matches(record.identity, selector, *identity)) continue;
        if (!identity->step_id.empty() && record.identity.step_id != identity->step_id) continue;
        matches.push_back(std::move(record));
    }
    sort_records(matches);

    if (matches.empty()) {
        if (ensure_registered) {
            // Plan 4.3: refresh discovery before declaring the target missing.
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
nlohmann::json collection_target_json(JobRegistry& registry, const JobRecord& record,
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


}  // namespace

void registerInventoryRpc(JobRegistry& registry,
                          std::shared_ptr<JobCollectionManager> manager) {
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
    // `manager` is the registry's control manager, created in the constructor
    // and shared with the automatic-selection path. Intent mutations are
    // serialized inside it and committed through JobRegistry::commitBindings.

    RPCServer::instance().register_method("JobRegistry/job_opt",
        [&registry, manager](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            try {
                Job job;
                std::string opt = json2JobOpt(req, job);
                if(opt.compare("add") == 0){
                    // §9.4: identity resolution first. A legacy JobID is only a
                    // compatibility hint, so it never allocates or overrides the
                    // internal id of an existing inventoried instance.
                    if (auto resolved = try_legacy_add_by_identity(registry, *manager, job, req)) {
                        return *resolved;
                    }
                    if (registry.addJob(job)) {
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
                    j = legacy_remove(registry, *manager, req);
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
    // §9.1 / §9.3 inventory read surface
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

            std::vector<JobRecord> matching;
            for (auto& record : registry.inventoryRecords()) {
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
    // §9.1 / §9.2 collection control surface
    // -----------------------------------------------------------------------
    RPCServer::instance().register_method("JobCollection/set",
        [&registry, manager](const nlohmann::json& req) -> nlohmann::json {
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
                has_instance ? resolve_instance_handle(registry, instance_handle)
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

            const auto outcome = operation == "set" ? manager->setCollectors(request)
                                                    : manager->resetAuto(request);
            if (!outcome.ok) {
                return error_payload(to_string(outcome.error), outcome.message);
            }

            nlohmann::json targets = nlohmann::json::array();
            for (const auto& target : outcome.targets) {
                const auto record = registry.inventoryRecord(target.internal_id);
                if (!record) continue;
                targets.push_back(
                    collection_target_json(registry, *record, target.binding, target.changed));
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
            std::string instance_id;
            try {
                instance_id = optional_string(req, "instance_id");
            } catch (const std::exception& error) {
                return error_payload("invalid_request", error.what());
            }
            if (instance_id.empty())
                return error_payload("invalid_request", "instance_id is required");

            const auto resolution = resolve_instance_handle(registry, instance_id);
            if (!resolution.ok())
                return error_payload(resolution.code, resolution.message);

            const auto& record = resolution.records.front();
            const auto binding = registry.bindingRecord(record.internal_id);
            auto target = collection_target_json(registry, record, binding, false);

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

