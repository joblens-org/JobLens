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
// Shared helpers for the resource-oriented job RPC surface.

#include "core/rpc/rpc_common.hpp"

#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <set>
#include <type_traits>

#include "common/config.hpp"
#include "common/local_rpc.hpp"
#include "core/collector_registry.hpp"
#include "core/job_identity.hpp"
#include "core/maintenance_config.hpp"
#include "core/scheduler_adapter.hpp"
#include "core/status_provider.hpp"

#define JOBLENS_HAS_STATUS_PROVIDER 1

namespace joblens::rpc {

std::pair<std::string, std::string>
split_two(std::string s, char sep = '.') {
    auto p = s.find(sep);
    return (p == s.npos)
           ? std::pair{s, ""}
           : std::pair{s.substr(0, p), s.substr(p + 1)};
}

// The request-level namespace aliases (clusterName/clusterTag) contradicted
// the typed sub_attr facts: an explicit rejection, distinct from a malformed
// request so the RPC surface can carry the documented error code.
void parseJobRequest(const nlohmann::json& obj, Job& job, const JobRegistry& registry) {
    // Internal IDs parse explicitly as uint64: a JSON number or a decimal
    // string, never a 32-bit default. Absent keeps the legacy auto-allocate
    // sentinel; reserved (0) keeps its historical meaning; anything else is
    // an explicit request error.
    const auto parsed_id = parse_job_id(
        obj.contains("JobID") ? obj["JobID"] : nlohmann::json(nullptr));
    if (parsed_id.status == LegacyIdStatus::Invalid) {
        throw std::runtime_error(
            "invalid JobID: expected a uint64 number or decimal string");
    }
    job.JobID = parsed_id.value;
    set_pids(job, obj.value("JobPIDs", nlohmann::json::array()).get<std::vector<pid_t>>());
    job.CollectorNames = obj.value("Lens", nlohmann::json::array()).get<std::vector<std::string>>();
    if (pids_of(job).size() == 0) {
        spdlog::warn("JobRegistry: job ID {} has empty PID list", job.JobID);
    }
    for (auto pid: pids_of(job)) {
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
    
    const ISchedulerAdapter* adapter = nullptr;
    if (subtype.compare("common") == 0){
        job.subtype = JobSubType::Common;
        job.sub_attr = CommonJobAttr{
            .auto_update_child = obj.value("auto_update_child", true)
        };
    }else{
        const std::string adapter_name = normalize_adapter_name(subtype);
        if (adapter_name.empty())
            throw std::runtime_error("invalid job subtype: " + subtype);
        adapter = registry.adapterNamed(adapter_name);
        if (adapter != nullptr) {
            // The adapter owns the request -> typed facts projection (and the
            // native id / namespace render that follows it).
            job.subtype_name = adapter->name();
            adapter->applyRequestFacts(job, obj);
        } else {
            // 开放世界：任何其它 subtype 名字即 adapter 名，typed facts 以 JSON
            // 原样入库（该 adapter 的 decodeFacts 是唯一解释者）。
            job.subtype_name = adapter_name;
            job.sub_attr = obj.value("sub_attr", nlohmann::json::object());
            job.JobNativeID = obj.value("JobNativeID", "");
        }
    }

    // Structured namespace, by business type: typed schedulers inject the
    // request-level aliases (clusterName/clusterTag) into their typed
    // facts — the facts win and a contradiction is an explicit rejection —
    // then render the common fields from the facts. Generic/open-world jobs
    // keep the common fields as their namespace storage; the aliases land
    // there verbatim.
    const std::string clusterName = obj.value("clusterName", std::string{});
    const std::string clusterTag = obj.value("clusterTag", std::string{});
    if (adapter != nullptr) {
        if (!adapter->adoptNamespaceAliases(job, clusterName, clusterTag)) {
            throw NamespaceConflictError(
                "clusterName/clusterTag contradict the sub_attr facts");
        }
        adapter->projectFacts(job);
    } else {
        job.clusterName = clusterName;
        job.clusterTag = clusterTag;
    }

    spdlog::info("JobRegistry: parsed type={}, subtype={}, JobID={}, "
                 "PIDs_count={}, Lens_count={}, nativeID={}",
                 type, subtype, job.JobID, pids_of(job).size(),
                 job.CollectorNames.size(), job.JobNativeID);
}


constexpr uint32_t kRpcSchemaVersion = 2;

// ---------------------------------------------------------------------------
// Local serialization helpers (the JobRecord wire form lives in job_store;
// the RPC only needs manager/process projections for job views)
// ---------------------------------------------------------------------------

nlohmann::json manager_to_json(const ProcessIdentity& manager) {
    return nlohmann::json{
        {"boot_id", manager.boot_id},
        {"manager_pid", manager.pid},
        {"manager_start_ticks", manager.start_ticks},
    };
}

nlohmann::json pid_identity_to_json(const ProcessIdentity& pid_identity) {
    return nlohmann::json{
        {"pid", pid_identity.pid},
        {"start_ticks", pid_identity.start_ticks},
        {"boot_id", pid_identity.boot_id},
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
uint64_t jobs_snapshot_revision(const std::vector<JobRecord>& records)
{
    std::vector<const JobRecord*> ordered;
    ordered.reserve(records.size());
    for (const auto& record : records) ordered.push_back(&record);
    std::sort(ordered.begin(), ordered.end(),
              [](const JobRecord* lhs, const JobRecord* rhs) {
                  return lhs->job.JobID < rhs->job.JobID;
              });

    uint64_t hash = 0xcbf29ce484222325ULL;
    auto mix = [&hash](uint64_t value) {
        for (int byte = 0; byte < 8; ++byte) {
            hash ^= (value >> (byte * 8)) & 0xffULL;
            hash *= 0x100000001b3ULL;
        }
    };
    for (const auto* record : ordered) {
        mix(record->job.JobID);
        mix(record->revision);
        mix(record->index_key.empty() ? 0u : 2u);
        mix(record->state == RecordState::Closing ? 3u : (record->presumed_live ? 5u : 4u));
    }
    // Stay inside the signed 63-bit range so JavaScript consumers keep the
    // exact value; the revision is an opaque token, not a counter.
    return hash & 0x7fffffffffffffffULL;
}

std::string job_source(const JobRecord& record)
{
    return record.job.subtype_name.empty() ? "manual" : record.job.subtype_name;
}

// Step claim as the RPC surface reports it: the value or empty, never a
// bare nullopt (the optional itself is store-internal, 0-vs-missing rule).
std::string step_id_text(const JobRegistry& registry, const JobRecord& record)
{
    // The step is a scheduler fact: the adapter owning the facts rebuilds it.
    if (const auto* adapter = registry.adapterNamed(record.job.subtype_name))
        return adapter->legacySelector(record.job).step_id;
    return {};
}

nlohmann::json job_record_json(JobRegistry& registry, const JobRecord& record)
{
    nlohmann::json out;
    // The one job handle the RPC surface emits: the record id as a decimal
    // string (JavaScript uint64 safety).
    out["JobID"] = format_job_id(record.job.JobID);
    out["schema_version"] = kRpcSchemaVersion;
    out["revision"] = record.revision;
    out["state"] = to_string(record.state);
    out["source"] = job_source(record);
    out["quarantined"] = false;
    out["identity_complete"] = !record.index_key.empty();
    // Desired intent is part of the job resource; applied state belongs to the
    // collection sub-resource and is not persisted here.
    out["mode"] = to_string(record.selection.mode());
    out["desired_collectors"] = record.selection.effectiveCollectors();
    // Selector fields are projections of the Job: the typed facts are the
    // single source of truth, no second copy rides the record.
    out["scheduler_type"] = record.job.subtype_name;
    out["clusterName"] = record.job.clusterName;
    out["clusterTag"] = record.job.clusterTag;
    out["JobNativeID"] = record.job.JobNativeID;
    out["step_id"] = step_id_text(registry, record);
    out["first_seen_us"] = record.first_seen_us;
    out["last_seen_us"] = record.last_seen_us;
    out["manager"] = manager_to_json(manager_of(record.job));
    out["pid_identities"] = nlohmann::json::array();
    for (const auto& pid_identity : pid_identities_of(record.job)) {
        out["pid_identities"].push_back(pid_identity_to_json(pid_identity));
    }
    if (const auto job = registry.findJob(record.job.JobID)) {
        out["attributes"] = std::visit(
            [](const auto& attr) { return nlohmann::json(attr); }, job->sub_attr);
    } else {
        out["attributes"] = nullptr;
    }
    return out;
}

std::vector<std::string> job_ids_of(const std::vector<JobRecord>& records)
{
    std::vector<std::string> ids;
    ids.reserve(records.size());
    // The job handle is the record id as a decimal string.
    for (const auto& record : records) ids.push_back(format_job_id(record.job.JobID));
    return ids;
}

void sort_records(std::vector<JobRecord>& records)
{
    std::sort(records.begin(), records.end(),
              [](const JobRecord& lhs, const JobRecord& rhs) {
                  return lhs.job.JobID < rhs.job.JobID;
              });
}

std::string optional_string(const nlohmann::json& node, const char* key)
{
    if (!node.is_object() || !node.contains(key) || node[key].is_null()) return {};
    const auto& value = node[key];
    if (value.is_string()) return value.get<std::string>();
    if (value.is_number_integer() || value.is_number_unsigned()) return value.dump();
    throw std::runtime_error(std::string(key) + " must be a string");
}

// One on-demand authoritative pass through the registry heartbeat. Returns
// false when no controller is running, so callers keep an explicit "not found"
// instead of inventing a record. Never allocates ids or guesses an identity.
bool refresh_jobs_once()
{
#ifdef JOBLENS_HAS_STATUS_PROVIDER
    auto provider = get_refresh_provider();
    if (!provider) return false;
    try {
        return provider();
    } catch (const std::exception& error) {
        spdlog::warn("JobRegistry: on-demand job refresh failed: {}", error.what());
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
            "incomplete native selector: scheduler_type, clusterName and JobNativeID "
            "must name one scheduler identity";
        result.message += kLegacyNativeSelectorFormatHint;
        return result;
    }

    std::vector<JobRecord> matches;
    // The selector's namespace facts vs the row's typed facts, compared in
    // the adapter-owned structured form (never by re-parsing the combined
    // canonical namespace).
    const auto selector_facts = registry.selectorFactsOf(selector);
    for (auto& record : registry.jobRecords()) {
        if (is_reserved_job_id(record.job.JobID)) continue;
        if (record.index_key.empty()) continue;
        const auto* adapter = registry.adapterNamed(record.job.subtype_name);
        if (adapter == nullptr) continue;
        const auto record_key = adapter->keyOf(record.job);
        if (!record_key) continue;
        if (record_key->adapter != identity->adapter) continue;
        if (record_key->canonical_native != identity->canonical_native) continue;
        if (!namespace_facts_compatible(selector_facts, registry.namespaceFactsOf(record.job)))
            continue;
        if (!identity->step_id.empty() && record_key->step_id != identity->step_id) continue;
        matches.push_back(std::move(record));
    }
    sort_records(matches);

    if (matches.empty()) {
        if (ensure_registered) {
            // Refresh discovery before declaring the target missing.
            // Only a real source pass may create the record; no caller-supplied
            // PID or native number authorizes an invented live instance.
            if (refresh_jobs_once()) {
                return resolve_native_target(registry, selector, scope,
                                             /*ensure_registered=*/false);
            }
            result.message =
                "no discovery source is running on this core, so ensure_registered "
                "cannot confirm a live instance for this request";
        } else {
            result.message = "no active instance for native job '" +
                             selector.job_native_id + "' in cluster '" +
                             selector.clusterName + "'";
        }
        result.code = "not_found";
        return result;
    }

    if (scope == "instance" && matches.size() > 1) {
        result.code = "ambiguous_selector";
        result.message = fmt::format(
            "native job '{}' matches {} current local instances in cluster '{}'; "
            "add step_id or use scope=job", selector.job_native_id, matches.size(),
            selector.clusterName);
        result.records = std::move(matches);
        return result;
    }

    for (const auto& record : matches) {
        if (!registry.findJob(record.job.JobID).has_value()) {
            result.code = "instance_closing";
            result.message = "instance " + format_job_id(record.job.JobID) +
                             " is closing and rejects new binding mutations";
            result.records.clear();
            return result;
        }
    }
    result.records = std::move(matches);
    return result;
}

// Instance addressing by JobID: the durable id resolves in O(1)
// through the registry's record map. Addressability: an active record only
// (findJob), a closing instance rejects with instance_closing, and the
// reserved system sentinel is never addressable.
TargetResolution resolve_instance_handle_by_id(JobRegistry& registry, uint64_t job_id)
{
    TargetResolution result;
    auto record = is_reserved_job_id(job_id)
                      ? std::nullopt
                      : registry.jobRecord(job_id);
    if (!record) {
        result.code = "not_found";
        result.message = "no active instance with JobID '" +
                         format_job_id(job_id) + "'";
        return result;
    }
    if (!registry.findJob(record->job.JobID).has_value()) {
        result.code = "instance_closing";
        result.message = "instance " + format_job_id(record->job.JobID) +
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
    // The one job handle this surface emits: the record id as a decimal string.
    out["JobID"] = format_job_id(record.job.JobID);
    out["JobNativeID"] = record.job.JobNativeID;
    // Accepted: the durable record revision the command committed.
    out["revision"] = revision;
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
// adapter-owned projection from the typed facts, plus the local scheduler
// configuration the discovery sources use. The adapter also folds the derived
// fields back onto the Job (fill-empty only).
std::optional<NativeSelector> legacy_add_selector(Job& job, const JobRegistry& registry)
{
    const std::string name = job.subtype_name.empty()
                                 ? std::string(adapter_name_for(job.subtype) ? adapter_name_for(job.subtype) : "")
                                 : job.subtype_name;
    const ISchedulerAdapter* adapter = registry.adapterNamed(name);
    if (adapter == nullptr) {
        // Generic/manual registration has no scheduler identity to resolve.
        return std::nullopt;
    }
    NativeSelector selector = adapter->registrationSelector(job);
    if (selector.clusterName.empty() || selector.job_native_id.empty()) return std::nullopt;
    return selector;
}

SubAttrMatcher parse_legacy_matcher(const nlohmann::json& matcher_json)
{
    SubAttrMatcher matcher;
    if (matcher_json.contains("subtype")) {
        const auto subtype = matcher_json["subtype"].get<std::string>();
        if (const auto legacy = legacy_subtype_named(subtype)) matcher.subtype = *legacy;
        else matcher.subtype_name = normalize_adapter_name(subtype);
    }
    if (matcher_json.contains("facts_fields") && matcher_json["facts_fields"].is_object())
        matcher.json_fields = matcher_json["facts_fields"];
    matcher.addLegacyFields(matcher_json);
    return matcher;
}

// Stop collection and keep the job record, or erase/remove the target for
// real. The two resource commands share target resolution but never the same
// destructive semantics.
nlohmann::json remove_jobs(JobRegistry& registry, const nlohmann::json& req, bool erase)
{
    std::vector<uint64_t> targets;
    if (req.contains("JobID") && !req["JobID"].is_null()) {
        const auto parsed = parse_job_id(req["JobID"]);
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
        // The erase tier: a real removal. The closing fact is durable
        // before anything destructive starts; collected instances end as a
        // history row, never-collected ones as a durable delete.
        std::vector<std::string> erased_instances;
        size_t erased = 0;
        for (const auto id : targets) {
            if (id == kSystemSentinelJobId) continue;
            if (!registry.removeJob(id)) {
                return error_payload(
                    "remove_failed",
                    fmt::format("could not remove instance {}; the record stays "
                                "active and the command is retryable",
                                format_job_id(id)));
            }
            ++erased;
            erased_instances.push_back(format_job_id(id));
        }
        nlohmann::json out;
        out["status"] = "success";
        out["mode"] = "erase";
        out["deleted_count"] = erased;
        out["erased_instances"] = std::move(erased_instances);
        out["record_retained"] = false;
        out["msg"] = "erase removed the instances (collected ones archived, "
                     "never-collected ones deleted)";
        return out;
    }

    std::vector<std::string> stopped_instances;
    nlohmann::json apply_errors = nlohmann::json::array();
    size_t stopped = 0;
    size_t removed = 0;
    for (const auto id : targets) {
        if (id == kSystemSentinelJobId) continue;
        if (!registry.jobRecord(id).has_value()) {
            if (registry.removeJob(id)) ++removed;
            continue;
        }
        if (!registry.findJob(id).has_value()) {
            // Already draining/removed: stopping again is idempotent.
            ++stopped;
            continue;
        }
        CollectionRequest stop;
        stop.targets = {id};
        stop.collectors = std::vector<std::string>{};
        const auto outcome = registry.setCollectors(stop);
        if (!outcome.ok) {
            spdlog::error("JobRegistry: stop could not stop collection for JobID {}: {}",
                          id, outcome.message);
            return error_payload(to_string(outcome.error), outcome.message);
        }
        ++stopped;
        stopped_instances.push_back(format_job_id(outcome.targets.front().job_id));
        const auto& target = outcome.targets.front();
        if (!target.actual.error.empty()) {
            apply_errors.push_back(nlohmann::json{
                {"JobID", format_job_id(target.job_id)},
                {"code", "apply_failed"},
                {"message", target.actual.error},
            });
        }
    }

    if (!targets.empty()) {
        spdlog::info("JobRegistry: stopped collection for {} instance(s); "
                     "job records are retained",
                     stopped);
    }

    nlohmann::json out;
    out["status"] = "success";
    out["mode"] = "stop";
    out["deleted_count"] = removed;
    out["stopped_count"] = stopped;
    out["stopped_instances"] = stopped_instances;
    out["apply_errors"] = std::move(apply_errors);
    out["record_retained"] = true;
    out["msg"] = "collection stopped; job records are retained";
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
nlohmann::json current_status_provider()
{
#ifdef JOBLENS_HAS_STATUS_PROVIDER
    auto provider = get_status_provider();
    return provider_payload(provider);
#else
    return nlohmann::json();
#endif
}

// Readiness gate for the job/control surface. A rejected schema or a
// closed store must not look like "no jobs" or "no such instance".
std::optional<nlohmann::json> store_not_ready_payload(const StoreHealth& health)
{
    if (health.db_running && health.migration_complete) return std::nullopt;
    if (!health.db_running) {
        if (health.migration_state == "malformed_schema_marker") {
            return error_payload("store_unavailable",
                                 "job store has a schema marker this build did not "
                                 "write; the database was left untouched (no rewrite, no "
                                 "deletion)");
        }
        if (health.migration_state == "schema_marker_unreadable") {
            return error_payload("store_unavailable",
                                 "job store schema marker could not be read (I/O); "
                                 "the database was left untouched");
        }
        if (health.migration_state == "legacy_watermark_unreadable") {
            return error_payload("store_unavailable",
                                 "the quarantined legacy store (<db>.legacy) cannot be "
                                 "read for the allocator watermark; resolve or remove "
                                 "it once its job ids are accounted for");
        }
        if (health.migration_state == "quarantine_reopen_failed") {
            return error_payload("store_unavailable",
                                 "the legacy store was quarantined but the fresh store "
                                 "could not be opened; check the JobLens data directory");
        }
        if (health.migration_state.rfind("unsupported_schema", 0) == 0) {
            return error_payload("store_unavailable",
                                 "job store schema is not supported by this build; "
                                 "it was left untouched (no migration, no deletion): " +
                                     health.migration_state);
        }
        return error_payload("store_unavailable",
                             "job store is not running on this core");
    }
    return error_payload("store_unavailable",
                         "job store is not ready (" +
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

nlohmann::json joblens_status_payload(JobRegistry& registry)
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
    body["enabled"] = health.db_running;
    body["sources"] = nlohmann::json{{"state", "not_configured"}};

    uint64_t active = 0;
    uint64_t suspect = 0;
    uint64_t closing = 0;
    for (const auto& record : registry.jobRecords()) {
        if (is_reserved_job_id(record.job.JobID)) continue;
        if (!record.presumed_live) ++suspect;
        else if (record.state == RecordState::Closing) ++closing;
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
    const auto provider = current_status_provider();
    if (provider.is_object()) body.update(provider);
    body["store"] = store_health_json(health);
    body["persistence"] = body["store"]["persistence"];
    body["db_schema_version"] = health.schema_version;
    return body;
}

}  // namespace joblens::rpc
