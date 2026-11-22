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

#include "core/rpc/collection_rpc.hpp"
#include "core/rpc/rpc_common.hpp"
#include "common/local_rpc.hpp"
#include "core/job_identity.hpp"
#include "core/scheduler_adapter.hpp"

#include <fmt/ranges.h>

namespace joblens::rpc {

void registerCollectionRpc(JobRegistry& registry) {
    auto collection_command = [&registry](const nlohmann::json& req) -> nlohmann::json {
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

            const bool has_job_id =
                selector.contains("JobID") &&
                !selector["JobID"].is_null();
            std::vector<std::string> native_fields;
            for (const char* name : {"scheduler_type", "adapter", "clusterName",
                                     "clusterTag", "JobNativeID", "step_id"}) {
                if (selector.contains(name) && !selector[name].is_null())
                    native_fields.emplace_back(name);
            }
            if (has_job_id && !native_fields.empty())
                return selector_error("selector must use exactly one form: a job "
                                      "handle (JobID) or native "
                                      "identity fields, not both");
            if (!has_job_id && native_fields.empty())
                return selector_error("selector is required: a job handle "
                                      "(JobID) or native identity "
                                      "fields");
            if (has_job_id && scope != "instance")
                return selector_error("job handle selectors only support "
                                      "scope=instance");

            NativeSelector native_selector;
            std::optional<uint64_t> job_id_handle;
            try {
                if (has_job_id) {
                    const auto raw = optional_string(selector, "JobID");
                    const auto parsed = parse_job_id(nlohmann::json(raw));
                    if (parsed.status == LegacyIdStatus::Absent ||
                        parsed.status == LegacyIdStatus::Invalid)
                        return selector_error(
                            "JobID must be a decimal job id string (got '" +
                            raw + "')");
                    job_id_handle = parsed.value;
                }
                if (!has_job_id) {
                    native_selector.clusterTag = optional_string(selector, "clusterTag");
                    native_selector.step_id = optional_string(selector, "step_id");
                    native_selector.job_native_id = optional_string(selector, "JobNativeID");
                    native_selector.clusterName = optional_string(selector, "clusterName");
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
                has_job_id
                    ? resolve_instance_handle_by_id(registry, *job_id_handle)
                    : resolve_native_target(registry, native_selector, scope,
                                            ensure_registered);
            if (!resolution.ok()) {
                if (resolution.code == "ambiguous_selector")
                    return error_payload(resolution.code, resolution.message,
                                         job_ids_of(resolution.records));
                return error_payload(resolution.code, resolution.message);
            }

            CollectionRequest request;
            request.collectors = collectors;
            request.expected_revision = expected_revision;
            for (const auto& record : resolution.records)
                request.targets.push_back(record.job.JobID);

            const auto outcome = operation == "set" ? registry.setCollectors(request)
                                                    : registry.resetAuto(request);
            if (!outcome.ok) {
                return error_payload(to_string(outcome.error), outcome.message);
            }

            nlohmann::json targets = nlohmann::json::array();
            for (const auto& target : outcome.targets) {
                const auto record = registry.jobRecord(target.job_id);
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
    };

    RPCServer::instance().register_method("jobs/collection/set", collection_command);
    RPCServer::instance().register_method("jobs/collection/reset",
        [collection_command](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json reset = req.is_object() ? req : nlohmann::json::object();
            reset["operation"] = "reset_auto";
            return collection_command(reset);
        }
    );

    RPCServer::instance().register_method("jobs/collection/get",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request", "request body must be a JSON object");
            if (auto not_ready = store_not_ready_payload(registry.storeHealth()))
                return *not_ready;
            // JobID is the addressing field: the record id as a decimal string.
            if (!req.contains("JobID") || req["JobID"].is_null())
                return error_payload("invalid_request", "JobID is required");
            const auto& job_id_node = req["JobID"];
            const std::string job_id_text = job_id_node.is_string()
                                                ? job_id_node.get<std::string>()
                                                : job_id_node.dump();
            const auto parsed = parse_job_id(job_id_node);
            if (parsed.status == LegacyIdStatus::Absent ||
                parsed.status == LegacyIdStatus::Invalid)
                return error_payload(
                    "invalid_request",
                    "JobID must be a decimal job id string (got '" +
                        job_id_text + "')");

            const auto resolution = resolve_instance_handle_by_id(registry, parsed.value);
            if (!resolution.ok())
                return error_payload(resolution.code, resolution.message);

            const auto& record = resolution.records.front();
            const auto selection = registry.selectionRecord(record.job.JobID);
            auto target = collection_target_json(registry, record, selection,
                                                 record.revision, false,
                                                 registry.actualCollection(record.job.JobID));

            nlohmann::json out;
            out["status"] = "ok";
            out["JobID"] = target["JobID"];
            out["JobNativeID"] = target["JobNativeID"];
            out["scheduler_type"] = record.job.subtype_name;
            out["clusterName"] = record.job.clusterName;
            out["clusterTag"] = record.job.clusterTag;
            out["step_id"] = step_id_text(registry, record);
            out["state"] = to_string(record.state);
            out["quarantined"] = false;
            out["mode"] = target["mode"];
            out["revision"] = target["revision"];
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

}  // namespace joblens::rpc
