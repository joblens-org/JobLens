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

#include "core/rpc/jobs_rpc.hpp"
#include "core/rpc/rpc_common.hpp"
#include "common/local_rpc.hpp"
#include "core/scheduler_adapter.hpp"

#include <fmt/ranges.h>

namespace joblens::rpc {

void registerJobsRpc(JobRegistry& registry) {
    RPCServer::instance().register_method("jobs/upsert",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            try {
                if (!req.is_object())
                    return error_payload("invalid_request",
                                         "request body must be a JSON object");
                Job job;
                parseJobRequest(req, job, registry);
                j = registry.addOrUpdateManualJob(job, req, legacy_add_selector(job, registry));
            } catch (const NamespaceConflictError& e) {
                j["status"] = "error";
                j["code"] = "namespace_conflict";
                j["msg"] = fmt::format("failed to upsert job: {}", e.what());
            } catch (const std::exception& e) {
                j["status"] = "error";
                j["code"] = "invalid_request";
                j["msg"] = fmt::format("failed to upsert job: {}", e.what());
            }
            return j;
        }
    );

    // Strict create: an existing identity is a conflict, never an update.
    RPCServer::instance().register_method("jobs/create",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            nlohmann::json j;
            try {
                if (!req.is_object())
                    return error_payload("invalid_request",
                                         "request body must be a JSON object");
                Job job;
                parseJobRequest(req, job, registry);
                j = registry.addOrUpdateManualJob(job, req, legacy_add_selector(job, registry),
                                                  /*create_only=*/true);
            } catch (const NamespaceConflictError& e) {
                j["status"] = "error";
                j["code"] = "namespace_conflict";
                j["msg"] = fmt::format("failed to create job: {}", e.what());
            } catch (const std::exception& e) {
                j["status"] = "error";
                j["code"] = "invalid_request";
                j["msg"] = fmt::format("failed to create job: {}", e.what());
            }
            return j;
        }
    );

    // Stop collection, retain the job record.
    RPCServer::instance().register_method("jobs/stop",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request",
                                     "request body must be a JSON object");
            return remove_jobs(registry, req, /*erase=*/false);
        }
    );

    // Delete/retire the job record for real.
    RPCServer::instance().register_method("jobs/erase",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request",
                                     "request body must be a JSON object");
            return remove_jobs(registry, req, /*erase=*/true);
        }
    );

    RPCServer::instance().register_method("jobs/get",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            if (!req.is_object())
                return error_payload("invalid_request",
                                     "request body must be a JSON object");
            if (!req.contains("JobID") || req["JobID"].is_null())
                return error_payload("invalid_request", "JobID is required");
            const auto& job_id_node = req["JobID"];
            const std::string job_id_text = job_id_node.is_string()
                                                ? job_id_node.get<std::string>()
                                                : job_id_node.dump();
            const auto parsed = parse_job_id(job_id_node);
            const auto record = parsed.status == LegacyIdStatus::Valid
                                    ? registry.jobRecord(parsed.value)
                                    : std::nullopt;
            if (!record)
                return error_payload(
                    "not_found",
                    "no current job record with JobID '" + job_id_text + "'");
            auto out = job_record_json(registry, *record);
            out["status"] = "ok";
            return out;
        }
    );

    RPCServer::instance().register_method("jobs/count",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            uint64_t count = 0;
            for (const auto& record : registry.jobRecords()) {
                if (is_reserved_job_id(record.job.JobID)) continue;
                if (record.state == RecordState::Active) ++count;
            }
            return nlohmann::json{{"status", "ok"}, {"job_count", count}};
        }
    );

    RPCServer::instance().register_method("jobs/list",
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
            std::string job_native_filter;
            if (params.contains("JobNativeID") && !params["JobNativeID"].is_null()) {
                if (!params["JobNativeID"].is_string())
                    return error_payload("invalid_request", "JobNativeID must be a string");
                job_native_filter = params["JobNativeID"].get<std::string>();
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
            for (auto& record : registry.jobRecords()) {
                if (is_reserved_job_id(record.job.JobID)) continue;
                if (!scheduler_filter.empty() &&
                    record.job.subtype_name != scheduler_filter)
                    continue;
                if (!job_native_filter.empty() &&
                    record.job.JobNativeID != job_native_filter)
                    continue;
                if (!state_filter.empty() &&
                    (!record_state_from_string(state_filter) ||
                     *record_state_from_string(state_filter) != record.state))
                    continue;
                matching.push_back(std::move(record));
            }
            sort_records(matching);

            const uint64_t snapshot_revision = jobs_snapshot_revision(matching);
            const std::string filter_fingerprint =
                scheduler_filter + "|" + job_native_filter + "|" + state_filter;

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
                        fmt::format("job snapshot {} no longer matches cursor snapshot {}; "
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
                if (record.job.JobID <= after) continue;
                if (emitted == limit) {
                    more = true;
                    break;
                }
                records.push_back(job_record_json(registry, record));
                last_emitted = record.job.JobID;
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
}

}  // namespace joblens::rpc
