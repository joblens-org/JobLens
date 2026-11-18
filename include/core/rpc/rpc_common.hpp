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
// Shared wire helpers for the resource-oriented job RPC surface.
#pragma once

#include <nlohmann/json.hpp>

#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "core/collection_runtime.hpp"
#include "core/job_registry.hpp"
#include "core/scheduler_adapter.hpp"

namespace joblens::rpc {

// The request-level namespace aliases contradicted the typed sub_attr facts.
struct NamespaceConflictError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Resolution outcome of collection selectors. Empty `code` means success.
struct TargetResolution {
    std::vector<JobRecord> records;
    std::string code;
    std::string message;

    bool ok() const { return code.empty(); }
};

void parseJobRequest(const nlohmann::json& obj, Job& job, const JobRegistry& registry);

nlohmann::json error_payload(const std::string& code, const std::string& message);
nlohmann::json error_payload(const std::string& code, const std::string& message,
                             const std::vector<std::string>& candidates);

std::string base64url_encode(const std::string& raw);
std::optional<std::string> base64url_decode(const std::string& text);

uint64_t jobs_snapshot_revision(const std::vector<JobRecord>& records);
nlohmann::json job_record_json(JobRegistry& registry, const JobRecord& record);
std::string step_id_text(const JobRegistry& registry, const JobRecord& record);
std::vector<std::string> job_ids_of(const std::vector<JobRecord>& records);
void sort_records(std::vector<JobRecord>& records);

std::string optional_string(const nlohmann::json& node, const char* key);
TargetResolution resolve_native_target(JobRegistry& registry, const NativeSelector& selector,
                                       const std::string& scope, bool ensure_registered);
TargetResolution resolve_instance_handle_by_id(JobRegistry& registry, uint64_t job_id);

nlohmann::json collection_target_json(JobRegistry& registry, const JobRecord& record,
                                      const CollectionSelection& selection,
                                      uint64_t revision, bool changed,
                                      const ActualCollectionState& actual);

std::optional<NativeSelector> legacy_add_selector(Job& job, const JobRegistry& registry);
nlohmann::json remove_jobs(JobRegistry& registry, const nlohmann::json& req, bool erase);

std::optional<nlohmann::json> store_not_ready_payload(const StoreHealth& health);
nlohmann::json joblens_status_payload(JobRegistry& registry);

}  // namespace joblens::rpc
