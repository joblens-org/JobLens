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

#include "core/rpc/status_rpc.hpp"
#include "core/rpc/rpc_common.hpp"
#include "common/local_rpc.hpp"

namespace joblens::rpc {

void registerStatusRpc(JobRegistry& registry) {
    RPCServer::instance().register_method("joblens/status",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            return joblens_status_payload(registry);
        }
    );

    RPCServer::instance().register_method("joblens/capabilities",
        [&registry](const nlohmann::json& req) -> nlohmann::json {
            (void)req;
            const auto health = registry.storeHealth();
            nlohmann::json out;
            out["status"] = "ok";
            out["api_version"] = 2;
            out["collection_control_version"] = 1;
            out["native_identity_selectors"] = true;
            out["job_scope"] = "current_local_instances";
            // Contract field name shared with the trigger Pydantic model.
            out["db_schema_version"] = health.schema_version;
            out["ready"] = health.db_running && health.migration_complete;
            return out;
        }
    );

}

}  // namespace joblens::rpc
