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

#include "core/rpc/diagnostics_rpc.hpp"
#include "common/local_rpc.hpp"

namespace joblens::rpc {

void registerDiagnosticsRpc(JobRegistry& registry) {
    // pid tracker diagnostics are not a job resource.
    RPCServer::instance().register_method("diagnostics/pid_tracker",
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

}

}  // namespace joblens::rpc
