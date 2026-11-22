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
#include "core/rpc/register_all.hpp"

#include "core/rpc/collection_rpc.hpp"
#include "core/rpc/diagnostics_rpc.hpp"
#include "core/rpc/jobs_rpc.hpp"
#include "core/rpc/status_rpc.hpp"

void registerJobRpc(JobRegistry& registry) {
    joblens::rpc::registerJobsRpc(registry);
    joblens::rpc::registerCollectionRpc(registry);
    joblens::rpc::registerStatusRpc(registry);
    joblens::rpc::registerDiagnosticsRpc(registry);
}
