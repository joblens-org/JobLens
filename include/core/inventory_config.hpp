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
// Inventory configuration contract (plan section 8). Split out of the
// maintenance controller in v2 stage S4. The scheduler identity here is the
// ADAPTER REGISTRY NAME (open world, no enum): a new scheduler appears the
// moment it registers and gets a `<name>_job_watcher` config section. The
// resolver keeps the legacy auto_add_condorjob/auto_add_slurmjob folding as
// explicit built-in special cases.
#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/scheduler_adapter.hpp"

// Inventory maintenance configuration. Names listed in `types` are validated
// against the adapter registry at startup; an unknown name is a startup
// error that lists the registered names.
struct InventoryMaintenanceConfig {
    // Adapter names that opted into broad discovery. Empty means "inventory
    // off", preserving the pre-decoupling opt-in behavior.
    std::vector<std::string> types;
    // Default 30 s; minimum 1 s (validated at startup).
    std::uint32_t reconcile_interval_sec = 30;
    // Keyed by adapter name; contains every registered adapter plus every
    // type the configuration mentions, so the resolved picture is complete.
    std::map<std::string, InventoryTypeSettings> settings;
};

// Throws std::invalid_argument with a stable message on an invalid document.
InventoryMaintenanceConfig resolve_inventory_maintenance_config(
    const nlohmann::json& config);
