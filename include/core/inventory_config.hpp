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
// maintenance controller in v2 stage S4; the resolver stays Condor/Slurm
// compatible (including the legacy auto_add_* folding) and is generalized to
// adapter names by the registry wiring.
#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/scheduler_adapter.hpp"

enum class InventoryType {
    Condor = 0,
    Slurm,
};

const char* to_string(InventoryType type);
std::optional<InventoryType> inventory_type_from_string(const std::string& raw);
// Adapter-registry names for the built-in types.
inline const char* adapter_name_of(InventoryType type)
{
    return type == InventoryType::Condor ? "condor" : "slurm";
}

struct InventoryMaintenanceConfig {
    // Types that opted into broad discovery. Empty means "inventory off",
    // preserving the pre-decoupling opt-in behavior.
    std::vector<InventoryType> types;
    // Default 30 s; minimum 1 s (validated at startup).
    std::uint32_t reconcile_interval_sec = 30;
    // Always contains both types so the resolved picture is complete.
    std::map<InventoryType, InventoryTypeSettings> settings;

    nlohmann::json to_json() const;
};

// Throws std::invalid_argument with a stable message on an invalid document.
InventoryMaintenanceConfig resolve_inventory_maintenance_config(
    const nlohmann::json& config);
