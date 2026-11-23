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
// Maintenance configuration contract. The scheduler identity here is the
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

// Maintenance configuration. Names listed in `auto_discovery_types` are
// validated against the adapter registry at startup; an unknown name is a
// startup error that lists the registered names. The list arms discovery
// watchers only: `settings` describes every registered adapter either way.
struct MaintenanceConfig {
    // Adapter names whose discovery source is armed
    // (job_registry_config.auto_discovery_types). Empty means "discovery off":
    // no watcher runs and no manager pass scans. Adapters themselves exist for
    // every registered scheduler regardless — this switch never removes the
    // facts/interface layer.
    std::vector<std::string> auto_discovery_types;
    // Blanket registration switch: true (the default) registers every
    // captured job unconditionally; false leaves admission to each type's
    // auto_add tri-state.
    bool capture_all = true;
    // Default 30 s; minimum 1 s (validated at startup).
    std::uint32_t reconcile_interval_sec = 30;
    // Keyed by adapter name; contains every registered adapter plus every
    // scheduler the configuration mentions, so the resolved picture is
    // complete whether or not its discovery source is enabled.
    std::map<std::string, SchedulerAdapterSettings> settings;
};

// Throws std::invalid_argument with a stable message on an invalid document.
MaintenanceConfig resolve_maintenance_config(
    const nlohmann::json& config);
