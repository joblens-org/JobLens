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
// Process-wide seam between the inventory maintenance controller (Task 3) and
// the RPC layer that serves `JobRegistry/inventory_status` (Task 6).
//
// The maintenance controller installs its provider on start() and clears it on
// stop(); the RPC handler combines the returned object with the persistent
// store health it reads itself. This header is deliberately header-only in
// contract and definition-stable: the RPC task compiles against these two
// declarations, and the only implementation lives in
// src/core/job_inventory_maintenance.cpp.
//
// Threading: both accessors are safe from any thread. The provider may be
// invoked while maintenance is running; it returns an empty function once
// maintenance has stopped, so a caller must re-read the provider per request.
#pragma once

#include <functional>

#include <nlohmann/json.hpp>

// Replaces the current provider (empty function clears it).
void set_inventory_status_provider(std::function<nlohmann::json()> provider);

// Returns the currently installed provider, or an empty function when
// inventory maintenance is not running.
std::function<nlohmann::json()> get_inventory_status_provider();
