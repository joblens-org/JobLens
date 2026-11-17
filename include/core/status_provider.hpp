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
// Process-wide seam between the JobRegistry heartbeat and the RPC layer that
// serves `joblens/status`.
//
// The heartbeat installs its provider on start and clears it on stop; the RPC
// handler combines the returned object with the persistent store health it
// reads itself. This header is deliberately header-only in contract and
// definition-stable: the only implementation lives in
// src/core/status_provider.cpp.
//
// Threading: both accessors are safe from any thread. The provider may be
// invoked while the heartbeat is running; it returns an empty function once
// the heartbeat has stopped, so a caller must re-read the provider per
// request.
#pragma once

#include <functional>

#include <nlohmann/json.hpp>

// Replaces the current provider (empty function clears it).
void set_status_provider(std::function<nlohmann::json()> provider);

// Returns the currently installed provider, or an empty function when
// registry maintenance is not running.
std::function<nlohmann::json()> get_status_provider();

// Optional: one authoritative discovery pass on demand (`ensure_registered`).
// The registry registers a closure that
// runs and waits for a pass; the RPC layer calls it before resolving a native
// selector that does not match yet. Returns false when no controller is
// running, so the caller keeps its explicit "not found" answer instead of
// inventing a record.
void set_refresh_provider(std::function<bool()> provider);
std::function<bool()> get_refresh_provider();
