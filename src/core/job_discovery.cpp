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
#include "core/job_discovery.hpp"

#include <atomic>

#include <algorithm>
#include <chrono>
#include <utility>

namespace {
std::atomic<std::uint64_t> g_read_order{0};

void merge_members(std::vector<ProcessIdentity>& into, const std::vector<ProcessIdentity>& extra)
{
    for (const auto& member : extra) {
        const auto found = std::find_if(into.begin(), into.end(), [&member](const ProcessIdentity& existing) {
            return existing.pid == member.pid;
        });
        if (found == into.end()) into.push_back(member);
    }
}

// Disambiguation key of one local execution: two observations merge only when
// they agree on the manager identity (boot id, pid, start ticks).
std::string generation_key(const ExecutionGeneration& generation)
{
    return generation.boot_id + '|' + std::to_string(generation.manager_pid) + '|' +
           std::to_string(generation.manager_start_ticks);
}

} // namespace

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

std::int64_t now_epoch_ms()
{
    const auto since_epoch =
        std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(since_epoch).count();
}

// ---------------------------------------------------------------------------
// Source state
// ---------------------------------------------------------------------------

const char* to_string(SourceState state)
{
    switch (state) {
        case SourceState::Healthy: return "healthy";
        case SourceState::Degraded: return "degraded";
        case SourceState::Initializing: break;
    }
    return "initializing";
}

// ---------------------------------------------------------------------------
// Discovered Job readiness moved to the watchers (CondorJobWatcher /
// SlurmJobWatcher discovery_ready/finalize_discovery): "is my discovery
// finished" is scheduler-owned semantics, while the registry keeps the
// admission rule (identity resolvable + generation complete).
// ---------------------------------------------------------------------------

std::uint64_t next_discovery_read_order()
{
    return g_read_order.fetch_add(1, std::memory_order_relaxed) + 1;
}
