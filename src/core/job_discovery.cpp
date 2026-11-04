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

std::string join_reasons(const std::vector<std::string>& reasons)
{
    std::string joined;
    for (const auto& reason : reasons) {
        if (!joined.empty()) joined += "; ";
        joined += reason;
    }
    return joined;
}

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
// Discovered Job finalization
// ---------------------------------------------------------------------------

bool discovery_job_ready(const Job& job)
{
    if (!job.execution || !job.execution->generation.complete()) return false;
    // A finalized discovery records every missing piece (identity, generation
    // AND source reasons — e.g. an unresolved Slurm step) in
    // incomplete_reason; any of them disqualifies admission, exactly like the
    // old Observation.identity_complete two-level rule. Jobs that never went
    // through finalize_discovery_job still get the honest recomputation.
    if (!job.execution->incomplete_reason.empty()) return false;
    return canonicalize(selector_of(job)).has_value();
}

void finalize_discovery_job(Job& job, std::vector<std::string> source_reasons)
{
    std::vector<std::string> reasons = std::move(source_reasons);

    if (!canonicalize(selector_of(job))) {
        reasons.push_back(
            "logical identity unresolved: scheduler type, cluster namespace or native job id missing");
    }

    if (!job.execution || !job.execution->generation.complete()) {
        reasons.push_back(
            "execution generation unresolved: manager pid, manager start ticks or boot id missing");
    }

    ensure_execution(job).incomplete_reason = join_reasons(reasons);
}

std::uint64_t next_discovery_read_order()
{
    return g_read_order.fetch_add(1, std::memory_order_relaxed) + 1;
}
