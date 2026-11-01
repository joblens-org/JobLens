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
// JobCollectionManager owns desired collection INTENT per inventoried
// instance (doc/job_inventory_collection_decoupling_plan.md section 6 and
// task 2).
//
// Deliberate boundaries:
//   * It never calls CollectorScheduler. Applied runtime state stays with the
//     scheduler; this component only records and revisions intent.
//   * It must not be called while holding any JobRegistry lock. All registry
//     interactions go through JobRegistry's public store API, which takes the
//     store's own locks briefly and never touches registry map locks while
//     doing I/O.
//   * Multi-target requests validate every target before one durable
//     transaction, so an invalid name or stale revision changes nothing.
#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "core/job_registry.hpp"

enum class CollectionError {
    None = 0,
    InvalidRequest,
    UnknownCollector,
    NotFound,
    StaleRevision,
    StoreUnavailable,
    // The caller tried to mutate intent from inside a gated mutation (a
    // lifecycle hook or collection applier). Nothing was committed.
    ReentrantMutation,
};

inline const char* to_string(CollectionError error)
{
    switch (error) {
        case CollectionError::None:             return "ok";
        case CollectionError::InvalidRequest:   return "invalid_request";
        case CollectionError::UnknownCollector: return "unknown_collector";
        case CollectionError::NotFound:         return "not_found";
        case CollectionError::StaleRevision:    return "stale_revision";
        case CollectionError::ReentrantMutation: return "reentrant_mutation";
        case CollectionError::StoreUnavailable: return "store_unavailable";
    }
    return "unknown";
}

// Already-resolved internal targets. Selector resolution (instance handle or
// native selector) belongs to the RPC layer; this component only accepts
// internal IDs so it can validate the exact target set before mutating.
struct CollectionRequest {
    std::vector<uint64_t> targets;
    // set_collectors: required, explicit [] means persistent stop.
    // reset_auto: must be absent.
    std::optional<std::vector<std::string>> collectors;
    // Optional binding revision guard, single-target requests only.
    std::optional<uint64_t> expected_revision;
};

struct CollectionTargetOutcome {
    uint64_t internal_id = 0;
    std::string instance_id;
    BindingRecord binding;
    // Runtime result reported by the collection applier; `applied.applied` is
    // false when no runtime exists or it rejected the intent.
    AppliedCollection applied;
    bool changed = false;
};

struct CollectionOutcome {
    bool ok = false;
    CollectionError error = CollectionError::None;
    std::string message;
    std::vector<CollectionTargetOutcome> targets;  // exact targets on success
    size_t unchanged = 0;
    size_t skipped_manual_override = 0;
    size_t skipped_invalid = 0;
};

// Automatic policy result for one instance (task 5 supplies decisions; task 2
// stores them and enforces manual precedence).
struct AutoSelection {
    uint64_t internal_id = 0;
    std::vector<std::string> collectors;
    std::string policy_revision;
};

class JobCollectionManager {
public:
    explicit JobCollectionManager(JobRegistry& registry);

    JobCollectionManager(const JobCollectionManager&) = delete;
    JobCollectionManager& operator=(const JobCollectionManager&) = delete;

    // Optional collector-name gate. When unset every non-empty name is
    // accepted; production wires this to the collector registry in task 6.
    void setCollectorValidator(std::function<bool(const std::string&)> validator);

    // operation = set: manual selection replacing any previous intent.
    // Empty list is a persistent stop.
    CollectionOutcome setCollectors(const CollectionRequest& request);
    // operation = reset_auto: clear the manual override and let the automatic
    // selection apply. `collectors` must be absent.
    CollectionOutcome resetAuto(const CollectionRequest& request);
    // Automatic policy decision for a batch of instances. Manual overrides,
    // including empty ones, are never overwritten.
    CollectionOutcome applyAutoPolicy(const std::vector<AutoSelection>& selections);

    // Desired intent (revision 0 when none recorded).
    BindingRecord binding(uint64_t internal_id) const;

private:
    CollectionOutcome mutate(const CollectionRequest& request, bool manual);
    bool validate_collectors(const std::vector<std::string>& collectors,
                             std::string& bad_name) const;
    JobRegistry& registry_;
    std::function<bool(const std::string&)> validator_;
    std::mutex mtx_;
};
