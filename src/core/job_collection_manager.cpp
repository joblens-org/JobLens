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
#include "core/job_collection_manager.hpp"
#include "core/reentrancy_gate.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <unordered_set>

namespace {

std::vector<std::string> normalized_names(std::vector<std::string> names)
{
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    return names;
}

std::vector<uint64_t> deduplicated(std::vector<uint64_t> ids)
{
    std::vector<uint64_t> out;
    std::unordered_set<uint64_t> seen;
    out.reserve(ids.size());
    for (const auto id : ids) {
        if (seen.insert(id).second) out.push_back(id);
    }
    return out;
}

} // namespace

JobCollectionManager::JobCollectionManager(JobRegistry& registry) : registry_(registry) {}

void JobCollectionManager::setCollectorValidator(
    std::function<bool(const std::string&)> validator) {
    std::lock_guard<std::mutex> lock(mtx_);
    validator_ = std::move(validator);
}

BindingRecord JobCollectionManager::binding(uint64_t internal_id) const {
    return registry_.bindingRecord(internal_id);
}

bool JobCollectionManager::validate_collectors(
    const std::vector<std::string>& collectors, std::string& bad_name) const {
    for (const auto& name : collectors) {
        if (name.empty()) {
            bad_name = "<empty>";
            return false;
        }
        if (validator_ && !validator_(name)) {
            bad_name = name;
            return false;
        }
    }
    return true;
}

namespace {
// Refuses a mutation issued from inside a gated mutation BEFORE any lock or
// commit: the intent must not move when the caller already owns the ordering.
CollectionOutcome reentrant_refusal(const char* what)
{
    CollectionOutcome outcome;
    outcome.ok = false;
    outcome.error = CollectionError::ReentrantMutation;
    outcome.message = std::string(what) +
        " was requested from inside a collection mutation; refused without "
        "committing anything";
    spdlog::warn("JobCollectionManager: reentrant {} refused", what);
    return outcome;
}
}  // namespace

CollectionOutcome JobCollectionManager::setCollectors(const CollectionRequest& request) {
    if (joblens::in_mutation_gate()) return reentrant_refusal("setCollectors");
    return mutate(request, /*manual=*/true);
}

CollectionOutcome JobCollectionManager::resetAuto(const CollectionRequest& request) {
    if (joblens::in_mutation_gate()) return reentrant_refusal("resetAuto");
    return mutate(request, /*manual=*/false);
}

CollectionOutcome JobCollectionManager::mutate(const CollectionRequest& request,
                                              bool manual) {
    CollectionOutcome outcome;
    // Control admission: serializes read-validate-commit so two control
    // requests cannot interleave between the revision check and the write.
    // Registry locks are never held here; only its public store API is used.
    std::lock_guard<std::mutex> admission(mtx_);

    if (request.targets.empty()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "at least one resolved target is required";
        return outcome;
    }
    if (manual && !request.collectors.has_value()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "set requires an explicit collectors list ([] stops collection)";
        return outcome;
    }
    if (!manual && request.collectors.has_value()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "reset_auto does not accept a collectors list";
        return outcome;
    }
    if (request.expected_revision.has_value() && request.targets.size() != 1) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "expected_revision is only valid for a single-instance request";
        return outcome;
    }

    const std::vector<uint64_t> targets = deduplicated(request.targets);
    const std::vector<std::string> names =
        manual ? normalized_names(*request.collectors) : std::vector<std::string>{};

    if (manual) {
        std::string bad_name;
        if (!validate_collectors(names, bad_name)) {
            outcome.error = CollectionError::UnknownCollector;
            outcome.message = "unknown collector name: " + bad_name;
            return outcome;
        }
    }

    // Phase 1: validate every target before any mutation.
    std::vector<CollectionTargetOutcome> planned;
    std::vector<BindingRecord> pending;
    planned.reserve(targets.size());

    for (const auto id : targets) {
        if (!registry_.hasInstance(id)) {
            outcome.error = CollectionError::NotFound;
            outcome.message = "no verified inventory instance with internal id " +
                              std::to_string(id);
            return outcome;
        }

        BindingRecord current = registry_.bindingRecord(id);
        if (request.expected_revision.has_value() &&
            current.revision != *request.expected_revision) {
            outcome.error = CollectionError::StaleRevision;
            outcome.message = "binding revision for instance " + std::to_string(id) +
                              " is " + std::to_string(current.revision) + ", expected " +
                              std::to_string(*request.expected_revision);
            return outcome;
        }

        CollectionTargetOutcome target;
        target.internal_id = id;
        target.instance_id = registry_.instanceId(id);

        bool changed = false;
        BindingRecord next = current;
        next.internal_id = id;
        if (manual) {
            if (current.mode != CollectionMode::Manual ||
                current.manual_collectors != names) {
                next.mode = CollectionMode::Manual;
                next.manual_collectors = names;
                next.revision = current.revision + 1;
                changed = true;
            }
        } else {
            if (current.mode != CollectionMode::Auto) {
                next.mode = CollectionMode::Auto;
                next.manual_collectors.clear();
                next.revision = current.revision + 1;
                changed = true;
            }
        }

        target.binding = changed ? next : current;
        target.changed = changed;
        planned.push_back(std::move(target));
        if (changed) {
            pending.push_back(std::move(next));
        } else {
            ++outcome.unchanged;
        }
    }

    // Phase 2: one durable transaction for every changed target. Identical
    // repeats perform no write at all.
    if (!pending.empty()) {
        std::string error;
        if (!registry_.commitBindings(pending, error)) {
            if (error == "not_found") {
                outcome.error = CollectionError::NotFound;
            } else if (error == "instance_closing") {
                outcome.error = CollectionError::StaleRevision;
            } else {
                outcome.error = CollectionError::StoreUnavailable;
            }
            outcome.message = "binding transaction rejected: " + error;
            return outcome;
        }
    }

    // Phase 3: convergence. Whether an application should run depends on the
    // runtime state, not on whether the desired list changed: a repeat of a
    // request whose first application failed (or was lost to a crash) must
    // retry the runtime instead of reporting "unchanged" against a broken
    // state. Targets the commit above already applied are not retried here —
    // the retry belongs to the next identical request or the next maintenance
    // pass, not to a double call inside one request.
    const bool commit_applied = !pending.empty();
    for (auto& target : planned) {
        if (!(commit_applied && target.changed))
            registry_.reconcileCollectionIntent(target.internal_id);
        target.applied = registry_.appliedCollection(target.internal_id);
    }

    outcome.ok = true;
    outcome.targets = std::move(planned);
    return outcome;
}

CollectionOutcome JobCollectionManager::applyAutoPolicy(
    const std::vector<AutoSelection>& selections) {
    if (joblens::in_mutation_gate()) return reentrant_refusal("applyAutoPolicy");
    CollectionOutcome outcome;
    std::lock_guard<std::mutex> admission(mtx_);

    if (selections.empty()) {
        outcome.error = CollectionError::InvalidRequest;
        outcome.message = "at least one automatic decision is required";
        return outcome;
    }

    std::unordered_set<uint64_t> seen;
    std::vector<CollectionTargetOutcome> planned;
    std::vector<BindingRecord> pending;

    for (const auto& selection : selections) {
        if (!seen.insert(selection.internal_id).second) {
            outcome.error = CollectionError::InvalidRequest;
            outcome.message = "duplicate target in one automatic policy batch";
            return outcome;
        }
    }

    // Automatic decisions are validated per target: an invalid result keeps
    // the previous valid decision instead of failing the whole batch.
    for (const auto& selection : selections) {
        if (!registry_.hasInstance(selection.internal_id)) {
            ++outcome.skipped_invalid;
            outcome.message = "no verified inventory instance with internal id " +
                              std::to_string(selection.internal_id);
            // Attribution: a skipped decision is still a per-item result, not
            // just a counter. The caller must be able to say which instance
            // did not get a decision and why.
            CollectionTargetOutcome skipped;
            skipped.internal_id = selection.internal_id;
            skipped.instance_id = registry_.instanceId(selection.internal_id);
            skipped.binding = registry_.bindingRecord(selection.internal_id);
            skipped.applied.applied = false;
            skipped.applied.error = "no verified inventory instance";
            skipped.changed = false;
            planned.push_back(std::move(skipped));
            continue;
        }

        const auto names = normalized_names(selection.collectors);
        std::string bad_name;
        if (!validate_collectors(names, bad_name)) {
            ++outcome.skipped_invalid;
            outcome.message = "unknown automatic collector name: " + bad_name;
            spdlog::warn("JobCollectionManager: ignoring invalid automatic selection for "
                         "instance {} (collector '{}')", selection.internal_id, bad_name);
            CollectionTargetOutcome skipped;
            skipped.internal_id = selection.internal_id;
            skipped.instance_id = registry_.instanceId(selection.internal_id);
            skipped.binding = registry_.bindingRecord(selection.internal_id);
            skipped.applied.applied = false;
            skipped.applied.error = "unknown automatic collector name: " + bad_name;
            skipped.changed = false;
            planned.push_back(std::move(skipped));
            continue;
        }

        const BindingRecord current = registry_.bindingRecord(selection.internal_id);
        CollectionTargetOutcome target;
        target.internal_id = selection.internal_id;
        target.instance_id = registry_.instanceId(selection.internal_id);

        if (current.auto_collectors == names &&
            current.policy_revision == selection.policy_revision) {
            target.binding = current;
            planned.push_back(std::move(target));
            ++outcome.unchanged;
            continue;
        }

        BindingRecord next = current;
        next.internal_id = selection.internal_id;
        next.auto_collectors = names;
        next.policy_revision = selection.policy_revision;
        // Manual mode, including an empty manual list, is never overwritten.
        // Only the stored automatic decision is refreshed, so the client-visible
        // binding revision (and any expected_revision guard) does not move for a
        // change the client cannot observe.
        if (current.mode == CollectionMode::Manual) {
            ++outcome.skipped_manual_override;
            next.mode = current.mode;
            next.manual_collectors = current.manual_collectors;
            target.binding = next;
            target.changed = false;
            planned.push_back(std::move(target));
            pending.push_back(std::move(next));
            continue;
        }

        next.revision = current.revision + 1;
        target.binding = next;
        target.changed = true;
        planned.push_back(std::move(target));
        pending.push_back(std::move(next));
    }

    if (!pending.empty()) {
        std::string error;
        if (!registry_.commitBindings(pending, error)) {
            if (error == "not_found") {
                outcome.error = CollectionError::NotFound;
            } else if (error == "instance_closing") {
                outcome.error = CollectionError::StaleRevision;
            } else {
                outcome.error = CollectionError::StoreUnavailable;
            }
            outcome.message = "automatic policy transaction rejected: " + error;
            // The transaction failed, but every planned item keeps its
            // attribution: the caller maps the failure onto the instances that
            // needed a decision instead of reporting a batch-wide failure.
            for (auto& target : planned) {
                target.applied.applied = false;
                target.applied.error = "intent write failed: " + error;
            }
            outcome.targets = std::move(planned);
            return outcome;
        }
    }

    // Same convergence rule as an explicit request: targets the transaction
    // above applied are not retried inside this call.
    const bool commit_applied = !pending.empty();
    for (auto& target : planned) {
        if (!(commit_applied && target.changed))
            registry_.reconcileCollectionIntent(target.internal_id);
        target.applied = registry_.appliedCollection(target.internal_id);
    }

    outcome.ok = true;
    outcome.targets = std::move(planned);
    return outcome;
}
