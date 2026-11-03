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
/* Deterministic collection-intent acceptance (task 2 of
 * doc/job_inventory_collection_decoupling_plan.md).
 *
 *   A05 Manual selection wins over observation updates and automatic policy.
 *   A06 A manual empty set (persistent stop) survives rescans and policy
 *       reload; reset_auto alone restores the current automatic selection.
 *   A07 Identical repeats add no revision, stale expected_revision conflicts,
 *       and an invalid collector in a multi-target request changes no target.
 *   A22 Durable intent survives a restart on the same DB; a replacement
 *       execution generation never inherits the old manual selection.
 */
#include "core/job_registry.hpp"
#include "core/reentrancy_gate.hpp"
#include "job_fixture.h"
#include "collection_runtime_fixture.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

std::string fresh_dir(const std::string& base, const std::string& name)
{
    const auto path = std::filesystem::path(base) / name;
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path.string();
}

JobRegistryOptions isolated(const std::string& path)
{
    JobRegistryOptions options;
    options.db_path = path;
    options.enable_inventory_maintenance = false;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    return options;
}

NativeSelector condor_selector(const std::string& native)
{
    NativeSelector selector;
    selector.adapter = "condor";
    selector.cluster_name = "htcondor01";
    selector.cluster_tag = "schedd-a";
    selector.native_job_id = native;
    return selector;
}

Job condor_observation(const std::string& native,
                       const std::string& boot_id = "boot-aaa",
                       pid_t manager_pid = 4242,
                       uint64_t start_ticks = 900000)
{
    ExecutionGeneration generation;
    generation.boot_id = boot_id;
    generation.manager_pid = manager_pid;
    generation.manager_start_ticks = start_ticks;
    generation.cgroup_identity = "/sys/fs/cgroup/htcondor/slot1";
    Job observation = discovered_job(condor_selector(native), generation,
                                     {ProcessIdentity{manager_pid, start_ticks, boot_id}}, true);

    CondorJobAttr attr{};
    attr.auto_update_child = false;
    const auto dot = native.find('.');
    attr.cluster_id = std::stoul(native.substr(0, dot));
    attr.proc_id = std::stoul(native.substr(dot + 1));
    attr.starter_pid = manager_pid;
    attr.owner = "alice";
    attr.slots_cgroup_path = "/sys/fs/cgroup/htcondor/slot1";
    attr.scheduler_host = "htcondor01";

    observation.subtype = JobSubType::Condor;
    observation.sub_attr = attr;
    return observation;
}

CollectionRequest set_request(std::vector<uint64_t> targets,
                              std::vector<std::string> collectors)
{
    CollectionRequest request;
    request.targets = std::move(targets);
    request.collectors = std::move(collectors);
    return request;
}

CollectionRequest reset_request(std::vector<uint64_t> targets)
{
    CollectionRequest request;
    request.targets = std::move(targets);
    return request;
}

bool same(const std::vector<std::string>& lhs, const std::vector<std::string>& rhs)
{
    return lhs == rhs;
}

// A05 -----------------------------------------------------------------------

void manual_selection_wins_over_observations_and_policy(const std::string& base)
{
    const auto path = fresh_dir(base, "a05");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector" || name == "disk_collector";
    });

    const auto observation = condor_observation("172.0");
    const auto created = registry.upsertObservedJob(observation);
    check(created.ok, "baseline registration must succeed");

    const auto manual = registry.setCollectors(
        set_request({created.internal_id}, {"cpumem_collector"}));
    check(manual.ok, "manual set must be accepted: " + manual.message);
    check(manual.targets.size() == 1, "one exact target must be reported");
    check(manual.targets.front().selection.mode() == CollectionMode::Manual,
          "manual set must switch the mode");
    check(manual.targets.front().revision == record_revision(registry, created.internal_id),
          "the accepted revision is the committed record revision");
    check(manual.targets.front().actual.tracked == false,
          "a command commits intent asynchronously; it must not invent an attachment");
    check(same(manual.targets.front().selection.effectiveCollectors(),
               {"cpumem_collector"}),
          "manual selection must be effective");

    // An inventory observation update must not rewrite collection intent.
    auto updated_observation = observation;
    std::get<CondorJobAttr>(updated_observation.sub_attr).owner = "bob";
    const auto updated = registry.upsertObservedJob(updated_observation);
    check(updated.ok && updated.disposition == UpsertDisposition::Updated,
          "observation update must be applied to facts");

    {
        const auto binding = registry.selectionRecord(created.internal_id);
        check(binding.mode() == CollectionMode::Manual,
              "discovery must not overwrite a manual override");
        check(binding.manual.has_value() &&
                  same(*binding.manual, {"cpumem_collector"}),
              "discovery must not rewrite the manual selection");
        check(same(binding.effectiveCollectors(), {"cpumem_collector"}),
              "manual selection must stay effective after discovery");
        // ONE record revision orders facts too, so a real fact change does
        // advance it; what must not move is the collection decision.
        check(record_revision(registry, created.internal_id) == updated.revision,
              "the record revision must be the one the fact update committed");
        check(updated.revision > manual.targets.front().revision,
              "a real fact change advances the record revision");
    }

    // An automatic policy decision must not overwrite the manual override,
    // though its decision is recorded for a later reset_auto.
    const uint64_t before_policy = record_revision(registry, created.internal_id);
    const auto policy = registry.applyAutoPolicy({
        AutoSelection{created.internal_id, {"disk_collector"}, "rules-v1"}});
    check(policy.ok, "automatic policy must be accepted: " + policy.message);
    check(policy.skipped_manual_override == 1,
          "manual targets must be reported as skipped by automatic policy");

    {
        const auto binding = registry.selectionRecord(created.internal_id);
        check(binding.mode() == CollectionMode::Manual,
              "automatic policy must never overwrite the manual mode");
        check(binding.manual.has_value() &&
                  same(*binding.manual, {"cpumem_collector"}),
              "automatic policy must never rewrite manual selection");
        check(same(binding.effectiveCollectors(), {"cpumem_collector"}),
              "manual selection must stay effective after an automatic decision");
        check(binding.automatic.has_value() &&
                  same(*binding.automatic, {"disk_collector"}),
              "the automatic decision must still be recorded");
        check(binding.automatic_source == "rules-v1",
              "the policy revision must be recorded");
        // ONE record revision orders every mutation, bookkeeping included, but
        // the effective selection did not move, so nothing detaches.
        check(record_revision(registry, created.internal_id) == before_policy + 1,
              "an automatic decision under a manual override advances the record "
              "revision exactly once");
        check(policy.targets.front().changed == false,
              "the client-visible automatic decision under manual is not a change");
    }
}

// A06 -----------------------------------------------------------------------

void persistent_stop_survives_rescan_and_reset_restores_auto(const std::string& base)
{
    const auto path = fresh_dir(base, "a06");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector";
    });

    const auto observation = condor_observation("172.0");
    const auto created = registry.upsertObservedJob(observation);
    check(created.ok, "baseline registration must succeed");

    const auto stopped = registry.setCollectors(set_request({created.internal_id}, {}));
    check(stopped.ok, "an explicit empty set must be accepted");
    check(stopped.targets.front().selection.mode() == CollectionMode::Manual,
          "an explicit empty set is manual mode");
    check(stopped.targets.front().selection.stopped(),
          "an explicit empty set is a persistent stop");
    check(stopped.targets.front().selection.effectiveCollectors().empty(),
          "an explicit empty set stops collection");
    const uint64_t stop_revision = stopped.targets.front().revision;

    // Rescan: the same observation must not resurrect collection.
    const auto rescan = registry.upsertObservedJob(observation);
    check(rescan.ok && rescan.disposition == UpsertDisposition::Unchanged,
          "rescan of the same facts must be unchanged");
    check(record_revision(registry, created.internal_id) == stop_revision,
          "rescan must not advance the record revision");
    check(registry.selectionRecord(created.internal_id).effectiveCollectors().empty(),
          "rescan must not restart a stopped instance");

    // Policy reload: the automatic decision is recorded but cannot win.
    const auto reload = registry.applyAutoPolicy({
        AutoSelection{created.internal_id, {"cpumem_collector"}, "rules-v2"}});
    check(reload.ok, "policy reload must be accepted: " + reload.message);
    check(reload.skipped_manual_override == 1,
          "policy reload must skip the manual override");
    check(registry.selectionRecord(created.internal_id).effectiveCollectors().empty(),
          "an empty manual override must survive a policy reload");

    // Restart-and-reload semantics at store level: the intent is re-read from
    // the durable store, never reconstructed from facts.
    const auto reloaded = registry.selectionRecord(created.internal_id);
    check(reloaded.mode() == CollectionMode::Manual,
          "durable intent must be manual after reload");
    check(reloaded.effectiveCollectors().empty(),
          "durable empty intent must still stop collection");

    const auto reset = registry.resetAuto(reset_request({created.internal_id}));
    check(reset.ok, "reset_auto must be accepted: " + reset.message);
    check(reset.targets.front().selection.mode() == CollectionMode::Auto,
          "reset_auto must clear the manual override");
    check(same(reset.targets.front().selection.effectiveCollectors(),
               {"cpumem_collector"}),
          "reset_auto must apply the recorded automatic selection");
    check(reset.targets.front().revision > stop_revision,
          "reset_auto must advance the record revision");

    const uint64_t reset_revision = reset.targets.front().revision;
    const auto repeated = registry.resetAuto(reset_request({created.internal_id}));
    check(repeated.ok, "repeated reset_auto must stay valid");
    check(repeated.targets.front().revision == reset_revision,
          "repeated reset_auto must not advance the revision");
    check(!repeated.targets.front().changed,
          "repeated reset_auto must report no change");
}

// A07 -----------------------------------------------------------------------

void duplicate_requests_stale_revisions_and_invalid_targets(const std::string& base)
{
    const auto path = fresh_dir(base, "a07");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector";
    });

    const auto first = registry.upsertObservedJob(condor_observation("172.0"));
    const auto second = registry.upsertObservedJob(condor_observation("173.0"));
    check(first.ok && second.ok, "baseline registrations must succeed");
    const std::vector<uint64_t> both{first.internal_id, second.internal_id};

    // Multi-target request commits one durable transaction.
    const auto accepted = registry.setCollectors(set_request(both, {"cpumem_collector"}));
    check(accepted.ok, "multi-target set must be accepted: " + accepted.message);
    check(accepted.targets.size() == 2, "both exact targets must be reported");
    const uint64_t first_revision = accepted.targets.front().revision;
    check(accepted.targets.back().revision == first_revision,
          "targets in one request share the same new revision");

    // Identical repeat: existing revision, no side effects.
    const auto repeat = registry.setCollectors(set_request(both, {"cpumem_collector"}));
    check(repeat.ok, "identical repeat must be accepted");
    check(repeat.unchanged == 2, "identical repeat must report both targets unchanged");
    for (const auto& target : repeat.targets) {
        check(target.revision == first_revision,
              "identical repeat must not advance revisions");
        check(!target.changed, "identical repeat must not report a change");
    }

    // Stale expected revision conflicts without mutating.
    auto stale = set_request({first.internal_id}, {"cpumem_collector"});
    stale.expected_revision = first_revision - 1;
    const auto conflict = registry.setCollectors(stale);
    check(!conflict.ok && conflict.error == CollectionError::StaleRevision,
          "stale expected_revision must return a conflict");
    check(record_revision(registry, first.internal_id) == first_revision,
          "a stale conflict must leave the revision untouched");

    // Invalid collector in a multi-target request: no target changes.
    const auto invalid = registry.setCollectors(
        set_request(both, {"cpumem_collector", "no_such_collector"}));
    check(!invalid.ok && invalid.error == CollectionError::UnknownCollector,
          "unknown collector names must reject the request");
    for (const auto id : both) {
        const auto binding = registry.selectionRecord(id);
        check(record_revision(registry, id) == first_revision,
              "an invalid multi-target request must not advance any revision");
        check(same(binding.effectiveCollectors(), {"cpumem_collector"}),
              "an invalid multi-target request must not rewrite any selection");
    }

    // Unknown target: no target changes.
    const auto missing = registry.setCollectors(
        set_request({first.internal_id, 987654}, {"cpumem_collector"}));
    check(!missing.ok && missing.error == CollectionError::NotFound,
          "unknown targets must be rejected before any mutation");
    check(record_revision(registry, first.internal_id) == first_revision,
          "an unknown target must not mutate the valid target");

    // Request shape errors.
    {
        CollectionRequest no_collectors;
        no_collectors.targets = {first.internal_id};
        const auto outcome = registry.setCollectors(no_collectors);
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "set without collectors must be invalid");
    }
    {
        auto reset_with_collectors = reset_request({first.internal_id});
        reset_with_collectors.collectors = std::vector<std::string>{"cpumem_collector"};
        const auto outcome = registry.resetAuto(reset_with_collectors);
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "reset_auto with collectors must be invalid");
    }
    {
        auto multi_expected = set_request(both, {"cpumem_collector"});
        multi_expected.expected_revision = first_revision;
        const auto outcome = registry.setCollectors(multi_expected);
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "expected_revision is only valid for a single target");
    }
    {
        const auto outcome = registry.setCollectors(set_request({}, {"cpumem_collector"}));
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "an empty target set must be invalid");
    }
    {
        const auto outcome = registry.setCollectors(
            set_request({first.internal_id}, {""}));
        check(!outcome.ok && outcome.error == CollectionError::UnknownCollector,
              "empty collector names must be rejected");
    }

    // An automatic decision with an invalid name is a per-target skip, never a
    // successful (empty) selection: the previous valid intent must not be
    // reported as committed for that target.
    {
        const auto outcome = registry.applyAutoPolicy(
            {AutoSelection{first.internal_id, {"no_such_collector"}, "rules-bad"}});
        check(outcome.ok && outcome.skipped_invalid == 1,
              "an invalid automatic decision must be attributed as skipped");
        check(outcome.targets.size() == 1 && outcome.targets.front().skipped,
              "the skipped target must be explicit");
        check(record_revision(registry, first.internal_id) == first_revision,
              "a skipped automatic decision must not move the stored revision");
        check(same(registry.selectionRecord(first.internal_id).effectiveCollectors(),
                   {"cpumem_collector"}),
              "a skipped automatic decision must not rewrite the selection");
    }
}

// A22 -----------------------------------------------------------------------

void durable_intent_survives_restart_and_generation_change(const std::string& base)
{
    const auto path = fresh_dir(base, "a22");
    uint64_t internal_id = 0;
    std::string instance_id;

    {
        JobRegistry registry(isolated(path));
                registry.setCollectorValidator([](const std::string& name) {
            return name == "cpumem_collector";
        });
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        internal_id = created.internal_id;
        instance_id = created.instance_id;

        const auto stopped = registry.setCollectors(set_request({internal_id}, {}));
        check(stopped.ok, "persistent stop must be accepted");
        check(stopped.targets.front().selection.effectiveCollectors().empty(),
              "persistent stop must be effective before restart");
    }

    {
        // Fresh JobRegistry instance on the same DB: durable intent reapply.
        JobRegistry registry(isolated(path));
        check(registry.migrationComplete(), "restart must reopen an initialized db");

        const auto record = registry.inventoryRecord(internal_id);
        check(record.has_value(), "restart must restore the inventory record");
        check(record->instance_id == instance_id,
              "restart must keep the same opaque instance handle");

        const auto same_generation = registry.upsertObservedJob(condor_observation("172.0"));
        check(same_generation.ok, "rescanning the same generation must succeed");
        check(same_generation.internal_id == internal_id,
              "the same execution generation must keep its internal id");

        const auto replayed = registry.selectionRecord(internal_id);
        check(replayed.mode() == CollectionMode::Manual,
              "restart must reapply the durable manual intent");
        check(replayed.effectiveCollectors().empty(),
              "restart must reapply the persistent stop");

        // A replacement execution generation must not silently inherit the
        // old manual selection. While the old generation is still live the
        // observation is a conflict, never a merge.
        const auto conflict = registry.upsertObservedJob(
            condor_observation("172.0", "boot-bbb", 5252, 990000));
        check(!conflict.ok && conflict.error == "execution_generation_conflict",
              "a different live generation must not merge into the old record");

        check(registry.retireInstance(internal_id),
              "retiring the old generation must be accepted");
        check(!registry.inventoryRecord(internal_id).has_value(),
              "retirement without a runtime archives the record immediately");

        const auto replacement = registry.upsertObservedJob(
            condor_observation("172.0", "boot-bbb", 5252, 990000));
        check(replacement.ok && replacement.disposition == UpsertDisposition::Created,
              "a replacement generation must register as a new instance");
        check(replacement.internal_id != internal_id,
              "a replacement generation must get a new internal id");
        check(replacement.instance_id != instance_id,
              "a replacement generation must get a new instance handle");

        const auto fresh = registry.selectionRecord(replacement.internal_id);
        check(fresh.mode() == CollectionMode::Auto,
              "a replacement generation must start in automatic mode");
        check(!fresh.manual.has_value(),
              "a replacement generation must not inherit any manual decision");
        check(fresh.effectiveCollectors().empty(),
              "a replacement generation must not inherit the old manual stop");
    }
}

// A27 -----------------------------------------------------------------------

// A selection command commits intent and only wakes the runtime. Convergence
// is the runtime's job: it retries a failed application on the next pass, and
// the command's answer never claims an attachment it did not observe.
void repeated_requests_converge_a_failed_application(const std::string& base)
{
    const auto path = fresh_dir(base, "a27-retry");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector";
    });

    // A runtime that fails the first application, then recovers.
    int calls = 0;
    bool healthy = false;
    install_collection_runtime(registry, [&](const Job& job, std::vector<std::string>& applied,
                                      std::string& error) {
        ++calls;
        if (!healthy) {
            error = "runtime not ready";
            return false;
        }
        applied = job.CollectorNames;
        return true;
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");

    const auto first = registry.setCollectors(
        set_request({created.internal_id}, {"cpumem_collector"}));
    check(first.ok, "the intent must be accepted even when the runtime fails");
    check(calls == 0, "a command must not run the runtime on its own stack");
    check(first.targets.front().actual.tracked == false,
          "the command must report the runtime's actual state, not the desired one");

    // The runtime's own pass is where the failure is observed. It must not be
    // reported as applied while it fails.
    test_runtime(registry).reconcile(created.internal_id);
    check(calls == 1, "the runtime pass must attempt the attachment");
    {
        const auto applied = test_runtime(registry).applied(created.internal_id);
        check(!applied.applied,
              "a failed runtime application must not be reported as applied");
        check(applied.error == "runtime not ready",
              "the runtime failure must be reported honestly, got '" + applied.error + "'");
    }
    const auto after_failure = test_runtime(registry).applied(created.internal_id);

    // The identical repeat changes no intent and only wakes the runtime again.
    const auto repeat = registry.setCollectors(
        set_request({created.internal_id}, {"cpumem_collector"}));
    check(repeat.ok && repeat.unchanged == 1,
          "the identical repeat changes no intent");
    check(test_runtime(registry).wakes() >= 2,
          "every accepted command must wake the runtime");

    healthy = true;
    test_runtime(registry).reconcile(created.internal_id);
    check(calls == 2, "the runtime pass must retry after the failure clears");
    {
        const auto repaired = registry.actualCollection(created.internal_id);
        check(repaired.tracked &&
                  repaired.collectors == std::vector<std::string>{"cpumem_collector"},
              "a converged runtime must report the attached collector set");
        check(repaired.last_revision ==
                  record_revision(registry, created.internal_id),
              "the actual state must name the record revision it converged to");
        check(repaired.attach_token > after_failure.attach_token,
              "a reattach after a failed generation gets a strictly greater token");
    }

    const auto stopped = registry.setCollectors(set_request({created.internal_id}, {}));
    check(stopped.ok, "the stop must be accepted");
    check(registry.actualCollection(created.internal_id).collectors ==
              std::vector<std::string>{"cpumem_collector"},
          "an accepted stop must not pretend the detach already happened");
    test_runtime(registry).reconcile(created.internal_id);
    check(registry.actualCollection(created.internal_id).collectors.empty(),
          "the runtime pass must release the holders for the manual stop");
}

} // namespace

// A04 (runtime refresh) -----------------------------------------------------

// A facts update with an unchanged binding must refresh the runtime snapshot:
// convergence compares the facts revision the runtime actually saw, not only
// the binding revision and collector set.
void facts_update_refreshes_a_converged_runtime(const std::string& base)
{
    const auto path = fresh_dir(base, "a04-refresh");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector";
    });

    int applies = 0;
    std::string last_owner;
    install_collection_runtime(registry, [&](const Job& job, std::vector<std::string>& applied,
                                      std::string& error) {
        ++applies;
        last_owner = std::get<CondorJobAttr>(job.sub_attr).owner;
        applied = job.CollectorNames;
        error.clear();
        return true;
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.internal_id;
    check(registry.setCollectors(set_request({id}, {"cpumem_collector"})).ok,
          "the intent must be accepted");
    test_runtime(registry).reconcile(id);
    check(applies == 1 && last_owner == "alice",
          "the first application must carry the stored facts");
    const auto converged = test_runtime(registry).applied(id);

    // Same binding, new facts: the runtime must be refreshed with them without
    // detaching and reattaching the instance (the token identifies the
    // attachment lifetime, not the facts revision).
    auto updated = condor_observation("172.0");
    std::get<CondorJobAttr>(updated.sub_attr).owner = "after";
    const auto upsert = registry.upsertObservedJob(updated);
    check(upsert.ok && upsert.disposition == UpsertDisposition::Updated,
          "the facts update must land");
    test_runtime(registry).reconcile(id);
    check(applies == 2,
          "the facts change must reach the runtime");
    check(last_owner == "after",
          "the refreshed snapshot must carry the new facts, got " + last_owner);
    {
        const auto refreshed = test_runtime(registry).applied(id);
        check(refreshed.applied && refreshed.collectors == converged.collectors,
              "a facts refresh must keep the same holders");
        check(refreshed.attach_token == converged.attach_token,
              "a facts refresh must not churn the attachment token");
        check(refreshed.revision > converged.revision,
              "the refreshed attachment must name the newer record revision");
    }
}

// A21 -----------------------------------------------------------------------

// A runtime is woken from the command stack. A runtime that mutates intent from
// its wake callback would recurse back into the command path; the mutation gate
// refuses it before anything is committed.
void reentrant_intent_mutation_is_refused(const std::string& base)
{
    const auto path = fresh_dir(base, "a21-reentrant");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector";
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.internal_id;

    const auto bound = registry.setCollectors(set_request({id}, {"cpumem_collector"}));
    check(bound.ok, "the outer intent must be accepted");
    const auto outer_binding = registry.selectionRecord(id);
    const uint64_t outer_revision = record_revision(registry, id);

    int nested_ok = 0;
    int nested_errors = 0;
    CollectionError nested_error = CollectionError::None;
    int observes = 0;
    install_collection_runtime(registry, [&](const Job&, std::vector<std::string>& applied,
                                             std::string& error) {
        ++observes;
        (void)applied;
        error.clear();
        return true;
    });

    // A wake that mutates: use a runner around setCollectors to prove the gate.
    // (The fake's wake is a plain counter; the reentrant call is injected by
    // calling the registry while a gate scope is active, which is exactly the
    // state a wake callback runs in.)
    {
        joblens::MutationGateScope gate;
        CollectionRequest nested;
        nested.targets = {id};
        nested.collectors = std::vector<std::string>{};
        const auto outcome = registry.setCollectors(nested);
        if (outcome.ok) ++nested_ok;
        else {
            ++nested_errors;
            nested_error = outcome.error;
        }
    }
    (void)observes;

    check(nested_ok == 0, "a nested mutation from inside the gate must not commit");
    check(nested_errors == 1, "the nested mutation must be refused exactly once");
    check(nested_error == CollectionError::ReentrantMutation,
          "the refusal must be typed as reentrant_mutation");

    const auto after = registry.selectionRecord(id);
    check(record_revision(registry, id) == outer_revision,
          "the nested attempt must not move the durable record revision");
    check(after.mode() == outer_binding.mode() &&
              after.manual == outer_binding.manual,
          "the nested manual stop must not survive in the durable selection");
}

// A31 -----------------------------------------------------------------------

// The expected_revision guard is compared against the same durable image the
// transaction writes. Two concurrent requests that both read revision R must
// not both commit: exactly one wins and the other reports a stale revision.
void concurrent_cas_requests_commit_once(const std::string& base)
{
    const auto path = fresh_dir(base, "a31-cas");
    JobRegistry registry(isolated(path));
    const std::vector<std::string> valid{"cpumem_collector", "disk_collector",
                                         "gpu_collector"};
    registry.setCollectorValidator([&valid](const std::string& name) {
        return std::find(valid.begin(), valid.end(), name) != valid.end();
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const auto initial = registry.setCollectors(
        set_request({created.internal_id}, {"cpumem_collector"}));
    check(initial.ok, "baseline selection must succeed");
    uint64_t expected = initial.targets.front().revision;

    // Every round races two real changes against the same expected revision.
    // Whatever the interleaving, exactly one may commit; the loser must see
    // the revision the winner wrote.
    for (int round = 0; round < 20; ++round) {
        const auto current = registry.selectionRecord(created.internal_id);
        const auto current_names = current.effectiveCollectors();
        std::vector<std::string> choices;
        for (const auto& name : valid) {
            if (std::find(current_names.begin(), current_names.end(), name) ==
                current_names.end()) {
                choices.push_back(name);
            }
        }
        check(choices.size() >= 2, "a round needs two changes distinct from the current one");

        CollectionOutcome outcomes[2];
        std::atomic<int> ready{0};
        std::atomic<bool> go{false};
        auto worker = [&](int index, const std::string& name) {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            auto request = set_request({created.internal_id}, {name});
            request.expected_revision = expected;
            outcomes[index] = registry.setCollectors(request);
        };
        std::thread first(worker, 0, choices[0]);
        std::thread second(worker, 1, choices[1]);
        while (ready.load(std::memory_order_acquire) < 2) std::this_thread::yield();
        go.store(true, std::memory_order_release);
        first.join();
        second.join();

        const int accepted = (outcomes[0].ok ? 1 : 0) + (outcomes[1].ok ? 1 : 0);
        check(accepted == 1, "two CAS requests from one revision must commit exactly once");
        const auto& rejected = outcomes[0].ok ? outcomes[1] : outcomes[0];
        check(!rejected.ok && rejected.error == CollectionError::StaleRevision,
              "the losing request must report stale_revision, got " +
                  std::string(to_string(rejected.error)));

        const auto after = registry.selectionRecord(created.internal_id);
        check(record_revision(registry, created.internal_id) == expected + 1,
              "the winning commit must advance the record revision exactly once");
        const auto& winner = outcomes[0].ok ? outcomes[0] : outcomes[1];
        check(winner.targets.size() == 1 &&
                  after.effectiveCollectors() ==
                      winner.targets.front().selection.effectiveCollectors(),
              "the published selection must be the winner's");
        expected = record_revision(registry, created.internal_id);
    }
}

// An automatic decision recorded while a manual override is effective changes
// bookkeeping only; it must not disturb the live runtime projection (applied
// state and the Job.collection view) of the manual binding.
void automatic_decision_under_manual_keeps_runtime_projection(const std::string& base)
{
    const auto path = fresh_dir(base, "a05-runtime-projection");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector" || name == "disk_collector";
    });

    int applies = 0;
    install_collection_runtime(registry, [&applies](const Job& job,
                                             std::vector<std::string>& applied,
                                             std::string& error) {
        ++applies;
        applied = job.CollectorNames;
        error.clear();
        return true;
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.internal_id;

    const auto manual = registry.setCollectors(
        set_request({id}, {"cpumem_collector"}));
    check(manual.ok, "manual selection must succeed: " + manual.message);
    test_runtime(registry).reconcile(id);
    check(applies == 1, "the runtime pass must attach the manual selection");
    const auto applied_before = test_runtime(registry).applied(id);
    check(applied_before.applied &&
              applied_before.collectors == std::vector<std::string>{"cpumem_collector"},
          "the manual binding must be the applied projection");
    const uint64_t revision = manual.targets.front().revision;
    check(applied_before.revision == revision,
          "the actual state must carry the record revision it converged to");

    const auto policy = registry.applyAutoPolicy(
        {AutoSelection{id, {"disk_collector"}, "rules-v2"}});
    check(policy.ok, "the automatic decision must be accepted: " + policy.message);
    check(policy.skipped_manual_override == 1,
          "the automatic decision must be attributed as a manual-override skip");

    const auto binding = registry.selectionRecord(id);
    check(binding.mode() == CollectionMode::Manual &&
              binding.effectiveCollectors() ==
                  std::vector<std::string>{"cpumem_collector"},
          "manual intent must stay effective after the automatic decision");
    check(binding.automatic.has_value() &&
              *binding.automatic == std::vector<std::string>{"disk_collector"} &&
              binding.automatic_source == "rules-v2",
          "the automatic decision must still be recorded for reset_auto");
    check(record_revision(registry, id) == revision + 1,
          "the automatic bookkeeping write advances the record revision exactly once");
    check(binding.effectiveCollectors() ==
              std::vector<std::string>{"cpumem_collector"},
          "the manual selection stays effective after the bookkeeping write");

    const auto applied_after = test_runtime(registry).applied(id);
    check(applied_after.applied == applied_before.applied &&
              applied_after.revision == applied_before.revision &&
              applied_after.collectors == applied_before.collectors &&
              applied_after.attach_token == applied_before.attach_token &&
              applied_after.error == applied_before.error,
          "automatic bookkeeping must not churn the live attachment");
    check(applies == 1,
          "bookkeeping under a manual override must not run the runtime");
    const auto actual = registry.actualCollection(id);
    check(actual.tracked &&
              actual.collectors == std::vector<std::string>{"cpumem_collector"},
          "the actual state must still name the manual runtime holders");
}

// A reported failure publishes nothing. FailAfterWrite models an ambiguous
// outcome: the batch landed but the status says failure, so the next identical
// request reads the durable image and sees the intent already applied. A
// successful retry must publish exactly the state it read and returned.
void ambiguous_selection_write_retry_publishes_durable_state(const std::string& base)
{
    const auto path = fresh_dir(base, "a21-ambiguous-selection");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector" || name == "disk_collector";
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.internal_id;
    const auto baseline = registry.setCollectors(
        set_request({id}, {"cpumem_collector"}));
    check(baseline.ok, "baseline selection must succeed");
    const uint64_t revision = baseline.targets.front().revision;

    registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0,
                                     /*count=*/1);
    const auto ambiguous = registry.setCollectors(
        set_request({id}, {"disk_collector"}));
    check(!ambiguous.ok && ambiguous.error == CollectionError::StoreUnavailable,
          "an ambiguous write must be reported as a store failure");
    {
        const auto published = registry.selectionRecord(id);
        check(record_revision(registry, id) == revision &&
                  published.manual.has_value() &&
                  *published.manual == std::vector<std::string>{"cpumem_collector"},
              "a reported failure must not publish the ambiguous write");
    }

    // The durable row did land. The retry must read it, report the new intent
    // and publish it so the in-memory projection stops disagreeing with disk.
    const auto retry = registry.setCollectors(set_request({id}, {"disk_collector"}));
    check(retry.ok, "the retry must succeed once the fault is gone: " + retry.message);
    check(retry.targets.size() == 1, "the retry must report the exact target");
    const auto& returned = retry.targets.front().selection;
    check(retry.targets.front().revision == revision + 1,
          "the retry must observe the revision the ambiguous write landed");
    const auto published = registry.selectionRecord(id);
    check(record_revision(registry, id) == retry.targets.front().revision,
          "a successful retry must publish the durable revision it read: returned " +
              std::to_string(retry.targets.front().revision) + ", published " +
              std::to_string(record_revision(registry, id)));
    check(published.mode() == returned.mode() &&
              published.manual == returned.manual,
          "a successful retry must publish the durable selection it read");
}

// A06/A22 -------------------------------------------------------------------

// The automatic decision recorded under a manual override is durable even
// though it is not effective, and reset_auto after a restart applies exactly
// that decision. Both the override and the reset survive their own restart.
void reset_after_restart_uses_durable_automatic_decision(const std::string& base)
{
    const auto path = fresh_dir(base, "a06-reset-restart");
    uint64_t id = 0;
    uint64_t reset_revision = 0;

    {
        JobRegistry registry(isolated(path));
        registry.setCollectorValidator([](const std::string& name) {
            return name == "cpumem_collector" || name == "disk_collector";
        });
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.internal_id;

        const auto manual = registry.setCollectors(
            set_request({id}, {"cpumem_collector"}));
        check(manual.ok, "the manual override must be accepted");
        const auto policy = registry.applyAutoPolicy(
            {AutoSelection{id, {"disk_collector"}, "rules-v1"}});
        check(policy.ok && policy.skipped_manual_override == 1,
              "the automatic decision must be recorded under the manual override");
        check(registry.deinit(), "the first shutdown must succeed");
    }

    {
        JobRegistry registry(isolated(path));
        const auto binding = registry.selectionRecord(id);
        check(binding.mode() == CollectionMode::Manual &&
                  binding.effectiveCollectors() ==
                      std::vector<std::string>{"cpumem_collector"},
              "the manual override must survive the restart");
        check(binding.automatic.has_value() &&
                  *binding.automatic == std::vector<std::string>{"disk_collector"} &&
                  binding.automatic_source == "rules-v1",
              "the recorded automatic decision must survive the restart");

        const auto reset = registry.resetAuto(reset_request({id}));
        check(reset.ok, "reset_auto must be accepted after the restart: " +
                            reset.message);
        check(reset.targets.front().selection.mode() == CollectionMode::Auto,
              "reset_auto must return to automatic mode");
        check(reset.targets.front().selection.effectiveCollectors() ==
                  std::vector<std::string>{"disk_collector"},
              "reset_auto must apply the durable automatic decision");
        reset_revision = reset.targets.front().revision;
        check(registry.deinit(), "the second shutdown must succeed");
    }

    {
        JobRegistry registry(isolated(path));
        const auto binding = registry.selectionRecord(id);
        check(binding.mode() == CollectionMode::Auto,
              "the reset must survive the restart");
        check(!binding.manual.has_value(),
              "the cleared manual override must stay cleared");
        check(binding.effectiveCollectors() ==
                  std::vector<std::string>{"disk_collector"},
              "the automatic decision must stay effective after the reset restart");
        check(record_revision(registry, id) == reset_revision,
              "the reset revision must be the durable one");
    }
}

// A21 (recovery latch) ------------------------------------------------------

// An ambiguous intent write leaves memory and disk disagreeing. Until an
// explicit recovery verifies the affected row, the store refuses mutations,
// runtime application and the shutdown checkpoint. A successful unrelated
// write must not clear that state; retrying the command performs the verified
// recovery and publishes exactly what it read.
void ambiguous_intent_write_latches_until_verified(const std::string& base)
{
    const auto path = fresh_dir(base, "a21-latch");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector" || name == "disk_collector";
    });
    install_collection_runtime(registry, [](const Job& job, std::vector<std::string>& applied,
                                     std::string& error) {
        applied = job.CollectorNames;
        error.clear();
        return true;
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.internal_id;
    const auto baseline = registry.setCollectors(set_request({id}, {"cpumem_collector"}));
    check(baseline.ok, "baseline selection must succeed");
    const uint64_t revision = baseline.targets.front().revision;

    registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0,
                                     /*count=*/1);
    const auto ambiguous = registry.setCollectors(set_request({id}, {"disk_collector"}));
    check(!ambiguous.ok && ambiguous.error == CollectionError::StoreUnavailable,
          "an ambiguous intent write must not report success");
    {
        const auto health = registry.storeHealth();
        check(health.recovery_required,
              "an unknown write outcome must latch recovery");
        check(health.recovery_state.find("mutate selection") != std::string::npos,
              "the latch must name the operation that lost certainty, got '" +
                  health.recovery_state + "'");
    }

    // Every control-plane command verifies the rows whose last write outcome is
    // unknown before it proceeds. The counter write below is a successful,
    // unrelated write: it must not clear the latch by itself.
    check(registry.getNextJobID() > 0, "the id-only write must still be allowed");
    check(registry.storeHealth().recovery_required,
          "an unrelated successful write must not clear the recovery latch");

    // The runtime must not act on a binding memory may not own.
    const auto applied_before = test_runtime(registry).applied(id);
    test_runtime(registry).reconcile(id);
    const auto applied_after = test_runtime(registry).applied(id);
    check(applied_after.applied == applied_before.applied &&
              applied_after.revision == applied_before.revision &&
              applied_after.collectors == applied_before.collectors,
          "collection application must be refused while recovery is required");

    // The retry is the explicit verified recovery: it adopts the durable image
    // first, then reports and publishes that image.
    const auto retry = registry.setCollectors(set_request({id}, {"disk_collector"}));
    check(retry.ok, "the retry must succeed once the rows were verified: " +
                        retry.message);
    check(retry.targets.front().revision == revision + 1,
          "the retry must observe the revision the ambiguous write landed");
    const auto published = registry.selectionRecord(id);
    check(record_revision(registry, id) == retry.targets.front().revision &&
              published.manual.has_value() &&
              *published.manual == std::vector<std::string>{"disk_collector"},
          "the retry must publish the durable revision and selection it read");
    check(!registry.storeHealth().recovery_required,
          "a completed recovery must release the latch");
}

// The destructor/shutdown checkpoint must never write the pre-failure memory
// image over an ambiguous write that landed. A fresh open on the same
// directory is the only authority.
void ambiguous_intent_write_survives_the_shutdown_checkpoint(const std::string& base)
{
    const auto path = fresh_dir(base, "a21-checkpoint");
    uint64_t id = 0;
    uint64_t revision = 0;
    {
        JobRegistry registry(isolated(path));
        registry.setCollectorValidator([](const std::string& name) {
            return name == "cpumem_collector" || name == "disk_collector";
        });
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.internal_id;
        const auto baseline = registry.setCollectors(
            set_request({id}, {"cpumem_collector"}));
        check(baseline.ok, "baseline selection must succeed");
        revision = baseline.targets.front().revision;

        registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0,
                                         /*count=*/1);
        const auto ambiguous = registry.setCollectors(
            set_request({id}, {"disk_collector"}));
        check(!ambiguous.ok, "the ambiguous write must be reported as a failure");
        check(registry.storeHealth().recovery_required,
              "the ambiguous write must latch recovery before the destructor runs");
        // No explicit deinit: the destructor is the checkpoint path under test.
    }
    {
        JobRegistry reopened(isolated(path));
        const auto record = reopened.inventoryRecord(id);
        check(record.has_value(),
              "the durable write that landed must survive the shutdown checkpoint");
        check(record->revision == revision + 1,
              "the landed revision must survive; found revision " +
                  std::to_string(record->revision));
        check(record->selection.manual.has_value() &&
                  *record->selection.manual == std::vector<std::string>{"disk_collector"},
              "the shutdown checkpoint must not rewrite the stale memory selection");
        check(!reopened.storeHealth().recovery_required,
              "a fresh open starts with no pending recovery");
    }
}

// A23 (closing runtime) -----------------------------------------------------

// Entering closing must not speculatively clear the applied set: the holders
// are still attached until the collectors acknowledge the drain, and the
// archive is the point where they disappear with the record.
void closing_keeps_the_runtime_holders_until_the_archive(const std::string& base)
{
    const auto path = fresh_dir(base, "a23-closing-runtime");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator(nullptr);
    install_collection_runtime(registry, [](const Job& job, std::vector<std::string>& applied,
                                     std::string& error) {
        applied = job.CollectorNames;
        error.clear();
        return true;
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.internal_id;
    const auto policy = registry.applyAutoPolicy(
        {{id, {"cpumem_collector"}, "rules-v1"}});
    check(policy.ok, "the automatic decision must be accepted");
    test_runtime(registry).reconcile(id);
    check(registry.actualCollection(id).collectors ==
              std::vector<std::string>{"cpumem_collector"},
          "the binding must be attached before the retirement");

    install_deferred_drain(registry, [](JobEvent, const Job&) {});
    check(registry.retireInstance(id),
          "retirement must be accepted and handed to the runtime");
    check(!registry.hasInstance(id), "a closing instance leaves the active map");

    const auto closing = registry.inventoryRecord(id);
    check(closing.has_value() && closing->state == "closing",
          "the record must stay closing until the drain acknowledges");
    check(registry.actualCollection(id).collectors ==
              std::vector<std::string>{"cpumem_collector"},
          "entering closing must not clear the holders that are still attached");

    check(test_runtime(registry).completeDrain(id), "the drain ack must archive the record");
    check(!registry.inventoryRecord(id).has_value(),
          "the archive removes the record together with its runtime holders");
}

int main(int argc, char** argv)
{
    try {
        const std::string base = argc > 1 ? argv[1] : "/tmp/joblens-collection-intent-test";
        std::filesystem::create_directories(base);
        manual_selection_wins_over_observations_and_policy(base);
        persistent_stop_survives_rescan_and_reset_restores_auto(base);
        duplicate_requests_stale_revisions_and_invalid_targets(base);
        durable_intent_survives_restart_and_generation_change(base);
        repeated_requests_converge_a_failed_application(base);
        facts_update_refreshes_a_converged_runtime(base);
        reentrant_intent_mutation_is_refused(base);
        concurrent_cas_requests_commit_once(base);
        automatic_decision_under_manual_keeps_runtime_projection(base);
        reset_after_restart_uses_durable_automatic_decision(base);
        ambiguous_selection_write_retry_publishes_durable_state(base);
        ambiguous_intent_write_latches_until_verified(base);
        ambiguous_intent_write_survives_the_shutdown_checkpoint(base);
        closing_keeps_the_runtime_holders_until_the_archive(base);
        std::cout << "PASS collection intent: A05 manual over discovery and "
                     "policy, A06 persistent stop and reset_auto, A07 "
                     "duplicate/stale/invalid request atomicity, A22 durable "
                     "intent across restart and generation change, A27 "
                     "repeated requests converge a failed application, A31 "
                     "transaction-bound revision CAS, A05 runtime projection "
                     "under a manual override, A21 ambiguous selection write, "
                     "A21 recovery latch/reopen, A23 closing runtime holders\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
