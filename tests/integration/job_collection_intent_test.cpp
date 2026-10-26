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
#include "core/job_collection_manager.hpp"
#include "core/job_registry.hpp"

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
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
    options.enable_job_watchers = false;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    return options;
}

NativeSelector condor_selector(const std::string& native)
{
    NativeSelector selector;
    selector.scheduler_type = SchedulerType::Condor;
    selector.cluster_name = "htcondor01";
    selector.cluster_tag = "schedd-a";
    selector.native_job_id = native;
    return selector;
}

ObservedExecution condor_observation(const std::string& native,
                                     const std::string& boot_id = "boot-aaa",
                                     pid_t manager_pid = 4242,
                                     uint64_t start_ticks = 900000)
{
    ObservedExecution observation;
    observation.selector = condor_selector(native);
    observation.generation.boot_id = boot_id;
    observation.generation.manager_pid = manager_pid;
    observation.generation.manager_start_ticks = start_ticks;
    observation.generation.cgroup_identity = "/sys/fs/cgroup/htcondor/slot1";
    observation.processes.push_back(ProcessIdentity{manager_pid, start_ticks, boot_id});

    CondorJobAttr attr{};
    attr.auto_update_child = false;
    const auto dot = native.find('.');
    attr.cluster_id = std::stoul(native.substr(0, dot));
    attr.proc_id = std::stoul(native.substr(dot + 1));
    attr.starter_pid = manager_pid;
    attr.owner = "alice";
    attr.slots_cgroup_path = "/sys/fs/cgroup/htcondor/slot1";
    attr.scheduler_host = "htcondor01";

    observation.facts.jobtype = JobType::Job;
    observation.facts.subtype = JobSubType::Condor;
    observation.facts.sub_attr = attr;
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
    JobCollectionManager manager(registry);
    manager.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector" || name == "disk_collector";
    });

    const auto observation = condor_observation("172.0");
    const auto created = registry.upsertObservedJob(observation);
    check(created.ok, "baseline registration must succeed");

    const auto manual = manager.setCollectors(
        set_request({created.internal_id}, {"cpumem_collector"}));
    check(manual.ok, "manual set must be accepted: " + manual.message);
    check(manual.targets.size() == 1, "one exact target must be reported");
    check(manual.targets.front().binding.mode == CollectionMode::Manual,
          "manual set must switch the mode");
    check(manual.targets.front().binding.revision == 1, "first intent revision is 1");
    check(same(manual.targets.front().binding.effectiveCollectors(),
               {"cpumem_collector"}),
          "manual selection must be effective");

    // An inventory observation update must not rewrite collection intent.
    auto updated_observation = observation;
    std::get<CondorJobAttr>(updated_observation.facts.sub_attr).owner = "bob";
    const auto updated = registry.upsertObservedJob(updated_observation);
    check(updated.ok && updated.disposition == UpsertDisposition::Updated,
          "observation update must be applied to facts");

    {
        const auto binding = manager.binding(created.internal_id);
        check(binding.mode == CollectionMode::Manual,
              "discovery must not overwrite a manual override");
        check(same(binding.manual_collectors, {"cpumem_collector"}),
              "discovery must not rewrite the manual selection");
        check(same(binding.effectiveCollectors(), {"cpumem_collector"}),
              "manual selection must stay effective after discovery");
        check(binding.revision == 1, "discovery must not advance binding revisions");
    }

    // An automatic policy decision must not overwrite the manual override,
    // though its decision is recorded for a later reset_auto.
    const auto policy = manager.applyAutoPolicy({
        AutoSelection{created.internal_id, {"disk_collector"}, "rules-v1"}});
    check(policy.ok, "automatic policy must be accepted: " + policy.message);
    check(policy.skipped_manual_override == 1,
          "manual targets must be reported as skipped by automatic policy");

    {
        const auto binding = manager.binding(created.internal_id);
        check(binding.mode == CollectionMode::Manual,
              "automatic policy must never overwrite the manual mode");
        check(same(binding.manual_collectors, {"cpumem_collector"}),
              "automatic policy must never rewrite manual selection");
        check(same(binding.effectiveCollectors(), {"cpumem_collector"}),
              "manual selection must stay effective after an automatic decision");
        check(same(binding.auto_collectors, {"disk_collector"}),
              "the automatic decision must still be recorded");
        check(binding.policy_revision == "rules-v1",
              "the policy revision must be recorded");
        check(binding.revision == 1,
              "an automatic decision under a manual override must not move the "
              "client-visible binding revision");
    }
}

// A06 -----------------------------------------------------------------------

void persistent_stop_survives_rescan_and_reset_restores_auto(const std::string& base)
{
    const auto path = fresh_dir(base, "a06");
    JobRegistry registry(isolated(path));
    JobCollectionManager manager(registry);
    manager.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector";
    });

    const auto observation = condor_observation("172.0");
    const auto created = registry.upsertObservedJob(observation);
    check(created.ok, "baseline registration must succeed");

    const auto stopped = manager.setCollectors(set_request({created.internal_id}, {}));
    check(stopped.ok, "an explicit empty set must be accepted");
    check(stopped.targets.front().binding.mode == CollectionMode::Manual,
          "an explicit empty set is manual mode");
    check(stopped.targets.front().binding.effectiveCollectors().empty(),
          "an explicit empty set stops collection");
    const uint64_t stop_revision = stopped.targets.front().binding.revision;

    // Rescan: the same observation must not resurrect collection.
    const auto rescan = registry.upsertObservedJob(observation);
    check(rescan.ok && rescan.disposition == UpsertDisposition::Unchanged,
          "rescan of the same facts must be unchanged");
    check(manager.binding(created.internal_id).revision == stop_revision,
          "rescan must not advance binding revisions");
    check(manager.binding(created.internal_id).effectiveCollectors().empty(),
          "rescan must not restart a stopped instance");

    // Policy reload: the automatic decision is recorded but cannot win.
    const auto reload = manager.applyAutoPolicy({
        AutoSelection{created.internal_id, {"cpumem_collector"}, "rules-v2"}});
    check(reload.ok, "policy reload must be accepted: " + reload.message);
    check(reload.skipped_manual_override == 1,
          "policy reload must skip the manual override");
    check(manager.binding(created.internal_id).effectiveCollectors().empty(),
          "an empty manual override must survive a policy reload");

    // Restart-and-reload semantics at store level: the intent is re-read from
    // the durable store, never reconstructed from facts.
    const auto reloaded = registry.bindingRecord(created.internal_id);
    check(reloaded.mode == CollectionMode::Manual,
          "durable intent must be manual after reload");
    check(reloaded.effectiveCollectors().empty(),
          "durable empty intent must still stop collection");

    const auto reset = manager.resetAuto(reset_request({created.internal_id}));
    check(reset.ok, "reset_auto must be accepted: " + reset.message);
    check(reset.targets.front().binding.mode == CollectionMode::Auto,
          "reset_auto must clear the manual override");
    check(same(reset.targets.front().binding.effectiveCollectors(),
               {"cpumem_collector"}),
          "reset_auto must apply the recorded automatic selection");
    check(reset.targets.front().binding.revision > stop_revision,
          "reset_auto must advance the binding revision");

    const uint64_t reset_revision = reset.targets.front().binding.revision;
    const auto repeated = manager.resetAuto(reset_request({created.internal_id}));
    check(repeated.ok, "repeated reset_auto must stay valid");
    check(repeated.targets.front().binding.revision == reset_revision,
          "repeated reset_auto must not advance the revision");
    check(!repeated.targets.front().changed,
          "repeated reset_auto must report no change");
}

// A07 -----------------------------------------------------------------------

void duplicate_requests_stale_revisions_and_invalid_targets(const std::string& base)
{
    const auto path = fresh_dir(base, "a07");
    JobRegistry registry(isolated(path));
    JobCollectionManager manager(registry);
    manager.setCollectorValidator([](const std::string& name) {
        return name == "cpumem_collector";
    });

    const auto first = registry.upsertObservedJob(condor_observation("172.0"));
    const auto second = registry.upsertObservedJob(condor_observation("173.0"));
    check(first.ok && second.ok, "baseline registrations must succeed");
    const std::vector<uint64_t> both{first.internal_id, second.internal_id};

    // Multi-target request commits one durable transaction.
    const auto accepted = manager.setCollectors(set_request(both, {"cpumem_collector"}));
    check(accepted.ok, "multi-target set must be accepted: " + accepted.message);
    check(accepted.targets.size() == 2, "both exact targets must be reported");
    const uint64_t first_revision = accepted.targets.front().binding.revision;
    check(accepted.targets.back().binding.revision == first_revision,
          "targets in one request share the same new revision");

    // Identical repeat: existing revision, no side effects.
    const auto repeat = manager.setCollectors(set_request(both, {"cpumem_collector"}));
    check(repeat.ok, "identical repeat must be accepted");
    check(repeat.unchanged == 2, "identical repeat must report both targets unchanged");
    for (const auto& target : repeat.targets) {
        check(target.binding.revision == first_revision,
              "identical repeat must not advance revisions");
        check(!target.changed, "identical repeat must not report a change");
    }

    // Stale expected revision conflicts without mutating.
    auto stale = set_request({first.internal_id}, {"cpumem_collector"});
    stale.expected_revision = first_revision - 1;
    const auto conflict = manager.setCollectors(stale);
    check(!conflict.ok && conflict.error == CollectionError::StaleRevision,
          "stale expected_revision must return a conflict");
    check(registry.bindingRecord(first.internal_id).revision == first_revision,
          "a stale conflict must leave the revision untouched");

    // Invalid collector in a multi-target request: no target changes.
    const auto invalid = manager.setCollectors(
        set_request(both, {"cpumem_collector", "no_such_collector"}));
    check(!invalid.ok && invalid.error == CollectionError::UnknownCollector,
          "unknown collector names must reject the request");
    for (const auto id : both) {
        const auto binding = registry.bindingRecord(id);
        check(binding.revision == first_revision,
              "an invalid multi-target request must not advance any revision");
        check(same(binding.effectiveCollectors(), {"cpumem_collector"}),
              "an invalid multi-target request must not rewrite any selection");
    }

    // Unknown target: no target changes.
    const auto missing = manager.setCollectors(
        set_request({first.internal_id, 987654}, {"cpumem_collector"}));
    check(!missing.ok && missing.error == CollectionError::NotFound,
          "unknown targets must be rejected before any mutation");
    check(registry.bindingRecord(first.internal_id).revision == first_revision,
          "an unknown target must not mutate the valid target");

    // Request shape errors.
    {
        CollectionRequest no_collectors;
        no_collectors.targets = {first.internal_id};
        const auto outcome = manager.setCollectors(no_collectors);
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "set without collectors must be invalid");
    }
    {
        auto reset_with_collectors = reset_request({first.internal_id});
        reset_with_collectors.collectors = std::vector<std::string>{"cpumem_collector"};
        const auto outcome = manager.resetAuto(reset_with_collectors);
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "reset_auto with collectors must be invalid");
    }
    {
        auto multi_expected = set_request(both, {"cpumem_collector"});
        multi_expected.expected_revision = first_revision;
        const auto outcome = manager.setCollectors(multi_expected);
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "expected_revision is only valid for a single target");
    }
    {
        const auto outcome = manager.setCollectors(set_request({}, {"cpumem_collector"}));
        check(!outcome.ok && outcome.error == CollectionError::InvalidRequest,
              "an empty target set must be invalid");
    }
    {
        const auto outcome = manager.setCollectors(
            set_request({first.internal_id}, {""}));
        check(!outcome.ok && outcome.error == CollectionError::UnknownCollector,
              "empty collector names must be rejected");
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
        JobCollectionManager manager(registry);
        manager.setCollectorValidator([](const std::string& name) {
            return name == "cpumem_collector";
        });
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        internal_id = created.internal_id;
        instance_id = created.instance_id;

        const auto stopped = manager.setCollectors(set_request({internal_id}, {}));
        check(stopped.ok, "persistent stop must be accepted");
        check(stopped.targets.front().binding.effectiveCollectors().empty(),
              "persistent stop must be effective before restart");
    }

    {
        // Fresh JobRegistry instance on the same DB: durable intent reapply.
        JobRegistry registry(isolated(path));
        check(registry.migrationComplete(), "restart must reopen a migrated db");
        JobCollectionManager manager(registry);

        const auto record = registry.inventoryRecord(internal_id);
        check(record.has_value(), "restart must restore the inventory record");
        check(record->instance_id == instance_id,
              "restart must keep the same opaque instance handle");

        const auto same_generation = registry.upsertObservedJob(condor_observation("172.0"));
        check(same_generation.ok, "rescanning the same generation must succeed");
        check(same_generation.internal_id == internal_id,
              "the same execution generation must keep its internal id");

        const auto replayed = manager.binding(internal_id);
        check(replayed.mode == CollectionMode::Manual,
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
              "retiring the old generation must succeed");
        check(!manager.binding(internal_id).revision,
              "retirement must drop the old binding record");

        const auto replacement = registry.upsertObservedJob(
            condor_observation("172.0", "boot-bbb", 5252, 990000));
        check(replacement.ok && replacement.disposition == UpsertDisposition::Created,
              "a replacement generation must register as a new instance");
        check(replacement.internal_id != internal_id,
              "a replacement generation must get a new internal id");
        check(replacement.instance_id != instance_id,
              "a replacement generation must get a new instance handle");

        const auto fresh = manager.binding(replacement.internal_id);
        check(fresh.mode == CollectionMode::Auto,
              "a replacement generation must start in automatic mode");
        check(fresh.revision == 0,
              "a replacement generation must not inherit any manual revision");
        check(fresh.effectiveCollectors().empty(),
              "a replacement generation must not inherit the old manual stop");
    }
}

} // namespace

int main(int argc, char** argv)
{
    try {
        const std::string base = argc > 1 ? argv[1] : "/tmp/joblens-collection-intent-test";
        std::filesystem::create_directories(base);
        manual_selection_wins_over_observations_and_policy(base);
        persistent_stop_survives_rescan_and_reset_restores_auto(base);
        duplicate_requests_stale_revisions_and_invalid_targets(base);
        durable_intent_survives_restart_and_generation_change(base);
        std::cout << "PASS collection intent: A05 manual over discovery and "
                     "policy, A06 persistent stop and reset_auto, A07 "
                     "duplicate/stale/invalid request atomicity, A22 durable "
                     "intent across restart and generation change\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
