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
/* Registry lifecycle acceptance (plan section 5.1 / 5.3):
 *
 *   * construction is the single assembly point: the store opens, the
 *     inventory maintenance controller starts (providers published) even with
 *     no configured discovery type;
 *   * stopInventoryDiscovery() is idempotent and keeps the status provider
 *     published while records drain;
 *   * deinit() finalizes retirements, closes the store, retracts the
 *     providers and is idempotent; the destructor only backstops it.
 */
#include "common/config.hpp"
#include "core/inventory_status_provider.hpp"
#include "core/job_registry.hpp"
#include "job_fixture.h"
#include "collection_runtime_fixture.hpp"
#include "job_fixture.h"
#include "collection_runtime_fixture.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

void pass_case(const char* id, const char* description)
{
    std::cout << "PASS " << id << " " << description << '\n' << std::flush;
}

std::string fresh_dir(const std::string& base, const std::string& name)
{
    const auto path = std::filesystem::path(base) / name;
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path.string();
}

const char* COLLECTORS_BLOCK =
    "collectors_config:\n"
    "  default_freq: 1\n"
    "  default_use_writers: []\n"
    "  collectors:\n"
    "    - {name: cpumem_collector, type: CPUMemCollector, config: cpumem_collector_config}\n"
    "    - {name: io_collector, type: IOUsageCollector, config: io_collector_config}\n"
    "cpumem_collector_config:\n  freq: 1\n  use_writers: []\n"
    "io_collector_config:\n  freq: 1\n  use_writers: []\n";

Job make_common_job()
{
    Job job;
    job.jobtype = JobType::Job;
    job.subtype = JobSubType::Common;
    job.JobPIDs = {getpid()};
    job.CollectorNames = {"cpumem_collector"};
    CommonJobAttr attr;
    attr.auto_update_child = false;
    job.sub_attr = attr;
    return job;
}

void run(const std::string& base)
{
    // One configuration file for the whole binary (Config::instance keeps the
    // first path): the first cases run with no inventory types, the automatic
    // selection cases get their own registry instances but read the same file.
    const auto rules_root = std::filesystem::path(base) / "rules";
    std::filesystem::create_directories(base);
    std::filesystem::create_directories(rules_root / "condor_jobs");
    std::filesystem::create_directories(rules_root / "condor_accept_only");
    {
        std::ofstream accept(rules_root / "condor_accept_only" / "condor_job_accept.lua");
        accept << "local rule = {}\n"
                  "rule.name = \"accept_cpumem\"\n"
                  "rule.description = \"always accept and collect cpumem\"\n"
                  "rule.priority = 10\n"
                  "function rule.condition(job)\n"
                  "    return {passed = true, collectors = {\"cpumem_collector\"}}\n"
                  "end\n"
                  "return rule\n";
        std::ofstream accept_shared(rules_root / "condor_jobs" / "condor_job_accept.lua");
        accept_shared << "local rule = {}\n"
                         "rule.name = \"accept_cpumem\"\n"
                         "rule.description = \"always accept and collect cpumem\"\n"
                         "rule.priority = 10\n"
                         "function rule.condition(job)\n"
                         "    return {passed = true, collectors = {\"cpumem_collector\"}}\n"
                         "end\n"
                         "return rule\n";
        std::ofstream reject(rules_root / "condor_jobs" / "condor_job_reject.lua");
        reject << "local rule = {}\n"
                  "rule.name = \"reject_everything\"\n"
                  "rule.description = \"reject: no collectors, job stays inventoried\"\n"
                  "rule.priority = 5\n"
                  "function rule.condition(job)\n"
                  "    return {passed = false, collectors = {}}\n"
                  "end\n"
                  "return rule\n";
    }
    const auto config_path = std::filesystem::path(base) / "config.yaml";
    {
        std::ofstream config(config_path);
        config << "job_registry_config:\n"
                  "  auto_add_condorjob: true\n"
                  "  job_db_path: " << base << "/cfg.db\n"
                  << COLLECTORS_BLOCK
                  << "condor_job_watcher:\n"
                  "  auto_add_collectors: [cpumem_collector, io_collector]\n"
                  "  use_rules: false\n"
                  "  cluster_name: htcondor01\n"
                  "  rules_dir: \"" << rules_root.string() << "/condor_jobs\"\n"
                  "  rules_prefix: condor_job_\n";
    }
    Config::instance(config_path.string());

    // Construction with an empty inventory configuration: no watchers, but the
    // maintenance controller still starts and publishes its providers.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-empty");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        check(static_cast<bool>(get_inventory_status_provider()),
              "construction must publish the inventory status provider");
        check(static_cast<bool>(get_inventory_refresh_provider()),
              "construction must publish the on-demand refresh provider");
        check(registry.storeHealth().db_running,
              "construction must leave the store running");

        // stopInventoryDiscovery is idempotent and keeps the status visible
        // while records drain (retraction belongs to finalize).
        registry.stopInventoryDiscovery();
        registry.stopInventoryDiscovery();
        check(static_cast<bool>(get_inventory_status_provider()),
              "the status provider must stay published while draining");

        check(registry.deinit(), "deinit must succeed");
        check(!registry.storeHealth().db_running, "deinit must close the store");
        check(!static_cast<bool>(get_inventory_status_provider()),
              "deinit must retract the status provider");
        check(!static_cast<bool>(get_inventory_refresh_provider()),
              "deinit must retract the refresh provider");
        check(registry.deinit(), "a second deinit must be a no-op that keeps the result");
    }
    // The providers of the previous registry are retracted by its deinit.
    check(!static_cast<bool>(get_inventory_status_provider()),
          "the previous registry must not leave providers installed");

    // A closing record is archived by deinit; the destructor does not need to.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-archive");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        const auto job = make_common_job();
        check(registry.addJob(job), "legacy registration must succeed");
        const uint64_t id = registry.inventoryRecords().front().internal_id;
        check(registry.retireInstance(id), "retire the instance");
        check(!registry.inventoryRecord(id).has_value(),
              "retirement without subscribers archives immediately");

        check(registry.deinit(), "deinit after retirement must succeed");
    }

    // Automatic selection (S5): with auto_add folded on, every created
    // instance receives the configured collectors; a manual override still
    // wins; rule evaluation is covered by the policy object below.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-autopolicy");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        NativeSelector selector;
        selector.adapter = "condor";
        selector.cluster_name = "htcondor01";
        selector.native_job_id = "700.0";
        ExecutionGeneration generation;
        generation.boot_id = "boot";
        generation.manager_pid = 4242;
        generation.manager_start_ticks = 900000;
        Job observation = discovered_job(selector, generation,
                                         {ProcessIdentity{4242, 900000, "boot"}}, true);
        observation.subtype = JobSubType::Condor;

        const auto created = registry.upsertObservedJob(observation);
        check(created.ok && created.disposition == UpsertDisposition::Created,
              "auto policy: the instance must be created");
        const auto binding = registry.selectionRecord(created.internal_id);
        check(binding.mode() == CollectionMode::Auto,
              "auto policy: the selection must be automatic");
        check(binding.automatic.has_value() && binding.automatic->size() == 2 &&
                  binding.automatic->front() == "cpumem_collector",
              "auto policy: defaults mode must add every configured collector");
        check(binding.effectiveCollectors() == *binding.automatic,
              "auto policy: without a manual override the defaults are effective");
        check(binding.automatic_source == "defaults",
              "auto policy: the revision must identify the defaults source");
        check(registry.findJob(created.internal_id)->CollectorNames.size() == 2,
              "auto policy: the derived snapshot must follow the selection");

        // A manual override keeps winning over the automatic decision.
                registry.setCollectorValidator([](const std::string& name) {
            return name == "cpumem_collector" || name == "io_collector";
        });
        CollectionRequest stop;
        stop.targets = {created.internal_id};
        stop.collectors = std::vector<std::string>{};
        check(registry.setCollectors(stop).ok, "auto policy: manual stop must be accepted");
        const auto second = registry.upsertObservedJob(observation);
        check(second.ok, "auto policy: a repeated observation must succeed");
        const auto after = registry.selectionRecord(created.internal_id);
        check(after.mode() == CollectionMode::Manual && after.manual.has_value() &&
                  after.manual->empty(),
              "auto policy: the manual override must survive re-registration");
        check(registry.deinit(), "auto policy: deinit after defaults");
    }

    // An observation whose execution generation is not verified yet must be
    // inventoried but never auto-collected. Completing the generation later is
    // the first complete registration and triggers the selection then.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-incomplete-generation");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        NativeSelector selector;
        selector.adapter = "condor";
        selector.cluster_name = "htcondor01";
        selector.native_job_id = "710.0";
        ExecutionGeneration generation;
        generation.boot_id = "boot";
        generation.manager_pid = 4343;
        generation.manager_start_ticks = 0;  // unknown: not verified
        Job observation = discovered_job(selector, generation,
                                         {ProcessIdentity{4343, 0, "boot"}}, true);
        observation.subtype = JobSubType::Condor;

        const auto created = registry.upsertObservedJob(observation);
        check(created.ok && created.disposition == UpsertDisposition::Created,
              "incomplete generation: the instance must still be inventoried");
        check(!registry.selectionRecord(created.internal_id).automatic.has_value() &&
                  registry.selectionRecord(created.internal_id).effectiveCollectors().empty(),
              "incomplete generation: an unverified execution must not be auto-collected");
        check(registry.findJob(created.internal_id)->CollectorNames.empty(),
              "incomplete generation: no collector may reach the derived snapshot");

        // The observation that completes the generation selects automatically.
        auto completed = observation;
        completed.execution->generation.manager_start_ticks = 910000;
        completed.execution->members.front().start_ticks = 910000;
        const auto updated = registry.upsertObservedJob(completed);
        check(updated.ok && updated.disposition == UpsertDisposition::Updated,
              "incomplete generation: the completing observation must update the record");
        const auto binding = registry.selectionRecord(created.internal_id);
        check(binding.automatic.has_value() && binding.effectiveCollectors().size() == 2,
              "incomplete generation: the completed generation must trigger the "
              "automatic selection");
        check(registry.findJob(created.internal_id)->CollectorNames.size() == 2,
              "incomplete generation: the derived snapshot must follow the late selection");

        check(registry.deinit(), "incomplete generation: deinit");
    }

    // The policy object itself: defaults add the list verbatim, rules decide
    // per job (accept = the rule's collectors, reject = empty) and an error
    // keeps the previous decision. A rejection is authoritative: a later
    // rejecting rule voids the collectors an earlier rule gathered.
    {
        const Job job = make_common_job();

        auto defaults = AutoCollectorPolicy::defaults({"cpumem_collector"});
        std::string revision;
        std::string error;
        const auto selected = defaults.select(job, revision, error);
        check(error.empty() && revision == "defaults" && selected.size() == 1 &&
                  selected.front() == "cpumem_collector",
              "auto policy: defaults must return the configured list verbatim");

        auto rules = AutoCollectorPolicy::rules(
            (rules_root / "condor_accept_only").string(), "condor_job_");
        const auto accepted = rules.select(job, revision, error);
        check(error.empty(), "auto policy: rule evaluation must not fail: " + error);
        check(revision.rfind("rules:", 0) == 0,
              "auto policy: the rules revision must identify the source");
        check(accepted.size() == 1 && accepted.front() == "cpumem_collector",
              "auto policy: an accepting rule must select its collectors");

        // The shared dir accepts with a high-priority rule and rejects with a
        // lower-priority one. The overall verdict must win: an empty
        // selection, not the passing rule's collectors.
        auto mixed = AutoCollectorPolicy::rules(
            (rules_root / "condor_jobs").string(), "condor_job_");
        const auto rejected = mixed.select(job, revision, error);
        check(error.empty() && rejected.empty(),
              "auto policy: a rejecting rule must void earlier collectors");

        auto broken = AutoCollectorPolicy::rules(
            (rules_root / "does-not-exist").string(), "nope_");
        const auto none = broken.select(job, revision, error);
        check(none.empty(), "auto policy: a rules policy with no rules selects nothing");
    }

    // A14/A32: facts durable but the initial intent write failed. The result
    // must say the decision is not committed (not "ok = everything worked"),
    // the instance must stay inventoried with no binding, and a repeated
    // registration (the record-driven repair) must converge.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-intent-failure");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        NativeSelector selector;
        selector.adapter = "condor";
        selector.cluster_name = "htcondor01";
        selector.native_job_id = "720.0";
        ExecutionGeneration generation;
        generation.boot_id = "boot";
        generation.manager_pid = 4545;
        generation.manager_start_ticks = 920000;
        Job job = discovered_job(selector, generation, {ProcessIdentity{4545, 920000, "boot"}},
                                 true);
        job.subtype = JobSubType::Condor;

        // Facts write succeeds, the following intent transaction fails.
        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/1);
        const auto failed = registry.upsertObservedJob(job);
        check(failed.ok, "the facts must stay durable when only the intent write fails");
        check(failed.intent_committed.has_value() && *failed.intent_committed == false,
              "a failed initial intent must be reported as not committed");
        check(failed.code == RegistrationError::InitialIntentWriteFailed,
              "the typed code must identify the intent failure, got " +
                  std::string(to_string(failed.code)));
        check(failed.retry_registration() == false,
              "an intent failure is repaired by record state, never by re-reading metadata");
        {
            const auto binding = registry.selectionRecord(failed.internal_id);
            check(!binding.automatic.has_value(),
                  "a failed intent must leave the automatic decision unrecorded");
            check(registry.inventoryRecord(failed.internal_id).has_value(),
                  "the instance must stay inventoried");
        }

        registry.clearStoreFaultsForTest();
        const auto repaired = registry.upsertObservedJob(job);
        check(repaired.ok, "the repeated registration must succeed");
        check(repaired.internal_id == failed.internal_id,
              "the repair must converge on the same instance");
        check(repaired.intent_committed.has_value() && *repaired.intent_committed,
              "the repeated registration must commit the initial decision");
        check(registry.selectionRecord(failed.internal_id).automatic.has_value(),
              "the repaired automatic decision must be durable");
        registry.deinit();
    }

    // A14 (production entry): the event path and a maintenance pass must repair
    // a missing initial decision, not only the single-upsert entry.
    {
        JobRegistryOptions options;
        options.db_path = fresh_dir(base, "lifecycle-intent-repair");
        options.enable_pid_tracker = false;
        options.register_rpc = false;
        JobRegistry registry(options);

        NativeSelector selector;
        selector.adapter = "condor";
        selector.cluster_name = "htcondor01";
        selector.native_job_id = "730.0";
        ExecutionGeneration generation;
        generation.boot_id = "boot";
        generation.manager_pid = 4646;
        generation.manager_start_ticks = 930000;
        Job job = discovered_job(selector, generation, {ProcessIdentity{4646, 930000, "boot"}},
                                 true);
        job.subtype = JobSubType::Condor;

        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/1);
        const auto failed = registry.upsertObservedJob(job);
        check(failed.ok,
              "the facts must stay durable when only the intent write fails");
        check(failed.intent_committed.has_value() && !*failed.intent_committed,
              "the registration must report the missing initial decision");
        check(failed.code == RegistrationError::InitialIntentWriteFailed,
              "the registration must carry the typed intent failure");

        registry.clearStoreFaultsForTest();
        const auto repaired = registry.upsertObservedJob(job);
        check(repaired.ok, "the repeated registration must succeed");
        check(repaired.intent_committed.has_value() && *repaired.intent_committed,
              "a repeated registration must repair the initial decision");

        // Same obligation, driven by a maintenance pass with no new observation.
        Job job2 = discovered_job(selector, generation, {ProcessIdentity{4646, 930000, "boot"}},
                                  true);
        job2.NativeJobID = "731.0";
        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/1);
        const auto failed2 = registry.upsertObservedJob(job2);
        check(failed2.ok && !*failed2.intent_committed,
              "the second instance must start without a decision");
        registry.clearStoreFaultsForTest();
        registry.runInventoryPass(false);
        const auto repaired2 = registry.selectionRecord(failed2.internal_id);
        check(repaired2.automatic.has_value(),
              "a maintenance pass must repair the missing initial decision");
        registry.deinit();
    }

    pass_case("LIFECYCLE",
              "construction starts inventory, stop/deinit are idempotent and ordered");
    pass_case("AUTOPOLICY",
              "auto_add folding selects collectors on registration; rules decide per job");
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string base =
        argc > 1 ? std::string(argv[1]) : std::string("/tmp/opencode/joblens-lifecycle-test");
    std::filesystem::create_directories(base);
    try {
        run(base + "/data");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAILED: " << error.what() << '\n';
        return 1;
    }
}
