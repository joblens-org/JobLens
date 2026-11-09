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
                  "    return true, \"cpumem_collector\"\n"
                  "end\n"
                  "return rule\n";
        std::ofstream accept_shared(rules_root / "condor_jobs" / "condor_job_accept.lua");
        accept_shared << "local rule = {}\n"
                         "rule.name = \"accept_cpumem\"\n"
                         "rule.description = \"always accept and collect cpumem\"\n"
                         "rule.priority = 10\n"
                         "function rule.condition(job)\n"
                         "    return true, \"cpumem_collector\"\n"
                         "end\n"
                         "return rule\n";
        std::ofstream reject(rules_root / "condor_jobs" / "condor_job_reject.lua");
        reject << "local rule = {}\n"
                  "rule.name = \"reject_everything\"\n"
                  "rule.description = \"never accepts: later rules may still win\"\n"
                  "rule.priority = 5\n"
                  "function rule.condition(job)\n"
                  "    return false\n"
                  "end\n"
                  "return rule\n";
    }
    // Contract fixtures: one directory per engine-level behaviour.
    {
        std::filesystem::create_directories(rules_root / "condor_contract_table");
        std::ofstream table(rules_root / "condor_contract_table" / "condor_job_table.lua");
        table << "local rule = {}\n"
                 "rule.name = \"table_return\"\n"
                 "rule.priority = 10\n"
                 "function rule.condition(job)\n"
                 "    return {passed = true, collectors = {\"cpumem_collector\"}}\n"
                 "end\n"
                 "return rule\n";
        std::filesystem::create_directories(rules_root / "condor_contract_comma");
        std::ofstream comma(rules_root / "condor_contract_comma" / "condor_job_comma.lua");
        comma << "local rule = {}\n"
                 "rule.name = \"comma_list\"\n"
                 "rule.priority = 10\n"
                 "function rule.condition(job)\n"
                 "    return true, \" io_collector,,cpumem_collector , \"\n"
                 "end\n"
                 "return rule\n";
        std::filesystem::create_directories(rules_root / "condor_contract_second");
        std::ofstream second(rules_root / "condor_contract_second" / "condor_job_second.lua");
        second << "local rule = {}\n"
                  "rule.name = \"bad_second\"\n"
                  "rule.priority = 10\n"
                  "function rule.condition(job)\n"
                  "    return true, 42\n"
                  "end\n"
                  "return rule\n";
        std::filesystem::create_directories(rules_root / "condor_order");
        std::ofstream decline(rules_root / "condor_order" / "condor_job_decline.lua");
        decline << "local rule = {}\n"
                   "rule.name = \"decline_high\"\n"
                   "rule.priority = 10\n"
                   "function rule.condition(job)\n"
                   "    return false\n"
                   "end\n"
                   "return rule\n";
        std::ofstream late(rules_root / "condor_order" / "condor_job_late.lua");
        late << "local rule = {}\n"
                "rule.name = \"accept_low\"\n"
                "rule.priority = 5\n"
                "function rule.condition(job)\n"
                "    return true, \"io_collector\"\n"
                "end\n"
                "return rule\n";
        std::filesystem::create_directories(rules_root / "condor_order_both");
        std::ofstream first(rules_root / "condor_order_both" / "condor_job_first.lua");
        first << "local rule = {}\n"
                 "rule.name = \"accept_first\"\n"
                 "rule.priority = 10\n"
                 "function rule.condition(job)\n"
                 "    return true, \"io_collector\"\n"
                 "end\n"
                 "return rule\n";
        std::ofstream also(rules_root / "condor_order_both" / "condor_job_also.lua");
        also << "local rule = {}\n"
                "rule.name = \"accept_second\"\n"
                "rule.priority = 5\n"
                "function rule.condition(job)\n"
                "    return true, \"cpumem_collector\"\n"
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
    // per job and an error keeps the previous decision. The walk is
    // first-accept-wins: the highest-priority accepting rule selects, a
    // rejecting rule only declines and never vetoes a later accept.
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

        // The shared dir accepts with a high-priority rule and declines with
        // a lower-priority one: the accepting rule wins, its list is the
        // selection and the declining rule cannot veto it.
        auto mixed = AutoCollectorPolicy::rules(
            (rules_root / "condor_jobs").string(), "condor_job_");
        const auto mixed_selected = mixed.select(job, revision, error);
        check(error.empty() && mixed_selected.size() == 1 &&
                  mixed_selected.front() == "cpumem_collector",
              "auto policy: a declining rule must not veto the accepting rule");

        // A high-priority decline followed by a low-priority accept: the walk
        // continues past the decline and the accept selects.
        auto ordered = AutoCollectorPolicy::rules(
            (rules_root / "condor_order").string(), "condor_job_");
        const auto late = ordered.select(job, revision, error);
        check(error.empty() && late.size() == 1 && late.front() == "io_collector",
              "auto policy: the walk must continue past a declining rule");

        // Two accepting rules: only the higher-priority one selects.
        auto both = AutoCollectorPolicy::rules(
            (rules_root / "condor_order_both").string(), "condor_job_");
        const auto first_win = both.select(job, revision, error);
        check(error.empty() && first_win.size() == 1 && first_win.front() == "io_collector",
              "auto policy: the first accepting rule must win exclusively");

        // The collector string is a comma-separated list: trimmed, empty
        // tokens dropped.
        auto comma = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_comma").string(), "condor_job_");
        const auto list = comma.select(job, revision, error);
        check(error.empty() && list.size() == 2 &&
                  ((list[0] == "cpumem_collector" && list[1] == "io_collector") ||
                   (list[0] == "io_collector" && list[1] == "cpumem_collector")),
              "auto policy: the collector string must split on commas");

        // Contract violations are errors, not verdicts: the table format is
        // gone, and a non-string second return rejects the evaluation.
        auto table = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_table").string(), "condor_job_");
        const auto table_selected = table.select(job, revision, error);
        check(!error.empty() && table_selected.empty(),
              "auto policy: a table return must be an evaluation error");
        error.clear();
        auto bad_second = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_second").string(), "condor_job_");
        const auto bad_second_selected = bad_second.select(job, revision, error);
        check(!error.empty() && bad_second_selected.empty(),
              "auto policy: a non-string second return must be an evaluation error");
        error.clear();

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
        registry.reconcileInventoryNow();
        const auto repaired2 = registry.selectionRecord(failed2.internal_id);
        check(repaired2.automatic.has_value(),
              "a maintenance pass must repair the missing initial decision");
        registry.deinit();
    }

    // Manual CRUD upsert (§9.4 merge): one entry point for the manual add and
    // update routes. Identity hit → UPDATE (collectors union, facts fill-only);
    // sub_attr duplicate without identity → UPDATE by internal id (the old
    // duplicate refusal is gone); no hit → CREATE.
    {
        // --- identity route: add the same identity twice -------------------
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-upsert-identity");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);

            NativeSelector selector;
            selector.adapter = "condor";
            selector.cluster_name = "htcondor01";
            selector.native_job_id = "740.0";
            ExecutionGeneration generation;
            generation.boot_id = "boot";
            generation.manager_pid = 4747;
            generation.manager_start_ticks = 940000;
            Job observation = discovered_job(
                selector, generation, {ProcessIdentity{4747, 940000, "boot"}}, true);
            observation.subtype = JobSubType::Condor;
            observation.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(observation.sub_attr).cluster_id = 740;
            std::get<CondorJobAttr>(observation.sub_attr).proc_id = 0;

            const auto created = registry.upsertObservedJob(observation);
            check(created.ok && created.disposition == UpsertDisposition::Created,
                  "upsert: the inventoried instance must exist");

            Job manual;
            manual.jobtype = JobType::Job;
            manual.subtype = JobSubType::Condor;
            manual.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(manual.sub_attr).cluster_id = 740;
            std::get<CondorJobAttr>(manual.sub_attr).proc_id = 0;
            manual.CollectorNames = {"cpumem_collector"};

            const auto first =
                registry.upsertJob(manual, nlohmann::json::object(), selector);
            check(first["status"] == "success",
                  "upsert: the identity route must succeed: " + first.dump());
            check(first["JobID"] == created.internal_id,
                  "upsert: the identity route must resolve the existing id");
            const uint64_t revision_first = first["record_revision"];
            {
                const auto binding = registry.selectionRecord(created.internal_id);
                check(binding.mode() == CollectionMode::Manual &&
                          binding.manual.has_value() &&
                          *binding.manual == std::vector<std::string>{"cpumem_collector"},
                      "upsert: the first manual add must set the manual selection");
            }

            manual.CollectorNames = {"io_collector"};
            const auto second =
                registry.upsertJob(manual, nlohmann::json::object(), selector);
            check(second["status"] == "success",
                  "upsert: the repeated identity add must succeed: " + second.dump());
            check(second["JobID"] == created.internal_id,
                  "upsert: the repeated add must keep the same instance");
            const uint64_t revision_second = second["record_revision"];
            check(revision_second > revision_first,
                  "upsert: the second add must advance the record revision");
            {
                const auto binding = registry.selectionRecord(created.internal_id);
                check(binding.manual.has_value() && binding.manual->size() == 2,
                      "upsert: the second add must UNION into the manual selection, "
                      "not replace it");
            }

            // Empty collectors on UPDATE = a pure facts update: valid, and the
            // selection is untouched.
            manual.CollectorNames.clear();
            const auto facts_only =
                registry.upsertJob(manual, nlohmann::json::object(), selector);
            check(facts_only["status"] == "success",
                  "upsert: an empty-collector update must stay valid: " + facts_only.dump());
            {
                const auto binding = registry.selectionRecord(created.internal_id);
                check(binding.manual.has_value() && binding.manual->size() == 2,
                      "upsert: a facts-only update must not touch the selection");
            }
            registry.deinit();
        }

        // --- sub_attr route: the duplicate refusal became an update --------
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-upsert-subattr");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);

            // A Condor-typed manual row (the Common matcher matches nothing by
            // definition); no identity selector is passed, so only the typed
            // facts route the request.
            Job manual;
            manual.jobtype = JobType::Job;
            manual.subtype = JobSubType::Condor;
            manual.NativeJobID = "810.0";
            manual.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(manual.sub_attr).cluster_id = 810;
            std::get<CondorJobAttr>(manual.sub_attr).proc_id = 0;
            manual.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(manual), "upsert: the manual create must succeed");
            const uint64_t created_id = registry.inventoryRecords().front().internal_id;
            const uint64_t revision_before =
                registry.inventoryRecord(created_id)->revision;

            Job repeat = manual;
            repeat.CollectorNames = {"io_collector"};
            const auto updated =
                registry.upsertJob(repeat, nlohmann::json::object(), std::nullopt);
            check(updated["status"] == "success",
                  "upsert: a sub_attr duplicate must update, not error: " + updated.dump());
            check(updated["JobID"] == created_id,
                  "upsert: the sub_attr route must resolve the existing id: " +
                      updated.dump());
            check(registry.inventoryRecords().size() == 1,
                  "upsert: the sub_attr route must not create a second record");
            const auto binding = registry.selectionRecord(created_id);
            check(binding.manual.has_value() && binding.manual->size() == 2 &&
                      (*binding.manual)[0] == "cpumem_collector",
                  "upsert: the sub_attr update must union into the manual selection");
            check(registry.inventoryRecord(created_id)->revision > revision_before,
                  "upsert: the sub_attr update must advance the revision");
            registry.deinit();
        }

        // --- no hit: CREATE keeps the legacy semantics ---------------------
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-upsert-create");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);

            Job manual = make_common_job();
            manual.NativeJobID = "820.0";
            const auto created = registry.upsertJob(manual, nlohmann::json::object());
            check(created["status"] == "success",
                  "upsert: a fresh manual add must create: " + created.dump());
            check(registry.inventoryRecords().size() == 1,
                  "upsert: the create branch must persist one record");

            Job empty_collectors = manual;
            empty_collectors.NativeJobID = "830.0";
            empty_collectors.CollectorNames.clear();
            const auto refused =
                registry.upsertJob(empty_collectors, nlohmann::json::object());
            check(refused["status"] == "error",
                  "upsert: empty collectors on CREATE must stay rejected");
            check(registry.inventoryRecords().size() == 1,
                  "upsert: the rejected create must persist nothing");
            registry.deinit();
        }
    }

    pass_case("LIFECYCLE",
              "construction starts inventory, stop/deinit are idempotent and ordered");
    pass_case("UPSERT",
              "manual upsert: identity/sub_attr routes update with collector union, "
              "a fresh add creates, create still rejects empty collectors");
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
