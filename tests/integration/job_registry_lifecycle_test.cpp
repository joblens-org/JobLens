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
#include "core/job_collection_manager.hpp"
#include "core/job_registry.hpp"

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

        ObservedExecution observation;
        observation.selector.adapter = "condor";
        observation.selector.cluster_name = "htcondor01";
        observation.selector.native_job_id = "700.0";
        observation.generation.boot_id = "boot";
        observation.generation.manager_pid = 4242;
        observation.generation.manager_start_ticks = 900000;
        observation.processes.push_back(ProcessIdentity{4242, 900000, "boot"});
        observation.facts.jobtype = JobType::Job;
        observation.facts.subtype = JobSubType::Condor;

        const auto created = registry.upsertObservedJob(observation);
        check(created.ok && created.disposition == UpsertDisposition::Created,
              "auto policy: the instance must be created");
        const auto binding = registry.bindingRecord(created.internal_id);
        check(binding.mode == CollectionMode::Auto,
              "auto policy: the selection must be automatic");
        check(binding.auto_collectors.size() == 2 &&
                  binding.auto_collectors[0] == "cpumem_collector",
              "auto policy: defaults mode must add every configured collector");
        check(binding.effectiveCollectors() == binding.auto_collectors,
              "auto policy: without a manual override the defaults are effective");
        check(binding.policy_revision == "defaults",
              "auto policy: the revision must identify the defaults source");
        check(registry.findJob(created.internal_id)->CollectorNames.size() == 2,
              "auto policy: the derived snapshot must follow the selection");

        // A manual override keeps winning over the automatic decision.
        JobCollectionManager manager(registry);
        manager.setCollectorValidator([](const std::string& name) {
            return name == "cpumem_collector" || name == "io_collector";
        });
        CollectionRequest stop;
        stop.targets = {created.internal_id};
        stop.collectors = std::vector<std::string>{};
        check(manager.setCollectors(stop).ok, "auto policy: manual stop must be accepted");
        const auto second = registry.upsertObservedJob(observation);
        check(second.ok, "auto policy: a repeated observation must succeed");
        const auto after = registry.bindingRecord(created.internal_id);
        check(after.mode == CollectionMode::Manual && after.manual_collectors.empty(),
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

        ObservedExecution observation;
        observation.selector.adapter = "condor";
        observation.selector.cluster_name = "htcondor01";
        observation.selector.native_job_id = "710.0";
        observation.generation.boot_id = "boot";
        observation.generation.manager_pid = 4343;
        observation.generation.manager_start_ticks = 0;  // unknown: not verified
        observation.processes.push_back(ProcessIdentity{4343, 0, "boot"});
        observation.facts.jobtype = JobType::Job;
        observation.facts.subtype = JobSubType::Condor;

        const auto created = registry.upsertObservedJob(observation);
        check(created.ok && created.disposition == UpsertDisposition::Created,
              "incomplete generation: the instance must still be inventoried");
        check(registry.bindingRecord(created.internal_id).revision == 0 &&
                  registry.bindingRecord(created.internal_id).effectiveCollectors().empty(),
              "incomplete generation: an unverified execution must not be auto-collected");
        check(registry.findJob(created.internal_id)->CollectorNames.empty(),
              "incomplete generation: no collector may reach the derived snapshot");

        // The observation that completes the generation selects automatically.
        auto completed = observation;
        completed.generation.manager_start_ticks = 910000;
        completed.processes.front().start_ticks = 910000;
        const auto updated = registry.upsertObservedJob(completed);
        check(updated.ok && updated.disposition == UpsertDisposition::Updated,
              "incomplete generation: the completing observation must update the record");
        const auto binding = registry.bindingRecord(created.internal_id);
        check(binding.revision == 1 && binding.effectiveCollectors().size() == 2,
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
