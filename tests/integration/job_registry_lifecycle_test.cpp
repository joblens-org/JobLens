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
/* Registry lifecycle acceptance:
 *
 *   * construction is the single assembly point: the store opens, the
 *     inventory heartbeat starts (providers published) even with no
 *     configured discovery type;
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
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>

// Exercise the actual two-stage update with both selections made before
// either mutation, without scheduling assumptions or production hooks.
struct JobRegistryNamespaceTestAccess {
    static void installSnapshotBarrier(JobRegistry& registry, uint64_t id,
                                       std::promise<void>& captured,
                                       std::shared_future<void> release)
    {
        registry.store_->setRowProvider([&registry, id, &captured, release](uint64_t current) {
            auto row = registry.inventoryRecord(current);
            if (current == id) {
                captured.set_value();
                release.wait();
            }
            return row;
        });
    }
    static void restoreProvider(JobRegistry& registry)
    {
        registry.store_->setRowProvider([&registry](uint64_t id) {
            return registry.inventoryRecord(id);
        });
    }
    static bool pass(JobRegistry& registry) { return registry.runPass(false, true); }
    static bool mutationLocked(JobRegistry& registry)
    {
        if (!registry.mutation_mtx_.try_lock()) return true;
        registry.mutation_mtx_.unlock();
        return false;
    }
    static std::optional<JobRecord> durableByIdentity(JobRegistry& registry,
                                                       const SchedulerKey& key)
    {
        return registry.store_->readRowByIdentity(key);
    }
    static JobRecord select(JobRegistry& registry, const Job& job)
    {
        auto target = registry.manualUpdateTarget(job, std::nullopt);
        if (target.kind != JobRegistry::ManualTarget::Kind::Update)
            throw std::runtime_error("namespace adoption: expected an update target");
        return target.record;
    }
    static nlohmann::json update(JobRegistry& registry, const JobRecord& target, const Job& job)
    {
        return registry.updateManualInstance(target, job, nlohmann::json::object());
    }
    static std::pair<nlohmann::json, nlohmann::json> updatePreselected(
        JobRegistry& registry, const Job& a, const Job& b)
    {
        auto first = registry.manualUpdateTarget(a, std::nullopt);
        auto second = registry.manualUpdateTarget(b, std::nullopt);
        if (first.kind != JobRegistry::ManualTarget::Kind::Update ||
            second.kind != JobRegistry::ManualTarget::Kind::Update ||
            first.record.job.JobID != second.record.job.JobID)
            throw std::runtime_error("namespace race: both claims must preselect the same row");
        auto first_result = registry.updateManualInstance(first.record, a, nlohmann::json::object());
        auto second_result = registry.updateManualInstance(second.record, b, nlohmann::json::object());
        return {std::move(first_result), std::move(second_result)};
    }
};

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
    set_pids(job, {getpid()});
    job.CollectorNames = {"cpumem_collector"};
    CommonJobAttr attr;
    attr.auto_update_child = false;
    job.sub_attr = attr;
    return job;
}

void manual_readd_preserves_creation_metadata(const std::string& base)
{
    JobRegistryOptions options;
    options.db_path = fresh_dir(base, "manual-frozen-metadata");
    options.test_static_inventory = true;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    JobRegistry registry(options);
    registry.setCollectorValidator(nullptr);
    NativeSelector selector{"condor", "pool", "schedd", "185.0", ""};
    Job observation = discovered_job(
        selector, ProcessIdentity{4747, 940000, "boot"},
        {ProcessIdentity{4747, 940000, "boot"}}, true);
    auto& original_attr = std::get<CondorJobAttr>(observation.sub_attr);
    original_attr.owner = "alice";
    original_attr.slots_cgroup_path = "/original/cgroup";
    const auto created = registry.upsertObservedJob(observation);
    check(created.ok, "manual freeze: the discovery fixture must register");
    const auto initial = registry.inventoryRecord(created.internal_id);
    const auto writes = registry.storeHealth().durable_commits;

    Job request = observation;
    request.jobtype = JobType::Sys;
    request.NativeJobID = "00185.0";
    request.execution.members = {{9999, 1234, "forged-boot"}};
    auto& changed_attr = std::get<CondorJobAttr>(request.sub_attr);
    changed_attr.owner = "replacement-owner";
    changed_attr.slots_cgroup_path = "/replacement/cgroup";
    changed_attr.auto_update_child = true;
    const auto noop = registry.upsertJob(request, nlohmann::json::object(), selector);
    const auto kept = registry.inventoryRecord(created.internal_id);
    check(noop.value("status", "") == "success" && kept && initial &&
              kept->revision == initial->revision &&
              registry.storeHealth().durable_commits == writes,
          "metadata-only manual re-add must be a write-free no-op");
    check(kept->job.NativeJobID == "185.0" && kept->job.jobtype == JobType::Job &&
              std::get<CondorJobAttr>(kept->job.sub_attr).owner == "alice" &&
              std::get<CondorJobAttr>(kept->job.sub_attr).slots_cgroup_path == "/original/cgroup" &&
              !std::get<CondorJobAttr>(kept->job.sub_attr).auto_update_child &&
              kept->job.execution.members == initial->job.execution.members &&
              kept->index_key == initial->index_key,
          "manual re-add must preserve metadata, identity and execution evidence");

    request.CollectorNames = {"cpumem_collector"};
    const auto selected = registry.upsertJob(request, nlohmann::json::object(), selector);
    const auto selected_record = registry.inventoryRecord(created.internal_id);
    check(selected.value("status", "") == "success" && selected_record &&
              selected_record->revision == initial->revision + 1 &&
              std::get<CondorJobAttr>(selected_record->job.sub_attr).owner == "alice" &&
              registry.selectionRecord(created.internal_id).manual ==
                  std::optional<std::vector<std::string>>(std::vector<std::string>{"cpumem_collector"}),
          "manual re-add may change selection without a separate metadata revision");
    const auto repeated = registry.upsertJob(request, nlohmann::json::object(), selector);
    check(repeated.value("status", "") == "success" &&
              registry.inventoryRecord(created.internal_id)->revision == selected_record->revision,
          "repeating the same manual selection must not create a metadata write");

    registry.setKernelCleanupForTest([](uint64_t, const Job&, std::string& error) {
        error = "leave the row closing for the stale-target check";
        return false;
    });
    check(!registry.retireInstance(created.internal_id),
          "manual freeze: failed cleanup must retain a closing row");
    request.CollectorNames.clear();
    const auto closing = JobRegistryNamespaceTestAccess::update(registry, *initial, request);
    check(closing.value("status", "") == "error" &&
              registry.inventoryRecord(created.internal_id)->state == RecordState::Closing &&
              std::get<CondorJobAttr>(registry.inventoryRecord(created.internal_id)->job.sub_attr).owner == "alice",
          "a stale manual target must not accept a closing metadata-only re-add");
    registry.setKernelCleanupForTest(nullptr);
    check(registry.deinit(), "manual freeze: cleanup must complete after restoring the seam");
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
        std::filesystem::create_directories(rules_root / "condor_contract_boolonly");
        std::ofstream boolonly(rules_root / "condor_contract_boolonly" / "condor_job_bare.lua");
        boolonly << "local rule = {}\n"
                   "rule.name = \"accept_bare\"\n"
                   "rule.priority = 10\n"
                   "function rule.condition(job)\n"
                   "    return true\n"
                   "end\n"
                   "return rule\n";
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
    // heartbeat still starts and publishes its providers.
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
        const uint64_t id = registry.inventoryRecords().front().job.JobID;
        check(registry.retireInstance(id), "retire the instance");
        check(!registry.inventoryRecord(id).has_value(),
              "retirement without subscribers archives immediately");

        check(registry.deinit(), "deinit after retirement must succeed");
    }

    // Automatic selection: with auto_add armed, every created
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
        ProcessIdentity generation;
        generation.boot_id = "boot";
        generation.pid = 4242;
        generation.start_ticks = 900000;
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
        check(registry.selectionRecord(created.internal_id).effectiveCollectors().size() == 2,
              "auto policy: the derived snapshot must follow the selection");
        check(registry.inventoryRecord(created.internal_id)->job.CollectorNames.empty(),
              "the record image must not carry the dispatch list");

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
        ProcessIdentity generation;
        generation.boot_id = "boot";
        generation.pid = 4343;
        generation.start_ticks = 0;  // unknown: not verified
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
        completed.execution.manager.start_ticks = 910000;
        completed.execution.members.front().start_ticks = 910000;
        const auto updated = registry.upsertObservedJob(completed);
        check(updated.ok && updated.disposition == UpsertDisposition::Updated,
              "incomplete generation: the completing observation must update the record");
        const auto binding = registry.selectionRecord(created.internal_id);
        check(binding.automatic.has_value() && binding.effectiveCollectors().size() == 2,
              "incomplete generation: the completed generation must trigger the "
              "automatic selection");
        check(registry.selectionRecord(created.internal_id).effectiveCollectors().size() == 2,
              "incomplete generation: the derived snapshot must follow the late selection");

        check(registry.deinit(), "incomplete generation: deinit");
    }

    // The policy object itself: defaults add the list verbatim, rules decide
    // per job and an error keeps the previous decision. The walk is
    // first-accept-wins: the highest-priority accepting rule selects, a
    // rejecting rule only declines and never vetoes a later accept. A rule
    // that accepts without naming collectors falls back to the configured
    // defaults; with no defaults the decision is refused as an error.
    {
        const Job job = make_common_job();

        auto defaults = AutoCollectorPolicy::defaults({"cpumem_collector"});
        std::string revision;
        std::string error;
        const auto selected = defaults.select(job, revision, error);
        check(error.empty() && revision == "defaults" && selected.accepted &&
                  selected.collectors.size() == 1 &&
                  selected.collectors.front() == "cpumem_collector",
              "auto policy: defaults must return the configured list verbatim");

        auto rules = AutoCollectorPolicy::rules(
            (rules_root / "condor_accept_only").string(), "condor_job_");
        const auto accepted = rules.select(job, revision, error);
        check(error.empty(), "auto policy: rule evaluation must not fail: " + error);
        check(revision.rfind("rules:", 0) == 0,
              "auto policy: the rules revision must identify the source");
        check(accepted.accepted && accepted.collectors.size() == 1 &&
                  accepted.collectors.front() == "cpumem_collector",
              "auto policy: an accepting rule must select its collectors");

        // The shared dir accepts with a high-priority rule and declines with
        // a lower-priority one: the accepting rule wins, its list is the
        // selection and the declining rule cannot veto it.
        auto mixed = AutoCollectorPolicy::rules(
            (rules_root / "condor_jobs").string(), "condor_job_");
        const auto mixed_selected = mixed.select(job, revision, error);
        check(error.empty() && mixed_selected.accepted &&
                  mixed_selected.collectors.size() == 1 &&
                  mixed_selected.collectors.front() == "cpumem_collector",
              "auto policy: a declining rule must not veto the accepting rule");

        // A high-priority decline followed by a low-priority accept: the walk
        // continues past the decline and the accept selects.
        auto ordered = AutoCollectorPolicy::rules(
            (rules_root / "condor_order").string(), "condor_job_");
        const auto late = ordered.select(job, revision, error);
        check(error.empty() && late.accepted && late.collectors.size() == 1 &&
                  late.collectors.front() == "io_collector",
              "auto policy: the walk must continue past a declining rule");

        // Two accepting rules: only the higher-priority one selects.
        auto both = AutoCollectorPolicy::rules(
            (rules_root / "condor_order_both").string(), "condor_job_");
        const auto first_win = both.select(job, revision, error);
        check(error.empty() && first_win.accepted && first_win.collectors.size() == 1 &&
                  first_win.collectors.front() == "io_collector",
              "auto policy: the first accepting rule must win exclusively");

        // The collector string is a comma-separated list: trimmed, empty
        // tokens dropped.
        auto comma = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_comma").string(), "condor_job_");
        const auto list = comma.select(job, revision, error);
        check(error.empty() && list.accepted && list.collectors.size() == 2 &&
                  ((list.collectors[0] == "cpumem_collector" &&
                    list.collectors[1] == "io_collector") ||
                   (list.collectors[0] == "io_collector" &&
                    list.collectors[1] == "cpumem_collector")),
              "auto policy: the collector string must split on commas");

        // A rule that accepts without naming collectors: the configured
        // defaults become the selection.
        auto bare = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_boolonly").string(), "condor_job_",
            {"cpumem_collector", "io_collector"});
        const auto fallback = bare.select(job, revision, error);
        check(error.empty() && fallback.accepted && fallback.collectors.size() == 2,
              "auto policy: a bare accept must fall back to the configured defaults");

        // The same bare accept without configured defaults: the decision is
        // refused as an error, not silently turned into a reject.
        auto bare_no_defaults = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_boolonly").string(), "condor_job_");
        const auto refused = bare_no_defaults.select(job, revision, error);
        check(!error.empty() && !refused.accepted && refused.collectors.empty(),
              "auto policy: a bare accept without defaults must be refused");
        error.clear();

        // Contract violations are errors, not verdicts: the table format is
        // gone, and a non-string second return rejects the evaluation.
        auto table = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_table").string(), "condor_job_");
        const auto table_selected = table.select(job, revision, error);
        check(!error.empty() && !table_selected.accepted && table_selected.collectors.empty(),
              "auto policy: a table return must be an evaluation error");
        error.clear();
        auto bad_second = AutoCollectorPolicy::rules(
            (rules_root / "condor_contract_second").string(), "condor_job_");
        const auto bad_second_selected = bad_second.select(job, revision, error);
        check(!error.empty() && !bad_second_selected.accepted &&
                  bad_second_selected.collectors.empty(),
              "auto policy: a non-string second return must be an evaluation error");
        error.clear();

        auto broken = AutoCollectorPolicy::rules(
            (rules_root / "does-not-exist").string(), "nope_");
        const auto none = broken.select(job, revision, error);
        check(!none.accepted && none.collectors.empty(),
              "auto policy: a rules policy with no rules selects nothing");
    }

    // /facts durable but the initial intent write failed. The result
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
        ProcessIdentity generation;
        generation.boot_id = "boot";
        generation.pid = 4545;
        generation.start_ticks = 920000;
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

    // production entry: the event path and a maintenance pass must repair
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
        ProcessIdentity generation;
        generation.boot_id = "boot";
        generation.pid = 4646;
        generation.start_ticks = 930000;
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

    // Manual CRUD upsert (merge): one entry point for the manual add and
    // update routes. Identity hit → UPDATE (collectors union, metadata frozen);
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
            ProcessIdentity generation;
            generation.boot_id = "boot";
            generation.pid = 4747;
            generation.start_ticks = 940000;
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

            // Empty collectors on UPDATE are a valid no-op; both the metadata
            // snapshot and selection stay untouched.
            manual.CollectorNames.clear();
            const auto facts_only =
                registry.upsertJob(manual, nlohmann::json::object(), selector);
            check(facts_only["status"] == "success",
                  "upsert: an empty-collector update must stay valid: " + facts_only.dump());
            {
                const auto binding = registry.selectionRecord(created.internal_id);
                check(binding.manual.has_value() && binding.manual->size() == 2,
                      "upsert: an empty re-add must not touch the selection");
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
            const uint64_t created_id = registry.inventoryRecords().front().job.JobID;
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

        // --- schedd discrimination: the same native id, another scheduler --
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-upsert-schedd");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);

            // Two manual rows of one native id, one per schedd. A manual add
            // carries no cluster_name, so the rows resolve no identity and
            // only the typed facts plus the scheduler (schedd) attribution
            // route them.
            const auto manual_condor = [](const char* name, const char* host) {
                Job job;
                job.jobtype = JobType::Job;
                job.subtype = JobSubType::Condor;
                job.NativeJobID = "172.0";
                job.sub_attr = CondorJobAttr{};
                std::get<CondorJobAttr>(job.sub_attr).cluster_id = 172;
                std::get<CondorJobAttr>(job.sub_attr).proc_id = 0;
                if (name != nullptr) {
                    std::get<CondorJobAttr>(job.sub_attr).scheduler_name = name;
                    std::get<CondorJobAttr>(job.sub_attr).scheduler_host = host;
                }
                job.CollectorNames = {"cpumem_collector"};
                return job;
            };
            check(registry.addJob(manual_condor("schedd-a", "sa.example.com")),
                  "schedd: the schedd-a create must succeed");
            check(registry.addJob(manual_condor("schedd-b", "sb.example.com")),
                  "schedd: the same native id under another schedd must create, "
                  "not refuse as a sub_attr duplicate");
            check(registry.inventoryRecords().size() == 2,
                  "schedd: two schedds must persist two records");
            const auto row_of = [&registry](const char* schedd) {
                for (const auto& record : registry.inventoryRecords()) {
                    const auto* attr = std::get_if<CondorJobAttr>(&record.job.sub_attr);
                    if (attr != nullptr && attr->scheduler_name == schedd)
                        return record;
                }
                throw std::runtime_error(std::string("schedd: no row for ") + schedd);
            };
            const uint64_t id_a = row_of("schedd-a").job.JobID;
            const uint64_t id_b = row_of("schedd-b").job.JobID;
            check(std::get<CondorJobAttr>(row_of("schedd-b").job.sub_attr).scheduler_name ==
                      "schedd-b",
                  "schedd: the created row must keep the claimed scheduler attribution");

            // Same-schedd duplicate: the historical CREATE refusal stays.
            check(!registry.addJob(manual_condor("schedd-b", "sb.example.com")),
                  "schedd: a same-schedd duplicate add must stay refused");
            check(registry.inventoryRecords().size() == 2,
                  "schedd: the refused duplicate must persist nothing");

            // A schedd-b claim routes to schedd-b's row only; schedd-a's
            // selection is a different job's and must stay untouched.
            Job claim_b = manual_condor("schedd-b", "sb.example.com");
            claim_b.CollectorNames = {"io_collector"};
            const auto updated_b =
                registry.upsertJob(claim_b, nlohmann::json::object(), std::nullopt);
            check(updated_b["status"] == "success" && updated_b["JobID"] == id_b,
                  "schedd: the schedd-b claim must update schedd-b's row: " +
                      updated_b.dump());
            {
                const auto binding_a = registry.selectionRecord(id_a);
                check(binding_a.manual.has_value() &&
                          *binding_a.manual == std::vector<std::string>{"cpumem_collector"},
                      "schedd: the schedd-a selection must stay untouched");
                const auto binding_b = registry.selectionRecord(id_b);
                check(binding_b.manual.has_value() && binding_b.manual->size() == 2,
                      "schedd: the schedd-b claim must union into schedd-b's selection");
            }
            Job claim_a = manual_condor("schedd-a", "sa.example.com");
            claim_a.CollectorNames = {"io_collector"};
            const auto updated_a =
                registry.upsertJob(claim_a, nlohmann::json::object(), std::nullopt);
            check(updated_a["status"] == "success" && updated_a["JobID"] == id_a,
                  "schedd: the schedd-a claim must update schedd-a's row: " +
                      updated_a.dump());

            // A legacy request with no scheduler attribution matches both
            // rows: an ambiguity the caller must resolve, never a guess.
            Job bare = manual_condor(nullptr, nullptr);
            bare.CollectorNames = {"io_collector"};
            const auto ambiguous =
                registry.upsertJob(bare, nlohmann::json::object(), std::nullopt);
            check(ambiguous["status"] == "error" &&
                      ambiguous["code"] == "ambiguous_selector",
                  "schedd: a bare native id across two schedds must be ambiguous: " +
                      ambiguous.dump());
            check(ambiguous["candidates"].size() == 2,
                  "schedd: the ambiguity must list both rows: " + ambiguous.dump());
            check(registry.inventoryRecords().size() == 2,
                  "schedd: the ambiguous route must create nothing");

            // An existing unknown namespace is frozen too. A later schedd
            // claim must not adopt it or change the selection as a side effect.
            Job legacy;
            legacy.jobtype = JobType::Job;
            legacy.subtype = JobSubType::Condor;
            legacy.NativeJobID = "180.0";
            legacy.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(legacy.sub_attr).cluster_id = 180;
            std::get<CondorJobAttr>(legacy.sub_attr).proc_id = 0;
            legacy.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(legacy),
                  "schedd: the attribution-less legacy row must create");
            const auto legacy_row = [&registry]() {
                for (const auto& record : registry.inventoryRecords()) {
                    const auto* attr = std::get_if<CondorJobAttr>(&record.job.sub_attr);
                    if (attr != nullptr && attr->cluster_id == 180) return record;
                }
                throw std::runtime_error("schedd: no legacy 180.0 row");
            }();
            Job claim_legacy = legacy;
            std::get<CondorJobAttr>(claim_legacy.sub_attr).scheduler_name = "schedd-a";
            std::get<CondorJobAttr>(claim_legacy.sub_attr).scheduler_host = "sa.example.com";
            claim_legacy.CollectorNames = {"io_collector"};
            const auto tolerated =
                registry.upsertJob(claim_legacy, nlohmann::json::object(), std::nullopt);
            check(tolerated["status"] == "error" &&
                      tolerated.value("code", "") == "namespace_conflict",
                  "schedd: a claim cannot adopt an unknown creation namespace: " +
                      tolerated.dump());
            const auto unchanged_legacy = registry.inventoryRecord(legacy_row.job.JobID);
            check(unchanged_legacy &&
                      std::get<CondorJobAttr>(unchanged_legacy->job.sub_attr).scheduler_name.empty() &&
                      registry.selectionRecord(legacy_row.job.JobID).manual ==
                          std::optional<std::vector<std::string>>(
                              std::vector<std::string>{"cpumem_collector"}) &&
                      registry.inventoryRecords().size() == 3,
                  "schedd: a rejected adoption must leave metadata, selection and row count unchanged");
            registry.deinit();
        }

        // --- schedd vs a resolved row with a serialization gap -------------
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-upsert-schedd-identity");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);

            // A discovered row with a resolved identity whose typed
            // scheduler_name predates serialization (the field sits outside
            // the old list): the "@schedd-a" namespace tag is the same fact's
            // durable form and must still discriminate.
            NativeSelector selector;
            selector.adapter = "condor";
            selector.cluster_name = "htcondor01";
            selector.cluster_tag = "schedd-a";
            selector.native_job_id = "172.0";
            ProcessIdentity generation;
            generation.boot_id = "boot";
            generation.pid = 4747;
            generation.start_ticks = 940000;
            Job observation = discovered_job(
                selector, generation, {ProcessIdentity{4747, 940000, "boot"}}, true);
            observation.subtype = JobSubType::Condor;
            observation.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(observation.sub_attr).cluster_id = 172;
            std::get<CondorJobAttr>(observation.sub_attr).proc_id = 0;
            const auto created = registry.upsertObservedJob(observation);
            check(created.ok && created.disposition == UpsertDisposition::Created,
                  "schedd: the resolved 172.0 instance must exist");
            const uint64_t resolved_id = created.internal_id;

            const auto manual_claim = [](const char* name, const char* host) {
                Job job;
                job.jobtype = JobType::Job;
                job.subtype = JobSubType::Condor;
                job.NativeJobID = "172.0";
                job.sub_attr = CondorJobAttr{};
                std::get<CondorJobAttr>(job.sub_attr).cluster_id = 172;
                std::get<CondorJobAttr>(job.sub_attr).proc_id = 0;
                if (name != nullptr) {
                    std::get<CondorJobAttr>(job.sub_attr).scheduler_name = name;
                    std::get<CondorJobAttr>(job.sub_attr).scheduler_host = host;
                }
                job.CollectorNames = {"cpumem_collector"};
                return job;
            };

            // Another schedd's claim must not adopt the resolved row: it
            // creates beside it and the resolved row stays untouched.
            const auto claim_b =
                registry.upsertJob(manual_claim("schedd-b", "sb.example.com"),
                                   nlohmann::json::object(), std::nullopt);
            check(claim_b["status"] == "success",
                  "schedd: another schedd must create beside the resolved row: " +
                      claim_b.dump());
            check(registry.inventoryRecords().size() == 2,
                  "schedd: the schedd-b claim must persist its own record");
            check(!registry.selectionRecord(resolved_id).manual.has_value(),
                  "schedd: the resolved schedd-a row must stay untouched by the "
                  "schedd-b claim");

            // The agreeing claim adopts the resolved row through the typed
            // fallback: the namespace tag agrees with the request.
            const auto claim_a =
                registry.upsertJob(manual_claim("schedd-a", "sa.example.com"),
                                   nlohmann::json::object(), std::nullopt);
            check(claim_a["status"] == "success" && claim_a["JobID"] == resolved_id,
                  "schedd: the agreeing claim must update the resolved row: " +
                      claim_a.dump());
            check(registry.inventoryRecords().size() == 2,
                  "schedd: the agreeing claim must not create a second record");
            {
                const auto binding = registry.selectionRecord(resolved_id);
                check(binding.manual.has_value() &&
                          *binding.manual == std::vector<std::string>{"cpumem_collector"},
                      "schedd: the agreeing claim must set the manual selection");
            }
            registry.deinit();
        }

        // Schema-4 delimiter aliases must not merge distinct observations.
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-namespace-alias");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            NativeSelector selector;
            selector.adapter = "condor";
            selector.cluster_name = "a";
            selector.cluster_tag = "b@c";
            selector.native_job_id = "902.0";
            ProcessIdentity generation{4747, 940000, "boot"};
            Job first = discovered_job(selector, generation,
                                       {ProcessIdentity{4747, 940000, "boot"}}, true);
            check(registry.upsertObservedJob(first).ok,
                  "namespace: first aliased observation must create");
            selector.cluster_name = "a@b";
            selector.cluster_tag = "c";
            Job second = discovered_job(selector, generation,
                                        {ProcessIdentity{4747, 940000, "boot"}}, true);
            const auto collision = registry.upsertObservedJob(second);
            check(!collision.ok && collision.error == "identity_index_mismatch" &&
                      collision.message.find("aliases") != std::string::npos,
                  "namespace: aliased observation must fail closed explicitly");
            check(registry.inventoryRecords().size() == 1,
                  "namespace: collision must not create or replace a row");
            const auto held = registry.inventoryRecords().front().job;
            check(held.cluster_name == "a" && held.clusterTag == "b@c",
                  "namespace: collision must not overwrite the original tuple");
            registry.deinit();
        }

        // Equal native IDs under distinct routing labels must not be merged.
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-namespace-pools");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job first;
            first.jobtype = JobType::Job;
            first.subtype = JobSubType::Condor;
            first.NativeJobID = "901.0";
            first.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(first.sub_attr).cluster_id = 901;
            std::get<CondorJobAttr>(first.sub_attr).collector_host = "pool-a";
            std::get<CondorJobAttr>(first.sub_attr).scheduler_name = "schedd@submit";
            first.cluster_name = "pool-a";
            first.clusterTag = "schedd@submit";
            first.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(first), "namespace: first pool must create");
            Job second = first;
            second.cluster_name = "pool-b";
            std::get<CondorJobAttr>(second.sub_attr).collector_host = "pool-b";
            check(registry.addJob(second),
                  "namespace: distinct pools must not be deduplicated");
            check(!registry.addJob(second),
                  "namespace: identical complete identity must deduplicate");
            const auto rows = registry.inventoryRecords();
            uint64_t second_id = 0;
            for (const auto& row : rows)
                if (row.job.cluster_name == "pool-b") second_id = row.job.JobID;
            check(second_id != 0, "namespace: second pool row exists");
            NativeSelector selector;
            selector.adapter = "condor";
            selector.cluster_name = "pool-b";
            selector.cluster_tag = "schedd@submit";
            selector.native_job_id = "901.0";
            second.CollectorNames = {"io_collector"};
            const auto update = registry.upsertJob(second, nlohmann::json::object(), selector);
            check(update.value("status", "") == "success" && update["JobID"] == second_id,
                  "namespace: exact complete selector updates only its pool");
            second.CollectorNames = {"cpumem_collector"};
            std::get<CondorJobAttr>(second.sub_attr).scheduler_host = "other-host";
            check(!registry.addJob(second),
                  "namespace: execute/scheduler host does not distinguish identity");
            registry.deinit();
        }

        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-namespace-manual-alias");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job a;
            a.jobtype = JobType::Job;
            a.subtype = JobSubType::Condor;
            a.NativeJobID = "903.0";
            a.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(a.sub_attr).cluster_id = 903;
            std::get<CondorJobAttr>(a.sub_attr).collector_host = "a";
            std::get<CondorJobAttr>(a.sub_attr).scheduler_name = "b@c";
            a.cluster_name = "a";
            a.clusterTag = "b@c";
            a.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(a), "namespace: first manual alias creates");
            Job b = a;
            b.cluster_name = "a@b";
            b.clusterTag = "c";
            std::get<CondorJobAttr>(b.sub_attr).collector_host = "a@b";
            std::get<CondorJobAttr>(b.sub_attr).scheduler_name = "c";
            check(!registry.addJob(b), "namespace: manual key alias fails closed");
            NativeSelector selector{"condor", "a@b", "c", "903.0", ""};
            const auto result = registry.upsertJob(b, nlohmann::json::object(), selector);
            check(result.value("status", "") == "error" &&
                      result.value("code", "") == "ambiguous_selector",
                  "namespace: manual alias must not update existing owner");
            check(registry.inventoryRecords().size() == 1,
                  "namespace: manual alias leaves one row");
            registry.deinit();
        }

        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-namespace-slurm");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job a;
            a.jobtype = JobType::Job;
            a.subtype = JobSubType::Slurm;
            a.NativeJobID = "904";
            a.cluster_name = "cluster-a";
            a.sub_attr = SlurmJobAttr{};
            std::get<SlurmJobAttr>(a.sub_attr).job_id = 904;
            std::get<SlurmJobAttr>(a.sub_attr).cluster_name = "cluster-a";
            a.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(a), "namespace: slurm cluster-a creates");
            Job b = a;
            b.cluster_name = "cluster-b";
            std::get<SlurmJobAttr>(b.sub_attr).cluster_name = "cluster-b";
            check(registry.addJob(b), "namespace: slurm cluster-b creates separately");
            check(!registry.addJob(b), "namespace: identical slurm create deduplicates");
            uint64_t id_b = 0;
            for (const auto& row : registry.inventoryRecords())
                if (row.job.cluster_name == "cluster-b") id_b = row.job.JobID;
            NativeSelector selector{"slurm", "cluster-b", "", "904", ""};
            b.CollectorNames = {"io_collector"};
            const auto updated = registry.upsertJob(b, nlohmann::json::object(), selector);
            check(id_b != 0 && updated.value("status", "") == "success" &&
                      updated["JobID"] == id_b,
                  "namespace: exact slurm cluster updates the matching row");
            Job conflict = b;
            std::get<SlurmJobAttr>(conflict.sub_attr).cluster_name = "cluster-a";
            check(!registry.addJob(conflict), "namespace: inconsistent cluster facts fail");
            registry.deinit();
        }

        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-namespace-incomplete");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job a;
            a.jobtype = JobType::Job;
            a.subtype = JobSubType::Condor;
            a.NativeJobID = "906.0";
            a.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(a.sub_attr).cluster_id = 906;
            std::get<CondorJobAttr>(a.sub_attr).collector_host = "pool-a";
            a.cluster_name = "pool-a";
            a.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(a), "namespace: partial first pool creates");
            Job b = a;
            b.cluster_name = "pool-b";
            std::get<CondorJobAttr>(b.sub_attr).collector_host = "pool-b";
            check(registry.addJob(b),
                  "namespace: known pool conflict excludes unknown schedd fallback");
            Job unknown = a;
            unknown.cluster_name.clear();
            std::get<CondorJobAttr>(unknown.sub_attr).collector_host.clear();
            const auto ambiguous = registry.upsertJob(unknown, nlohmann::json::object());
            check(ambiguous.value("code", "") == "ambiguous_selector" &&
                      ambiguous["candidates"].size() == 2,
                  "namespace: unknown pool cannot select one compatible row");
            Job c = a;
            c.NativeJobID = "907.0";
            std::get<CondorJobAttr>(c.sub_attr).cluster_id = 907;
            std::get<CondorJobAttr>(c.sub_attr).scheduler_name = "schedd-a@submit";
            check(registry.addJob(c), "namespace: typed schedd-a creates");
            Job d = c;
            std::get<CondorJobAttr>(d.sub_attr).scheduler_name = "schedd-b@submit";
            check(registry.addJob(d),
                  "namespace: typed schedd-b creates separately in same pool");
            registry.deinit();
        }

        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-namespace-restart");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            uint64_t saved = 0;
            {
                JobRegistry registry(options);
                Job job;
                job.jobtype = JobType::Job;
                job.subtype = JobSubType::Condor;
                job.NativeJobID = "905.0";
                job.cluster_name = "pool";
                job.clusterTag = "schedd#one@submit";
                job.sub_attr = CondorJobAttr{};
                auto& attr = std::get<CondorJobAttr>(job.sub_attr);
                attr.cluster_id = 905;
                attr.scheduler_name = job.clusterTag;
                job.CollectorNames = {"cpumem_collector"};
                check(registry.addJob(job), "namespace: restart fixture creates");
                saved = registry.inventoryRecords().front().job.JobID;
                registry.deinit();
            }
            {
                JobRegistry registry(options);
                const auto restored = registry.inventoryRecord(saved);
                check(restored.has_value(), "namespace: restart restores row");
                check(restored->job.clusterTag == "schedd#one@submit" &&
                          std::get<CondorJobAttr>(restored->job.sub_attr).scheduler_name ==
                              "schedd#one@submit",
                      "namespace: restart preserves intact structured schedd");
                Job request = restored->job;
                request.JobID = 0;
                request.CollectorNames = {"io_collector"};
                const auto updated = registry.upsertJob(request, nlohmann::json::object());
                check(updated.value("status", "") == "success" && updated["JobID"] == saved,
                      "namespace: intact schedd matches after restart without reparsing");
                registry.deinit();
            }
        }

        // Two stale selections of one unknown legacy namespace must not let
        // the second claim overwrite the first or add its collectors.
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-namespace-stale-claim");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job legacy;
            legacy.jobtype = JobType::Job;
            legacy.subtype = JobSubType::Condor;
            legacy.NativeJobID = "908.0";
            legacy.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(legacy.sub_attr).cluster_id = 908;
            legacy.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(legacy), "namespace race: legacy row creates");
            const auto id = registry.inventoryRecords().front().job.JobID;
            Job a = legacy;
            std::get<CondorJobAttr>(a.sub_attr).scheduler_name = "schedd-a@submit";
            Job b = legacy;
            std::get<CondorJobAttr>(b.sub_attr).scheduler_name = "schedd-b@submit";
            b.CollectorNames = {"io_collector"};
            const auto results = JobRegistryNamespaceTestAccess::updatePreselected(registry, a, b);
            check(results.first.value("status", "") == "error" &&
                      results.first.value("code", "") == "namespace_conflict",
                  "namespace freeze: the first claim cannot adopt an unknown namespace");
            check(results.second.value("status", "") == "error" &&
                      results.second.value("code", "") == "namespace_conflict",
                  "namespace race: stale conflicting second claim fails");
            const auto row = registry.inventoryRecord(id);
            check(row && std::get<CondorJobAttr>(row->job.sub_attr).scheduler_name.empty(),
                  "namespace freeze: both rejected claims leave the initial namespace unknown");
            const auto selection = registry.selectionRecord(id);
            check(selection.manual &&
                      *selection.manual == std::vector<std::string>{"cpumem_collector"},
                  "namespace race: rejected claim must not change collectors");

            const auto same = JobRegistryNamespaceTestAccess::updatePreselected(registry, legacy, legacy);
            check(same.first.value("status", "") == "success" &&
                      same.second.value("status", "") == "success",
                  "namespace race: matching stale claims remain compatible");

            Job other = legacy;
            other.NativeJobID = "909.0";
            std::get<CondorJobAttr>(other.sub_attr).cluster_id = 909;
            check(registry.addJob(other), "namespace race: selector-only fixture creates");
            uint64_t other_id = 0;
            for (const auto& candidate : registry.inventoryRecords())
                if (candidate.job.NativeJobID == "909.0") other_id = candidate.job.JobID;
            NativeSelector selector{"condor", "pool-a", "schedd-a@submit", "909.0", ""};
            const auto selected = registry.upsertJob(other, nlohmann::json::object(), selector);
            const auto selected_row = registry.inventoryRecord(other_id);
            check(selected.value("status", "") == "error" &&
                      selected.value("code", "") == "namespace_conflict" && selected_row &&
                      selected_row->job.cluster_name.empty() && selected_row->job.clusterTag.empty(),
                  "namespace freeze: a selector cannot rewrite an existing namespace");

            Job pool_legacy = legacy;
            pool_legacy.NativeJobID = "910.0";
            std::get<CondorJobAttr>(pool_legacy.sub_attr).cluster_id = 910;
            check(registry.addJob(pool_legacy), "namespace race: pool fixture creates");
            Job pool_a = pool_legacy;
            Job pool_b = pool_legacy;
            pool_a.cluster_name = "pool-a";
            std::get<CondorJobAttr>(pool_a.sub_attr).collector_host = "pool-a";
            pool_b.cluster_name = "pool-b";
            std::get<CondorJobAttr>(pool_b.sub_attr).collector_host = "pool-b";
            pool_b.CollectorNames = {"io_collector"};
            const auto pool_results =
                JobRegistryNamespaceTestAccess::updatePreselected(registry, pool_a, pool_b);
            check(pool_results.first.value("code", "") == "namespace_conflict" &&
                      pool_results.second.value("code", "") == "namespace_conflict",
                  "namespace race: stale conflicting pool claim fails");

            Job slurm;
            slurm.jobtype = JobType::Job;
            slurm.subtype = JobSubType::Slurm;
            slurm.NativeJobID = "911";
            slurm.sub_attr = SlurmJobAttr{};
            std::get<SlurmJobAttr>(slurm.sub_attr).job_id = 911;
            slurm.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(slurm), "namespace race: slurm fixture creates");
            Job slurm_a = slurm;
            Job slurm_b = slurm;
            std::get<SlurmJobAttr>(slurm_a.sub_attr).cluster_name = "cluster-a";
            std::get<SlurmJobAttr>(slurm_b.sub_attr).cluster_name = "cluster-b";
            slurm_b.CollectorNames = {"io_collector"};
            const auto slurm_results =
                JobRegistryNamespaceTestAccess::updatePreselected(registry, slurm_a, slurm_b);
            check(slurm_results.first.value("code", "") == "namespace_conflict" &&
                      slurm_results.second.value("code", "") == "namespace_conflict",
                  "namespace race: stale conflicting slurm cluster claim fails");
            registry.deinit();
        }

        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-adoption-occupied");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job legacy;
            legacy.jobtype = JobType::Job;
            legacy.subtype = JobSubType::Condor;
            legacy.NativeJobID = "912.0";
            legacy.cluster_name = "pool";
            legacy.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(legacy.sub_attr).cluster_id = 912;
            legacy.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(legacy), "adoption occupied: legacy create");
            const auto target = JobRegistryNamespaceTestAccess::select(registry, legacy);
            NativeSelector selector{"condor", "pool", "schedd@submit", "912.0", ""};
            ProcessIdentity generation{4912, 9912, "boot"};
            auto observation = discovered_job(selector, generation,
                                               {ProcessIdentity{4912, 9912, "boot"}}, true);
            const auto owner = registry.upsertObservedJob(observation);
            check(owner.ok && owner.internal_id != target.job.JobID,
                  "adoption occupied: other owner creates");
            const auto old_before = registry.inventoryRecord(target.job.JobID);
            const auto owner_before = registry.inventoryRecord(owner.internal_id);
            const auto owner_selection = registry.selectionRecord(owner.internal_id);
            Job claim = legacy;
            claim.clusterTag = selector.cluster_tag;
            std::get<CondorJobAttr>(claim.sub_attr).scheduler_name = selector.cluster_tag;
            claim.CollectorNames = {"io_collector"};
            const auto rejected = JobRegistryNamespaceTestAccess::update(registry, target, claim);
            check(rejected.value("status", "") == "error",
                  "adoption occupied: stale update must reject occupied key");
            const auto old = registry.inventoryRecord(target.job.JobID);
            const auto held = registry.inventoryRecord(owner.internal_id);
            check(old && held && old_before && owner_before &&
                      old->revision == old_before->revision && held->revision == owner_before->revision &&
                      old->job.clusterTag.empty() &&
                      held->job.clusterTag == "schedd@submit" &&
                      registry.inventoryRecords().size() == 2,
                  "adoption occupied: both rows remain unchanged");
            check(registry.selectionRecord(target.job.JobID).manual ==
                      std::optional<std::vector<std::string>>(
                          std::vector<std::string>{"cpumem_collector"}),
                  "adoption occupied: rejected collectors remain unchanged");
            check(registry.selectionRecord(owner.internal_id).manual == owner_selection.manual,
                  "adoption occupied: owner collectors remain unchanged");
            NativeSelector old_selector{"condor", "pool", "", "912.0", ""};
            const auto old_identity = canonicalize(old_selector);
            const auto new_identity = canonicalize(selector);
            check(old_identity && new_identity, "adoption occupied: valid test identities");
            const auto old_index = registry.inventoryRecordByKey(*old_identity);
            const auto owner_index = registry.inventoryRecordByKey(*new_identity);
            check(old_index && owner_index && old_index->job.JobID == target.job.JobID &&
                      owner_index->job.JobID == owner.internal_id,
                  "adoption occupied: both index owners remain unchanged");
            registry.deinit();
            JobRegistry reopened(options);
            const auto old_reopened = reopened.inventoryRecordByKey(*old_identity);
            const auto owner_reopened = reopened.inventoryRecordByKey(*new_identity);
            check(old_reopened && owner_reopened &&
                      old_reopened->job.JobID == target.job.JobID &&
                      owner_reopened->job.JobID == owner.internal_id,
                  "adoption occupied: durable index owners survive restart");
            reopened.deinit();
        }

        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-adoption-observation");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job legacy;
            legacy.jobtype = JobType::Job;
            legacy.subtype = JobSubType::Condor;
            legacy.NativeJobID = "913.0";
            legacy.cluster_name = "pool";
            legacy.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(legacy.sub_attr).cluster_id = 913;
            legacy.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(legacy), "adoption observed: legacy create");
            const auto id = registry.inventoryRecords().front().job.JobID;
            NativeSelector selector{"condor", "pool", "schedd@submit", "913.0", ""};
            Job claim = legacy;
            claim.CollectorNames.clear();
            const auto sync_before = registry.storeHealth().sync_commits;
            const auto adopted = registry.upsertJob(claim, nlohmann::json::object(), selector);
            check(adopted.value("status", "") == "error" &&
                      adopted.value("code", "") == "namespace_conflict",
                  "namespace freeze: a selector-only update cannot move the identity");
            check(registry.storeHealth().sync_commits == sync_before,
                  "adoption observed: index adoption must not fsync");
            NativeSelector old_selector{"condor", "pool", "", "913.0", ""};
            const auto old_identity = canonicalize(old_selector);
            const auto new_identity = canonicalize(selector);
            check(old_identity && new_identity &&
                      registry.inventoryRecordByKey(*old_identity) &&
                      registry.inventoryRecordByKey(*old_identity)->job.JobID == id &&
                      !registry.inventoryRecordByKey(*new_identity),
                  "namespace freeze: a rejected claim leaves the old identity index intact");
            ProcessIdentity generation{4913, 9913, "boot"};
            auto observation = discovered_job(selector, generation,
                                               {ProcessIdentity{4913, 9913, "boot"}}, true);
            const auto result = registry.upsertObservedJob(observation);
            check(result.ok && result.internal_id != id && registry.inventoryRecords().size() == 2,
                  "namespace freeze: a distinct complete identity must create its own record");
            registry.deinit();
            JobRegistry reopened(options);
            const auto restored = reopened.inventoryRecordByKey(*new_identity);
            check(restored && restored->job.JobID == result.internal_id &&
                      reopened.inventoryRecordByKey(*old_identity) &&
                      reopened.inventoryRecordByKey(*old_identity)->job.JobID == id,
                  "namespace freeze: both original identities survive restart");
            reopened.deinit();
        }

        for (const auto mode : {StoreFaultMode::FailBeforeWrite, StoreFaultMode::FailAfterWrite}) {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, mode == StoreFaultMode::FailBeforeWrite
                                                  ? "lifecycle-selection-fail-before"
                                                  : "lifecycle-selection-fail-after");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            JobRegistry registry(options);
            Job legacy;
            legacy.jobtype = JobType::Job;
            legacy.subtype = JobSubType::Condor;
            legacy.NativeJobID = "914.0";
            legacy.cluster_name = "pool";
            legacy.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(legacy.sub_attr).cluster_id = 914;
            legacy.CollectorNames = {"cpumem_collector"};
            check(registry.addJob(legacy), "selection recovery: legacy create");
            const auto id = registry.inventoryRecords().front().job.JobID;
            Job claim = legacy;
            claim.CollectorNames = {"io_collector"};
            NativeSelector selector{"condor", "pool", "", "914.0", ""};
            registry.injectStoreFaultForTest(mode);
            const auto failed = registry.upsertJob(claim, nlohmann::json::object(), selector);
            check(failed.value("status", "") == "error",
                  "selection recovery: injected write failure is reported");
            const auto retried = registry.upsertJob(claim, nlohmann::json::object(), selector);
            check(retried.value("status", "") == "success" && retried["JobID"] == id,
                  "selection recovery: retry resolves original ID");
            const auto identity = canonicalize(selector);
            check(identity && registry.inventoryRecordByKey(*identity) &&
                      registry.inventoryRecordByKey(*identity)->job.JobID == id &&
                      registry.inventoryRecords().size() == 1,
                  "selection recovery: row and index resolve together");
            check(registry.selectionRecord(id).manual ==
                      std::optional<std::vector<std::string>>(std::vector<std::string>{"cpumem_collector", "io_collector"}) &&
                      registry.inventoryRecord(id)->job.clusterTag.empty(),
                  "selection recovery: retry changes only collectors, never the namespace");
            registry.deinit();
        }

        // Pause a real maintenance checkpoint after it snapshots an old row.
        // The selection update either completes before release or blocks on
        // checkpoint serialization (fixed code), without scheduler timing.
        {
            JobRegistryOptions options;
            options.db_path = fresh_dir(base, "lifecycle-adoption-checkpoint");
            options.enable_pid_tracker = false;
            options.register_rpc = false;
            options.test_heartbeat = true;
            JobRegistry registry(options);
            NativeSelector old_selector{"condor", "pool", "", "915.0", ""};
            NativeSelector new_selector{"condor", "pool", "schedd@submit", "915.0", ""};
            ProcessIdentity generation{4915, 9915, "boot"};
            auto old_job = discovered_job(old_selector, generation,
                                          {ProcessIdentity{4915, 9915, "boot"}}, true);
            old_job.subtype = JobSubType::Condor;
            old_job.sub_attr = CondorJobAttr{};
            std::get<CondorJobAttr>(old_job.sub_attr).cluster_id = 915;
            const auto original = registry.upsertObservedJob(old_job);
            check(original.ok, "checkpoint selection: dirty pool-only row creates");

            std::promise<void> captured;
            std::promise<void> release;
            auto captured_future = captured.get_future();
            JobRegistryNamespaceTestAccess::installSnapshotBarrier(
                registry, original.internal_id, captured, release.get_future().share());
            bool pass_ok = false;
            std::thread checkpoint([&] { pass_ok = JobRegistryNamespaceTestAccess::pass(registry); });
            captured_future.wait();
            const bool serialized = JobRegistryNamespaceTestAccess::mutationLocked(registry);
            Job claim = old_job;
            claim.CollectorNames = {"io_collector"};
            std::promise<void> adoption_started;
            auto started = adoption_started.get_future();
            nlohmann::json adopted;
            std::thread adopter([&] {
                adoption_started.set_value();
                adopted = registry.upsertJob(claim, nlohmann::json::object(), old_selector);
            });
            started.wait();
            if (!serialized) adopter.join();
            release.set_value();
            checkpoint.join();
            if (serialized) adopter.join();
            JobRegistryNamespaceTestAccess::restoreProvider(registry);
            check(pass_ok && adopted.value("status", "") == "success" &&
                      adopted["JobID"] == original.internal_id,
                  "checkpoint selection: both operations complete on original ID");
            check(registry.deinit(), "checkpoint selection: clean shutdown");

            JobRegistry reopened(options);
            const auto old_identity = canonicalize(old_selector);
            const auto new_identity = canonicalize(new_selector);
            check(old_identity && new_identity, "checkpoint selection: valid identities");
            const auto durable = JobRegistryNamespaceTestAccess::durableByIdentity(reopened, *old_identity);
            check(durable && durable->index_key == old_identity->index_key() &&
                      durable->job.clusterTag.empty() &&
                      !JobRegistryNamespaceTestAccess::durableByIdentity(reopened, *new_identity) &&
                      reopened.selectionRecord(original.internal_id).manual ==
                          std::optional<std::vector<std::string>>(std::vector<std::string>{"io_collector"}),
                  "checkpoint selection: the frozen identity and new selection must survive restart");
            auto new_job = old_job;
            const auto observed = reopened.upsertObservedJob(new_job);
            check(observed.ok && observed.internal_id == original.internal_id &&
                      reopened.inventoryRecords().size() == 1,
                  "checkpoint selection: observation must reuse original ID after restart");
            reopened.deinit();
            pass_case("CHECKPOINT_NAMESPACE", "checkpoint and selection preserve frozen row/index across restart");
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
              "the sub_attr route discriminates by the subtype's scheduler (schedd) "
              "with frozen creation namespaces, a fresh add creates, "
              "create still rejects empty collectors");
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
        manual_readd_preserves_creation_metadata(base + "/data");
        run(base + "/data");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAILED: " << error.what() << '\n';
        return 1;
    }
}
