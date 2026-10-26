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
/* Deterministic inventory-store acceptance (task 2 of
 * doc/job_inventory_collection_decoupling_plan.md).
 *
 *   A04 One complete upsert with zero bindings, repeat and fact update:
 *       one entry, stable ID/instance handle, revision advances only on real
 *       fact change, nothing activated.
 *   A19 Legacy restore with non-empty CollectorNames: inventory appears
 *       without any lifecycle subscriber, identity index built, selection
 *       migrated to a manual binding.
 *   A20 Interrupted migration resumes idempotently with preserved IDs;
 *       conflicting live identities are quarantined, never merged.
 *   A21 Injected LevelDB write failures: no false durable success, no ID
 *       reuse, health records the failure.
 *
 * Fault-injection mechanism (honest and deterministic): JobRegistry exposes
 * injectStoreFaultForTest(mode, skip, count), a seam around its single durable
 * WriteBatch helper. FailBeforeWrite returns an IOError status without calling
 * LevelDB::Write (a write that never landed). FailAfterWrite performs the real
 * DB::Write and then reports failure, modelling LevelDB's ambiguous outcome
 * where the batch may be durable while the caller sees an error. `skip`
 * selects which durable write fails, so a test can fail the registration batch
 * after the allocator reservation succeeded.
 *
 * Each scenario uses its own temporary LevelDB directory.
 */
#include "core/job_collection_manager.hpp"
#include "core/job_registry.hpp"

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
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

JobRegistryOptions isolated(const std::string& path, size_t migration_batch_limit = 0)
{
    JobRegistryOptions options;
    options.db_path = path;
    options.enable_inventory_maintenance = false;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    options.migration_batch_limit = migration_batch_limit;
    return options;
}

std::string hex64(uint64_t value)
{
    char buffer[17];
    std::snprintf(buffer, sizeof(buffer), "%016llx",
                  static_cast<unsigned long long>(value));
    return std::string(buffer);
}

NativeSelector condor_selector(const std::string& native = "172.0",
                               const std::string& cluster = "htcondor01",
                               const std::string& tag = "schedd-a")
{
    NativeSelector selector;
    selector.scheduler_type = SchedulerType::Condor;
    selector.cluster_name = cluster;
    selector.cluster_tag = tag;
    selector.native_job_id = native;
    return selector;
}

ObservedExecution condor_observation(const std::string& native = "172.0",
                                     const std::string& cluster = "htcondor01",
                                     const std::string& tag = "schedd-a")
{
    ObservedExecution observation;
    observation.selector = condor_selector(native, cluster, tag);
    observation.generation.boot_id = "boot-aaa";
    observation.generation.manager_pid = 4242;
    observation.generation.manager_start_ticks = 900000;
    observation.generation.cgroup_identity = "/sys/fs/cgroup/htcondor/slot1";
    observation.processes.push_back(ProcessIdentity{4242, 900000, "boot-aaa"});

    CondorJobAttr attr{};
    attr.auto_update_child = false;
    const auto dot = native.find('.');
    attr.cluster_id = std::stoul(native.substr(0, dot));
    attr.proc_id = std::stoul(native.substr(dot + 1));
    attr.starter_pid = 4242;
    attr.owner = "alice";
    attr.slots_cgroup_path = "/sys/fs/cgroup/htcondor/slot1";
    attr.scheduler_host = "htcondor01";

    observation.facts.jobtype = JobType::Job;
    observation.facts.subtype = JobSubType::Condor;
    observation.facts.sub_attr = attr;
    return observation;
}

nlohmann::json legacy_condor_record(const std::string& native,
                                    const std::string& cluster,
                                    const std::string& tag,
                                    const std::vector<std::string>& collectors,
                                    pid_t starter_pid)
{
    CondorJobAttr attr{};
    attr.auto_update_child = false;
    const auto dot = native.find('.');
    attr.cluster_id = std::stoul(native.substr(0, dot));
    attr.proc_id = std::stoul(native.substr(dot + 1));
    attr.starter_pid = starter_pid;
    attr.owner = "alice";
    attr.slots_cgroup_path = "/sys/fs/cgroup/htcondor/slot1";
    attr.scheduler_host = cluster;

    nlohmann::json record;
    record["jobtype"] = static_cast<int>(JobType::Job);
    record["subtype"] = static_cast<int>(JobSubType::Condor);
    record["pids"] = nlohmann::json::array();
    record["collectors"] = collectors;
    record["native_job_id"] = native;
    record["cluster_name"] = cluster;
    record["clusterTag"] = tag;
    record["sub_attr"] = attr;
    return record;
}

void write_raw_db(const std::string& path,
                  const std::vector<std::pair<std::string, std::string>>& entries)
{
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB* db = nullptr;
    const auto opened = leveldb::DB::Open(options, path, &db);
    check(opened.ok(), "open raw fixture db: " + opened.ToString());
    {
        leveldb::WriteBatch batch;
        for (const auto& [key, value] : entries) batch.Put(key, value);
        const auto written = db->Write(leveldb::WriteOptions(), &batch);
        check(written.ok(), "write raw fixture db: " + written.ToString());
    }
    delete db;
}

// A04 -----------------------------------------------------------------------

void upsert_is_idempotent_and_activates_nothing(const std::string& base)
{
    const auto path = fresh_dir(base, "a04");
    JobRegistry registry(isolated(path));

    const auto observation = condor_observation();
    const auto created = registry.upsertObservedJob(observation);
    check(created.ok, "complete observation must be accepted: " + created.error);
    check(created.disposition == UpsertDisposition::Created,
          "first observation must report created");
    check(created.internal_id > 0, "created inventory must carry an internal id");
    check(created.instance_id.rfind("ji1-", 0) == 0,
          "created inventory must carry an opaque instance handle");
    check(created.facts_revision == 1, "first revision must be 1");

    {
        const auto record = registry.inventoryRecord(created.internal_id);
        check(record.has_value(), "inventory record must be readable");
        check(record->identity_complete, "canonicalizable identity must be complete");
        check(record->identity.key() == canonicalize(observation.selector)->key(),
              "record must carry the canonical identity");
        check(record->selector.native_job_id == "172.0",
              "original native string must be preserved");
    }

    {
        const auto job = registry.findJob(created.internal_id);
        check(job.has_value(), "registered inventory must be findable");
        check(job->CollectorNames.empty(),
              "an observation upsert must not activate collection");
        check(job->JobPIDs.size() == 1 && job->JobPIDs.front() == 4242,
              "observed processes must become facts");
    }
    check(registry.bindingRecord(created.internal_id).revision == 0,
          "no binding intent may be created by an observation");
    check(registry.inventoryRecords().size() == 1, "exactly one inventory entry");

    const auto repeat = registry.upsertObservedJob(observation);
    check(repeat.ok && repeat.disposition == UpsertDisposition::Unchanged,
          "repeat observation must be unchanged");
    check(repeat.internal_id == created.internal_id, "repeat must keep the internal id");
    check(repeat.instance_id == created.instance_id, "repeat must keep the instance handle");
    check(repeat.facts_revision == created.facts_revision,
          "unchanged facts must not advance the revision");
    check(registry.inventoryRecords().size() == 1, "repeat must not duplicate the entry");

    auto updated_source = observation;
    auto& attr = std::get<CondorJobAttr>(updated_source.facts.sub_attr);
    attr.owner = "bob";
    updated_source.processes.push_back(ProcessIdentity{4343, 910000, "boot-aaa"});

    const auto updated = registry.upsertObservedJob(updated_source);
    check(updated.ok && updated.disposition == UpsertDisposition::Updated,
          "changed facts must report updated");
    check(updated.internal_id == created.internal_id,
          "fact updates must not change the internal id");
    check(updated.instance_id == created.instance_id,
          "fact updates must not change the instance handle");
    check(updated.facts_revision == created.facts_revision + 1,
          "changed facts must advance the revision exactly once");

    {
        const auto job = registry.findJob(created.internal_id);
        check(job.has_value(), "updated inventory must stay findable");
        check(std::get<CondorJobAttr>(job->sub_attr).owner == "bob",
              "attribute changes must merge into facts");
        check(job->JobPIDs.size() == 2, "new processes must merge into facts");
        check(job->CollectorNames.empty(),
              "fact updates must not activate collection either");
    }

    const auto repeat_update = registry.upsertObservedJob(updated_source);
    check(repeat_update.disposition == UpsertDisposition::Unchanged,
          "repeating the same update must be unchanged");

    // Incomplete identity is an explicit rejection, never a new record.
    auto incomplete = condor_observation("172.0");
    incomplete.selector.cluster_name.clear();
    const auto rejected = registry.upsertObservedJob(incomplete);
    check(!rejected.ok, "an incomplete identity must be rejected");
    check(rejected.error == "incomplete_identity",
          "incomplete identity must report a stable code, got " + rejected.error);
    check(rejected.internal_id == 0, "rejected observations must not allocate an id");
    check(registry.inventoryRecords().size() == 1,
          "rejected observations must not create records");
}

// A19 -----------------------------------------------------------------------

void legacy_restore_migrates_inventory_and_selection(const std::string& base)
{
    const auto path = fresh_dir(base, "a19");
    const uint64_t active_id = 41;
    const uint64_t history_id = 7;

    write_raw_db(path, {
        {"j:" + hex64(active_id),
         legacy_condor_record("172.0", "htcondor01", "schedd-a",
                              {"cpumem_collector"}, 4100).dump()},
        {"h:" + hex64(history_id) + ":" + hex64(1234567),
         legacy_condor_record("9.0", "htcondor01", "schedd-a", {}, 900).dump()},
        {"c:job_id", "41"},
    });

    JobRegistry registry(isolated(path));
    check(registry.migrationComplete(), "legacy db must migrate to schema 2");

    const auto health = registry.storeHealth();
    check(health.schema_version == 2, "migrated schema version must be 2");
    check(health.migration_state == "complete", "migration state must be complete");
    check(!health.db_uuid.empty(), "migration must persist an Agent DB UUID");
    check(health.active_instances == 1, "migration must restore the active record");

    // Inventory appears independently of any lifecycle-callback subscriber.
    const auto job = registry.findJob(active_id);
    check(job.has_value(), "restored inventory must be readable without a subscriber");

    const auto identity = canonicalize(condor_selector("172.0"));
    check(identity.has_value(), "fixture selector must canonicalize");
    const auto by_identity = registry.inventoryRecordByIdentity(*identity);
    check(by_identity.has_value(), "migration must build the identity index");
    check(by_identity->internal_id == active_id,
          "migration must preserve the existing internal id");
    check(by_identity->instance_id.rfind("ji1-", 0) == 0,
          "restored records must expose an instance handle");

    const auto binding = registry.bindingRecord(active_id);
    check(binding.mode == CollectionMode::Manual,
          "legacy non-empty CollectorNames must migrate to a manual binding");
    check(binding.manual_collectors == std::vector<std::string>{"cpumem_collector"},
          "migrated manual selection must preserve the legacy names");
    check(binding.revision == 1, "migrated binding starts at revision 1");
    check(binding.policy_revision == "legacy",
          "legacy origin must not be reported as an automatic policy");

    // The allocator high-water mark covers active, history and counter values.
    const auto next = registry.getNextJobID();
    check(next > active_id, "allocator must stay above restored active ids");
    check(next > history_id, "allocator must stay above restored history ids");
}

// A20 -----------------------------------------------------------------------

void interrupted_migration_resumes_idempotently(const std::string& base)
{
    const auto path = fresh_dir(base, "a20-resume");
    std::vector<uint64_t> ids;
    for (uint64_t index = 0; index < 70; ++index) ids.push_back(100 + index);

    std::vector<std::pair<std::string, std::string>> entries;
    for (size_t index = 0; index < ids.size(); ++index) {
        const auto native = std::to_string(1000 + index) + ".0";
        entries.emplace_back("j:" + hex64(ids[index]),
                             legacy_condor_record(native, "htcondor01", "schedd-a",
                                                  {}, 1000 + static_cast<pid_t>(index)).dump());
    }
    entries.emplace_back("c:job_id", "100");
    write_raw_db(path, entries);

    {
        JobRegistry interrupted(isolated(path, /*migration_batch_limit=*/1));
        check(!interrupted.migrationComplete(),
              "batch-limited migration must stop before advertising readiness");
        check(interrupted.storeHealth().migration_state.rfind("in_progress", 0) == 0,
              "interrupted migration must persist in-progress state");
        check(interrupted.inventoryRecords().empty(),
              "inventory must not be published before migration completes");
    }

    {
        JobRegistry resumed(isolated(path));
        check(resumed.migrationComplete(), "migration must resume and complete");
        check(resumed.inventoryRecords().size() == ids.size(),
              "resume must preserve every record exactly once");
        for (const auto id : ids) {
            const auto record = resumed.inventoryRecord(id);
            check(record.has_value(), "resume must preserve internal id " + std::to_string(id));
            check(record->identity_complete, "resume must build complete identities");
            check(record->facts_revision == 1, "migration must not fork revisions");
        }
    }

    {
        JobRegistry reopened(isolated(path));
        check(reopened.migrationComplete(), "migrated db must stay migrated");
        check(reopened.inventoryRecords().size() == ids.size(),
              "reopening a migrated db must not duplicate records");
        check(reopened.storeHealth().quarantined_identities == 0,
              "clean migration must not quarantine anything");
    }
}

void conflicting_live_identities_are_quarantined(const std::string& base)
{
    const auto path = fresh_dir(base, "a20-conflict");
    const uint64_t first_id = 61;
    const uint64_t second_id = 62;

    write_raw_db(path, {
        {"j:" + hex64(first_id),
         legacy_condor_record("172.0", "htcondor01", "schedd-a", {}, 6100).dump()},
        {"j:" + hex64(second_id),
         legacy_condor_record("172.0", "htcondor01", "schedd-a", {}, 6200).dump()},
        {"c:job_id", "62"},
    });

    JobRegistry registry(isolated(path));
    check(registry.migrationComplete(), "conflicting db must still migrate");

    const auto records = registry.inventoryRecords();
    check(records.size() == 2, "conflicting identities must not be merged");
    check(registry.findJob(first_id).has_value() && registry.findJob(second_id).has_value(),
          "both conflicting internal ids must be preserved");

    const auto identity = canonicalize(condor_selector("172.0"));
    const auto indexed = registry.inventoryRecordByIdentity(*identity);
    check(indexed.has_value(), "one conflict survivor must own the identity index");
    check(indexed->internal_id == first_id,
          "the earliest internal id must win the identity index deterministically");

    size_t quarantined = 0;
    for (const auto& record : records) quarantined += record.quarantined ? 1 : 0;
    check(quarantined == 1, "exactly one conflicting record must be quarantined");
    check(registry.storeHealth().quarantined_identities == 1,
          "health must expose the quarantined identity count");

    // A quarantined record is not eligible for identity resolution.
    const auto conflict = registry.upsertObservedJob(condor_observation("172.0"));
    check(conflict.ok, "upsert must resolve to the indexed survivor");
    check(conflict.internal_id == first_id,
          "upsert must never merge into the quarantined record");
    check(conflict.disposition != UpsertDisposition::Created,
          "an observation must not create a third record for a known identity");
    check(registry.inventoryRecords().size() == 2,
          "upsert must not duplicate the quarantined record");
}

// Restore validation (supports A03/A20) --------------------------------------

void stale_generation_is_not_replayed(const std::string& base)
{
    const auto path = fresh_dir(base, "a20-stale");
    const pid_t manager_pid = 2000001;   // no such process
    uint64_t internal_id = 0;

    {
        JobRegistry registry(isolated(path));
        auto observation = condor_observation("172.0");
        observation.generation = ExecutionGeneration{
            "boot-does-not-exist", manager_pid, 123456, ""};
        observation.processes = {
            ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"}};
        const auto created = registry.upsertObservedJob(observation);
        check(created.ok, "registration with a generation must succeed");
        internal_id = created.internal_id;
    }

    {
        JobRegistry registry(isolated(path));
        const auto record = registry.inventoryRecord(internal_id);
        check(record.has_value(), "the record must still be restored");
        check(record->state == "suspect",
              "a stale execution generation must be marked suspect on restore");
        const auto job = registry.findJob(internal_id);
        check(job.has_value() && job->JobPIDs.empty(),
              "stale PIDs must not be replayed into the registry");

        // The same generation observed again recovers the record; a different
        // generation is a conflict, not a revival (covered by A22).
        auto same = condor_observation("172.0");
        same.generation = ExecutionGeneration{"boot-does-not-exist", manager_pid, 123456, ""};
        same.processes = {ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"}};
        const auto recovered = registry.upsertObservedJob(same);
        check(recovered.ok && recovered.internal_id == internal_id,
              "the same generation must resolve to the restored record");
        const auto active = registry.inventoryRecord(internal_id);
        check(active.has_value() && active->state == "active",
              "a valid observation must reactivate a suspect record");
    }
}

// A21 -----------------------------------------------------------------------

void write_failures_reject_mutations_without_id_reuse(const std::string& base)
{
    const auto path = fresh_dir(base, "a21");
    JobRegistry registry(isolated(path));

    const auto first = registry.upsertObservedJob(condor_observation("172.0"));
    check(first.ok && first.disposition == UpsertDisposition::Created,
          "baseline registration must succeed");

    // (1) Clean pre-write failure of the registration batch. The allocator's
    // durable reservation still runs first (skip=1), so the burned id can
    // never be handed to another record.
    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/1);
    const auto failed = registry.upsertObservedJob(condor_observation("200.0"));
    check(!failed.ok, "a failed durable write must not report success");
    check(failed.error == "store_write_failed",
          "write failure must report a stable code, got " + failed.error);
    check(!registry.inventoryRecordByIdentity(*canonicalize(condor_selector("200.0")))
               .has_value(),
          "a failed registration must not leave a durable record");

    {
        const auto health = registry.storeHealth();
        check(health.degraded, "store health must expose the failure");
        check(health.write_failures >= 1, "store health must count write failures");
        check(!health.last_write_error.empty(),
              "store health must record the last write error");
    }

    const auto retried = registry.upsertObservedJob(condor_observation("200.0"));
    check(retried.ok && retried.disposition == UpsertDisposition::Created,
          "registration must succeed once the store recovers");
    check(retried.internal_id > first.internal_id,
          "the burned id must never be reused");

    // (2) Ambiguous outcome: the batch landed but the status reported failure.
    registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/1);
    const auto ambiguous = registry.upsertObservedJob(condor_observation("300.0"));
    check(!ambiguous.ok, "an ambiguous write outcome must not report success");

    const auto resolved = registry.upsertObservedJob(condor_observation("300.0"));
    check(resolved.ok, "an ambiguous registration must resolve by identity lookup");
    check(resolved.internal_id > retried.internal_id,
          "ambiguous allocation must not reuse a lower internal id");

    // (3) Binding transaction failure must leave the previous intent intact.
    JobCollectionManager manager(registry);
    CollectionRequest set;
    set.targets = {first.internal_id};
    set.collectors = std::vector<std::string>{"cpumem_collector"};
    const auto accepted = manager.setCollectors(set);
    check(accepted.ok, "baseline binding must be accepted: " + accepted.message);

    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
    set.collectors = std::vector<std::string>{"disk_collector"};
    const auto rejected = manager.setCollectors(set);
    check(!rejected.ok, "a failed binding write must not report success");
    check(rejected.error == CollectionError::StoreUnavailable,
          "binding store failure must be distinguishable");
    {
        const auto binding = registry.bindingRecord(first.internal_id);
        check(binding.revision == accepted.targets.front().binding.revision,
              "a failed binding write must not advance the revision");
        check(binding.manual_collectors == std::vector<std::string>{"cpumem_collector"},
              "a failed binding write must not rewrite the intent");
    }

    // (4) Archive transaction failure must keep the instance active.
    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
    check(!registry.retireInstance(retried.internal_id),
          "a failed archive must not report durable retirement");
    check(registry.findJob(retried.internal_id).has_value(),
          "a failed archive must leave the instance readable");
    check(registry.inventoryRecord(retried.internal_id).has_value(),
          "a failed archive must leave the inventory record intact");

    check(registry.retireInstance(retried.internal_id),
          "retirement must succeed once the store recovers");
    check(!registry.findJob(retried.internal_id).has_value(),
          "successful retirement removes the active record");
    check(!registry.bindingRecord(retried.internal_id).revision,
          "successful retirement removes the binding record");

    registry.clearStoreFaultsForTest();
}

} // namespace

int main(int argc, char** argv)
{
    try {
        const std::string base = argc > 1 ? argv[1] : "/tmp/joblens-inventory-store-test";
        std::filesystem::create_directories(base);
        upsert_is_idempotent_and_activates_nothing(base);
        legacy_restore_migrates_inventory_and_selection(base);
        interrupted_migration_resumes_idempotently(base);
        conflicting_live_identities_are_quarantined(base);
        stale_generation_is_not_replayed(base);
        write_failures_reject_mutations_without_id_reuse(base);
        std::cout << "PASS inventory store: A04 empty-binding upsert idempotency, "
                     "A19 legacy restore and manual migration, A20 resumable "
                     "migration, quarantine and stale-generation rejection, A21 "
                     "durable-write failure handling and no ID reuse\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
