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
#include "core/registration_error.hpp"
#include "core/job_registry.hpp"
#include "job_fixture.h"

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <limits>
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
    selector.adapter = "condor";
    selector.cluster_name = cluster;
    selector.cluster_tag = tag;
    selector.native_job_id = native;
    return selector;
}

Job condor_observation(const std::string& native = "172.0",
                       const std::string& cluster = "htcondor01",
                       const std::string& tag = "schedd-a")
{
    ExecutionGeneration generation;
    generation.boot_id = "boot-aaa";
    generation.manager_pid = 4242;
    generation.manager_start_ticks = 900000;
    generation.cgroup_identity = "/sys/fs/cgroup/htcondor/slot1";
    Job observation = discovered_job(condor_selector(native, cluster, tag), generation,
                                     {ProcessIdentity{4242, 900000, "boot-aaa"}}, true);

    CondorJobAttr attr{};
    attr.auto_update_child = false;
    const auto dot = native.find('.');
    attr.cluster_id = std::stoul(native.substr(0, dot));
    attr.proc_id = std::stoul(native.substr(dot + 1));
    attr.starter_pid = 4242;
    attr.owner = "alice";
    attr.slots_cgroup_path = "/sys/fs/cgroup/htcondor/slot1";
    attr.scheduler_host = "htcondor01";

    observation.subtype = JobSubType::Condor;
    observation.sub_attr = attr;
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

std::vector<std::pair<std::string, std::string>> read_raw_db(const std::string& path)
{
    leveldb::Options options;
    options.create_if_missing = false;
    leveldb::DB* db = nullptr;
    const auto opened = leveldb::DB::Open(options, path, &db);
    check(opened.ok(), "open raw db for inspection: " + opened.ToString());
    std::vector<std::pair<std::string, std::string>> entries;
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next())
        entries.emplace_back(it->key().ToString(), it->value().ToString());
    check(it->status().ok(), "iterate raw db: " + it->status().ToString());
    delete it;
    delete db;
    return entries;
}

std::vector<std::string> raw_keys_with_prefix(const std::string& path,
                                              const std::string& prefix)
{
    std::vector<std::string> keys;
    for (const auto& [key, value] : read_raw_db(path)) {
        (void)value;
        if (key.rfind(prefix, 0) == 0) keys.push_back(key);
    }
    return keys;
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
        check(record->identity.key() == canonicalize(selector_of(observation))->key(),
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
    auto& attr = std::get<CondorJobAttr>(updated_source.sub_attr);
    attr.owner = "bob";
    updated_source.execution->members.push_back(ProcessIdentity{4343, 910000, "boot-aaa"});

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
    incomplete.cluster_name.clear();
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
        observation.execution->generation = ExecutionGeneration{"boot-does-not-exist", manager_pid, 123456, ""};
        observation.execution->members = {
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
        same.execution->generation = ExecutionGeneration{"boot-does-not-exist", manager_pid, 123456, ""};
        same.execution->members = {ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"}};
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

    // (1) Clean pre-write failure of the registration batch. The id
    // reservation rides in the same batch as the record (skip=0 fails it), so a
    // registration that never landed cannot hand its id to another record.
    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/0);
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
    // The record batch is the first durable write of a registration now
    // (skip=0), so this is the create itself landing ambiguously.
    registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0);
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

// A24 -----------------------------------------------------------------------

void closing_state_is_durable_and_not_resurrected(const std::string& base)
{
    const auto path = fresh_dir(base, "a24-closing");
    uint64_t internal_id = 0;

    {
        JobRegistry registry(isolated(path));
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        internal_id = created.internal_id;

        // A subscriber makes the retirement a drain: the record must survive
        // the process as closing, not as an active instance. The injected
        // fault models the archive failing (or the process dying) before the
        // drain completes — exactly the restart scenario being tested.
        registry.addLifecycleCb([](JobEvent, const Job&) {});
        check(registry.retireInstance(internal_id),
              "retirement with a subscriber must be accepted");
        check(!registry.hasInstance(internal_id),
              "a closing instance must leave the active map");
        check(registry.inventoryRecord(internal_id).has_value(),
              "a closing instance stays visible until the archive");
        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
        check(!registry.deinit(),
              "deinit must report a shutdown that could not archive the drain");
    }

    {
        // Restart on the same DB: the interrupted retirement must be restored
        // as closing, never as an active job, and observations of the same
        // identity must be refused instead of resurrecting the instance.
        JobRegistry registry(isolated(path));
        const auto record = registry.inventoryRecord(internal_id);
        check(record.has_value(), "the closing record must be restored");
        check(record->state == "closing",
              "the restored record must carry the durable closing state, got " +
                  record->state);
        check(!registry.hasInstance(internal_id),
              "a restored closing instance must not be an active job");
        check(!registry.findJob(internal_id).has_value(),
              "a restored closing instance must not be findable as active");

        const auto late = registry.upsertObservedJob(condor_observation("172.0"));
        check(!late.ok && late.error == "instance_closing",
              "a closing instance must reject late observations, got " + late.error);

        // The closing cleanup finishes the interrupted retirement.
        check(registry.cleanupClosingJobs(), "the closing cleanup must archive the record");
        check(!registry.inventoryRecord(internal_id).has_value(),
              "the archive must remove the closing record");
        const auto history = registry.upsertObservedJob(condor_observation("172.0"));
        check(history.ok && history.disposition == UpsertDisposition::Created,
              "after the archive the identity can register again as a new instance");
    }
}

// A25 -----------------------------------------------------------------------

void restored_suspect_instances_are_not_replayed(const std::string& base)
{
    const auto path = fresh_dir(base, "a25-suspect");
    const pid_t manager_pid = 2000002;   // no such process
    uint64_t internal_id = 0;

    {
        JobRegistry registry(isolated(path));
        auto observation = condor_observation("172.0");
        observation.execution->generation = ExecutionGeneration{"boot-does-not-exist", manager_pid, 123456, ""};
        observation.execution->members = {
            ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"}};
        const auto created = registry.upsertObservedJob(observation);
        check(created.ok, "registration must succeed");
        internal_id = created.internal_id;
    }

    {
        JobRegistry registry(isolated(path));
        check(registry.inventoryRecord(internal_id).has_value() &&
                  registry.inventoryRecord(internal_id)->state == "suspect",
              "the stale generation must be restored as suspect");

        size_t added = 0;
        registry.addLifecycleCb([&added, internal_id](JobEvent event, const Job& job) {
            if (event == JobEvent::Added && job.JobID == internal_id) ++added;
        });
        check(added == 0,
              "a suspect execution must not be replayed to collectors after a restart");
    }
}

// A32 -----------------------------------------------------------------------

// Every registration error enumerator has one stable wire string and one fixed
// retry projection; an unmapped value must fail loudly instead of degrading to
// "success" or an empty code.
void registration_errors_are_typed_and_exhaustively_mapped(const std::string& base)
{
    (void)base;
    using E = RegistrationError;
    const std::vector<E> codes = {
        E::None, E::IncompleteIdentity, E::IncompleteExecution, E::SourceMetadataUnavailable,
        E::StoreUnavailable, E::StoreReadFailed, E::StoreWriteFailed, E::WriteOutcomeUnknown,
        E::IdExhausted, E::GenerationConflict, E::InstanceClosing, E::IdentityQuarantined,
        E::IdentityIndexMismatch, E::StaleRevision, E::PolicyEvaluationFailed,
        E::InitialIntentWriteFailed, E::ObserverDeliveryFailed, E::ReentrantMutation,
    };
    for (const auto code : codes) {
        const std::string wire = to_string(code);
        check(!wire.empty() || code == E::None,
              "every error code but None must have a wire string");
        check(wire != "unknown_registration_error",
              "error code " + std::to_string(static_cast<int>(code)) +
                  " is missing from the mapping table");
        if (code == E::None) continue;
        bool known = false;
        const auto round_trip = registration_error_from_string(wire, known);
        check(known, "wire string '" + wire + "' must parse back to a code");
        // StoreWriteFailed and WriteOutcomeUnknown share the HTTP surface, so
        // the round trip is checked by retry projection, not by identity.
        check(needs_source_retry(round_trip) == needs_source_retry(code),
              "the retry projection must survive the wire round trip for '" + wire + "'");
    }

    // The source-retry projection is fixed, not decided by callers.
    check(needs_source_retry(E::IncompleteIdentity), "incomplete identity needs a source re-read");
    check(needs_source_retry(E::IncompleteExecution),
          "incomplete execution needs a source re-read");
    check(needs_source_retry(E::WriteOutcomeUnknown),
          "an ambiguous write is resolved by re-reading and looking the identity up");
    check(!needs_source_retry(E::IdExhausted), "exhaustion is not fixed by re-reading");
    check(!needs_source_retry(E::InstanceClosing), "a closing instance must not be retried");
    check(!needs_source_retry(E::PolicyEvaluationFailed),
          "a policy failure is repaired by maintenance, not by re-reading metadata");
    check(!needs_source_retry(E::InitialIntentWriteFailed),
          "an intent write failure is repaired by record state");
    check(!needs_source_retry(E::None), "success never asks for a retry");

    bool known = false;
    (void)registration_error_from_string("not_a_real_code", known);
    check(!known, "an unknown wire string must not silently map to a code");

    // The typed code and the wire string travel together on a real failure.
    const auto path = fresh_dir(base, "a32-typed");
    JobRegistry registry(isolated(path));
    auto incomplete = condor_observation("503.0");
    incomplete.cluster_name.clear();
    const auto rejected = registry.upsertObservedJob(incomplete);
    check(!rejected.ok && rejected.code == E::IncompleteIdentity,
          "an incomplete identity must carry its typed code");
    check(rejected.error == "incomplete_identity",
          "the wire string must stay unchanged, got " + rejected.error);
    check(rejected.retry_registration(), "the typed result must project to a source retry");
}

// A26 (allocator order) ------------------------------------------------------

// A standalone allocation and an inventory batch share one admission: the
// standalone call must wait for the batch whose id reservation is already in
// flight, otherwise the batch would write an older counter back and reissue a
// published id after a restart (plan §5.3).
void standalone_allocation_cannot_interleave_with_a_batch(const std::string& base)
{
    const auto path = fresh_dir(base, "alloc-order");
    std::uint64_t batch_id = 0;
    {
        JobRegistry registry(isolated(path));
        std::mutex mutex;
        std::condition_variable cv;
        bool reserved = false;
        bool release = false;
        registry.setPreCommitHookForTest([&] {
            std::unique_lock<std::mutex> lock(mutex);
            reserved = true;
            cv.notify_all();
            cv.wait(lock, [&] { return release; });
        });

        std::vector<UpsertResult> results;
        std::thread batch_thread([&] {
            results = registry.registerDiscoveredJobs({condor_observation("901.0")});
        });
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait_for(lock, std::chrono::seconds(10), [&] { return reserved; });
        }

        std::atomic<bool> allocated{false};
        std::atomic<std::uint64_t> standalone{0};
        std::thread allocator([&] {
            standalone = registry.getNextJobID();
            allocated = true;
        });

        // The batch is between its id reservation and its commit; a standalone
        // allocation must wait for that admission instead of persisting a
        // counter the batch would then write back.
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        const bool blocked_while_in_flight = !allocated.load();

        {
            std::lock_guard<std::mutex> lock(mutex);
            release = true;
        }
        cv.notify_all();
        batch_thread.join();
        allocator.join();
        registry.setPreCommitHookForTest({});

        check(reserved, "the batch must reach its pre-commit point");
        check(results.size() == 1 && results[0].ok, "the batch registration must succeed");
        batch_id = results[0].internal_id;
        check(blocked_while_in_flight,
              "a standalone allocation must wait for the batch admission");
        check(allocated.load(), "the standalone allocation must complete after the batch");
        check(standalone.load() > batch_id,
              "the standalone id must follow the batch id, got " +
                  std::to_string(standalone.load()) + " after " + std::to_string(batch_id));
        check(registry.deinit(), "shutdown must succeed");
    }
    {
        JobRegistry registry(isolated(path));
        const auto next = registry.getNextJobID();
        check(next > batch_id, "the durable counter must not go backwards after a restart");
    }
}

// A03 (failure attribution) -------------------------------------------------

// A batch that contains one identity twice must fail *both* results when the
// shared facts transaction fails: the duplicate rode on the same commit and no
// instance may be published for it.
void duplicate_input_shares_the_batch_failure(const std::string& base)
{
    const auto path = fresh_dir(base, "dup-failure");
    JobRegistry registry(isolated(path));

    const auto job = condor_observation("902.0");
    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/0);
    const auto results = registry.registerDiscoveredJobs({job, job});
    check(results.size() == 2, "both inputs must be answered");
    check(!results[0].ok, "the first copy must fail with the transaction");
    check(!results[1].ok, "the duplicate copy must fail with the same transaction");
    check(results[1].code == RegistrationError::StoreWriteFailed,
          "the duplicate must carry the store failure code");
    check(registry.inventoryRecords().empty(),
          "a failed batch must not publish any instance");

    registry.clearStoreFaultsForTest();
    const auto retried = registry.registerDiscoveredJobs({job, job});
    check(retried[0].ok && retried[1].ok, "the retry must succeed");
    check(retried[0].internal_id == retried[1].internal_id,
          "the duplicate must resolve to the same instance");
    check(registry.inventoryRecords().size() == 1, "exactly one instance may exist");
}

// A05 (fresh candidates) ----------------------------------------------------

// Two *fresh* candidates for one identity with different live manager triples
// conflict exactly like a merge would: the second never overwrites the first.
void conflicting_fresh_candidates_are_rejected(const std::string& base)
{
    const auto path = fresh_dir(base, "fresh-conflict");
    JobRegistry registry(isolated(path));

    Job first = condor_observation("903.0");
    Job second = condor_observation("903.0");
    second.execution->generation =
        ExecutionGeneration{"boot-aaa", 8888, 777777, "/sys/fs/cgroup/htcondor/slot8"};

    const auto results = registry.registerDiscoveredJobs({first, second});
    check(results[0].ok && results[0].disposition == UpsertDisposition::Created,
          "the first candidate must be created");
    check(!results[1].ok && results[1].code == RegistrationError::GenerationConflict,
          "a different manager triple for the same fresh identity must conflict");
    const auto stored = registry.inventoryRecord(results[0].internal_id);
    check(stored.has_value(), "the first candidate must stay readable");
    check(stored->generation.manager_pid == 4242,
          "the conflicting candidate must not overwrite the stored generation");
    check(registry.inventoryRecords().size() == 1, "only one instance may exist");
}

// A04 (working set) ---------------------------------------------------------

// Repeated inputs for one existing identity merge into a single candidate, and
// the published memory state equals what a restart reads back.
void batch_working_set_keeps_memory_and_disk_in_sync(const std::string& base)
{
    const auto path = fresh_dir(base, "working-set");
    std::uint64_t id = 0;
    std::string published_owner;
    std::uint64_t published_revision = 0;
    {
        JobRegistry registry(isolated(path));
        const auto created = registry.upsertObservedJob(condor_observation("904.0"));
        check(created.ok, "the seed instance must register");
        id = created.internal_id;

        Job after = condor_observation("904.0");
        std::get<CondorJobAttr>(after.sub_attr).owner = "after";
        Job before = condor_observation("904.0");
        std::get<CondorJobAttr>(before.sub_attr).owner = "before";

        const auto results = registry.registerDiscoveredJobs({after, before});
        check(results[0].ok && results[1].ok, "both repeated inputs must be accepted");

        const auto live = registry.inventoryRecord(id);
        check(live.has_value(), "the instance must stay readable");
        published_owner = std::get<CondorJobAttr>(live->job.sub_attr).owner;
        published_revision = live->facts_revision;
        check(published_revision == results[1].facts_revision,
              "the published revision must be the final candidate's");
        check(published_owner == "before",
              "the working set must publish the last input, got " + published_owner);
        check(registry.deinit(), "shutdown must succeed");
    }
    {
        // A restart reads the durable row: it must agree with what was
        // published before the shutdown instead of diverging from it.
        JobRegistry registry(isolated(path));
        const auto reopened = registry.inventoryRecord(id);
        check(reopened.has_value(), "the instance must survive the restart");
        const auto owner = std::get<CondorJobAttr>(reopened->job.sub_attr).owner;
        check(owner == published_owner,
              "the restarted record must agree with the published owner ('" + owner +
                  "' vs '" + published_owner + "')");
        check(reopened->facts_revision == published_revision,
              "the restarted revision must match the published revision");
    }
}

// A26 -----------------------------------------------------------------------

void member_semantics_distinguish_unknown_from_confirmed_empty(const std::string& base)
{
    const auto path = fresh_dir(base, "a26-members");
    JobRegistry registry(isolated(path));

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok && created.disposition == UpsertDisposition::Created,
          "baseline registration must succeed");
    check(registry.findJob(created.internal_id)->JobPIDs.size() == 1,
          "the fixture must register one member pid");

    // A partially-known membership with an empty list must not erase members:
    // the source could not read everything, so "no members" is not proven.
    auto partial = condor_observation("172.0");
    partial.execution->members.clear();
    partial.execution->members_authoritative = false;
    const auto kept = registry.upsertObservedJob(partial);
    check(kept.ok && kept.disposition == UpsertDisposition::Unchanged,
          "a non-authoritative empty list must change nothing");
    check(registry.findJob(created.internal_id)->JobPIDs.size() == 1,
          "a non-authoritative empty list must not erase members");

    // An authoritative empty list is positive evidence: the stored members
    // are replaced by the confirmed empty set.
    auto confirmed_empty = condor_observation("172.0");
    confirmed_empty.execution->members.clear();
    confirmed_empty.execution->members_authoritative = true;
    const auto cleared = registry.upsertObservedJob(confirmed_empty);
    check(cleared.ok && cleared.disposition == UpsertDisposition::Updated,
          "a confirmed empty membership must be applied to facts");
    check(registry.findJob(created.internal_id)->JobPIDs.empty(),
          "a confirmed empty membership must clear stale member pids");
    const auto record = registry.inventoryRecord(created.internal_id);
    check(record.has_value() && record->members.empty(),
          "the stored process evidence must follow the confirmed empty set");
}

} // namespace

// A29 -----------------------------------------------------------------------

void batch_registration_keeps_single_upsert_semantics(const std::string& base)
{
    const auto path = fresh_dir(base, "a29");
    JobRegistry registry(isolated(path));

    auto first = condor_observation("501.0");
    auto second = condor_observation("502.0");
    auto incomplete = condor_observation("503.0");
    incomplete.cluster_name.clear();   // no namespace -> no identity

    // A burst of fresh executions: all created, all in one durable batch.
    const auto batch = registry.registerDiscoveredJobs({first, second, incomplete});
    check(batch.size() == 3, "batch results must stay aligned with the input");
    check(batch[0].ok && batch[1].ok, "fresh executions must be created");
    check(batch[0].disposition == UpsertDisposition::Created,
          "batch create must report created");
    check(batch[0].internal_id != batch[1].internal_id,
          "each instance gets its own internal id");
    check(!batch[2].ok && batch[2].error == "incomplete_identity",
          "an incomplete identity stays rejected in the batch path");
    check(registry.inventoryRecords().size() == 2, "only the complete identities persist");

    // The same batch again is a merge, not a duplicate: the ids are unchanged.
    const auto repeat = registry.registerDiscoveredJobs({first, second});
    check(repeat[0].ok && repeat[1].ok, "replaying the batch must succeed");
    check(repeat[0].disposition != UpsertDisposition::Created,
          "a replayed batch must not create a second instance");
    check(repeat[0].internal_id == batch[0].internal_id &&
              repeat[1].internal_id == batch[1].internal_id,
          "a replayed batch must resolve to the same instances");

    // The automatic intent is still one transaction for the batch: the applier
    // result is visible per instance.
    std::vector<BindingRecord> bindings;
    for (const auto& result : batch) {
        if (!result.ok) continue;
        BindingRecord binding;
        binding.internal_id = result.internal_id;
        binding.revision = 1;
        binding.mode = CollectionMode::Auto;
        binding.auto_collectors = {"cpumem_collector"};
        binding.policy_revision = "test";
        bindings.push_back(std::move(binding));
    }
    std::string error;
    check(registry.commitBindings(bindings, error),
          "one batch intent transaction must accept every binding: " + error);
    for (const auto& binding : bindings) {
        check(registry.bindingRecord(binding.internal_id).auto_collectors ==
                  std::vector<std::string>{"cpumem_collector"},
              "the batch intent must be readable per instance");
        registry.reconcileCollectionIntent(binding.internal_id);
        check(!registry.appliedCollection(binding.internal_id).applied,
              "an isolated registry has no runtime, so applied stays false");
        check(registry.appliedCollection(binding.internal_id).error == "no_collection_runtime",
              "the missing runtime must be reported honestly");
    }
}

// A26 -----------------------------------------------------------------------

// The internal ID space must never reissue a value it already handed out. The
// legacy allocator stores only `c:job_id`; at the ceiling that is ambiguous
// ("MAX is still available" vs "MAX was already issued"), so the process must
// refuse, and the refusal must survive a restart.
void max_internal_id_is_never_reissued(const std::string& base)
{
    constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();

    // (a) Legacy counter one below the ceiling, no records: this process may
    // consume the last two values and then must refuse; after a restart the
    // already-issued MAX may not be handed out again.
    {
        const auto path = fresh_dir(base, "a26a");
        write_raw_db(path, {{"c:job_id", std::to_string(kMax - 1)}});
        {
            JobRegistry registry(isolated(path));
            check(registry.getNextJobID() == kMax - 1,
                  "the first allocation must be MAX-1");
            check(registry.getNextJobID() == kMax, "the second allocation must be MAX");
            check(registry.getNextJobID() == 0, "the process must refuse after MAX");
        }
        {
            JobRegistry registry(isolated(path));
            check(registry.getNextJobID() == 0,
                  "MAX must never be reissued after a restart");
        }
    }

    // (b) Legacy database whose counter is already MAX with no marker: the old
    // format cannot prove MAX was never issued, so it must be treated as
    // exhausted rather than reissued.
    {
        const auto path = fresh_dir(base, "a26b");
        write_raw_db(path, {{"c:job_id", std::to_string(kMax)}});
        JobRegistry registry(isolated(path));
        check(registry.getNextJobID() == 0,
              "a marker-less MAX counter must be treated as exhausted");
    }

    // (c) New format: an explicit marker decides. `false` allows exactly one
    // final allocation, which flips the marker to `true` in the same durable
    // transaction; the exhaustion then survives the restart.
    {
        const auto path = fresh_dir(base, "a26c");
        write_raw_db(path, {{"c:job_id", std::to_string(kMax)},
                            {"m:job_id_exhausted", "false"}});
        {
            JobRegistry registry(isolated(path));
            check(registry.getNextJobID() == kMax,
                  "an explicit marker=false must allow the final MAX allocation");
            check(registry.getNextJobID() == 0,
                  "the final allocation must exhaust the allocator");
        }
        {
            JobRegistry registry(isolated(path));
            check(registry.getNextJobID() == 0,
                  "the durable exhaustion marker must survive a restart");
        }
    }

    // (d) An explicit `true` marker refuses immediately even when the counter
    // still holds MAX.
    {
        const auto path = fresh_dir(base, "a26d");
        write_raw_db(path, {{"c:job_id", std::to_string(kMax)},
                            {"m:job_id_exhausted", "true"}});
        JobRegistry registry(isolated(path));
        check(registry.getNextJobID() == 0, "marker=true must refuse every allocation");
    }

    // (e) A live record already holding MAX forces exhaustion even when the
    // counter value alone would look free.
    {
        const auto path = fresh_dir(base, "a26e");
        write_raw_db(path, {
            {"j:" + hex64(kMax),
             legacy_condor_record("172.0", "htcondor01", "schedd-a", {}, 4100).dump()},
            {"c:job_id", std::to_string(kMax - 1)},
        });
        JobRegistry registry(isolated(path));
        check(registry.getNextJobID() == 0,
              "an existing MAX record must force exhaustion");
    }
}

// A11 -----------------------------------------------------------------------

// The runtime result is memory-only truth for this process: unchanged facts,
// changed facts and intent commits must never clear or fabricate it.
void runtime_state_survives_later_durable_writes(const std::string& base)
{
    const auto path = fresh_dir(base, "a11-runtime");
    JobRegistry registry(isolated(path));
    registry.setCollectionApplier([](const Job& job, std::vector<std::string>& applied,
                                     std::string& error) {
        applied = job.CollectorNames;
        error.clear();
        return true;
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.internal_id;

    std::string error;
    BindingRecord binding;
    binding.internal_id = id;
    binding.revision = 1;
    binding.mode = CollectionMode::Auto;
    binding.auto_collectors = {"cpumem_collector"};
    binding.policy_revision = "a11";
    check(registry.commitBindings({binding}, error), "binding must commit: " + error);
    registry.reconcileCollectionIntent(id);
    {
        const auto applied = registry.appliedCollection(id);
        check(applied.applied &&
                  applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "the runtime result must be visible after apply");
        check(applied.revision == 1, "the applied revision must track the binding");
    }

    // (1) Unchanged discovery pass: neither facts nor runtime state may move.
    const auto unchanged = registry.upsertObservedJob(condor_observation("172.0"));
    check(unchanged.ok && unchanged.disposition == UpsertDisposition::Unchanged,
          "a repeated observation must stay unchanged");
    {
        const auto applied = registry.appliedCollection(id);
        check(applied.applied &&
                  applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "an unchanged upsert must not clear the runtime holders");
        check(applied.revision == 1,
              "an unchanged upsert must not move the applied revision");
    }

    // (2) Changed facts must not pretend the new evidence is already applied,
    // and must not drop the holders that are really attached.
    auto changed = condor_observation("172.0");
    changed.execution->members.push_back(ProcessIdentity{5151, 424242, "boot-aaa"});
    const auto updated = registry.upsertObservedJob(changed);
    check(updated.ok && updated.disposition == UpsertDisposition::Updated,
          "a changed observation must be an update");
    {
        const auto applied = registry.appliedCollection(id);
        check(applied.applied &&
                  applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "a facts update must not erase the current runtime result");
    }

    // (3) An intent the runtime only partially honoured must not be reported
    // as applied, and the holders it really reports must stay visible.
    registry.setCollectionApplier([](const Job&, std::vector<std::string>& applied,
                                     std::string& error) {
        applied = {"cpumem_collector"};   // the old lane is still attached
        error = "collector_failed";
        return false;
    });
    BindingRecord second = binding;
    second.revision = 2;
    second.auto_collectors = {"cpumem_collector", "disk_collector"};
    check(registry.commitBindings({second}, error), "second binding must commit: " + error);
    {
        const auto applied = registry.appliedCollection(id);
        check(!applied.applied, "a rejected apply must not report the intent as applied");
        check(applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "the holders the runtime still reports must stay visible");
        check(applied.revision == 2,
              "the applied record must point at the binding revision it tried");
        check(applied.error == "collector_failed",
              "the runtime error must be visible, got '" + applied.error + "'");
    }

    // The retry path converges once the runtime accepts the intent again.
    registry.setCollectionApplier([](const Job& job, std::vector<std::string>& applied,
                                     std::string& error) {
        applied = job.CollectorNames;
        error.clear();
        return true;
    });
    registry.reconcileCollectionIntent(id);
    {
        const auto converged = registry.appliedCollection(id);
        check(converged.applied && converged.collectors.size() == 2,
              "the retry must converge to the new intent");
        check(converged.revision == 2, "the applied revision must converge to revision 2");
    }
}

// A12 -----------------------------------------------------------------------

// Archiving must leave no active `j:` row behind, even when the runtime result
// of that instance was recorded as a coalesced hint that had not been flushed
// to disk yet. This is the deterministic half of A12; the coalesced flush is
// deleted in P2b, so a concurrent interleaving has no code path to race after
// that change.
void archived_instances_leave_no_active_row(const std::string& base)
{
    const auto path = fresh_dir(base, "a12-orphan");
    uint64_t id = 0;
    {
        JobRegistry registry(isolated(path));
        registry.setCollectionApplier([](const Job& job, std::vector<std::string>& applied,
                                         std::string& error) {
            applied = job.CollectorNames;
            error.clear();
            return true;
        });
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.internal_id;

        std::string error;
        BindingRecord binding;
        binding.internal_id = id;
        binding.revision = 1;
        binding.mode = CollectionMode::Auto;
        binding.auto_collectors = {"cpumem_collector"};
        check(registry.commitBindings({binding}, error), "binding must commit: " + error);
        registry.reconcileCollectionIntent(id);

        // The runtime result is still a pending coalesced hint here.
        check(registry.retireInstance(id), "retirement must archive the instance");
        check(!registry.inventoryRecord(id).has_value(),
              "the archived instance must leave the active map");
        check(registry.deinit(), "shutdown must not fail after a clean archive");
    }

    const auto active_keys = raw_keys_with_prefix(path, "j:");
    check(active_keys.empty(), "no active row may survive an archive, found " +
                                   std::to_string(active_keys.size()));
    const auto history_keys = raw_keys_with_prefix(path, "h:");
    check(history_keys.size() == 1, "the archive must leave exactly one history row");
}

// A10 -----------------------------------------------------------------------

// An ambiguous create (the batch landed but LevelDB reported failure) must
// resolve to the same instance after a restart instead of minting a new one.
void ambiguous_registration_resolves_after_restart(const std::string& base)
{
    const auto path = fresh_dir(base, "a10-restart");
    uint64_t first_id = 0;
    {
        JobRegistry registry(isolated(path));
        const auto first = registry.upsertObservedJob(condor_observation("172.0"));
        check(first.ok, "baseline registration must succeed");
        first_id = first.internal_id;

        registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0);
        const auto ambiguous = registry.upsertObservedJob(condor_observation("200.0"));
        check(!ambiguous.ok, "an ambiguous write must not report success");
        check(registry.deinit(), "shutdown after an ambiguous write must succeed");
    }
    {
        JobRegistry registry(isolated(path));
        const auto identity = canonicalize(condor_selector("200.0"));
        check(identity.has_value(), "the fixture selector must canonicalize");
        const auto restored = registry.inventoryRecordByIdentity(*identity);
        check(restored.has_value(),
              "the record that landed ambiguously must be restored by identity");
        check(restored->internal_id > first_id,
              "the restored instance must keep its own internal id");

        const auto again = registry.upsertObservedJob(condor_observation("200.0"));
        check(again.ok, "the identity must be re-registrable after the restart");
        check(again.disposition != UpsertDisposition::Created,
              "an ambiguous create must not mint a second instance after a restart");
        check(again.internal_id == restored->internal_id,
              "the identity must resolve to the same internal id");
        check(registry.inventoryRecords().size() == 2,
              "exactly the two real instances may exist");
    }
}

// A03/A04/A05 ---------------------------------------------------------------

// One batch that both creates and merges costs exactly one facts transaction:
// the merge, the new record and its identity index land in the same commit, a
// conflicting item is reported per item without blocking the others, and an
// in-batch duplicate resolves to the candidate being written.
void batch_merges_share_one_facts_transaction(const std::string& base)
{
    const auto path = fresh_dir(base, "a03-batch-merge");
    JobRegistry registry(isolated(path));

    const auto seed_a = registry.upsertObservedJob(condor_observation("601.0"));
    const auto seed_b = registry.upsertObservedJob(condor_observation("602.0"));
    check(seed_a.ok && seed_b.ok, "the seed instances must register");
    const uint64_t before = registry.storeHealth().durable_commits;

    Job fresh = condor_observation("603.0");
    Job changed = condor_observation("601.0");
    changed.execution->members.push_back(ProcessIdentity{5151, 424242, "boot-aaa"});
    Job conflict = condor_observation("602.0");
    conflict.execution->generation =
        ExecutionGeneration{"boot-aaa", 7777, 888888, "/sys/fs/cgroup/htcondor/slot9"};
    Job duplicate = condor_observation("603.0");
    duplicate.execution->members.push_back(ProcessIdentity{5252, 424242, "boot-aaa"});

    const auto results =
        registry.registerDiscoveredJobs({fresh, changed, conflict, duplicate});
    check(results.size() == 4, "batch results must stay aligned with the input");

    check(results[0].ok && results[0].disposition == UpsertDisposition::Created,
          "the fresh identity must be created");
    check(results[1].ok && results[1].disposition == UpsertDisposition::Updated,
          "the changed identity must merge in the same batch");
    check(results[1].internal_id == seed_a.internal_id,
          "the merge must resolve to the stored instance");
    check(!results[2].ok && results[2].code == RegistrationError::GenerationConflict,
          "a different live generation must be reported as a conflict");
    check(results[2].internal_id == seed_b.internal_id,
          "the conflict must name the instance it belongs to");
    check(results[3].ok && results[3].internal_id == results[0].internal_id,
          "an in-batch duplicate must resolve to the candidate being written");

    const uint64_t commits = registry.storeHealth().durable_commits - before;
    check(commits == 1,
          "create + merge + index repair must share one facts transaction, saw " +
              std::to_string(commits));

    const auto stored = registry.inventoryRecord(results[0].internal_id);
    check(stored.has_value() && stored->facts_revision == 2,
          "the in-batch duplicate must have merged into the created record");
    check(registry.inventoryRecords().size() == 3,
          "exactly three instances may exist");
}

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
        closing_state_is_durable_and_not_resurrected(base);
        restored_suspect_instances_are_not_replayed(base);
        member_semantics_distinguish_unknown_from_confirmed_empty(base);
        batch_registration_keeps_single_upsert_semantics(base);
        registration_errors_are_typed_and_exhaustively_mapped(base);
        max_internal_id_is_never_reissued(base);
        runtime_state_survives_later_durable_writes(base);
        archived_instances_leave_no_active_row(base);
        ambiguous_registration_resolves_after_restart(base);
        batch_merges_share_one_facts_transaction(base);
        standalone_allocation_cannot_interleave_with_a_batch(base);
        duplicate_input_shares_the_batch_failure(base);
        conflicting_fresh_candidates_are_rejected(base);
        batch_working_set_keeps_memory_and_disk_in_sync(base);
        std::cout << "PASS inventory store: A04 empty-binding upsert idempotency, "
                     "A19 legacy restore and manual migration, A20 resumable "
                     "migration, quarantine and stale-generation rejection, A21 "
                     "durable-write failure handling and no ID reuse, A24 "
                     "durable closing state, A25 suspect replay filter, A26 "
                     "member semantics (unknown vs confirmed empty), A29 batch "
                     "registration parity, A26 max-ID exhaustion across restart, "
                     "A11 runtime state across later durable writes, A12 archive "
                     "leaves no active row\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
