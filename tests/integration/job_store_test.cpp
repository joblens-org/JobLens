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
/* Deterministic job-store acceptance.
 *
 *   * One complete upsert with zero bindings, repeat and fact update: one
 *     entry, stable ID/instance handle, revision advances only on real fact
 *     change, nothing activated.
 *   * Legacy restore with non-empty CollectorNames: job record appears
 *     without any lifecycle subscriber, identity index built, selection
 *     migrated to a manual binding.
 *   * Interrupted migration resumes idempotently with preserved IDs;
 *     conflicting live identities are quarantined, never merged.
 *   * Injected LevelDB write failures: no false durable success, no ID
 *     reuse, health records the failure.
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
#include "core/registration_error.hpp"
#include "core/job_registry.hpp"
#include "job_fixture.h"
#include "collection_runtime_fixture.hpp"

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include <cstdint>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>

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
    options.test_static_registry = true;
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    return options;
}

// hasInstance()/jobRecordByIdentity() were removed as production-dead
// registry APIs; the assertions derive the same facts from the public record
// snapshot so their semantics stay unchanged.
bool has_active(const JobRegistry& registry, uint64_t job_id)
{
    const auto record = registry.jobRecord(job_id);
    return record.has_value() && record->state != RecordState::Closing;
}

std::optional<JobRecord> record_by_identity(const JobRegistry& registry,
                                            const SchedulerKey& key)
{
    const std::string index_key = key.index_key();
    for (const auto& record : registry.jobRecords()) {
        if (!record.index_key.empty() && record.index_key == index_key) return record;
    }
    return std::nullopt;
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
    selector.clusterName = cluster;
    selector.clusterTag = tag;
    selector.job_native_id = native;
    return selector;
}

Job condor_observation(const std::string& native = "172.0",
                       const std::string& cluster = "htcondor01",
                       const std::string& tag = "schedd-a")
{
    ProcessIdentity manager;
    manager.boot_id = "boot-aaa";
    manager.pid = 4242;
    manager.start_ticks = 900000;
    Job observation = discovered_job(condor_selector(native, cluster, tag), manager,
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
    record["JobNativeID"] = native;
    record["clusterName"] = cluster;
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
// Stable metadata is a creation snapshot; only execution evidence refreshes.
void stable_metadata_is_a_creation_snapshot(const std::string& base)
{
    Job condor = condor_observation("177.0");
    Job slurm = discovered_job(
        NativeSelector{"slurm", "hpc", "", "178", "batch"},
        ProcessIdentity{4242, 900000, "boot-aaa"},
        {ProcessIdentity{4242, 900000, "boot-aaa"}}, true);
    auto& slurm_attr = std::get<SlurmJobAttr>(slurm.sub_attr);
    slurm_attr.user = "alice";
    slurm_attr.job_name = "launch-name";
    slurm_attr.partition = "batch";
    slurm_attr.cgroup_path = "/step/original";
    Job unknown_owner = condor_observation("179.0");
    std::get<CondorJobAttr>(unknown_owner.sub_attr).owner.clear();

    const auto metadata_of = [](const Job& job) {
        auto metadata = job_to_json(job);
        metadata.erase("JobPIDs");
        metadata["subtype_name"] = job.subtype_name;
        return metadata;
    };
    for (Job initial : {condor, slurm, unknown_owner}) {
        const auto path = fresh_dir(base, "frozen-" + initial.JobNativeID);
        JobRegistry registry(isolated(path));
        initial.execution.read_order = 10;
        const auto created = registry.upsertObservedJob(initial);
        check(created.ok, "the immutable metadata fixture must register");
        const auto original = registry.jobRecord(created.job_id);
        check(original.has_value(), "the created snapshot must be readable");
        const auto metadata = metadata_of(original->job);
        const auto writes_before = registry.storeHealth().durable_commits;

        Job incoming = initial;
        incoming.jobtype = JobType::Sys;
        incoming.JobNativeID = "00" + initial.JobNativeID;  // same canonical key
        incoming.execution.read_order = 11;
        if (auto* attr = std::get_if<CondorJobAttr>(&incoming.sub_attr)) {
            attr->owner = "replacement-owner";
            attr->starter_pid = 8888;
            attr->cluster_id = 999;
            attr->proc_id = 7;
            attr->scheduler_host = "replacement-host";
            attr->slots_cgroup_path = "/replacement/cgroup";
            attr->job_ad_path = "/replacement/job.ad";
            attr->auto_update_child = true;
        } else {
            auto& slurm_update = std::get<SlurmJobAttr>(incoming.sub_attr);
            slurm_update.user = "replacement-user";
            slurm_update.job_name = "replacement-name";
            slurm_update.partition = "replacement-partition";
            slurm_update.stepd_pid = 8888;
            slurm_update.job_id = 999;
            slurm_update.cgroup_path = "/replacement/cgroup";
            slurm_update.auto_update_child = true;
            incoming.clusterTag = "replacement-display-tag";
        }
        const auto repeated = registry.upsertObservedJob(incoming);
        check(repeated.ok && repeated.disposition == UpsertDisposition::Unchanged &&
                  repeated.revision == created.revision,
              "metadata-only observations must not update the creation snapshot or revision");
        const auto kept = registry.jobRecord(created.job_id);
        check(kept && metadata_of(kept->job) == metadata &&
                  kept->job.execution.read_order == 11 &&
                  kept->index_key == original->index_key &&
                  registry.storeHealth().durable_commits == writes_before,
              "metadata-only observations must refresh ordering without a record write");

        incoming.execution.pid_identities.push_back({4343, 910000, "boot-aaa"});
        incoming.execution.read_order = 12;
        const auto refreshed = registry.upsertObservedJob(incoming);
        check(refreshed.ok && refreshed.disposition == UpsertDisposition::Updated &&
                  refreshed.revision == created.revision + 1,
              "member evidence must still advance the existing record exactly once");
        const auto updated = registry.jobRecord(created.job_id);
        check(updated && metadata_of(updated->job) == metadata &&
                  updated->job.execution.pid_identities ==
                      std::vector<ProcessIdentity>{{4242, 900000, "boot-aaa"},
                                                   {4343, 910000, "boot-aaa"}},
              "evidence refresh must preserve every stable metadata field");

        incoming.execution.read_order = 11;
        incoming.execution.pid_identities.clear();
        const auto stale = registry.upsertObservedJob(incoming);
        check(stale.ok && stale.disposition == UpsertDisposition::Unchanged &&
                  registry.jobRecord(created.job_id)->job.execution.pid_identities.size() == 2,
              "an older read must not clear the refreshed member evidence");
        check(registry.deinit(), "the frozen snapshot must checkpoint");
        JobRegistry reopened(isolated(path));
        const auto restored = reopened.jobRecord(created.job_id);
        check(restored && metadata_of(restored->job) == metadata &&
                  restored->revision == refreshed.revision,
              "the creation metadata and evidence revision must survive restart");
    }
}

// The unified update chain may use temporary metadata to locate members, but
// must never write those guesses back into the immutable creation image.
void host_refresh_preserves_creation_metadata(const std::string& base)
{
    JobRegistryOptions options;
    options.db_path = fresh_dir(base, "frozen-host-refresh");
    options.enable_pid_tracker = false;
    options.register_rpc = false;
    // The member refresh is part of the maintenance pass now; no discovery
    // source is wired, so the pass re-reads the records themselves.
    options.test_heartbeat = true;
    JobRegistry registry(options);
    registry.setCollectorValidator(nullptr);
    Job manual;
    manual.jobtype = JobType::Job;
    manual.subtype = JobSubType::Common;
    manual.JobNativeID = "manual-without-native-id";
    manual.sub_attr = CommonJobAttr{.auto_update_child = true};
    set_pids(manual, {getpid()});
    manual.CollectorNames = {"cpumem_collector"};
    check(registry.addJob(manual), "the manual-only refresh fixture must register");
    const auto initial = registry.jobRecords().front();
    // Create a real member only after the initial snapshot. The child uses
    // async-signal-safe I/O after fork and remains alive until the guard exits.
    int release[2];
    check(pipe(release) == 0, "the host refresh child pipe must open");
    const pid_t child_pid = fork();
    if (child_pid < 0) {
        close(release[0]);
        close(release[1]);
        throw std::runtime_error("the host refresh child must fork");
    }
    if (child_pid == 0) {
        close(release[1]);
        char signal;
        while (read(release[0], &signal, 1) < 0 && errno == EINTR) {}
        close(release[0]);
        _exit(0);
    }
    close(release[0]);
    struct WaitingChild {
        pid_t pid;
        int release_fd;
        ~WaitingChild() {
            close(release_fd);
            while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {}
        }
    } child{child_pid, release[1]};
    check(registry.reconcileNow(), "the host member refresh pass must run");
    const auto refreshed = registry.jobRecord(initial.job.JobID);
    check(refreshed && refreshed->job.JobNativeID == "manual-without-native-id" &&
              std::get<CommonJobAttr>(refreshed->job.sub_attr).auto_update_child &&
              refreshed->index_key == initial.index_key,
          "host refresh must not persist metadata guesses or alter identity");
    const auto refreshed_pids = pids_of(refreshed->job);
    check(std::find(refreshed_pids.begin(), refreshed_pids.end(), child.pid) !=
              refreshed_pids.end(),
          "host refresh must discover and persist the newly created member");
    check(registry.deinit(), "the host refresh fixture must checkpoint");
}

void upsert_is_idempotent_and_activates_nothing(const std::string& base)
{
    const auto path = fresh_dir(base, "a04");
    JobRegistry registry(isolated(path));

    const auto observation = condor_observation();
    const auto created = registry.upsertObservedJob(observation);
    check(created.ok, "complete observation must be accepted: " + created.error);
    check(created.disposition == UpsertDisposition::Created,
          "first observation must report created");
    check(created.job_id > 0, "created job record must carry a job id");
    check(created.revision == 1, "first revision must be 1");

    {
        const auto record = registry.jobRecord(created.job_id);
        check(record.has_value(), "job record must be readable");
        check(!record->index_key.empty(), "canonicalizable identity must be complete");
        check(record->index_key == canonicalize(selector_of(observation))->index_key(),
              "record must carry the canonical identity");
        check(record->job.JobNativeID == "172.0",
              "original native string must be preserved");
    }

    {
        const auto job = registry.findJob(created.job_id);
        check(job.has_value(), "registered job record must be findable");
        check(job->CollectorNames.empty(),
              "an observation upsert must not activate collection");
        check(pids_of(*job).size() == 1 && pids_of(*job).front() == 4242,
              "observed processes must become facts");
    }
    {
        // ONE record revision: the observation created revision 1, but it must
        // not decide collection. No automatic and no manual decision exist.
        const auto record = registry.jobRecord(created.job_id);
        check(record.has_value() && record->revision == created.revision,
              "the observation commits the record revision it reports");
        check(!record->selection.automatic.has_value() &&
                  !record->selection.manual.has_value() &&
                  record->selection.effectiveCollectors().empty(),
              "an observation must not create a collection decision");
    }
    check(registry.jobRecords().size() == 1, "exactly one job record");

    const auto repeat = registry.upsertObservedJob(observation);
    check(repeat.ok && repeat.disposition == UpsertDisposition::Unchanged,
          "repeat observation must be unchanged");
    check(repeat.job_id == created.job_id, "repeat must keep the job id");
    check(repeat.revision == created.revision,
          "unchanged facts must not advance the revision");
    check(registry.jobRecords().size() == 1, "repeat must not duplicate the entry");

    auto updated_source = observation;
    auto& attr = std::get<CondorJobAttr>(updated_source.sub_attr);
    attr.owner = "bob";
    updated_source.execution.pid_identities.push_back(ProcessIdentity{4343, 910000, "boot-aaa"});

    const auto updated = registry.upsertObservedJob(updated_source);
    check(updated.ok && updated.disposition == UpsertDisposition::Updated,
          "changed facts must report updated");
    check(updated.job_id == created.job_id,
          "fact updates must not change the job id");
    check(updated.revision == created.revision + 1,
          "changed facts must advance the revision exactly once");

    {
        const auto job = registry.findJob(created.job_id);
        check(job.has_value(), "updated job record must stay findable");
        check(std::get<CondorJobAttr>(job->sub_attr).owner == "alice",
              "execution updates must retain the creation-time owner");
        check(pids_of(*job).size() == 2, "new processes must merge into facts");
        check(job->CollectorNames.empty(),
              "fact updates must not activate collection either");
    }

    const auto repeat_update = registry.upsertObservedJob(updated_source);
    check(repeat_update.disposition == UpsertDisposition::Unchanged,
          "repeating the same update must be unchanged");

    // Incomplete identity is an explicit rejection, never a new record.
    auto incomplete = condor_observation("172.0");
    incomplete.clusterName.clear();
    const auto rejected = registry.upsertObservedJob(incomplete);
    check(!rejected.ok, "an incomplete identity must be rejected");
    check(rejected.error == "incomplete_identity",
          "incomplete identity must report a stable code, got " + rejected.error);
    check(rejected.job_id == 0, "rejected observations must not allocate an id");
    check(registry.jobRecords().size() == 1,
          "rejected observations must not create records");
}
// Schema 3 is a hard boundary. A store with data but no schema marker is an
// old-schema store: this build rejects it and leaves every row untouched. It
// never migrates or deletes in place.
void legacy_schema_is_rejected_without_migration(const std::string& base)
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

    {
        JobRegistry registry(isolated(path));
        const auto health = registry.storeHealth();
        check(!health.db_running, "an old-schema store must be rejected, not opened");
        check(!health.migration_complete,
              "a rejected store must not advertise readiness");
        check(health.migration_state.rfind("unsupported_schema", 0) == 0,
              "rejection must name the unsupported schema, got " + health.migration_state);
        check(registry.jobRecords().empty(),
              "a rejected store must not publish partial record set");
        check(revision_of(registry, active_id) == 0,
              "a rejected store must not synthesize a record");
        check(registry.getNextJobID() == 0,
              "a rejected store must not hand out ids");
    }

    // The rows are still there: rejection did not migrate or delete anything.
    check(raw_keys_with_prefix(path, "j:").size() == 1,
          "rejection must not delete active records");
    check(raw_keys_with_prefix(path, "h:").size() == 1,
          "rejection must not delete history");

    {
        JobRegistry again(isolated(path));
        check(!again.storeHealth().db_running,
              "an old-schema store must stay rejected on every open");
    }
}
// Rejection is all-or-nothing and idempotent, even for a populated store.
void unsupported_schema_is_rejected_idempotently(const std::string& base)
{
    const auto path = fresh_dir(base, "a20-reject");
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
        JobRegistry rejected(isolated(path));
        check(!rejected.storeHealth().db_running,
              "a populated old-schema store must be rejected");
        check(rejected.jobRecords().empty(),
              "rejection must not publish a partial record set");
    }
    check(raw_keys_with_prefix(path, "j:").size() == ids.size(),
          "rejection must preserve every record");

    {
        JobRegistry reopened(isolated(path));
        check(!reopened.storeHealth().db_running,
              "reopening must not switch rejection into migration");
        check(reopened.jobRecords().empty(),
              "a rejected store publishes no records on retry");
    }
    check(raw_keys_with_prefix(path, "j:").size() == ids.size(),
          "repeated rejection must not delete rows");
}

void conflicting_live_execution_is_not_merged(const std::string& base)
{
    const auto path = fresh_dir(base, "a20-conflict");
    JobRegistry registry(isolated(path));

    const auto first = registry.upsertObservedJob(condor_observation("172.0"));
    check(first.ok, "the first live execution must register");

    // A different live manager for the same identity is a conflict, not a
    // merge: the second execution must not receive a second internal id.
    auto conflicting = condor_observation("172.0");
    ProcessIdentity manager;
    manager.boot_id = "boot-bbb";
    manager.pid = 5252;
    manager.start_ticks = 990000;
    conflicting.execution.manager = manager;
    conflicting.execution.pid_identities = {ProcessIdentity{5252, 990000, "boot-bbb"}};
    const auto conflict = registry.upsertObservedJob(conflicting);
    check(!conflict.ok && conflict.error == "execution_manager_conflict",
          "a different live manager must not merge into the old record");

    check(registry.jobRecords().size() == 1,
          "a conflicting live execution must not create a second instance");
    const auto identity = canonicalize(condor_selector("172.0"));
    const auto indexed = record_by_identity(registry, *identity);
    check(indexed.has_value() && indexed->job.JobID == first.job_id,
          "the original live execution keeps the identity index");
    check(registry.storeHealth().quarantined_identities == 0,
          "a live conflict is rejected, not quarantined");
}

// Restore validation (supports /) --------------------------------------

void stale_manager_is_not_replayed(const std::string& base)
{
    const auto path = fresh_dir(base, "a20-stale");
    const pid_t manager_pid = 2000001;   // no such process
    uint64_t job_id = 0;

    {
        JobRegistry registry(isolated(path));
        auto observation = condor_observation("172.0");
        observation.execution.manager = ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"};
        observation.execution.pid_identities = {
            ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"}};
        const auto created = registry.upsertObservedJob(observation);
        check(created.ok, "registration with a manager must succeed");
        job_id = created.job_id;
    }

    {
        JobRegistry registry(isolated(path));
        const auto record = registry.jobRecord(job_id);
        check(record.has_value(), "the record must still be restored");
        check(record->state == RecordState::Active,
              "a stale manager is not a lifecycle state: the record stays Active");
        check(!record->presumed_live,
              "a stale execution manager must be restored as unverified");
        const auto job = registry.findJob(job_id);
        check(job.has_value() && pids_of(*job).empty(),
              "stale PIDs must not be replayed into the registry");

        // The same manager observed again recovers the record; a different
        // manager is a conflict, not a revival (covered by ).
        auto same = condor_observation("172.0");
        same.execution.manager = ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"};
        same.execution.pid_identities = {ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"}};
        const auto recovered = registry.upsertObservedJob(same);
        check(recovered.ok && recovered.job_id == job_id,
              "the same manager must resolve to the restored record");
        const auto active = registry.jobRecord(job_id);
        check(active.has_value() && active->state == RecordState::Active,
              "a valid observation must reactivate a suspect record");
    }
}
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
    check(!record_by_identity(registry, *canonicalize(condor_selector("200.0")))
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
    check(retried.job_id > first.job_id,
          "the burned id must never be reused");

    // (2) Ambiguous outcome: the batch landed but the status reported failure.
    // The record batch is the first durable write of a registration now
    // (skip=0), so this is the create itself landing ambiguously.
    registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0);
    const auto ambiguous = registry.upsertObservedJob(condor_observation("300.0"));
    check(!ambiguous.ok, "an ambiguous write outcome must not report success");

    const auto resolved = registry.upsertObservedJob(condor_observation("300.0"));
    check(resolved.ok, "an ambiguous registration must resolve by identity lookup");
    check(resolved.job_id > retried.job_id,
          "ambiguous allocation must not reuse a lower internal id");

    // (3) Binding transaction failure must leave the previous intent intact.
    // No Config here: accept every name, the focus is the write failure.
    registry.setCollectorValidator(nullptr);
    CollectionRequest set;
    set.targets = {first.job_id};
    set.collectors = std::vector<std::string>{"cpumem_collector"};
    const auto accepted = registry.setCollectors(set);
    check(accepted.ok, "baseline binding must be accepted: " + accepted.message);

    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
    set.collectors = std::vector<std::string>{"disk_collector"};
    const auto rejected = registry.setCollectors(set);
    check(!rejected.ok, "a failed binding write must not report success");
    check(rejected.error == CollectionError::StoreUnavailable,
          "binding store failure must be distinguishable");
    {
        const auto selection = registry.selectionRecord(first.job_id);
        const auto selection_revision = revision_of(registry, first.job_id);
        check(selection_revision == accepted.targets.front().revision,
              "a failed selection write must not advance the revision");
        check(selection.manual.has_value() &&
                  *selection.manual == std::vector<std::string>{"cpumem_collector"},
              "a failed selection write must not rewrite the intent");
    }

    // (4) Archive transaction failure must keep the instance active.
    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
    check(!registry.retireInstance(retried.job_id),
          "a failed archive must not report durable retirement");
    check(registry.findJob(retried.job_id).has_value(),
          "a failed archive must leave the instance readable");
    check(registry.jobRecord(retried.job_id).has_value(),
          "a failed archive must leave the job record intact");

    check(registry.retireInstance(retried.job_id),
          "retirement must succeed once the store recovers");
    check(!registry.findJob(retried.job_id).has_value(),
          "successful retirement removes the active record");
    check(revision_of(registry, retried.job_id) == 0,
          "successful retirement removes the record, not just its selection");

    registry.clearStoreFaultsForTest();
}
void closing_state_is_durable_and_not_resurrected(const std::string& base)
{
    const auto path = fresh_dir(base, "a24-closing");
    uint64_t job_id = 0;

    {
        JobRegistry registry(isolated(path));
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        job_id = created.job_id;

        // A subscriber makes the retirement a drain: the record must survive
        // the process as closing, not as an active instance. The injected
        // fault models the archive failing (or the process dying) before the
        // drain completes — exactly the restart scenario being tested.
        install_deferred_drain(registry, [](JobEvent, const Job&) {});
        check(registry.retireInstance(job_id),
              "retirement with a subscriber must be accepted");
        check(!has_active(registry, job_id),
              "a closing instance must leave the active map");
        check(registry.jobRecord(job_id).has_value(),
              "a closing instance stays visible until the archive");
        // The shutdown checkpoint is the first durable write deinit performs;
        // skip it so the injected failure lands on the archive (the failure
        // under test), exactly like a disk error during archival.
        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/1);
        check(!registry.deinit(),
              "deinit must report a shutdown that could not archive the drain");
    }

    {
        // Restart on the same DB: the interrupted retirement must be restored
        // as closing, never as an active job, and observations of the same
        // identity must be refused instead of resurrecting the instance.
        JobRegistry registry(isolated(path));
        const auto record = registry.jobRecord(job_id);
        check(record.has_value(), "the closing record must be restored");
        check(record->state == RecordState::Closing,
              "the restored record must carry the durable closing state, got " +
                  std::string(to_string(record->state)));
        check(!has_active(registry, job_id),
              "a restored closing instance must not be an active job");
        check(!registry.findJob(job_id).has_value(),
              "a restored closing instance must not be findable as active");

        const auto late = registry.upsertObservedJob(condor_observation("172.0"));
        check(!late.ok && late.error == "instance_closing",
              "a closing instance must reject late observations, got " + late.error);

        // The closing cleanup finishes the interrupted retirement.
        check(registry.cleanupClosingJobs(), "the closing cleanup must archive the record");
        check(!registry.jobRecord(job_id).has_value(),
              "the archive must remove the closing record");
        const auto history = registry.upsertObservedJob(condor_observation("172.0"));
        check(history.ok && history.disposition == UpsertDisposition::Created,
              "after the archive the identity can register again as a new instance");
    }
}
void restored_suspect_instances_are_not_replayed(const std::string& base)
{
    const auto path = fresh_dir(base, "a25-suspect");
    const pid_t manager_pid = 2000002;   // no such process
    uint64_t job_id = 0;

    {
        JobRegistry registry(isolated(path));
        auto observation = condor_observation("172.0");
        observation.execution.manager = ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"};
        observation.execution.pid_identities = {
            ProcessIdentity{manager_pid, 123456, "boot-does-not-exist"}};
        const auto created = registry.upsertObservedJob(observation);
        check(created.ok, "registration must succeed");
        job_id = created.job_id;
    }

    {
        JobRegistry registry(isolated(path));
        const auto restored = registry.jobRecord(job_id);
        check(restored.has_value() && restored->state == RecordState::Active &&
                  !restored->presumed_live,
              "the stale manager must be restored Active and not presumed live");

        size_t added = 0;
        install_deferred_drain(registry, [&added, job_id](JobEvent event, const Job& job) {
            if (event == JobEvent::Added && job.JobID == job_id) ++added;
        });
        test_runtime(registry).reconcile(job_id);
        check(added == 0,
              "an unverified execution must not be replayed to collectors after a restart");
        check(registry.actualCollection(job_id).collectors.empty(),
              "a facts-only restored record selects no collector");
    }
}
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
        E::IdExhausted, E::ManagerConflict, E::InstanceClosing, E::IdentityQuarantined,
        E::IdentityIndexMismatch, E::StaleRevision, E::NotAdmitted, E::PolicyEvaluationFailed,
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
    incomplete.clusterName.clear();
    const auto rejected = registry.upsertObservedJob(incomplete);
    check(!rejected.ok && rejected.code == E::IncompleteIdentity,
          "an incomplete identity must carry its typed code");
    check(rejected.error == "incomplete_identity",
          "the wire string must stay unchanged, got " + rejected.error);
    check(rejected.retry_registration(), "the typed result must project to a source retry");
}

// durability tiering ---------------------------------------------------

// The registration hot path must not fence: facts and intent are written
// without a sync and made durable by one checkpoint (or by shutdown).
void hot_path_registration_does_not_fsync(const std::string& base)
{
    const auto path = fresh_dir(base, "sync-tiering");
    JobRegistry registry(isolated(path));

    const auto before = registry.storeHealth();
    const auto first = registry.upsertObservedJob(condor_observation("801.0"));
    const auto second = registry.upsertObservedJob(condor_observation("802.0"));
    const auto third = registry.upsertObservedJob(condor_observation("803.0"));
    check(first.ok && second.ok && third.ok, "the registrations must succeed");
    registry.setCollectorValidator(nullptr);
    const auto committed = registry.applyAutoPolicy(
        {{first.job_id, {"cpumem_collector"}, "sync-tiering"}});
    check(committed.ok, "the binding must commit: " + committed.message);

    // A registry-owned selection command is also intent-hot: it must not turn
    // the unsynced control path into a per-request fence.
    CollectionRequest selection;
    selection.targets = {first.job_id};
    selection.collectors = std::vector<std::string>{"disk_collector"};
    const auto selected = registry.setCollectors(selection);
    check(selected.ok, "the selection command must commit: " + selected.message);
    check(selected.targets.front().revision == committed.targets.front().revision + 1,
          "the selection command must advance the stored revision exactly once");
    check(revision_of(registry, first.job_id) ==
              selected.targets.front().revision,
          "the accepted revision must be the committed record revision");

    const auto after = registry.storeHealth();
    check(after.sync_commits == before.sync_commits,
          "registration and intent must not fsync, saw " +
              std::to_string(after.sync_commits - before.sync_commits) + " sync commit(s)");
    check(after.durable_commits > before.durable_commits,
          "the writes must still reach LevelDB (unsynced)");

    // The checkpoint (or shutdown) is where the fence happens.
    const auto synced_before = registry.storeHealth().sync_commits;
    check(registry.deinit(), "shutdown must succeed");
    (void)synced_before;
}

// shutdown recovery ----------------------------------------------------

// Everything written during runtime survives a graceful restart even though the
// hot path never fenced: deinit() flushes the dirty set before closing.
void shutdown_flush_persists_runtime_changes(const std::string& base)
{
    const auto path = fresh_dir(base, "shutdown-flush");
    std::uint64_t id = 0;
    {
        JobRegistry registry(isolated(path));
        registry.setCollectorValidator(nullptr);
        const auto created = registry.upsertObservedJob(condor_observation("810.0"));
        check(created.ok, "the instance must register");
        id = created.job_id;

        std::string error;
        CollectionRequest selection;
        selection.targets = {id};
        selection.collectors = std::vector<std::string>{"cpumem_collector"};
        const auto committed = registry.setCollectors(selection);
        check(committed.ok, "the binding must commit: " + committed.message);
        check(registry.deinit(), "shutdown must succeed");
    }
    {
        // Fresh process on the same directory: no re-observation, no rescan —
        // the state comes back from the shutdown flush only.
        JobRegistry registry(isolated(path));
        const auto record = registry.jobRecord(id);
        check(record.has_value(), "the instance must survive the restart");
        check(record->job.JobNativeID == "810.0", "the facts must survive the restart");
        const auto binding = registry.selectionRecord(id);
        check(binding.mode() == CollectionMode::Manual &&
                  binding.manual.value_or(std::vector<std::string>{}) == std::vector<std::string>{"cpumem_collector"},
              "the intent must survive the restart");
    }
}

// fresh candidates ----------------------------------------------------

// A second observation for one identity with a different live manager triple
// conflicts exactly like a merge would: it never overwrites the first.
void conflicting_fresh_candidates_are_rejected(const std::string& base)
{
    const auto path = fresh_dir(base, "fresh-conflict");
    JobRegistry registry(isolated(path));

    Job first = condor_observation("903.0");
    Job second = condor_observation("903.0");
    second.execution.manager =
        ProcessIdentity{8888, 777777, "boot-aaa"};

    const auto created = registry.upsertObservedJob(first);
    check(created.ok && created.disposition == UpsertDisposition::Created,
          "the first candidate must be created");
    const auto conflict = registry.upsertObservedJob(second);
    check(!conflict.ok && conflict.code == RegistrationError::ManagerConflict,
          "a different manager triple for the same identity must conflict");
    const auto stored = registry.jobRecord(created.job_id);
    check(stored.has_value(), "the first candidate must stay readable");
    check(stored->job.execution.manager.pid == 4242,
          "the conflicting candidate must not overwrite the stored manager");
    check(registry.jobRecords().size() == 1, "only one instance may exist");
}

// working set ---------------------------------------------------------

// Repeated observations for one existing identity publish the last state, and
// the published memory state equals what a restart reads back.
void repeated_observations_keep_memory_and_disk_in_sync(const std::string& base)
{
    const auto path = fresh_dir(base, "working-set");
    std::uint64_t id = 0;
    std::string published_owner;
    std::uint64_t published_revision = 0;
    {
        JobRegistry registry(isolated(path));
        const auto created = registry.upsertObservedJob(condor_observation("904.0"));
        check(created.ok, "the seed instance must register");
        id = created.job_id;

        Job after = condor_observation("904.0");
        std::get<CondorJobAttr>(after.sub_attr).owner = "after";
        Job before = condor_observation("904.0");
        std::get<CondorJobAttr>(before.sub_attr).owner = "before";

        const auto first = registry.upsertObservedJob(after);
        const auto second = registry.upsertObservedJob(before);
        check(first.ok && second.ok &&
                  first.disposition == UpsertDisposition::Unchanged &&
                  second.disposition == UpsertDisposition::Unchanged &&
                  second.revision == created.revision,
              "metadata-only repeats must be accepted without changing the record revision");

        const auto live = registry.jobRecord(id);
        check(live.has_value(), "the instance must stay readable");
        published_owner = std::get<CondorJobAttr>(live->job.sub_attr).owner;
        published_revision = live->revision;
        check(published_revision == second.revision,
              "the published revision must be the final observation's");
        check(published_owner == "alice",
              "the working set must retain the initial owner, got " + published_owner);
        check(registry.deinit(), "shutdown must succeed");
    }
    {
        // A restart reads the durable row: it must agree with what was
        // published before the shutdown instead of diverging from it.
        JobRegistry registry(isolated(path));
        const auto reopened = registry.jobRecord(id);
        check(reopened.has_value(), "the instance must survive the restart");
        const auto owner = std::get<CondorJobAttr>(reopened->job.sub_attr).owner;
        check(owner == published_owner,
              "the restarted record must agree with the published owner ('" + owner +
                  "' vs '" + published_owner + "')");
        check(reopened->revision == published_revision,
              "the restarted revision must match the published revision");
    }
}
void member_semantics_distinguish_unknown_from_confirmed_empty(const std::string& base)
{
    const auto path = fresh_dir(base, "a26-members");
    JobRegistry registry(isolated(path));

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok && created.disposition == UpsertDisposition::Created,
          "baseline registration must succeed");
    check(pids_of(*registry.findJob(created.job_id)).size() == 1,
          "the fixture must register one member pid");

    // A partially-known membership with an empty list must not erase members:
    // the source could not read everything, so "no members" is not proven.
    auto partial = condor_observation("172.0");
    partial.execution.pid_identities.clear();
    partial.execution.pid_identities_authoritative = false;
    const auto kept = registry.upsertObservedJob(partial);
    check(kept.ok && kept.disposition == UpsertDisposition::Unchanged,
          "a non-authoritative empty list must change nothing");
    check(pids_of(*registry.findJob(created.job_id)).size() == 1,
          "a non-authoritative empty list must not erase members");

    // An authoritative empty list is positive evidence: the stored members
    // are replaced by the confirmed empty set.
    auto confirmed_empty = condor_observation("172.0");
    confirmed_empty.execution.pid_identities.clear();
    confirmed_empty.execution.pid_identities_authoritative = true;
    const auto cleared = registry.upsertObservedJob(confirmed_empty);
    check(cleared.ok && cleared.disposition == UpsertDisposition::Updated,
          "a confirmed empty membership must be applied to facts");
    check(pids_of(*registry.findJob(created.job_id)).empty(),
          "a confirmed empty membership must clear stale member pids");
    const auto record = registry.jobRecord(created.job_id);
    check(record.has_value() && record->job.execution.pid_identities.empty(),
          "the stored process evidence must follow the confirmed empty set");

    // Normalization removes exact duplicates, not distinct bindings of one PID.
    auto member_facts = condor_observation("173.0");
    member_facts.execution.pid_identities = {
        {4444, 920000, "boot-aaa"}, {4343, 910000, "boot-aaa"},
        {4343, 0, {}}, {4343, 910000, "boot-aaa"}};
    const auto with_members = registry.upsertObservedJob(member_facts);
    check(with_members.ok && with_members.disposition == UpsertDisposition::Created,
          "the member normalization fixture must register");
    const auto normalized = registry.jobRecord(with_members.job_id);
    check(normalized.has_value() && normalized->job.execution.pid_identities ==
              std::vector<ProcessIdentity>{{4343, 0, {}}, {4343, 910000, "boot-aaa"},
                                           {4444, 920000, "boot-aaa"}},
          "create must sort full identities and retain distinct bindings of one PID");
    const auto repeated = registry.upsertObservedJob(member_facts);
    check(repeated.ok && repeated.disposition == UpsertDisposition::Unchanged &&
              repeated.revision == with_members.revision,
          "reordered and duplicate input members must not advance the revision");

    // A binding-only change is a fact change even though the PID list is equal.
    member_facts.execution.pid_identities = {
        {4444, 920000, "boot-aaa"}, {4343, 930000, "boot-aaa"}, {4343, 0, {}}};
    const auto rebound = registry.upsertObservedJob(member_facts);
    check(rebound.ok && rebound.disposition == UpsertDisposition::Updated &&
              rebound.revision == with_members.revision + 1,
          "a changed start tick with the same PIDs must advance the revision");
    const auto rebound_record = registry.jobRecord(with_members.job_id);
    check(rebound_record.has_value() && rebound_record->job.execution.pid_identities ==
              std::vector<ProcessIdentity>{{4343, 0, {}}, {4343, 930000, "boot-aaa"},
                                           {4444, 920000, "boot-aaa"}},
          "a merge must retain the newly observed complete process identities");

    // Preserve the current partial-nonempty rule: replace, do not union or bind.
    member_facts.execution.pid_identities = {{4444, 0, {}}};
    member_facts.execution.pid_identities_authoritative = false;
    const auto replaced = registry.upsertObservedJob(member_facts);
    check(replaced.ok && replaced.disposition == UpsertDisposition::Updated &&
              replaced.revision == rebound.revision + 1,
          "a nonempty partial member observation must replace the stored set");
    const auto partial_record = registry.jobRecord(with_members.job_id);
    check(partial_record.has_value() && partial_record->job.execution.pid_identities ==
              std::vector<ProcessIdentity>{{4444, 0, {}}},
          "partial replacement must neither retain absent PIDs nor reuse old bindings");
}

} // namespace
void registration_keeps_single_upsert_semantics(const std::string& base)
{
    const auto path = fresh_dir(base, "a29");
    JobRegistry registry(isolated(path));
    // No Config in this test: the name gate is not what is under test here.
    registry.setCollectorValidator(nullptr);

    auto first = condor_observation("501.0");
    auto second = condor_observation("502.0");
    auto incomplete = condor_observation("503.0");
    incomplete.clusterName.clear();   // no namespace -> no identity

    // Fresh executions: each is created with its own internal id.
    const auto first_result = registry.upsertObservedJob(first);
    const auto second_result = registry.upsertObservedJob(second);
    const auto incomplete_result = registry.upsertObservedJob(incomplete);
    check(first_result.ok && second_result.ok, "fresh executions must be created");
    check(first_result.disposition == UpsertDisposition::Created,
          "a fresh registration must report created");
    check(first_result.job_id != second_result.job_id,
          "each instance gets its own internal id");
    check(!incomplete_result.ok && incomplete_result.error == "incomplete_identity",
          "an incomplete identity stays rejected");
    check(registry.jobRecords().size() == 2, "only the complete identities persist");

    // The same input again is a merge, not a duplicate: the ids are unchanged.
    const auto repeat_first = registry.upsertObservedJob(first);
    const auto repeat_second = registry.upsertObservedJob(second);
    check(repeat_first.ok && repeat_second.ok, "replaying the input must succeed");
    check(repeat_first.disposition != UpsertDisposition::Created,
          "a replayed input must not create a second instance");
    check(repeat_first.job_id == first_result.job_id &&
              repeat_second.job_id == second_result.job_id,
          "a replayed input must resolve to the same instances");

    // The automatic intent is one transaction for the batch of selections: the
    // applier result is visible per instance.
    std::vector<AutoSelection> selections;
    for (const auto& result : {first_result, second_result}) {
        if (!result.ok) continue;
        selections.push_back(AutoSelection{result.job_id, {"cpumem_collector"}, "test"});
    }
    const auto committed = registry.applyAutoPolicy(selections);
    check(committed.ok,
          "one intent transaction must accept every binding: " + committed.message);
    for (const auto& selection : selections) {
        const auto stored = registry.selectionRecord(selection.job_id);
        check(stored.automatic.has_value() &&
                  *stored.automatic == std::vector<std::string>{"cpumem_collector"},
              "the automatic decision must be readable per instance");
        // No runtime was installed on this isolated registry: the actual state
        // is untracked, never inferred from the desired selection.
        const auto actual = registry.actualCollection(selection.job_id);
        check(!actual.tracked && actual.collectors.empty() && actual.attach_token == 0,
              "without a runtime the actual state must stay untracked");
    }
}
// The internal ID space must never reissue a value it already handed out. The
// legacy allocator stores only `c:job_id`; at the ceiling that is ambiguous
// ("MAX is still available" vs "MAX was already issued"), so the process must
// refuse, and the refusal must survive a restart.
void max_job_id_is_never_reissued(const std::string& base)
{
    constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();

    // (a) Legacy counter one below the ceiling, no records: this process may
    // consume the last two values and then must refuse; after a restart the
    // already-issued MAX may not be handed out again.
    {
        const auto path = fresh_dir(base, "a26a");
        write_raw_db(path, {{"m:schema_version", "5"},
                            {"c:job_id", std::to_string(kMax - 1)}});
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
        write_raw_db(path, {{"m:schema_version", "5"},
                            {"c:job_id", std::to_string(kMax)}});
        JobRegistry registry(isolated(path));
        check(registry.getNextJobID() == 0,
              "a marker-less MAX counter must be treated as exhausted");
    }

    // (c) New format: an explicit marker decides. `false` allows exactly one
    // final allocation, which flips the marker to `true` in the same durable
    // transaction; the exhaustion then survives the restart.
    {
        const auto path = fresh_dir(base, "a26c");
        write_raw_db(path, {{"m:schema_version", "5"},
                            {"c:job_id", std::to_string(kMax)},
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
        write_raw_db(path, {{"m:schema_version", "5"},
                            {"c:job_id", std::to_string(kMax)},
                            {"m:job_id_exhausted", "true"}});
        JobRegistry registry(isolated(path));
        check(registry.getNextJobID() == 0, "marker=true must refuse every allocation");
    }

    // (e) A live record already holding MAX forces exhaustion even when the
    // counter value alone would look free.
    {
        const auto path = fresh_dir(base, "a26e");
        write_raw_db(path, {
            {"m:schema_version", "5"},
            {"j:" + hex64(kMax),
             legacy_condor_record("172.0", "htcondor01", "schedd-a", {}, 4100).dump()},
            {"c:job_id", std::to_string(kMax - 1)},
        });
        JobRegistry registry(isolated(path));
        check(registry.getNextJobID() == 0,
              "an existing MAX record must force exhaustion");
    }
}
// The runtime result is memory-only truth for this process: unchanged facts,
// changed facts and intent commits must never clear or fabricate it.
void runtime_state_survives_later_durable_writes(const std::string& base)
{
    const auto path = fresh_dir(base, "a11-runtime");
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
    const uint64_t id = created.job_id;

    std::string error;
    const auto initial = registry.applyAutoPolicy(
        {{id, {"cpumem_collector"}, "a11"}});
    check(initial.ok, "binding must commit: " + initial.message);
    test_runtime(registry).reconcile(id);
    const uint64_t applied_revision = revision_of(registry, id);
    {
        const auto applied = test_runtime(registry).applied(id);
        check(applied.applied &&
                  applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "the runtime result must be visible after apply");
        check(applied.revision == applied_revision,
              "the actual revision must be the record revision the runtime read");
    }

    // (1) Unchanged discovery pass: neither facts nor runtime state may move.
    const auto unchanged = registry.upsertObservedJob(condor_observation("172.0"));
    check(unchanged.ok && unchanged.disposition == UpsertDisposition::Unchanged,
          "a repeated observation must stay unchanged");
    {
        const auto applied = test_runtime(registry).applied(id);
        check(applied.applied &&
                  applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "an unchanged upsert must not clear the runtime holders");
        check(applied.revision == applied_revision,
              "an unchanged upsert must not move the applied revision");
    }

    // (2) Changed facts must not pretend the new evidence is already applied,
    // and must not drop the holders that are really attached.
    auto changed = condor_observation("172.0");
    changed.execution.pid_identities.push_back(ProcessIdentity{5151, 424242, "boot-aaa"});
    const auto updated = registry.upsertObservedJob(changed);
    check(updated.ok && updated.disposition == UpsertDisposition::Updated,
          "a changed observation must be an update");
    {
        const auto applied = test_runtime(registry).applied(id);
        check(applied.applied &&
                  applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "a facts update must not erase the current runtime result");
    }

    // (3) An intent the runtime only partially honoured must not be reported
    // as applied, and the holders it really reports must stay visible.
    install_collection_runtime(registry, [](const Job&, std::vector<std::string>& applied,
                                     std::string& error) {
        applied = {"cpumem_collector"};   // the old lane is still attached
        error = "collector_failed";
        return false;
    });
    const uint64_t before_second = revision_of(registry, id);
    const auto actual_before_second = test_runtime(registry).applied(id);
    const auto second = registry.applyAutoPolicy(
        {{id, {"cpumem_collector", "disk_collector"}, "a11"}});
    check(second.ok, "second binding must commit: " + second.message);
    check(second.targets.front().revision == before_second + 1,
          "the second binding must advance the record revision exactly once");
    {
        // Acceptance is not application: until the runtime's own pass runs, the
        // actual state still names the previous attachment and revision.
        const auto before = registry.actualCollection(id);
        check(before.tracked &&
                  before.collectors == std::vector<std::string>{"cpumem_collector"},
              "the accepted intent must not be reported as attached");
        check(before.last_revision == actual_before_second.revision,
              "the actual state must still name the revision it converged to");
    }
    test_runtime(registry).reconcile(id);
    {
        const auto applied = test_runtime(registry).applied(id);
        check(!applied.applied, "a rejected apply must not report the intent as applied");
        check(applied.collectors == std::vector<std::string>{"cpumem_collector"},
              "the holders the runtime still reports must stay visible");
        check(applied.revision == second.targets.front().revision,
              "the applied record must point at the revision it tried");
        check(applied.error == "collector_failed",
              "the runtime error must be visible, got '" + applied.error + "'");
    }

    // The retry path converges once the runtime accepts the intent again.
    install_collection_runtime(registry, [](const Job& job, std::vector<std::string>& applied,
                                     std::string& error) {
        applied = job.CollectorNames;
        error.clear();
        return true;
    });
    test_runtime(registry).reconcile(id);
    {
        const auto converged = test_runtime(registry).applied(id);
        check(converged.applied && converged.collectors.size() == 2,
              "the retry must converge to the new intent");
        check(converged.revision == revision_of(registry, id),
              "the applied revision must converge to the current record revision");
    }
}
// Archiving must leave no active `j:` row behind, even when the runtime result
// of that instance was recorded as a coalesced hint that had not been flushed
// to disk yet. This is the deterministic half of ; the coalesced flush is
// deleted in P2b, so a concurrent interleaving has no code path to race after
// that change.
void archived_instances_leave_no_active_row(const std::string& base)
{
    const auto path = fresh_dir(base, "a12-orphan");
    uint64_t id = 0;
    {
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
        id = created.job_id;

        const auto committed = registry.applyAutoPolicy(
            {{id, {"cpumem_collector"}, "a12"}});
        check(committed.ok, "binding must commit: " + committed.message);
        test_runtime(registry).reconcile(id);

        // Retirement is accepted as a durable Closing revision; the runtime's
        // own pass proves the boundary and the archive releases the identity.
        check(registry.retireInstance(id), "retirement must be accepted");
        check(registry.jobRecord(id).has_value() &&
                  registry.jobRecord(id)->state == RecordState::Closing,
              "the accepted retirement must stay Closing until the runtime drains");
        test_runtime(registry).reconcile(id);
        check(!registry.jobRecord(id).has_value(),
              "the archived instance must leave the active map");
        check(registry.deinit(), "shutdown must not fail after a clean archive");
    }

    const auto active_keys = raw_keys_with_prefix(path, "j:");
    check(active_keys.empty(), "no active row may survive an archive, found " +
                                   std::to_string(active_keys.size()));
    const auto history_keys = raw_keys_with_prefix(path, "h:");
    check(history_keys.size() == 1, "the archive must leave exactly one history row");
}
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
        first_id = first.job_id;

        registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0);
        const auto ambiguous = registry.upsertObservedJob(condor_observation("200.0"));
        check(!ambiguous.ok, "an ambiguous write must not report success");
        check(registry.deinit(), "shutdown after an ambiguous write must succeed");
    }
    {
        JobRegistry registry(isolated(path));
        const auto identity = canonicalize(condor_selector("200.0"));
        check(identity.has_value(), "the fixture selector must canonicalize");
        const auto restored = record_by_identity(registry, *identity);
        check(restored.has_value(),
              "the record that landed ambiguously must be restored by identity");
        check(restored->job.JobID > first_id,
              "the restored instance must keep its own internal id");

        const auto again = registry.upsertObservedJob(condor_observation("200.0"));
        check(again.ok, "the identity must be re-registrable after the restart");
        check(again.disposition != UpsertDisposition::Created,
              "an ambiguous create must not mint a second instance after a restart");
        check(again.job_id == restored->job.JobID,
              "the identity must resolve to the same internal id");
        check(registry.jobRecords().size() == 2,
              "exactly the two real instances may exist");
    }
}
// A create, a fact merge, a conflicting manager and a repeat of the newly
// created identity each resolve to the single-upsert semantics; the conflict is
// reported per call without blocking the others.
void sequential_merges_keep_single_upsert_semantics(const std::string& base)
{
    const auto path = fresh_dir(base, "a03-merge");
    JobRegistry registry(isolated(path));

    const auto seed_a = registry.upsertObservedJob(condor_observation("601.0"));
    const auto seed_b = registry.upsertObservedJob(condor_observation("602.0"));
    check(seed_a.ok && seed_b.ok, "the seed instances must register");

    Job fresh = condor_observation("603.0");
    Job changed = condor_observation("601.0");
    changed.execution.pid_identities.push_back(ProcessIdentity{5151, 424242, "boot-aaa"});
    Job conflict = condor_observation("602.0");
    conflict.execution.manager =
        ProcessIdentity{7777, 888888, "boot-aaa"};
    Job duplicate = condor_observation("603.0");
    duplicate.execution.pid_identities.push_back(ProcessIdentity{5252, 424242, "boot-aaa"});

    const auto created = registry.upsertObservedJob(fresh);
    const auto merged = registry.upsertObservedJob(changed);
    const auto conflicted = registry.upsertObservedJob(conflict);
    const auto repeated = registry.upsertObservedJob(duplicate);

    check(created.ok && created.disposition == UpsertDisposition::Created,
          "the fresh identity must be created");
    check(merged.ok && merged.disposition == UpsertDisposition::Updated,
          "the changed identity must merge");
    check(merged.job_id == seed_a.job_id,
          "the merge must resolve to the stored instance");
    check(!conflicted.ok && conflicted.code == RegistrationError::ManagerConflict,
          "a different live manager must be reported as a conflict");
    check(conflicted.job_id == seed_b.job_id,
          "the conflict must name the instance it belongs to");
    check(repeated.ok && repeated.job_id == created.job_id,
          "a repeated observation must resolve to the stored instance");

    const auto stored = registry.jobRecord(created.job_id);
    check(stored.has_value() && stored->revision == 2,
          "the repeated observation must have merged into the created record");
    check(registry.jobRecords().size() == 3,
          "exactly three instances may exist");
}

// Schema 3: desired selection is one field of the record that owns it. There
// is no second `b:` row to keep consistent, and a restart reads the intent
// from the same row as the facts.
void selection_intent_lives_in_the_record_row(const std::string& base)
{
    const auto path = fresh_dir(base, "schema3-intent");
    uint64_t id = 0;
    {
        JobRegistry registry(isolated(path));
        registry.setCollectorValidator(nullptr);
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.job_id;

        CollectionRequest request;
        request.targets = {id};
        request.collectors = std::vector<std::string>{"cpumem_collector"};
        const auto selected = registry.setCollectors(request);
        check(selected.ok, "the manual selection must commit: " + selected.message);
        check(selected.targets.front().revision == created.revision + 1,
              "the manual selection must advance the one record revision");
        check(registry.deinit(), "shutdown must succeed");
    }

    check(raw_keys_with_prefix(path, "b:").empty(),
          "schema 5 must not write a separate binding row");
    const auto rows = read_raw_db(path);
    bool found_intent = false;
    for (const auto& [key, value] : rows) {
        if (key != "j:" + hex64(id)) continue;
        const auto record = nlohmann::json::parse(value);
        check(record.contains("selection"), "the record row must carry the selection");
        check(record["selection"].contains("manual") && !record["selection"]["manual"].is_null(),
              "the stored selection must be the manual one");
        check(record.value("revision", uint64_t{0}) == 2,
              "the stored revision must be the one the selection committed");
        found_intent = true;
    }
    check(found_intent, "the instance row must still exist");

    {
        JobRegistry registry(isolated(path));
        const auto binding = registry.selectionRecord(id);
        const auto binding_revision = revision_of(registry, id);
        check(binding.mode() == CollectionMode::Manual &&
                  binding.manual.value_or(std::vector<std::string>{}) == std::vector<std::string>{"cpumem_collector"},
              "a restart must restore the intent from the record row");
        check(binding_revision == 2,
              "the restored revision must be the one the selection committed");
    }
}

// A failed closing write must not start any destructive path: the record stays
// active, no Removed event is delivered and the drain is never entered.
void closing_write_failure_keeps_the_instance(const std::string& base)
{
    const auto path = fresh_dir(base, "closing-sync-failure");
    JobRegistry registry(isolated(path));
    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");

    size_t removed_events = 0;
    install_deferred_drain(registry, [&removed_events](JobEvent event, const Job&) {
        if (event == JobEvent::Removed) ++removed_events;
    });

    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite);
    check(!registry.retireInstance(created.job_id),
          "a failed closing sync must refuse retirement");
    registry.clearStoreFaultsForTest();

    check(removed_events == 0,
          "a failed closing sync must not deliver Removed");
    const auto record = registry.jobRecord(created.job_id);
    check(record.has_value() && record->state == RecordState::Active,
          "a failed closing sync must leave the instance active");
    check(registry.findJob(created.job_id).has_value(),
          "the instance must remain readable after the refused retirement");
}

// canonical marker ----------------------------------------------------

// The schema marker this build writes is a canonical decimal. Anything else -
// a prefix parse like "3garbage", a value that only fits after truncation, an
// older/newer schema, or non-digit bytes - is not this build's layout: it is
// rejected before any UUID, id counter or record is initialized, and every
// original key/value stays exactly as it was.
void schema_marker_must_be_canonical(const std::string& base)
{
    const std::vector<std::string> markers = {
        "3garbage", "4294967299", "2", "4", " 3", "3 ", "03", "-3", "", "0x3",
    };
    for (size_t index = 0; index < markers.size(); ++index) {
        const auto marker = markers[index];
        const auto path = fresh_dir(base, "a19b-" + std::to_string(index));
        write_raw_db(path, {
            {"m:schema_version", marker},
            {"m:db_uuid", "uuid-keep"},
            {"m:custom_metadata", "keep-me"},
            {"j:" + hex64(41),
             legacy_condor_record("172.0", "htcondor01", "schedd-a",
                                  {"cpumem_collector"}, 4100).dump()},
            {"c:job_id", "41"},
        });
        const auto before = read_raw_db(path);
        {
            JobRegistry registry(isolated(path));
            const auto health = registry.storeHealth();
            check(!health.db_running,
                  "marker '" + marker + "' must be rejected, not opened");
            check(!health.migration_complete,
                  "marker '" + marker + "' must not advertise readiness");
            check(!health.recovery_required,
                  "a rejected schema is a read-only rejection, not a pending recovery");
            check(registry.jobRecords().empty(),
                  "marker '" + marker + "' must not publish records");
            check(revision_of(registry, 41) == 0,
                  "marker '" + marker + "' must not synthesize intent");
            check(registry.getNextJobID() == 0,
                  "marker '" + marker + "' must not hand out ids");
        }
        const auto after = read_raw_db(path);
        check(after == before,
              "rejecting marker '" + marker + "' must preserve every key and value");
    }
}

// ambiguous closing ---------------------------------------------------

// A closing write whose outcome is unknown may have landed. The process must
// keep the pre-failure view, refuse the shutdown checkpoint for that row, and
// report a failed deinit; the next open reads the durable closing state.
void ambiguous_closing_state_is_not_reverted(const std::string& base)
{
    const auto path = fresh_dir(base, "a24b-ambiguous-closing");
    uint64_t id = 0;
    {
        JobRegistry registry(isolated(path));
        install_deferred_drain(registry, [](JobEvent, const Job&) {});
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.job_id;

        registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0,
                                         /*count=*/1);
        check(!registry.retireInstance(id),
              "an ambiguous closing write must not report success");
        check(registry.storeHealth().recovery_required,
              "the ambiguous closing write must latch recovery");
        const auto record = registry.jobRecord(id);
        check(record.has_value() && record->state == RecordState::Active,
              "memory keeps the pre-failure state until recovery");
        check(!registry.deinit(),
              "a shutdown with an unrecovered store must report failure");
    }
    {
        JobRegistry reopened(isolated(path));
        const auto record = reopened.jobRecord(id);
        check(record.has_value() && record->state == RecordState::Closing,
              "the closing fact that landed must survive; the shutdown checkpoint "
              "must not rewrite the stale active row");
        check(!has_active(reopened, id), "a closing instance is never active");
        const auto late = reopened.upsertObservedJob(condor_observation("172.0"));
        check(!late.ok && late.error == "instance_closing",
              "the restored closing reservation must reject a late observation");
        check(reopened.cleanupClosingJobs(),
              "the closing cleanup must finish the interrupted retirement");
        check(!reopened.jobRecord(id).has_value(),
              "the archive removes the closing record");
        const auto replacement = reopened.upsertObservedJob(condor_observation("172.0"));
        check(replacement.ok && replacement.job_id != id,
              "after the archive the identity can be registered as a new instance");
    }
}

// cleanup ordering ----------------------------------------------------

// The archive releases the identity index, so it must come last: a failed
// owner-sensitive kernel cleanup keeps the durable closing reservation, blocks
// a replacement from taking the identity, and stays retryable.
void cleanup_failure_keeps_the_closing_reservation(const std::string& base)
{
    const auto path = fresh_dir(base, "a24c-cleanup-retry");
    JobRegistry registry(isolated(path));
    size_t cleanup_calls = 0;
    bool record_visible_during_cleanup = false;
    registry.setKernelCleanupForTest(
        [&](uint64_t job_id, const Job&, std::string& error) {
            ++cleanup_calls;
            // The archive releases the identity index, so at cleanup time the
            // record must still be there; if it were already archived this
            // would be false.
            record_visible_during_cleanup = registry.jobRecord(job_id).has_value();
            if (cleanup_calls == 1) {
                error = "injected cleanup failure";
                return false;
            }
            error.clear();
            return true;
        });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    const uint64_t id = created.job_id;

    // The runtime owns the drain: install it before the command so retirement
    // is accepted as Closing and the cleanup runs only after the runtime
    // proves the boundary.
    install_deferred_drain(registry);
    check(registry.retireInstance(id), "the retirement must be accepted");
    const auto closing = registry.jobRecord(id);
    check(closing.has_value() && closing->state == RecordState::Closing,
          "the accepted retirement is durable Closing before any cleanup");
    check(test_runtime(registry).reconcile(id),
          "the runtime must accept the Closing record");
    check(!test_runtime(registry).completeDrain(id),
          "a failed kernel cleanup must not report a completed retirement");
    check(cleanup_calls == 1, "the cleanup must be attempted");
    check(record_visible_during_cleanup,
          "the cleanup must run while the durable record still exists");
    check(registry.jobRecord(id).has_value() &&
              registry.jobRecord(id)->state == RecordState::Closing,
          "a failed cleanup keeps the durable closing reservation");
    check(!has_active(registry, id), "a closing reservation is not an active instance");

    // The closing reservation still owns the identity: no replacement may take
    // it, which is exactly what makes the retry safe.
    const auto late = registry.upsertObservedJob(condor_observation("172.0"));
    check(!late.ok && late.error == "instance_closing",
          "the closing reservation must reject a replacement observation");
    const auto identity = canonicalize(condor_selector("172.0"));
    check(identity.has_value(), "the fixture identity must canonicalize");
    const auto by_identity = record_by_identity(registry, *identity);
    check(by_identity.has_value() && by_identity->job.JobID == id,
          "the identity must still resolve to the closing instance");

    check(test_runtime(registry).completeDrain(id),
          "the retry must finish the retirement once cleanup succeeds");
    check(cleanup_calls == 2, "the retry must run the cleanup again");
    check(!registry.jobRecord(id).has_value(),
          "the successful retry archives the record");
    const auto replacement = registry.upsertObservedJob(condor_observation("172.0"));
    check(replacement.ok && replacement.disposition == UpsertDisposition::Created &&
              replacement.job_id != id,
          "only after the archive may the identity be registered again");
}

// ambiguous closing retry ---------------------------------------------

// A closing write whose outcome is unknown may have landed. Memory keeps the
// active view until recovery, so the first retire reports failure without a
// Removed. The retry adopts the durable closing row and must dispatch the
// missing drain exactly once instead of returning "closing and subscribed"
// forever; after the subscriber acks, the record is archived and the identity
// is released.
void ambiguous_closing_retry_restores_the_drain(const std::string& base)
{
    const auto path = fresh_dir(base, "a24d-closing-retry");
    JobRegistry registry(isolated(path));
    size_t removed_events = 0;
    bool ack_ok = false;
    uint64_t id = 0;
    install_deferred_drain(registry, [&](JobEvent event, const Job& job) {
        if (event != JobEvent::Removed || job.JobID != id) return;
        ++removed_events;
        ack_ok = test_runtime(registry).completeDrain(job.JobID);
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    id = created.job_id;

    registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0,
                                     /*count=*/1);
    check(!registry.retireInstance(id),
          "an ambiguous closing write must not report success");
    check(registry.storeHealth().recovery_required,
          "the ambiguous closing write must latch recovery");
    check(removed_events == 0, "a failed closing sync must not deliver Removed");
    const auto active = registry.jobRecord(id);
    check(active.has_value() && active->state == RecordState::Active,
          "memory keeps the active view until recovery");

    registry.clearStoreFaultsForTest();
    check(registry.retireInstance(id),
          "the retry must accept the recovered closing record");
    check(test_runtime(registry).reconcile(id),
          "the runtime must resume the drain for the recovered record");
    check(removed_events == 1,
          "exactly one Removed may be dispatched for the recovered closing record");
    check(ack_ok, "the recovered drain ack must archive the record");
    check(!registry.jobRecord(id).has_value(),
          "the ack archives the recovered closing record");
    check(!has_active(registry, id), "an archived instance is no longer active");

    const auto replacement = registry.upsertObservedJob(condor_observation("172.0"));
    check(replacement.ok && replacement.disposition == UpsertDisposition::Created &&
              replacement.job_id != id,
          "after the archive the identity can be registered as a new instance");
}

// cleanup after ack ---------------------------------------------------

// The cleanup happens after the last collection ack, so a failed cleanup is
// not a lost drain: the closing reservation stays and the same ack path can be
// retried to completion.
void cleanup_failure_after_ack_is_retryable(const std::string& base)
{
    const auto path = fresh_dir(base, "a24e-ack-retry");
    JobRegistry registry(isolated(path));
    size_t cleanup_calls = 0;
    size_t removed_events = 0;
    bool first_ack = true;
    registry.setKernelCleanupForTest(
        [&](uint64_t, const Job&, std::string& error) {
            ++cleanup_calls;
            if (cleanup_calls == 1) {
                error = "injected cleanup failure after the ack";
                return false;
            }
            error.clear();
            return true;
        });
    uint64_t id = 0;
    install_deferred_drain(registry, [&](JobEvent event, const Job& job) {
        if (event != JobEvent::Removed || job.JobID != id) return;
        ++removed_events;
        first_ack = test_runtime(registry).completeDrain(job.JobID);
    });

    const auto created = registry.upsertObservedJob(condor_observation("172.0"));
    check(created.ok, "baseline registration must succeed");
    id = created.job_id;

    check(registry.retireInstance(id), "retirement must be accepted");
    check(removed_events == 0, "acceptance alone does not dispatch a drain");
    check(test_runtime(registry).reconcile(id), "the runtime must start the drain");
    check(removed_events == 1, "one Removed starts the drain");
    check(!first_ack, "the ack must report the failed cleanup");
    const auto closing = registry.jobRecord(id);
    check(closing.has_value() && closing->state == RecordState::Closing,
          "a cleanup failure after the ack keeps the closing reservation");
    check(!has_active(registry, id), "a closing reservation is not an active instance");
    check(registry.retireInstance(id),
          "a second retire is an idempotent acceptance of the same Closing record");
    check(removed_events == 1,
          "the second retire must not emit a second Removed");

    check(test_runtime(registry).completeDrain(id),
          "the ack retry must finish the cleanup and archive");
    check(cleanup_calls == 2, "the retry must run the cleanup again");
    check(!registry.jobRecord(id).has_value(),
          "the successful retry archives the record");
    check(removed_events == 1, "the retry must not emit a second Removed");
}

// /(runtime projection) ----------------------------------------------

// The runtime result and the Job.collection projection are one piece of
// in-memory truth. A whole-record publication (an ordinary unchanged
// observation followed by a changed one) must rebuild the projection from the
// live result, and an unchanged observation must not re-invoke the runtime.
void runtime_projection_survives_repeated_observation(const std::string& base)
{
    const auto path = fresh_dir(base, "runtime-projection");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator(nullptr);
    size_t applies = 0;
    install_collection_runtime(registry,
        [&](const Job&, std::vector<std::string>& applied, std::string& error) {
            ++applies;
            applied = {"cpumem_collector"};
            error.clear();
            return true;
        });

    const auto created = registry.upsertObservedJob(condor_observation("905.0"));
    check(created.ok, "the instance must register");
    const auto committed = registry.applyAutoPolicy(
        {{created.job_id, {"cpumem_collector"}, "projection-test"}});
    check(committed.ok, "the binding must commit: " + committed.message);
    check(applies == 0, "a command must not run the runtime on its own stack");
    test_runtime(registry).reconcile(created.job_id);
    check(applies == 1, "the runtime pass must attach the selection once");

    {
        const auto actual = registry.actualCollection(created.job_id);
        check(actual.tracked &&
                  actual.collectors == std::vector<std::string>{"cpumem_collector"},
              "the runtime holders must be reported as actual");
        const auto projected = registry.jobRecord(created.job_id);
        check(projected.has_value() &&
                  actual.last_revision == projected->revision,
              "the actual state must name the record revision it converged to");
    }

    // An ordinary unchanged observation republishes the record but must not
    // detach or refresh the attachment.
    const auto unchanged = registry.upsertObservedJob(condor_observation("905.0"));
    check(unchanged.ok && unchanged.disposition == UpsertDisposition::Unchanged,
          "the repeat observation must be unchanged");
    {
        const auto actual = registry.actualCollection(created.job_id);
        check(actual.collectors == std::vector<std::string>{"cpumem_collector"},
              "an unchanged observation must not erase the runtime holders");
    }
    check(applies == 1, "an unchanged observation must not re-run the runtime");

    // A real facts change reaches the same attachment without a detach.
    Job changed = condor_observation("905.0");
    std::get<CondorJobAttr>(changed.sub_attr).owner = "changed";
    changed.execution.pid_identities.push_back({5151, 424242, "boot-aaa"});
    const auto updated = registry.upsertObservedJob(changed);
    check(updated.ok && updated.disposition == UpsertDisposition::Updated,
          "the fact change must be applied");
    const auto actual = registry.actualCollection(created.job_id);
    check(actual.collectors == std::vector<std::string>{"cpumem_collector"},
          "a fact change must not erase the runtime holders");
}

// recovery fence ------------------------------------------------------

// An ambiguous unsynced selection may have landed. Recovery adopts the durable
// row, but adoption alone is not durable: the recovered row (and any remaining
// dirty row) must be rewritten in one fenced batch before the latch is
// released. A failed fence keeps both the latch and the dirty obligation.
void recovery_fences_the_adopted_selection(const std::string& base)
{
    const auto path = fresh_dir(base, "a21-recovery-fence");
    JobRegistry registry(isolated(path));
    registry.setCollectorValidator(nullptr);

    const auto created = registry.upsertObservedJob(condor_observation("906.0"));
    check(created.ok, "the instance must register");
    const uint64_t id = created.job_id;

    CollectionRequest first;
    first.targets = {id};
    first.collectors = std::vector<std::string>{"cpumem_collector"};
    check(registry.setCollectors(first).ok, "the first selection must commit");
    const uint64_t revision_before_ambiguous = revision_of(registry, id);
    const auto before = registry.storeHealth();
    check(before.dirty_records > 0,
          "an unsynced selection must leave a dirty obligation");

    registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/0,
                                     /*count=*/1);
    CollectionRequest second;
    second.targets = {id};
    second.collectors = std::vector<std::string>{"disk_collector"};
    const auto ambiguous = registry.setCollectors(second);
    check(!ambiguous.ok && ambiguous.error == CollectionError::StoreUnavailable,
          "an ambiguous selection write must be reported as store-unavailable");
    check(registry.storeHealth().recovery_required,
          "the ambiguous selection must latch recovery");
    check(registry.storeHealth().sync_commits == before.sync_commits,
          "the failed selection must not count as a fence");

    // A failing recovery fence must not clear the latch or the dirty row. The
    // selection command is the recovery retry point: it verifies the uncertain
    // rows before anything else and reports the refused recovery.
    registry.clearStoreFaultsForTest();
    registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/0,
                                     /*count=*/1);
    const auto refused_retry = registry.setCollectors(second);
    check(!refused_retry.ok && refused_retry.error == CollectionError::StoreUnavailable,
          "a failed recovery fence must report failure");
    check(registry.storeHealth().recovery_required,
          "a failed recovery fence must keep the latch");
    check(registry.storeHealth().dirty_records > 0,
          "a failed recovery fence must keep the dirty obligation");
    check(registry.storeHealth().sync_commits == before.sync_commits,
          "a failed recovery fence must not count as a durable commit");

    // The retry fences the adopted row before reporting recovery complete; the
    // identical selection writes nothing, so only the fence moves the counters.
    registry.clearStoreFaultsForTest();
    const auto recovered_retry = registry.setCollectors(second);
    check(recovered_retry.ok,
          "the recovery retry must succeed through the selection command");
    const auto after = registry.storeHealth();
    check(!after.recovery_required, "a completed recovery must release the latch");
    check(after.sync_commits > before.sync_commits,
          "recovery must fence the adopted selection row");
    check(after.dirty_records == 0,
          "recovery must clear the dirty obligation it fenced");

    const auto binding = registry.selectionRecord(id);

    const auto binding_revision = revision_of(registry, id);
    check(binding.mode() == CollectionMode::Manual &&
              binding.manual.value_or(std::vector<std::string>{}) == std::vector<std::string>{"disk_collector"},
          "recovery must publish the durable selection that landed");
    check(binding_revision == revision_before_ambiguous + 1,
          "recovery must publish the ONE revision of the landed write");
}

// /(ambiguous archive image) -----------------------------------------

// An archive write that landed but reported failure leaves no active row to
// rewrite and no dirty snapshot to fence. Recovery must read the durable `h:`
// row back and re-assert that exact key/value (plus the matching active and
// identity deletions) in one fenced batch; adopting the archive and clearing
// the latch without any write would claim a durability nobody proved, and
// rebuilding the history row from memory would write a second, different one.
void ambiguous_archive_is_fenced_from_the_durable_image(const std::string& base)
{
    const auto path = fresh_dir(base, "a24f-archive-recovery");
    const auto identity = canonicalize(condor_selector("907.0"));
    check(identity.has_value(), "the fixture identity must canonicalize");

    uint64_t id = 0;
    int64_t archived_first_seen = 0;
    uint64_t archived_revision = 0;
    uint64_t fence_entries = 0;
    {
        JobRegistry registry(isolated(path));
        // The archive path only fires for an instance that WAS collected: the
        // registry captures the runtime's actual attachment at the retire
        // decision point. Attach a collector first so this stays the archive
        // (history-row) scenario; a never-collected instance is the discard
        // scenario covered by its own case below.
        registry.setCollectorValidator(nullptr);
        install_collection_runtime(registry,
                                   [](const Job& job, std::vector<std::string>& applied,
                                      std::string& error) {
                                       applied = job.CollectorNames;
                                       error.clear();
                                       return true;
                                   });
        const auto created = registry.upsertObservedJob(condor_observation("907.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.job_id;

        const auto committed = registry.applyAutoPolicy(
            {{id, {"disk_collector"}, "a24f"}});
        check(committed.ok, "binding must commit: " + committed.message);
        test_runtime(registry).reconcile(id);

        const auto before_archive = registry.jobRecord(id);
        check(before_archive.has_value(), "the instance must be readable before retirement");
        archived_first_seen = before_archive->first_seen_us;

        // This retirement makes two durable writes: the closing state and then
        // the archive. `skip=1` lands the injected failure on the archive.
        registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/1,
                                         /*count=*/1);
        // With a runtime holding the instance the retirement is accepted
        // immediately; the runtime's own pass drives the drain and the archive,
        // and it is that archive write whose outcome becomes ambiguous.
        check(registry.retireInstance(id),
              "retirement with a subscriber must be accepted");
        test_runtime(registry).reconcile(id);
        registry.clearStoreFaultsForTest();

        // The closing transition is itself a record mutation, so the durable
        // image the archive write copies carries the revision after that bump.
        const auto closing_row = registry.jobRecord(id);
        check(closing_row.has_value() && closing_row->state == RecordState::Closing,
              "the closing reservation must be durable before the archive");
        archived_revision = closing_row->revision;
        check(archived_revision == before_archive->revision + 1,
              "the closing transition must advance the ONE record revision (" +
                  std::to_string(before_archive->revision) + " -> " +
                  std::to_string(archived_revision) + ")");

        const auto latched = registry.storeHealth();
        check(latched.recovery_required, "the ambiguous archive must latch recovery");
        check(latched.active_instances == 1, "memory keeps the instance until recovery");
        check(registry.jobRecord(id).has_value(),
              "the instance stays readable until recovery adopts the archive");

        // A failing fence must keep the latch and the obligation. The instance
        // has no active row left to rewrite, so a zero entry count here would
        // mean recovery attempted no durable write at all.
        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/0,
                                         /*count=*/1);
        registry.setCollectorValidator(nullptr);
        CollectionRequest fence_probe;
        fence_probe.targets = {id};
        fence_probe.collectors = std::vector<std::string>{"disk_collector"};
        const auto refused_retry = registry.setCollectors(fence_probe);
        check(!refused_retry.ok && refused_retry.error == CollectionError::StoreUnavailable,
              "a failed archive fence must report failure");
        const auto failed = registry.storeHealth();
        check(failed.recovery_required, "a failed archive fence must keep the latch");
        check(failed.recovery_fence_entries >= 2,
              "the archive fence must re-assert the history row and the active "
              "deletion, got " + std::to_string(failed.recovery_fence_entries) + " entry(s)");
        check(failed.sync_commits == latched.sync_commits,
              "a failed archive fence must not count as a durable commit");
        check(failed.active_instances == 0,
              "the adopted archive leaves no pending active row");
        check(!has_active(registry, id), "an archived instance is never active");
        check(!record_by_identity(registry, *identity).has_value(),
              "the adopted archive releases the identity to the store");
        fence_entries = failed.recovery_fence_entries;

        // Only a successful fence may complete the recovery.
        registry.clearStoreFaultsForTest();
        const auto recovered_retry = registry.setCollectors(fence_probe);
        check(recovered_retry.error == CollectionError::NotFound,
              "the archive recovery retry must succeed: the refusal must come "
              "from the missing instance, not the latch (" + recovered_retry.message + ")");
        const auto recovered = registry.storeHealth();
        check(!recovered.recovery_required,
              "a completed archive recovery must release the latch");
        check(recovered.sync_commits > latched.sync_commits,
              "recovery must fence the reconstructed archive durably");
        check(recovered.recovery_fence_entries == fence_entries,
              "the retry must fence the same non-empty image");
        check(recovered.active_instances == 0,
              "a recovered archive leaves no active instance behind");
        check(registry.deinit(), "shutdown must succeed after the archive recovery");
    }

    // The durable image, read with the store closed.
    const auto rows = read_raw_db(path);
    size_t history_rows = 0;
    std::string history_key;
    std::string history_value;
    bool archived_active_row_present = false;
    bool archived_id_in_index = false;
    for (const auto& [key, value] : rows) {
        if (key == "j:" + hex64(id)) archived_active_row_present = true;
        if (key.rfind("h:" + hex64(id) + ":", 0) == 0) {
            ++history_rows;
            history_key = key;
            history_value = value;
        }
        // The index row names its owner by the record id key ("job_id").
        if (key.rfind("i:", 0) == 0 &&
            nlohmann::json::parse(value).value("job_id", uint64_t{0}) == id) {
            archived_id_in_index = true;
        }
    }
    check(!archived_active_row_present,
          "the recovered archive must leave no active row for the archived instance");
    check(history_rows == 1,
          "the recovery must preserve exactly one history row, found " +
              std::to_string(history_rows));
    check(!archived_id_in_index,
          "the archive must release the identity index owner it held");
    check(raw_keys_with_prefix(path, "j:").empty(),
          "the recovery must leave no active row at all");

    // The history key encodes the archive timestamp and the value carries the
    // summary of the same archive. A regenerated row would land under a new
    // key (a second history row) and with a different end_time_us.
    const auto stamp = std::stoull(history_key.substr(history_key.size() - 16), nullptr, 16);
    const auto history = nlohmann::json::parse(history_value);
    check(history.value("end_time_us", int64_t{0}) == static_cast<int64_t>(stamp),
          "the fenced history value must belong to its own key");
    check(history.value("first_seen_us", int64_t{-1}) == archived_first_seen,
          "the fenced history row must keep the observed facts, not a guess");
    check(history.value("revision", uint64_t{0}) == archived_revision,
          "the fenced history row must keep the archived record revision");
    check(history.value("state", std::string{}) == to_string(RecordState::Closing),
          "the fenced history row must keep the closing state");
    check(history.contains("selection"),
          "the fenced history row must keep the audit selection");
    const auto stored_selection = history.value("selection", nlohmann::json::object());
    check(stored_selection.is_object(),
          "the archived selection must be an object, not a dropped field");
    check(history.value("JobNativeID", std::string{}) == "907.0",
          "the fenced history row must keep the archived facts");
    check(history.contains("index_key"),
          "the fenced history row must keep the derived identity key");
    const auto stored_applied = history.value("applied_collectors", nlohmann::json::array());
    check(stored_applied.is_array() && stored_applied.size() == 1 &&
              stored_applied[0] == "disk_collector",
          "the fenced history row must keep the applied fact snapshot of what the "
          "runtime really had attached");
    check(history.value("applied_provenance", std::string{}) == "applied",
          "the fact snapshot must record that it was observed on the live runtime");

    {
        JobRegistry reopened(isolated(path));
        check(!reopened.jobRecord(id).has_value(),
              "the recovered archive must not be resurrected by a restart");
        check(!record_by_identity(reopened, *identity).has_value(),
              "the archived identity must not resolve to the archived instance");
        const auto replacement = reopened.upsertObservedJob(condor_observation("907.0"));
        check(replacement.ok && replacement.disposition == UpsertDisposition::Created &&
                  replacement.job_id != id,
              "after the archive the identity must register as a new instance");
        const auto binding = reopened.selectionRecord(replacement.job_id);
        const auto binding_revision = revision_of(reopened, replacement.job_id);
        check(binding_revision == 1 && !binding.manual.has_value(),
              "the replacement must not inherit the archived intent");
        check(reopened.deinit(), "shutdown must succeed after the replacement");
    }
}

// Archive split, fact criterion: an instance that was NEVER collected leaves
// no history row at all — its removal is a durable delete.
void never_collected_instances_are_discarded(const std::string& base)
{
    const auto path = fresh_dir(base, "discard-never-collected");
    uint64_t id = 0;
    {
        JobRegistry registry(isolated(path));
        // No runtime is installed: nothing can ever have collected the job, and
        // the retirement finalizes synchronously.
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.job_id;
        check(registry.retireInstance(id), "retirement must be accepted");
        check(!registry.jobRecord(id).has_value(),
              "the discarded instance must leave the active map");
        check(registry.deinit(), "shutdown must not fail after a clean discard");
    }

    check(raw_keys_with_prefix(path, "j:").empty(),
          "no active row may survive a discard");
    check(raw_keys_with_prefix(path, "h:").empty(),
          "a never-collected instance must leave no history row");
}

// An ambiguous discard (the delete batch landed but the write reported
// failure) is confirmed by the row's own absence; recovery must clear the
// latch and durably re-assert the deletion, and never write a history row.
void ambiguous_discard_is_confirmed_by_the_missing_row(const std::string& base)
{
    const auto path = fresh_dir(base, "discard-ambiguous");
    uint64_t id = 0;
    {
        JobRegistry registry(isolated(path));
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.job_id;

        // Two durable writes: the closing state (skipped), then the discard
        // (lands, reports failure).
        registry.injectStoreFaultForTest(StoreFaultMode::FailAfterWrite, /*skip=*/1,
                                         /*count=*/1);
        check(!registry.retireInstance(id),
              "an ambiguous discard must not report a completed retirement");
        registry.clearStoreFaultsForTest();

        const auto latched = registry.storeHealth();
        check(latched.recovery_required, "the ambiguous discard must latch recovery");

        // A probe mutation drives the recovery, exactly like production paths.
        registry.setCollectorValidator(nullptr);
        CollectionRequest probe;
        probe.targets = {id};
        probe.collectors = std::vector<std::string>{"disk_collector"};
        const auto probe_outcome = registry.setCollectors(probe);
        check(probe_outcome.error == CollectionError::NotFound,
              "the discard recovery must succeed; the refusal must come from the "
              "missing instance (" + probe_outcome.message + ")");
        const auto recovered = registry.storeHealth();
        check(!recovered.recovery_required,
              "the confirmed deletion must release the latch");
        check(recovered.recovery_fence_entries >= 1,
              "the discard fence must durably re-assert the deletion");
        check(registry.deinit(), "shutdown must succeed after the discard recovery");
    }

    check(raw_keys_with_prefix(path, "j:").empty(),
          "no active row may survive a recovered discard");
    check(raw_keys_with_prefix(path, "h:").empty(),
          "a discard must never write a history row");
    check(raw_keys_with_prefix(path, "i:").empty(),
          "the delete batch is atomic: the identity index cannot survive the row");
}

// A closing restored from disk carries no captured runtime fact (the capture
// is runtime state and dies with the process). The archive decision then
// falls back to the durable effective selection and records that provenance.
void restored_closing_archives_with_effective_fallback(const std::string& base)
{
    const auto path = fresh_dir(base, "restored-closing-fallback");
    uint64_t id = 0;
    {
        JobRegistry registry(isolated(path));
        registry.setCollectorValidator(nullptr);
        install_deferred_drain(registry, [](JobEvent, const Job&) {});
        const auto created = registry.upsertObservedJob(condor_observation("172.0"));
        check(created.ok, "baseline registration must succeed");
        id = created.job_id;
        // A durable non-empty selection is what the fallback will read.
        const auto committed = registry.applyAutoPolicy(
            {{id, {"disk_collector"}, "fallback"}});
        check(committed.ok, "binding must commit: " + committed.message);
        check(registry.retireInstance(id),
              "retirement with a subscriber must be accepted");
        // The shutdown checkpoint is skipped so the injected failure lands on
        // the archive write: the closing row survives as if the process died
        // between the closing transition and the archive.
        registry.injectStoreFaultForTest(StoreFaultMode::FailBeforeWrite, /*skip=*/1);
        check(!registry.deinit(),
              "deinit must fail to archive the drain (the crash model)");
    }

    {
        JobRegistry registry(isolated(path));
        const auto record = registry.jobRecord(id);
        check(record.has_value() && record->state == RecordState::Closing,
              "the closing record must be restored");
        check(registry.cleanupClosingJobs(),
              "the closing cleanup must finalize the restored closing");
        check(!registry.jobRecord(id).has_value(),
              "the restored closing must be archived by the cleanup");
        check(registry.deinit(), "shutdown must succeed after the fallback archive");
    }

    const auto history_keys = raw_keys_with_prefix(path, "h:");
    check(history_keys.size() == 1,
          "the fallback must keep exactly one history row for a selected instance");
    for (const auto& [key, value] : read_raw_db(path)) {
        if (key.rfind("h:" + hex64(id) + ":", 0) != 0) continue;
        const auto history = nlohmann::json::parse(value);
        check(history.value("applied_provenance", std::string{}) == "effective",
              "a restored closing must record the effective-selection fallback");
        const auto applied = history.value("applied_collectors", nlohmann::json::array());
        check(applied.size() == 1 && applied[0] == "disk_collector",
              "the fallback snapshot must carry the durable effective selection");
    }
    check(raw_keys_with_prefix(path, "j:").empty(), "no active row may survive");
}

int main(int argc, char** argv)
{
    try {
        const std::string base = argc > 1 ? argv[1] : "/tmp/joblens-store-test";
        std::filesystem::create_directories(base);
        stable_metadata_is_a_creation_snapshot(base);
        host_refresh_preserves_creation_metadata(base);
        upsert_is_idempotent_and_activates_nothing(base);
        legacy_schema_is_rejected_without_migration(base);
        unsupported_schema_is_rejected_idempotently(base);
        conflicting_live_execution_is_not_merged(base);
        stale_manager_is_not_replayed(base);
        write_failures_reject_mutations_without_id_reuse(base);
        closing_state_is_durable_and_not_resurrected(base);
        restored_suspect_instances_are_not_replayed(base);
        member_semantics_distinguish_unknown_from_confirmed_empty(base);
        registration_keeps_single_upsert_semantics(base);
        registration_errors_are_typed_and_exhaustively_mapped(base);
        max_job_id_is_never_reissued(base);
        runtime_state_survives_later_durable_writes(base);
        archived_instances_leave_no_active_row(base);
        ambiguous_registration_resolves_after_restart(base);
        sequential_merges_keep_single_upsert_semantics(base);
        hot_path_registration_does_not_fsync(base);
        shutdown_flush_persists_runtime_changes(base);
        conflicting_fresh_candidates_are_rejected(base);
        repeated_observations_keep_memory_and_disk_in_sync(base);
        selection_intent_lives_in_the_record_row(base);
        closing_write_failure_keeps_the_instance(base);
        schema_marker_must_be_canonical(base);
        ambiguous_closing_state_is_not_reverted(base);
        ambiguous_closing_retry_restores_the_drain(base);
        cleanup_failure_keeps_the_closing_reservation(base);
        cleanup_failure_after_ack_is_retryable(base);
        runtime_projection_survives_repeated_observation(base);
        recovery_fences_the_adopted_selection(base);
        ambiguous_archive_is_fenced_from_the_durable_image(base);
        never_collected_instances_are_discarded(base);
        ambiguous_discard_is_confirmed_by_the_missing_row(base);
        restored_closing_archives_with_effective_fallback(base);
        std::cout << "PASS job store: empty-binding upsert idempotency, "
                     "old-schema rejection without migration or deletion, "
                     "canonical schema marker, idempotent rejection and "
                     "live-manager conflict, "
                     "durable-write failure handling and no ID reuse, "
                     "durable closing state, ambiguous closing write and "
                     "missing-drain recovery, cleanup ordering and retry "
                     "after the ack, suspect replay filter, "
                     "member semantics (unknown vs confirmed empty), "
                     "registration parity, max-ID exhaustion across restart, "
                     "runtime state across later durable writes, archive "
                     "leaves no active row, runtime projection across "
                     "republication, recovery fences the adopted selection, "
                     "ambiguous archive recovery fences the durable history "
                     "image, archive split by the fact criterion (never-collected "
                      "discard, ambiguous discard recovery, restored-closing "
                      "effective fallback), "
                     "schema-3 single-row intent and closing-sync failure safety\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
