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
// JobStore — the single durable representation of inventoried jobs
// (doc/job_inventory_v2_architecture.md §2.1/§2.4).
//
// A JobRecord is "a Job plus what JobLens must remember to manage it":
// execution evidence (identity, generation, members), the durable lifecycle
// state (active | suspect | closing), the collection intent and the
// runtime-reported applied state. JobStore is the ONLY writer of that state;
// every other view (Job snapshots handed to collectors, scheduler lanes,
// kernel maps) is a projection refreshed through store results and events.
//
// LevelDB namespaces (schema 2, unchanged layout):
//   j:  active JobRecords (legacy namespace, additive fields)
//   h:  archived history (final binding summary)
//   c:  durable allocator high-water mark
//   m:  schema version, Agent DB UUID, migration state
//   i:  canonical identity key -> internal ID
//   b:  per-instance collection intent (kept as its own key; folding it into
//       j: is schema-3 work and deliberately not done here)
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#include "core/collector_type.h"
#include "core/registration_contract.hpp"
#include "core/job_identity.hpp"

class JobStore;
struct JobRecord;

// ---------------------------------------------------------------------------
// Wire/store value types (shared with the registry facade)
// ---------------------------------------------------------------------------

enum class CollectionMode {
    Auto,
    Manual,
};

inline const char* to_string(CollectionMode mode)
{
    return mode == CollectionMode::Manual ? "manual" : "auto";
}

std::optional<CollectionMode> collection_mode_from_string(const std::string& raw);

// Per-instance collection intent. Revisions are monotonically increasing
// per instance; revision 0 means "no intent recorded yet" (auto, empty).
struct BindingRecord {
    uint64_t internal_id = 0;
    uint64_t revision = 0;
    CollectionMode mode = CollectionMode::Auto;
    std::vector<std::string> auto_collectors;
    std::vector<std::string> manual_collectors;
    std::string policy_revision;

    // effective_collectors = manual if mode == manual, else auto. An explicit
    // empty manual list is a persistent stop and must win over auto.
    std::vector<std::string> effectiveCollectors() const;
};

// Runtime state of a committed binding. Intent is owned by the store; applied
// state is reported by the collection runtime and persisted here so a restart
// can reconcile instead of guessing. `applied=false` with a non-empty
// `collectors` list is a partial success and stays visible.
struct AppliedCollection {
    bool applied = false;
    uint64_t revision = 0;          // binding revision this result belongs to
    // Facts revision the runtime actually saw. A facts change with an unchanged
    // binding must refresh the runtime snapshot, so convergence compares this
    // too (plan §7.2).
    uint64_t facts_revision = 0;
    std::vector<std::string> collectors;
    std::string error;
};

struct StoreHealth {
    bool db_running = false;
    bool migration_complete = false;
    std::string migration_state;         // empty | in_progress:<stage> | complete
    uint32_t schema_version = 0;
    std::string db_uuid;
    bool degraded = false;
    // Durable commits (one LevelDB WriteBatch with sync=true each). Exposed so
    // the cost of a registration path can be reasoned about, not guessed.
    uint64_t durable_commits = 0;
    uint64_t write_failures = 0;
    std::string last_write_error;
    uint64_t incomplete_identities = 0;
    uint64_t quarantined_identities = 0;
    uint64_t corrupt_records = 0;
    uint64_t active_instances = 0;
};

// Test-only fault injection. The next durable control-plane writes at
// `skip` offset (0 = the very next one) fail. FailBeforeWrite models a LevelDB
// write that never landed; FailAfterWrite models the ambiguous case where the
// batch landed but the status reported failure.
enum class StoreFaultMode {
    None,
    FailBeforeWrite,
    FailAfterWrite,
};

// One scheduler observation. `facts` carries the typed scheduler attributes;
// its JobID and CollectorNames are deliberately ignored: inventory upserts
// never allocate a caller-chosen ID and never activate collection.
//
// `members_complete` mirrors the discovery flag: true means the process list
// is authoritative, an empty list included ("read succeeded, no members");
// false means the member set is only partially known, so an empty list must
// never erase stored members.
// ---------------------------------------------------------------------------
// JobRecord — the one durable representation
// ---------------------------------------------------------------------------

// Lifecycle state persisted with the record. "closing" is written before the
// collector drain starts so an interrupted retirement can never resurrect as
// active; "suspect" is a maintenance verdict (re-admitted by observation).
inline constexpr const char* kStateActive = "active";
inline constexpr const char* kStateSuspect = "suspect";
inline constexpr const char* kStateClosing = "closing";

struct JobRecord {
    // --- the Job as collectors see it (projection source) ---------------
    Job job;
    // --- execution evidence ---------------------------------------------
    std::string adapter;                 // adapter name, e.g. "condor"
    NativeSelector selector;             // original wire identity (verbatim)
    CanonicalIdentity identity;          // valid only when identity_complete
    bool identity_complete = false;
    bool quarantined = false;
    ExecutionGeneration generation;      // optional; unverified != dead
    std::vector<ProcessIdentity> members;
    bool members_authoritative = false;  // empty list then means "confirmed empty"
    // --- JobLens management state ----------------------------------------
    uint64_t internal_id = 0;
    std::string instance_id;             // opaque handle (db uuid + id)
    std::string state = kStateActive;    // active | suspect | closing
    uint64_t facts_revision = 0;
    int64_t first_seen_us = 0;
    int64_t last_seen_us = 0;
    // --- collection intent + runtime result ------------------------------
    BindingRecord intent;
    AppliedCollection applied;           // durable reconciliation hint (I7)
};

// ---------------------------------------------------------------------------
// JobStore
// ---------------------------------------------------------------------------

class JobStore {
public:
    struct Options {
        std::string db_path;                 // empty: job_registry_config.job_db_path
        size_t migration_batch_limit = 0;    // test seam: stop a migration early
    };

    explicit JobStore(const Options& options);
    ~JobStore();

    JobStore(const JobStore&) = delete;
    JobStore& operator=(const JobStore&) = delete;

    // --- health / test faults -------------------------------------------
    StoreHealth health() const;
    bool ready() const;
    bool migrationComplete() const { return migration_complete_; }
    void injectFaultForTest(StoreFaultMode mode, size_t skip = 0, size_t count = 1);
    // Test-only seam: runs inside the registration admission, after the id
    // reservation and before the facts commit. Used to prove that a standalone
    // allocation cannot interleave with a batch reservation (plan §5.3).
    void setPreCommitHookForTest(std::function<void()> hook);
    void clearFaultsForTest();

    // Adapter-owned identity->facts derivation, injected by the registry
    // (dispatches through the scheduler adapters by identity.adapter). The
    // store itself never branches on a scheduler; without an applier the
    // observed facts are stored verbatim.
    void setIdentityApplier(
        std::function<void(Job&, const NativeSelector&, const CanonicalIdentity&)> applier)
    {
        identity_applier_ = std::move(applier);
    }
    std::string dbUuid() const { return db_uuid_; }

    // --- restore (once, right after construction) ------------------------
    // Validates restored executions: stale generations become suspect with
    // cleared members, interrupted closings keep their state, dead PIDs are
    // dropped. Never replays anything into kernel maps.
    std::vector<JobRecord> restore();

    // --- internal id allocation ------------------------------------------
    // Durable allocation on its own (manual registration): reserves the id with
    // its own commit before it is handed out.
    uint64_t allocateId(std::string& error);
    uint64_t nextJobId();   // ready-checked; 0 when the store is not ready

    // --- manual (legacy) registration -------------------------------------
    // Durable insert of a caller-built record plus the identity index.
    // Rejects a duplicate live identity. Returns false on any failure.
    bool insertManual(JobRecord record);

    // --- inventory upsert --------------------------------------------------
    // Identity-resolving idempotent upsert; rejects incomplete identities,
    // closing instances and conflicting live generations. Never fires
    // lifecycle callbacks and never starts collection.
    UpsertResult upsert(const Job& job, JobRecord& out);
    // Write-free merge decision for one observation against an already-loaded
    // record, shared by the single upsert and the batch path.
    UpsertResult merge_existing(const Job& job, const NativeSelector& selector,
                                const CanonicalIdentity& identity, uint64_t internal_id,
                                const JobRecord& stored, int64_t observed_at,
                                JobRecord& updated_out);
    // Batch form of upsert for a burst of fresh executions: every new record,
    // its identity index entry and the id counter go into ONE durable commit.
    // Identities already indexed fall back to the per-record path (they are
    // merges, not creates) so their result stays exactly the single-upsert one.
    // `out` is aligned with `jobs`; entries for rejected or merged jobs are
    // left empty.
    std::vector<UpsertResult> upsertBatch(const std::vector<Job>& jobs,
                                          std::vector<JobRecord>& out);

    // --- record mutations ---------------------------------------------------
    // Generic in-memory read-modify-write under the store lock. Does not
    // persist; combine with persistRecord for durable mutations.
    bool mutateRecord(uint64_t id, const std::function<void(JobRecord&)>& mutate);
    // Generic durable read-modify-write of one record.
    bool updateRecord(uint64_t id, const std::function<void(JobRecord&)>& mutate,
                      const char* operation);
    // Durable lifecycle-state write (the closing transition).
    bool persistState(uint64_t id, const std::string& state);
    // Durable archive (active/closing -> history, identity index and intent
    // removed). Returns false on a store failure.
    bool archive(uint64_t id);
    // Re-read process/attribute facts for one stored job (legacy/manual
    // registrations): /proc + scheduler metadata I/O happen in `refresh`
    // OUTSIDE every store lock; only the result write happens here.
    bool refreshFacts(uint64_t id, const std::function<void(Job&)>& refresh);

    // --- intent + applied ---------------------------------------------------
    BindingRecord binding(uint64_t id) const;
    // One durable transaction for every submitted binding. All targets must
    // exist and not be closing; on any write failure nothing is committed.
    // `updated` receives the stored records plus whether the derived
    // CollectorNames actually changed (the runtime-application trigger).
    bool commitBindings(const std::vector<BindingRecord>& bindings,
                        std::vector<std::pair<JobRecord, bool>>& updated,
                        std::string& error);
    AppliedCollection applied(uint64_t id) const;
    // Records the runtime application result. The value is memory truth for
    // this process only: it is updated together with the Job.collection
    // projection and never written back to the durable row. A restart clears
    // the live holders and re-attaches through replay; the durable `applied`
    // field is a fixed "not applied" projection that feeds diagnostics only,
    // and archive copies the final in-memory summary into h:.
    bool recordApplied(uint64_t id, const AppliedCollection& applied);

    // --- queries -------------------------------------------------------------
    bool hasActive(uint64_t id) const;
    bool isClosing(uint64_t id) const;
    size_t closingCount() const;
    size_t activeCount() const;
    std::optional<JobRecord> record(uint64_t id) const;
    std::optional<JobRecord> recordByIdentity(const CanonicalIdentity& identity) const;
    std::vector<JobRecord> records() const;
    // Active (non-closing) records whose job satisfies `pred`.
    std::vector<JobRecord> findActive(
        const std::function<bool(const Job&)>& pred) const;
    std::string instanceId(uint64_t id) const;

private:
    // Injected by the registry; see setIdentityApplier.
    std::function<void(Job&, const NativeSelector&, const CanonicalIdentity&)>
        identity_applier_;

    bool init_db(const Options& options);
    void close_db();
    bool load_or_create_db_uuid();
    bool resume_migration_state(std::string& stage, std::string& cursor);
    bool migrate_allocator();
    bool migrate_index(std::string& cursor, size_t& batches, size_t batch_limit);
    bool migrate_finalize();
    bool run_migration();
    void init_id_counter();
    // Copies the live runtime result over a record read from disk (db_mtx_ ->
    // mtx_ lock order) and keeps the Job.collection projection in sync.
    bool overlay_runtime(JobRecord& record) const;
    void apply_runtime_projection_locked(JobRecord& record) const;
    // In-memory reservation for the inventory create path: the caller writes
    // `durable_counter` in the same batch as the record, so a new instance
    // costs one durable commit instead of two.
    bool reserveIdInMemory(uint64_t& id, uint64_t& durable_counter,
                           bool& durable_exhausted, std::string& error);
    bool store_ready() const;
    leveldb::Status write_batch_locked(leveldb::WriteBatch& batch, const char* operation);
    void record_store_failure(const std::string& message);
    void record_store_success();
    void note_record(JobRecord record);
    bool update_job_info(uint64_t id, const Job& job);
    std::string identity_key(const CanonicalIdentity& identity) const;
    // In-memory identity index, guarded by mtx_. Callers hold mtx_.
    std::unordered_map<std::string, uint64_t>& identity_index() { return identity_index_; }
    const std::unordered_map<std::string, uint64_t>& identityIndex() const {
        return identity_index_;
    }
    // Serializes inventory admission (identity lookup -> allocate -> commit).
    std::mutex upsert_mtx_;
    // Serializes job_db transactions.
    mutable std::mutex db_mtx_;
    // Guards the in-memory record map and the identity index.
    mutable std::shared_mutex mtx_;
    std::unordered_map<uint64_t, JobRecord> records_;
    std::unordered_map<std::string, uint64_t> identity_index_;

    leveldb::DB* db_ = nullptr;
    bool db_running_ = false;
    Options options_;

    std::string db_uuid_;
    uint32_t schema_version_ = 0;
    bool migration_complete_ = false;
    std::string migration_state_;
    size_t migration_batch_limit_ = 0;
    std::atomic<bool> store_degraded_{false};
    std::atomic<uint64_t> store_write_failures_{0};
    std::atomic<uint64_t> durable_commits_{0};
    uint64_t corrupt_records_ = 0;
    std::string last_store_error_;   // guarded by db_mtx_
    std::function<void()> pre_commit_hook_;
    StoreFaultMode fault_mode_ = StoreFaultMode::None;
    size_t fault_skip_ = 0;
    size_t fault_count_ = 0;

    uint64_t next_id_ = 1;
    bool id_exhausted_ = false;
    std::mutex id_mtx_;
};
