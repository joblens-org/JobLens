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
// (doc/jobstore_persistence_service.md).
//
// A JobRecord is "a Job plus what JobLens must remember to manage it":
// execution evidence (identity, generation, members), the durable lifecycle
// state (open | closing), and the desired collection selection. JobStore is the
// ONLY writer of that state; every other view (Job snapshots handed to
// collectors, scheduler lanes, kernel maps) is a projection.
//
// The store holds DESIRED state only. Which collectors are actually attached is
// owned by the collection runtime and is never mirrored here.
//
// LevelDB namespaces (schema 4):
//   j:  active JobRecords, including the desired selection
//   h:  archived history (final record + selection summary)
//   c:  durable allocator high-water mark
//   m:  schema version, Agent DB UUID
//   i:  canonical identity key -> internal ID
//
// Schema 4 replaces schema 3's separate intent object (its own revision plus a
// separate facts revision) with one `selection` object ordered by the record's
// single revision, and drops the persisted runtime `applied` projection. Older
// schemas are rejected explicitly at open time: this build never migrates or
// deletes an existing database. The `m:schema_version` marker is a canonical
// decimal (digits only, no truncation); anything else is rejected read-only
// before the UUID, id counter or any record is initialized.
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
#include "core/liveness.hpp"

// Canonical key of the durable identity index entry (`i:` layout). Shared by
// the store (row maintenance) and the registry (its in-memory index mirror).
std::string identity_index_key(const CanonicalIdentity& identity);

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

// Desired collection selection. It carries no revision and no identity of its
// own: JobRecord::revision is the ONE monotonic counter that orders every
// record mutation, including the selection commands guarded by a CAS.
//
//   automatic        absent  = no automatic decision was ever recorded
//                    present = the policy's last decision (may be empty)
//   automatic_source provenance label of that decision (rule revision), not a
//                    concurrency token
//   manual           absent  = no manual override
//                    present = the operator's selection; present-but-empty is
//                              a persistent stop and wins over automatic
struct CollectionSelection {
    std::optional<std::vector<std::string>> automatic;
    std::string automatic_source;
    std::optional<std::vector<std::string>> manual;

    bool hasManual() const { return manual.has_value(); }
    bool stopped() const { return manual.has_value() && manual->empty(); }
    // manual wins over automatic, the explicit empty stop included.
    std::vector<std::string> effectiveCollectors() const;
    CollectionMode mode() const
    {
        return manual.has_value() ? CollectionMode::Manual : CollectionMode::Auto;
    }
};

// A control-plane selection command. The store re-reads the record inside the
// same transaction that writes it, so the transform, the revision guard and
// the commit are one atomic step; a caller-side read-modify-write window does
// not exist.
enum class SelectionOperation {
    SetManual,   // replace the manual selection; [] is a persistent stop
    ResetAuto,   // clear the manual override, keep the automatic selection
    SetAuto,     // record an automatic decision; manual always wins
};

struct SelectionMutation {
    uint64_t internal_id = 0;
    SelectionOperation operation = SelectionOperation::SetManual;
    // SetManual: manual names. SetAuto: automatic names. ResetAuto: empty.
    std::vector<std::string> collectors;
    std::string automatic_source;                // SetAuto only
    std::optional<uint64_t> expected_revision;   // CAS guard, checked in-transaction
};

struct SelectionMutationResult {
    uint64_t internal_id = 0;
    // The record revision after the command: the accepted revision a caller may
    // report. This is an acceptance stamp, not proof the runtime applied it.
    uint64_t revision = 0;
    CollectionSelection selection;
    // The client-visible desired selection changed.
    bool intent_changed = false;
    // The effective collector set changed, so the runtime must be reconciled.
    // An automatic decision under a manual override changes bookkeeping only.
    bool runtime_changed = false;
    // A row was written even though the client-visible intent did not move
    // (automatic bookkeeping under a manual override).
    bool stored_changed = false;
    bool skipped_manual_override = false;
};

struct StoreHealth {
    bool db_running = false;
    bool migration_complete = false;
    // empty | complete | unsupported_schema:<n> | malformed_schema_marker
    std::string migration_state;
    uint32_t schema_version = 0;
    std::string db_uuid;
    bool degraded = false;
    // A write reported failure without proving whether it landed. Until an
    // explicit recovery verifies the affected rows (or the store is reopened)
    // no mutation, runtime application or checkpoint may run: memory and disk
    // may disagree, and a blind write could publish the older image.
    bool recovery_required = false;
    std::string recovery_state;          // last operation that lost certainty
    // Write batches (any durability) and the subset that actually fsynced.
    // Exposed so the cost of a registration path can be reasoned about instead
    // of guessed: the hot path must not move `sync_commits`.
    uint64_t durable_commits = 0;
    uint64_t sync_commits = 0;
    uint64_t dirty_records = 0;
    // Entries in the last attempted recovery fence batch. A recovery that
    // adopts an instance must re-assert it durably; when the adopted instance
    // was archived, that fence is the only proof the archive landed, so a zero
    // here after a failed recovery means the obligation is still open.
    uint64_t recovery_fence_entries = 0;
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

// Durable lifecycle state persisted with the record. "closing" is written
// before the collector drain starts, so an interrupted retirement can never
// resurrect as active. Unverified liveness is NOT a lifecycle state: a restored
// record is Open unless it is Closing, and an unreadable process is "unknown",
// never dead.
inline constexpr const char* kStateActive = "active";
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
    // Which path owns the execution facts.  A discovery observation EXPECTS a
    // manager generation, so an incomplete one is an unverified execution, not
    // a manual registration.  A manual/legacy row owns no generation and its
    // bound member identities are its only execution evidence.  Set by upsert()
    // (true) and insertManual() (false); never inferred from a member count.
    bool execution_observed = false;
    // --- JobLens management state ----------------------------------------
    uint64_t internal_id = 0;
    std::string instance_id;             // opaque handle (db uuid + id)
    std::string state = kStateActive;    // active | closing
    // The ONE record revision. Every durable mutation of this record advances
    // it: facts, automatic bookkeeping and manual selection commands alike. A
    // CAS reads and compares this same value inside the write transaction.
    uint64_t revision = 0;
    // Liveness evidence from the last restore/observation. False means the
    // execution could not be verified (unreadable process, incomplete discovery
    // identity). It is NOT a lifecycle state: the record stays Open, admission
    // is refused, and absence never authorizes retirement or PID deletion.
    bool execution_verified = true;
    int64_t first_seen_us = 0;
    int64_t last_seen_us = 0;
    // --- desired selection (the runtime's actual state is not stored) -----
    CollectionSelection selection;
};

// ---------------------------------------------------------------------------
// JobStore
// ---------------------------------------------------------------------------

class JobStore {
public:
    struct Options {
        std::string db_path;                 // empty: job_registry_config.job_db_path
        // NOTE: process-identity verification no longer lives here. The registry
        // owns the probe (admission and restore validation share it) and the
        // record map; this class only executes persistence.
    };

    // Read-only access to the current in-memory image of one record. The
    // registry owns the records; the store needs their content to fence
    // unsynced rows during flushDirty() and recovery. The provider MUST NOT be
    // called with any store lock held (the store calls it in snapshot phases
    // outside db_mtx_), and it must not fail spuriously: a missing row is
    // reported as nullopt.
    using RowProvider = std::function<std::optional<JobRecord>(uint64_t)>;

    explicit JobStore(const Options& options);
    ~JobStore();

    JobStore(const JobStore&) = delete;
    JobStore& operator=(const JobStore&) = delete;

    // --- health / test faults -------------------------------------------
    StoreHealth health() const;
    bool ready() const;
    // True while the last write outcome is unknown. Every durable mutation and
    // the checkpoint refuse to run in this state.
    bool recoveryRequired() const { return recovery_required_.load(); }
    // Explicit, complete recovery: adopts the durable row for every instance
    // whose last write lost certainty, rewrites the dirty set durably, and
    // clears the latch only when every step succeeded. `adopted` returns the
    // durable rows that are now authoritative and `removed` the instances the
    // durable state proves gone (archived or delete-confirmed); the registry
    // applies both to its record map. Reading one row is not a recovery, and a
    // successful write elsewhere never clears the latch.
    bool recoverPending(std::vector<JobRecord>& adopted, std::vector<uint64_t>& removed,
                        std::string& error);
    void injectFaultForTest(StoreFaultMode mode, size_t skip = 0, size_t count = 1);
    void clearFaultsForTest();
    // See RowProvider; inject before the first flush/recovery use.
    void setRowProvider(RowProvider provider) { row_provider_ = std::move(provider); }

    // Adapter-owned identity->facts derivation, injected by the registry
    // (dispatches through the scheduler adapters by identity.adapter). The
    // store itself never branches on a scheduler; without an applier the
    // observed facts are stored verbatim.
    void setIdentityApplier(
        std::function<void(Job&, const NativeSelector&, const CanonicalIdentity&)> applier)
    {
        identity_applier_ = std::move(applier);
    }

    // --- restore (once, right after construction) ------------------------
    // Raw rows only: parse every `j:` row and return it with its instance_id
    // derived. NO validation, NO suspect marking, NO index building — the
    // registry owns the records and decides what a restored execution is
    // worth. Corrupt rows are counted and skipped.
    std::vector<JobRecord> loadAll();

    // --- internal id allocation ------------------------------------------
    uint64_t nextJobId();   // ready-checked; 0 when the store is not ready

    // --- manual (legacy) registration -------------------------------------
    // Durable insert of a caller-built record plus the identity index.
    // Rejects a duplicate live identity. Returns false on any failure.
    bool insertManual(JobRecord record);

    // --- inventory upsert --------------------------------------------------
    // Identity-resolving idempotent upsert; rejects incomplete identities,
    // closing instances and conflicting live generations. Never fires
    // lifecycle callbacks and never starts collection. `out` carries the
    // authoritative record (the caller owns the record map and must publish
    // it).
    UpsertResult upsert(const Job& job, JobRecord& out);
    // --- record mutations ---------------------------------------------------
    // Durable lifecycle-state write (the closing transition). `out` receives
    // the authoritative record after the revision bump.
    bool persistState(uint64_t id, const std::string& state,
                      std::optional<JobRecord>& out);
    // Durable archive (active/closing -> history, identity index and intent
    // removed) for an instance that WAS collected: the history row keeps the
    // final record, the fact snapshot of what the runtime really had attached
    // (`applied_collectors`, captured by the registry at the retire decision
    // point) and how that snapshot was established (`applied_provenance`).
    // Returns false on a store failure.
    bool archive(uint64_t id, const std::vector<std::string>& applied_collectors,
                 const std::string& applied_provenance);
    // Durable removal WITHOUT a history row, for an instance that was never
    // collected. Same admission, durability and recovery fencing as archive,
    // except an ambiguous batch is confirmed by the absence of the `j:` row
    // itself (a delete-only batch is atomic, so the identity index cannot
    // outlive the row it pointed at).
    bool discard(uint64_t id);
    // Re-read process/attribute facts for one stored job (legacy/manual
    // registrations): /proc + scheduler metadata I/O happen in `refresh`
    // OUTSIDE every store lock; only the result write happens here. `out`
    // receives the authoritative record after the revision bump.
    bool refreshFacts(uint64_t id, const std::function<void(Job&)>& refresh,
                      std::optional<JobRecord>& out);

    // --- intent + applied ---------------------------------------------------
    // Atomic selection command batch: for every mutation the stored record is
    // re-read under the transaction locks, the expected revision is compared
    // there (never in the caller), and the transform is applied. Every changed
    // record is written in ONE unsynced batch; a write failure commits and
    // publishes nothing. `out` is aligned with `mutations` and carries the
    // authoritative revision/selection the caller must publish.
    bool mutateSelections(const std::vector<SelectionMutation>& mutations,
                          std::vector<SelectionMutationResult>& out,
                          std::string& error);
    // Flush the rows written without a fence (checkpoint/shutdown). Public: the
    // registry schedules it from the maintenance pass and from deinit(). Rows
    // whose last write outcome are unknown are never rewritten; the call then
    // fails so the caller cannot report a durable state it did not reach.
    // Row content comes from the RowProvider.
    bool flushDirty(const char* reason);

    // Pure derivation from the db uuid: no record access.
    std::string instanceId(uint64_t id) const;

private:
    // Injected by the registry; see setIdentityApplier.
    std::function<void(Job&, const NativeSelector&, const CanonicalIdentity&)>
        identity_applier_;
    // Injected by the registry; see setRowProvider.
    RowProvider row_provider_;

    bool init_db(const Options& options);
    void close_db();
    bool load_or_create_db_uuid();
    // True when the opened database has no user keys yet (a brand-new store).
    bool db_is_empty() const;
    // Persists the current schema marker for a brand-new store.
    bool initialize_schema();
    // Closes a database this build must not touch and records why it is not
    // ready. Never deletes data.
    void reject_unsupported_schema(uint32_t found);
    void reject_schema(const std::string& state, uint32_t found);
    void init_id_counter();
    // In-memory reservation for the inventory create path: the caller writes
    // `durable_counter` in the same batch as the record, so a new instance
    // costs one durable commit instead of two.
    bool reserveIdInMemory(uint64_t& id, uint64_t& durable_counter,
                           bool& durable_exhausted, std::string& error);
    bool store_ready() const;

    // --- internal implementation entry points (no external callers) -------
    // Durable allocation on its own (manual registration): reserves the id
    // with its own commit before it is handed out.
    uint64_t allocateId(std::string& error);
    // Write-free merge decision for one observation against an already-loaded
    // record, used by the single upsert path.
    UpsertResult merge_existing(const Job& job, const NativeSelector& selector,
                                const CanonicalIdentity& identity, uint64_t internal_id,
                                const JobRecord& stored, int64_t observed_at,
                                JobRecord& updated_out);
    // Generic durable read-modify-write of one record. `out` receives the
    // authoritative record after the revision bump.
    bool updateRecord(uint64_t id, const std::function<void(JobRecord&)>& mutate,
                      const char* operation, std::optional<JobRecord>& out);
    // `durable` = true fsyncs the batch (that is, also every earlier unsynced
    // write in the log). The registration hot path writes with durable=false
    // and leaves the row in the dirty set for the checkpoint / shutdown flush.
    // `touched` names the instances whose row this batch rewrites; a failed
    // write of such a batch latches recovery because the disk may have landed
    // the very image memory did not publish. `delete_only` marks a batch whose
    // only row effect is deleting `j:`/`i:` entries: an ambiguous outcome is
    // then confirmed by the row's absence instead of an archive fence.
    leveldb::Status write_batch_locked(leveldb::WriteBatch& batch, const char* operation,
                                       bool durable = true,
                                       const std::vector<uint64_t>* touched = nullptr,
                                       bool delete_only = false);
    // Which side is authoritative after an ambiguous write.
    enum class RecoveryKind {
        None,
        // The write may have landed while memory kept the previous image: the
        // durable row is truth and is adopted for `recovery_ids_`.
        AdoptDurable,
        // A checkpoint failed after memory became truth: the snapshot is
        // rewritten instead of read back.
        RewriteMemory,
    };
    // Callers hold db_mtx_ (write_batch_locked is named for that contract).
    // `delete_only` records that the latched batch only deleted rows, so a
    // recovery that finds the `j:` row missing treats that as a confirmed
    // deletion instead of demanding an archive fence.
    void latch_recovery_locked(const char* operation, RecoveryKind kind,
                               const std::vector<uint64_t>& ids,
                               bool delete_only = false);
    bool recover_pending_locked(std::vector<JobRecord>& adopted,
                                std::vector<uint64_t>& removed, std::string& error);
    // The exact durable image of an archive that landed ambiguously: the
    // history key (it encodes the archive timestamp), its value verbatim, and
    // the identity index row the same batch deleted while this instance still
    // owned it. Recovery re-asserts this image instead of rebuilding one from
    // memory, which would invent a different timestamp and summary.
    struct ArchiveFence {
        uint64_t internal_id = 0;
        std::string history_key;
        std::string history_value;
        std::string identity_key;   // empty when there is nothing to release
    };
    // Reads the durable `h:` row for an instance whose `j:` row is gone. Fails
    // when there is no history row (nothing to prove) or more than one (an
    // image this process cannot disambiguate).
    bool read_archive_fence_locked(uint64_t internal_id, ArchiveFence& fence,
                                   std::string& error);
    // Reads one durable row and hands it back for adoption. `archive` is set
    // when the row was archived: the recovered history image must be fenced.
    // `delete_confirmed` is set when the row is missing and the latched batch
    // was delete-only: the deletion landed and the fence re-asserts it by
    // deleting the row again. Returns false only when the outcome is still
    // unknown (a read error or an image that cannot be proven).
    bool adopt_durable_locked(uint64_t internal_id, std::optional<ArchiveFence>& archive,
                              bool& delete_confirmed, JobRecord& adopted_record,
                              std::string& error);
    // Phase-1 helper of recover_pending_locked (callers hold db_mtx_).
    bool adopt_locked_pass1(uint64_t id, std::vector<ArchiveFence>& archives,
                            std::vector<JobRecord>& adopted,
                            std::vector<uint64_t>& removed, std::string& error);
    // Marks one record / binding as "written but not yet fsynced".
    void mark_record_dirty(uint64_t internal_id);
    void record_store_failure(const std::string& message);
    void record_store_success();
    bool update_job_info(uint64_t id, const Job& job,
                         std::optional<JobRecord>& out);
    std::string identity_key(const CanonicalIdentity& identity) const;
    // Serializes job_db transactions.
    mutable std::mutex db_mtx_;
    // Rows written with durable=false since the last flush (guarded by db_mtx_).
    // Record content lives in the registry (see RowProvider); this set is the
    // persistence-side obligation to fence them.
    std::set<uint64_t> dirty_records_;

    leveldb::DB* db_ = nullptr;
    bool db_running_ = false;
    Options options_;
    // Boot id this process runs under: an execution stamped with any other boot
    // cannot be live here. Read once at construction for restore validation,
    // which the registry performs with its own probe.
    std::string boot_id_;

    std::string db_uuid_;
    uint32_t schema_version_ = 0;
    // true only when this build owns the on-disk schema. Older/newer layouts
    // are rejected at open and never migrated or deleted.
    bool migration_complete_ = false;
    std::string migration_state_;
    std::atomic<bool> store_degraded_{false};
    // Guarded by db_mtx_ (the rejected-state fields are set before ready()).
    RecoveryKind recovery_kind_ = RecoveryKind::None;
    // True while every latched batch was delete-only (see write_batch_locked).
    bool recovery_delete_only_ = false;
    std::set<uint64_t> recovery_ids_;
    std::string recovery_operation_;
    std::atomic<bool> recovery_required_{false};
    // Entries in the last attempted recovery fence batch (guarded by db_mtx_).
    uint64_t recovery_fence_entries_ = 0;
    std::atomic<uint64_t> store_write_failures_{0};
    std::atomic<uint64_t> durable_commits_{0};
    std::atomic<uint64_t> sync_commits_{0};
    uint64_t corrupt_records_ = 0;
    std::string last_store_error_;   // guarded by db_mtx_
    StoreFaultMode fault_mode_ = StoreFaultMode::None;
    size_t fault_skip_ = 0;
    size_t fault_count_ = 0;

    uint64_t next_id_ = 1;
    bool id_exhausted_ = false;
    std::mutex id_mtx_;
};
