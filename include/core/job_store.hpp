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
// JobStore — the pure persistence service behind JobRegistry.
//
// The store owns the LevelDB layout and nothing else: key namespaces, row
// encode/decode, batch atomicity, tiered durability, recovery fences and the
// internal-ID high-water mark. Every business decision — job-record merging,
// restore validation, selection strategy, archive-versus-discard — lives in
// JobRegistry, the single source of truth for JobRecords. The registry runs its
// whole read-decide-transform sequence under its record lock and then calls
// commit() with the resulting rows; the store never interprets them.
//
// A JobRecord is "a Job plus what JobLens must remember to manage it":
// execution evidence (identity, manager, pid identities), the durable lifecycle
// state (active | closing), and the desired collection selection. Which
// collectors are actually attached is owned by the collection runtime and is
// never persisted here.
//
// LevelDB namespaces (schema 5):
//   j:  active JobRecords, including the desired selection
//   h:  archived history (final record + applied-collector snapshot)
//   c:  durable allocator high-water mark
//   m:  schema version, Agent DB UUID, id-exhaustion marker
//   i:  canonical identity key -> job ID
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

class JobStore;
struct JobRecord;

// ---------------------------------------------------------------------------
// Wire/store value types (shared with the registry facade)
// ---------------------------------------------------------------------------

enum class CollectionMode {
    Auto,
    Manual,
};

// Who owns the record's facts. Discovery observations carry execution evidence
// and are re-observed; a manual registration has no source behind it. This is
// provenance, not a lifecycle state.
enum class FactSource {
    Manual,
    Discovery,
};

inline const char* to_string(CollectionMode mode)
{
    return mode == CollectionMode::Manual ? "manual" : "auto";
}

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

    bool operator==(const CollectionSelection& other) const
    {
        return automatic == other.automatic &&
               automatic_source == other.automatic_source && manual == other.manual;
    }

    bool stopped() const { return manual.has_value() && manual->empty(); }
    // manual wins over automatic, the explicit empty stop included.
    std::vector<std::string> effectiveCollectors() const;
    CollectionMode mode() const
    {
        return manual.has_value() ? CollectionMode::Manual : CollectionMode::Auto;
    }
};

// Canonical pid identity list form of a durable row: sorted by (pid, start_ticks)
// and deduplicated. Shared by the row codec (parse_record) and the registry's
// merge path so both write the same j: image.
std::vector<ProcessIdentity> normalized_pid_identities(std::vector<ProcessIdentity> processes);

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

// One scheduler discovery. `facts` carries the typed scheduler attributes;
// its JobID and CollectorNames are deliberately ignored: registry upserts
// never allocate a caller-chosen ID and never activate collection.
//
// `pid_identities_authoritative` mirrors the discovery flag: true means the
// pid identity list is authoritative, an empty list included ("read succeeded,
// no pid identities"); false means the set is only partially known, so an
// empty list must never erase stored pid identities.
// ---------------------------------------------------------------------------
// JobRecord — the one durable representation
// ---------------------------------------------------------------------------

// Durable lifecycle state persisted with the record. Closing is written before
// the collector drain starts, so an interrupted removal can never resurrect
// as active. A missing presumed-live verdict (JobRecord::presumed_live ==
// false) is NOT a lifecycle state: a restored record is Active unless it is
// Closing, and an unreadable process is "unknown", never dead.
enum class RecordState {
    Active,
    Closing,
};

inline const char* to_string(RecordState state)
{
    return state == RecordState::Closing ? "closing" : "active";
}

// Strict wire/row parse: an unknown value is a corrupt row, never Active.
inline std::optional<RecordState> record_state_from_string(const std::string& raw)
{
    if (raw == "active") return RecordState::Active;
    if (raw == "closing") return RecordState::Closing;
    return std::nullopt;
}

struct JobRecord {
    // The one core object: facts + evidence. Management state below is the
    // only thing the record adds. `job.JobID` is the storage key; there is no
    // second job id. A record image never carries intent:
    // job.CollectorNames must stay empty — the dispatch projection is built
    // from `selection` when the runtime asks for it.
    Job job;
    RecordState state = RecordState::Active;
    FactSource source = FactSource::Manual;
    // Derived cache of the record's durable i: key. The store must release that
    // entry during recovery and owns no adapter to derive it there, so the
    // registry stores the one string. Empty = the facts do not name a job.
    std::string index_key;
    // The ONE record revision. Every durable mutation of this record advances
    // it: facts, automatic bookkeeping and manual selection commands alike. A
    // CAS reads and compares this same value inside the write transaction.
    uint64_t revision = 0;
    int64_t first_seen_us = 0;
    int64_t last_seen_us = 0;
    // --- desired selection (the runtime's actual state is not stored) -----
    CollectionSelection selection;
    // Last liveness verdict for this execution: true when the execution is
    // presumed still present (not absent). It is NOT an identity check — the
    // manager triple in the evidence is what names the run. Not a lifecycle
    // state, not persisted: rebuilt by the load pass and by the heartbeat's
    // absence evaluation. Its opposite is the `suspect` count the status
    // surface reports (see jobs health: suspect = !presumed_live).
    bool presumed_live = true;
};

// ---------------------------------------------------------------------------
// JobStore
// ---------------------------------------------------------------------------

class JobStore {
public:
    struct Options {
        std::string db_path;                 // empty: job_registry_config.job_db_path
        // Scheduler-owned row projection. The store parses the row envelope
        // (revision/selection/identity), but the Job fields and typed facts
        // are decoded by the adapter that owns them. Empty keeps the generic
        // open-world/Common decoder (tests and registry-only builds).
        using RowDecoder = std::function<Job(const nlohmann::json&)>;
        RowDecoder row_decoder;
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

    // Adapter-owned identity->facts derivation is registry business: the
    // registry dispatches through its scheduler adapters while building the
    // record image, before commit() ever sees a row.

    // --- restore (once, right after construction) ------------------------
    // Raw rows only: parse every `j:` row and return it. NO validation, NO
    // suspect marking, NO index building — the registry owns the records and
    // decides what a restored execution is worth. Corrupt rows are counted
    // and skipped.
    std::vector<JobRecord> loadAll();
    // Durable identity resolution for the registry's upsert: reports the `j:`
    // row the `i:` index names. The registry consults this when its in-memory
    // mirror misses — an earlier create may have landed ambiguously without
    // publishing anything, and the durable index is the documented repair
    // source. nullopt when no entry exists or the row it names is gone (a
    // stale entry the next create's PutIdentity overwrites).
    std::optional<JobRecord> readRowByIdentity(const SchedulerKey& key);

    // --- job id allocation ------------------------------------------
    uint64_t nextJobId();   // ready-checked; 0 when the store is not ready
    // In-memory half of a single-batch create: reserves the next job id
    // (burned even when the paired commit() later fails — ids are never
    // reused) and reports the durable counter value the ReserveCounter op
    // must carry, so a new instance costs one batch instead of two commits.
    // Pair with commit() inside one registry record lock.
    bool reserveId(uint64_t& id, uint64_t& durable_counter,
                   bool& durable_exhausted, std::string& error);

    // --- the write primitive ----------------------------------------------
    // Every durable mutation is one atomic batch of ops, built and ordered by
    // the registry (the only writer):
    //   PutRecord / DeleteRecord       j: rows (the image is registry truth)
    //   PutIdentity / DeleteIdentity   i: index maintenance
    //   PutHistory                     h: archive row (final record + applied
    //                                  collector snapshot + end time)
    //   ReserveCounter                 c: allocator high-water riding a create
    // PutIdentity refuses a duplicate LIVE identity (an entry owned by a
    // different instance whose j: row still exists); stale entries are
    // overwritten. DeleteIdentity releases an entry only while the named
    // instance still owns it on disk.
    //
    // `durable` = true fsyncs the batch (and every earlier unsynced write in
    // the log); false leaves each PutRecord row in the dirty set for the
    // checkpoint / shutdown flush — the registration hot path stays there.
    // `touched` (input) names the instances whose rows this batch rewrites
    // while the registry still holds a previous image: a write whose outcome
    // becomes unknown latches recovery for exactly these ids. A create that
    // published nothing passes an empty list — identity lookup repairs it.
    // A batch of only deletes proves its own ambiguous outcome by the row's
    // absence (delete-only recovery confirmation).
    struct WriteOp {
        enum Kind {
            PutRecord,
            DeleteRecord,
            PutIdentity,
            DeleteIdentity,
            PutHistory,
            ReserveCounter,
        } kind = PutRecord;
        uint64_t job_id = 0;
        std::optional<JobRecord> record;              // PutRecord / PutHistory
        std::optional<SchedulerKey> key;              // i: operations
        // PutHistory: hex end-time suffix of the h: key; empty = stamped by
        // the store at commit time (the value's end_time_us always matches).
        std::string history_key_suffix;
        std::vector<std::string> applied_collectors;  // PutHistory: fact snapshot
        std::string history_provenance;               // PutHistory: how established
        uint64_t durable_counter = 0;                 // ReserveCounter
        // ReserveCounter: the m: exhaustion marker rides the same op so a
        // restart cannot conclude MAX is still free.
        bool counter_exhausted = false;
    };
    bool commit(std::vector<WriteOp> ops, bool durable,
                std::vector<uint64_t>& touched, const char* reason,
                std::string& error);

    // Flush the rows written without a fence (checkpoint/shutdown). Public: the
    // registry schedules it from the maintenance pass and from deinit(). Rows
    // whose last write outcome are unknown are never rewritten; the call then
    // fails so the caller cannot report a durable state it did not reach.
    // Row content comes from the RowProvider.
    bool flushDirty(const char* reason);

private:    // Injected by the registry; see setRowProvider.
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
    bool store_ready() const;

    // --- internal implementation entry points (no external callers) -------
    // Durable allocation on its own (manual registration): reserves the id
    // with its own commit before it is handed out.
    uint64_t allocateId(std::string& error);
    // Applies one WriteOp to `batch` (the i: ownership reads ride the same
    // db_mtx_ section as the write). Returns false + error on an invalid op
    // or a refused identity operation. `put_seen` records that the batch
    // writes at least one row (a delete-only batch proves its own ambiguous
    // outcome by the row's absence).
    bool apply_write_op_locked(const WriteOp& op, leveldb::WriteBatch& batch,
                               bool& put_seen, std::string& error);
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
        uint64_t job_id = 0;
        std::string history_key;
        std::string history_value;
        std::string identity_key;   // empty when there is nothing to release
    };
    // Reads the durable `h:` row for an instance whose `j:` row is gone. Fails
    // when there is no history row (nothing to prove) or more than one (an
    // image this process cannot disambiguate).
    bool read_archive_fence_locked(uint64_t job_id, ArchiveFence& fence,
                                   std::string& error);
    // Reads one durable row and hands it back for adoption. `archive` is set
    // when the row was archived: the recovered history image must be fenced.
    // `delete_confirmed` is set when the row is missing and the latched batch
    // was delete-only: the deletion landed and the fence re-asserts it by
    // deleting the row again. Returns false only when the outcome is still
    // unknown (a read error or an image that cannot be proven).
    bool adopt_durable_locked(uint64_t job_id, std::optional<ArchiveFence>& archive,
                              bool& delete_confirmed, JobRecord& adopted_record,
                              std::string& error);
    // Phase-1 helper of recover_pending_locked (callers hold db_mtx_).
    bool adopt_locked_pass1(uint64_t id, std::vector<ArchiveFence>& archives,
                            std::vector<JobRecord>& adopted,
                            std::vector<uint64_t>& removed, std::string& error);
    // Marks one record / binding as "written but not yet fsynced".
    void mark_record_dirty(uint64_t job_id);
    void record_store_failure(const std::string& message);
    void record_store_success();
    // Reads the durable `i:` owner of one identity. Returns false on a read
    // error; owner == 0 means "no parsable owner" (a stale entry).
    bool identity_owner_locked(const SchedulerKey& key, uint64_t& owner,
                               std::string& error);
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
