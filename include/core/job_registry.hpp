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
// job_registry.h — the registry facade
//
// JobRegistry owns the JobStore: the single durable JobRecord representation
// and the only writer of facts, desired selection and Active/Closing state. It
// holds NO runtime mirror. Which collectors are actually attached belongs to
// the collection runtime, reached through the explicit ICollectionRuntime seam;
// the runtime converges by reading records through IJobRecordSource and reports
// a completed stop through IDrainCompletionSink.
#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <tuple>
#include <set>

#include "core/collector_type.h"
#include "core/job_identity.hpp"
#include "job_lifecycle_event.h"
#include "core/job_discovery.hpp"
#include "core/job_store.hpp"
#include "core/collection_runtime.hpp"
#include "core/kernel_attribution.hpp"
#include "core/job_auto_policy.hpp"
#include "core/liveness.hpp"
#include "core/scheduler_adapter.hpp"

// The resolved maintenance picture (discovery switches, per-adapter settings).
// Passed by reference through the construction path; the full definition lives
// in core/maintenance_config.hpp.
struct MaintenanceConfig;

// ---------------------------------------------------------------------------
// Collection selection control (registry-owned)
// ---------------------------------------------------------------------------
// These are the control-plane commands the registry executes itself: target
// validation, manual-stop precedence, revision CAS and the durable transaction
// live behind one entry point instead of a separate manager object calling
// back into the registry.
enum class CollectionError {
    None = 0,
    InvalidRequest,
    UnknownCollector,
    NotFound,
    StaleRevision,
    StoreUnavailable,
    // The caller tried to mutate intent from inside a gated mutation (a
    // lifecycle hook or collection applier). Nothing was committed.
    ReentrantMutation,
};

inline const char* to_string(CollectionError error)
{
    switch (error) {
        case CollectionError::None:              return "ok";
        case CollectionError::InvalidRequest:    return "invalid_request";
        case CollectionError::UnknownCollector:  return "unknown_collector";
        case CollectionError::NotFound:          return "not_found";
        case CollectionError::StaleRevision:     return "stale_revision";
        case CollectionError::ReentrantMutation: return "reentrant_mutation";
        case CollectionError::StoreUnavailable:  return "store_unavailable";
    }
    return "unknown";
}

struct CollectionRequest {
    std::vector<uint64_t> targets;
    // set_collectors: required, explicit [] means persistent stop.
    // reset_auto: must be absent.
    std::optional<std::vector<std::string>> collectors;
    // Optional binding revision guard, single-target requests only. It is
    // compared inside the store transaction, not in a caller-side lock.
    std::optional<uint64_t> expected_revision;
};

struct CollectionTargetOutcome {
    uint64_t job_id = 0;
    // The accepted desired state after the command.
    CollectionSelection selection;
    // The record revision the command committed. Acceptance only: the runtime
    // converges asynchronously, so this is not proof the collectors attached.
    uint64_t revision = 0;
    // What the runtime actually holds right now, sampled after the commit. It
    // may still show the previous attachment; that is the honest answer.
    ActualCollectionState actual;
    bool changed = false;
    // The automatic decision was refused before the transaction (unknown
    // instance or invalid collector). Such a target is never "committed", even
    // when a previous valid revision exists.
    bool skipped = false;
};

struct CollectionOutcome {
    bool ok = false;
    CollectionError error = CollectionError::None;
    std::string message;
    std::vector<CollectionTargetOutcome> targets;  // exact targets on success
    size_t unchanged = 0;
    size_t skipped_manual_override = 0;
    size_t skipped_invalid = 0;
};

// Automatic policy result for one instance.
struct AutoSelection {
    uint64_t job_id = 0;
    std::vector<std::string> collectors;
    std::string policy_revision;
};

// Construction options. Production uses the singleton (all defaults); tests
// and registry-only service startup construct an isolated store without
// watchers, eBPF or RPC registration.
struct JobRegistryOptions {
    std::string db_path;                 // empty: job_registry_config.job_db_path
    bool enable_pid_tracker = true;
    bool register_rpc = true;
    // --- heartbeat test seams (used instead of the config-driven adapters) --
    // Pre-built discovery sources to drive, with the adapter name each
    // discovered Job belongs to. When non-empty, the configuration is not
    // consulted and automatic selection stays disabled.
    std::vector<std::pair<std::string, IDiscoverySource*>> test_sources;
    // With this flag the heartbeat runs without reading Config (test wiring;
    // combine with test_sources, or leave it empty for manual-only upkeep).
    bool test_heartbeat = false;
    // Test-only escape hatch: construct a purely static registry
    // (no heartbeat, no discovery sources, no maintenance pass — only the
    // restored rows, manual CRUD, the read surface and explicit removal).
    // Production never sets it: the registry stack stays armed so registered
    // jobs keep their removal/checkpoint upkeep.
    bool test_static_registry = false;
    // Liveness probe overrides (empty = production /proc/cgroup probes).
    ProcessLivenessProbe test_process_probe;
    CgroupMembershipProbe test_cgroup_probe;
    // Automatic-selection policy for test wiring (production builds it from
    // job_registry_config.maintenance + <type>_job_watcher). The adapter name
    // below is the key it is armed under.
    std::function<AutoCollectorPolicy()> test_auto_policy;
    std::string test_auto_policy_adapter;
    // Admission-gate test wiring (production reads capture_all and
    // the per-type auto_add tri-state from the resolved configuration).
    // test_capture_all=false arms the gate; the state map carries the
    // per-subtype tri-state (a missing entry behaves as Off).
    bool test_capture_all = true;
    std::map<std::string, AutoAdd> test_auto_add_states;
};

class JobRegistry : private KernelAttribution::Host,
                    public IDrainCompletionSink,
                    public IJobRecordSource {
public:
    static JobRegistry& instance();          // 仍保留单例，方便迁移；也可由 main() 构造
    explicit JobRegistry(const JobRegistryOptions& options);
    ~JobRegistry();

    // 禁止拷贝
    JobRegistry(const JobRegistry&)            = delete;
    JobRegistry& operator=(const JobRegistry&) = delete;

    // Job registration and manual selection updates.
    // Manual upsert: an active typed match constrained by request/selector
    // namespaces updates only the manual collector selection (union); metadata
    // and execution evidence remain unchanged. Unknown namespaces are never
    // adopted on UPDATE. No match takes CREATE, which rejects empty collectors.
    // This path is independent of the automatic admission gate.
    nlohmann::json addOrUpdateManualJob(
        const Job& job, const nlohmann::json& request,
        std::optional<NativeSelector> identity_selector = std::nullopt,
        bool create_only = false);
    // Thin alias for the CREATE branch — the historical manual registration
    // entry.
    bool addJob(const Job& job);
    // Begin removal: durably mark the record Closing, capture the
    // runtime-applied collector snapshot and report whether that durable
    // Closing was accepted. Deletion/archival is asynchronous — the drain,
    // checked kernel cleanup and the archive belong to onDrained() /
    // cleanupThenArchive() — so true means "the removal started", not "the
    // record is gone". A record whose Closing write fails stays Active and
    // retryable.
    bool removeJob(uint64_t jobID);
    // 通过 sub_attr 匹配发起移除，返回被接受的移除数量（不是已删除数量）
    size_t removeMatchingJobs(const SubAttrMatcher& matcher);
    size_t closingJobCount() const;
    bool cleanupClosingJobs();
    // Read-only snapshot: no refresh, no PID pruning, no removal.
    std::optional<Job> findJob(uint64_t jobID);
    // 通过 sub_attr 匹配查找作业，返回匹配的作业列表
    std::vector<Job> findJob(const SubAttrMatcher& matcher) const;
    std::vector<Job> snapshot() const;

    // 获取下一个全局唯一递增的JobID（durable 预留；失败返回 0）
    uint64_t getNextJobID();

    // --- JobStore facade --------------------------------------------
    // Admission outcome for one observation (capture_all=false). A present
    // decision means the observation may register; the optional initial
    // collector list is the gate's pre-evaluated automatic selection, applied
    // instead of re-running the policy after registration (no double Lua).
    // A nullopt decision from the gate drops the observation before any store
    // work.
    struct InitialDecision {
        std::optional<std::vector<std::string>> initial_collectors;
        std::string policy_revision;
    };
    // Identity-resolving idempotent upsert. Incomplete identities and
    // conflicting live executions are rejected explicitly and never indexed.
    // Existing records retain their creation metadata; observations refresh
    // execution evidence and observation bookkeeping only.
    // An upsert never fires lifecycle callbacks or starts collection: runtime
    // application is binding-driven. `admission` (internal) carries a
    // pre-evaluated admission decision: when it holds an initial collector
    // list, that list is applied as the initial automatic selection instead
    // of re-evaluating the policy.
    UpsertResult upsertObservedJob(const Job& job, const InitialDecision* admission = nullptr);
    std::optional<JobRecord> jobRecord(uint64_t jobID) const;
    std::optional<JobRecord> jobRecordByKey(const SchedulerKey& key) const;
    std::vector<JobRecord> jobRecords() const;
    CollectionSelection selectionRecord(uint64_t jobID) const;
    // Optional collector-name gate. Unset accepts every non-empty name.
    void setCollectorValidator(std::function<bool(const std::string&)> validator);
    // operation = set: manual selection replacing any previous intent; []
    // stops collection. `collectors` must be present.
    CollectionOutcome setCollectors(const CollectionRequest& request);
    // operation = reset_auto: clear the manual override and let the recorded
    // automatic selection apply. `collectors` must be absent.
    CollectionOutcome resetAuto(const CollectionRequest& request);
    // Automatic policy decision for a batch of instances. Manual overrides,
    // including empty ones, are never overwritten.
    CollectionOutcome applyAutoPolicy(const std::vector<AutoSelection>& selections);
    // Test seam: replaces the kernel-cleanup step so the removal ordering
    // (closing -> cleanup -> archive) can be exercised without BPF. Production
    // leaves it unset and uses KernelAttribution; it grants no way to write job
    // state.
    void setKernelCleanupForTest(
        std::function<bool(uint64_t, const Job&, std::string&)> cleanup);
    StoreHealth storeHealth() const;

    // --- Collection runtime seam ------------------------------------------
    // The component that actually runs collectors (CollectorScheduler) is
    // injected at composition time. The registry never reaches for its
    // singleton, and it never calls into it on a source or RPC stack: a command
    // commits desired state, then only wakes the runtime.
    void setCollectionRuntime(ICollectionRuntime* runtime);
    ICollectionRuntime* collectionRuntime() const;
    // Coalesced wake of the installed runtime. Entered under the mutation gate:
    // a runtime that mutates intent from its wake callback is refused instead
    // of recursing back into the command path.
    void wake_runtime();
    // Actual state as the runtime reports it (untracked when no runtime).
    ActualCollectionState actualCollection(uint64_t jobID);
    // IDrainCompletionSink: the runtime proved the instance released every
    // collector. Runs checked kernel cleanup then the archive; a failure keeps
    // the Closing record retryable.
    void onDrained(uint64_t job_id) override;
    // IJobRecordSource: the runtime's read surface for desired state.
    bool runtimeRecords(std::vector<JobRecord>& out) const override;
    bool runtimeRecord(uint64_t job_id,
                       std::optional<JobRecord>& out) const override;
    // Liveness evidence from the last scan. Not a lifecycle state: an instance
    // that is not presumed live stays Active but must not be admitted to the
    // kernel.
    void setExecutionPresumedLive(uint64_t jobID, bool presumed_live);

    // --- Lifecycle -----------------------------------------------------------
    // Stop discovery admission and polling. Must run before the collector
    // scheduler drains. Idempotent.
    void stopDiscovery();
    // Finalize removals, stop the heartbeat, archive closing records and
    // close the store. Returns false when a closing record could not be
    // archived (the incomplete state stays persisted). Idempotent; the
    // destructor calls it as a fallback.
    bool deinit();

    // --- Registry heartbeat --------------------------------------------------
    // One on-demand authoritative pass (the `ensure_registered` refresh
    // provider): scan -> upsert -> hint merge -> removal -> reconciliation,
    // exactly what the periodic worker runs. Returns false when the heartbeat
    // is not running.
    bool reconcileNow();
    // Kernel exit hint entry (cheap; schedules a coalesced refresh).
    void notifyExit(pid_t pid);

    // Automatic selection entry point: record the decision as the instance's
    // automatic intent (manual overrides are never touched). Returns false
    // when the transaction was rejected.
    bool applyOneAutoSelection(uint64_t job_id, const std::vector<std::string>& collectors,
                               const std::string& policy_revision);

    // Kernel attribution stats for the RPC surface.
    bool attributionRunning() const;
    uint64_t attributionPushedEvents() const;
    uint64_t attributionDroppedEvents() const;

    // ---- structured namespace facade ---------------------------------------
    //
    // The scheduler-owned namespace semantics (scheduler_adapter.hpp) reached
    // through this registry's live adapters. Every caller that must compare,
    // inject or render namespace facts goes through here so no scheduler
    // knowledge leaks into core call sites. Jobs/selectors without a
    // configured adapter keep the generic two-component form carried by the
    // common fields themselves.

    // The record's derived logical identity: the adapter owning the facts
    // rebuilds the selector and the native-id rule canonicalizes it. nullopt
    // means the facts do not name a job — never a guess.
    std::optional<SchedulerKey> keyOf(const Job& job) const;
    // The structured namespace facts of one Job / one addressing selector.
    NamespaceFacts namespaceFactsOf(const Job& job) const;
    NamespaceFacts selectorFactsOf(const NativeSelector& selector) const;
    // The adapter that interprets a scheduler name, configured or lazily
    // built for facts interpretation only. nullptr for an unknown name.
    const ISchedulerAdapter* adapterNamed(const std::string& name) const;
    // Folds request-level aliases into the typed facts (facts win); false =
    // the aliases contradict known facts.
    bool adoptNamespaceAliases(Job& job, const std::string& clusterName,
                               const std::string& clusterTag) const;
    // Projects record-visible fields using the adapter's policy, independently
    // of namespace-alias normalization.
    void projectRecordFacts(Job& job) const;
    // Adopts common namespace aliases into empty typed slots (facts win), then
    // renders the typed namespace back for ingress and restored Jobs alike.
    void normalizeNamespaceFacts(Job& job) const;

    // Test seam only (see StoreFaultMode).
    void injectStoreFaultForTest(StoreFaultMode mode, size_t skip = 0, size_t count = 1);
    void clearStoreFaultsForTest();

private:
    friend struct JobRegistryNamespaceTestAccess;
    JobRegistry();
    // Which wiring the heartbeat was assembled from: injected test seams or
    // the production configuration/adapter stack.
    enum class DiscoveryWiring { Disabled, Test, Production };
    // Decides WHICH discovery sources, probes and facts encoder the heartbeat
    // drives, once, before any source starts. Test wiring copies the
    // constructor-provided seams verbatim; production arms the configuration
    // resolved before the store restored rows. The adapter set itself comes
    // from buildAdapters(): one per registered scheduler, independent of
    // `auto_discovery_types`.
    // Construction phases, called in order by the constructor: build the
    // adapters, open the store, restore and validate existing rows, gate
    // collector names, arm the registry components (policies/probes/
    // capability gate/sources), then start sources, the first authoritative
    // pass, the status providers and the maintenance worker.
    void openStore(const JobRegistryOptions& options);
    // Adapter-owned row projection for JobStore: the adapter that owns the
    // facts decodes the Job fields; open-world/Common rows keep the generic
    // decoder. Runs during restore, before the RPC surface opens.
    Job decodeStoredJob(const nlohmann::json& row) const;
    void restoreExistingRecords();
    void installCollectorValidator();
    void resetHeartbeatState();
    // Assembles one settings-driven adapter per registered scheduler before
    // the store restores rows. Discovery arming is separate: an adapter
    // exists whether or not its watcher is enabled.
    bool buildAdapters(const MaintenanceConfig& config);
    DiscoveryWiring armDiscovery(const JobRegistryOptions& options,
                                 const MaintenanceConfig& config,
                                 bool maintenance_ready);
    void runInitialPass();
    // Admit restored active records to kernel ownership through the single
    // admission path (a restarted process starts with an empty namespace).
    void admitRestoredOwners();
    void startDiscoverySources(DiscoveryWiring wiring);
    // Automatic selection for one newly created instance, evaluated outside
    // every registry lock: a Lua rule may be slow or re-enter the registry,
    // so it must never run under a lock that serializes record changes.
    // Returns nullopt when no automatic decision was needed, true when the
    // decision (including an explicit reject) is durable, false otherwise.
    std::optional<bool> applyAutoPolicyForJob(const Job& job, const std::string& adapter);
    // Shared implementation of set/reset_auto. Validation happens before the
    // store call; the durable read-transform-write and its revision guard are
    // one store transaction.
    CollectionOutcome mutateSelection(const CollectionRequest& request, bool manual);
    bool validate_collectors(const std::vector<std::string>& collectors,
                             std::string& bad_name) const;
    // Record-driven repair of a missing initial automatic decision: the
    // predicate is derived from durable state so a restart repairs too.
    bool missing_initial_decision(const JobRecord& record) const;
    std::optional<bool> repair_initial_decision(const JobRecord& record,
                                                UpsertResult& result);
    // The policy decision for one newly created instance, or nullopt when the
    // adapter has no enabled policy.
    struct AutoDecision {
        std::vector<std::string> collectors;
        std::string policy_revision;
    };
    std::optional<AutoDecision> autoSelectionFor(const Job& job, const std::string& adapter);
    void regRPCHandle();
    // --- Manual CRUD upsert internals ----------------------------------------
    // One construction path for every new record. The Job may arrive without
    // what creation owns: the JobID, revision, state, provenance, the manual
    // selection, the execution-evidence seed (pid identities bound from the
    // pids projection when no observation brought any), the timestamps and the
    // identity cache are filled here. `known_key` is the identity the caller
    // already resolved (an observation); empty derives it from the facts. Both
    // are empty only for a Common row.
    struct BuiltRecord {
        JobRecord record;
        std::optional<SchedulerKey> key;
    };
    BuiltRecord buildJobRecord(Job job, uint64_t job_id, FactSource source,
                               std::optional<std::vector<std::string>> manual_collectors,
                               const std::optional<SchedulerKey>& known_key,
                               int64_t observed_at);
    // One allocator answer: the id and the high-water counter that rides the
    // same batch. An explicit caller id reserves nothing (writes_counter
    // false); an allocator answer always persists the advanced counter.
    struct NewRecordId {
        uint64_t id = 0;
        uint64_t counter = 0;
        bool exhausted = false;
        bool writes_counter = false;
    };
    std::optional<NewRecordId> reserveJobId(uint64_t requested, std::string& error);
    // One insert path: primary-key/identity conflict checks, one RecordTxn
    // (row + index + counter) and the publication its commit performs. Callers
    // hold mutation_mtx_.
    bool commitNewRecord(BuiltRecord built, const NewRecordId& id, bool durable,
                         std::string& error);
    // The CREATE branch: the manual registration body. The result reuses the
    // registration contract: the typed code is what addOrUpdateManualJob maps
    // onto wire codes the trigger can translate (409/400/503), so a refused
    // create never surfaces as an unknown-code 503.
    UpsertResult createManualJob(Job& job);
    // UPDATE-branch routing, in two steps: the derived identity first (when the
    // facts name one), then the typed sub_attr candidates for a registration
    // whose namespace is unknown (offline/no attribution). The candidate route
    // discriminates by the subtype's scheduler (schedd) attribution; more than
    // one surviving candidate is reported through `ambiguous`, never guessed.
    // A closing target is reported through `closing`; no target falls through
    // to CREATE.
    struct ManualTarget {
        enum class Kind { None, Closing, Ambiguous, Update };
        Kind kind = Kind::None;
        JobRecord record;
        std::vector<std::string> candidates;
        std::string message;
    };
    ManualTarget manualUpdateTarget(const Job& job,
                                    const std::optional<NativeSelector>& selector);
    // UPDATE changes selection only: collectors union into the manual list;
    // metadata and request members are ignored after namespace validation.
    nlohmann::json updateManualInstance(const JobRecord& target, const Job& job,
                                        const nlohmann::json& request);
    // System JobID 0: in-memory only, never stored, never mutated.
    Job sentinel_job_;

    // The single durable representation. Everything below reads state through
    // it; the registry never duplicates a JobRecord field.
    std::unique_ptr<JobStore> store_;

    // The single source of truth for inventoried records (moved from the
    // store): every other view (scheduler lanes, kernel maps, RPC payloads) is
    // a projection or a snapshot copy. Mutations publish under it briefly
    // after their durable commit; reads take it shared.
    mutable std::shared_mutex records_mtx_;
    std::unordered_map<uint64_t, JobRecord> records_;
    // In-memory mirror of the durable `i:` identity index (same key layout).
    std::unordered_map<std::string, uint64_t> identity_index_;
    // Serializes business mutations (the record transaction lock, the role the
    // store's upsert mutex/db transaction used to play). Every write path
    // holds it across read -> decide -> commit -> publish, so a decided
    // revision cannot move before its commit lands. It is NOT held over
    // reader-facing records_mtx_ sections during the durable commit itself:
    // the collection runtime keeps reading the previous image while the store
    // writes, exactly as it did when publication happened after the store
    // call — the runtime converges in that window (bind -> attach before a
    // removal's drain).
    mutable std::mutex mutation_mtx_;

    // Registry-side record-map accessors and publish points. Query helpers
    // return snapshot copies.
    std::optional<JobRecord> ownRecord(uint64_t id) const;
    std::vector<JobRecord> ownRecords() const;
    std::vector<JobRecord> ownFindActive(
        const std::function<bool(const Job&)>& pred) const;
    bool ownHasActive(uint64_t id) const;
    size_t ownClosingCount() const;
    std::optional<JobRecord> ownRecordByKey(const SchedulerKey& key) const;
    CollectionSelection ownSelection(uint64_t id) const;
    void publishRecord(JobRecord record);
    void eraseRecordLocked(uint64_t id);   // caller holds records_mtx_ exclusively

    // One record transaction: the single durable write path for record state.
    //
    // A caller builds one atomic JobStore batch together with the memory images
    // that batch produces, then commits. Publication runs only after the store
    // accepted the batch, so a failed write can never leave the registry
    // claiming a state the store refused. The caller holds mutation_mtx_ across
    // build -> commit -> publish; records_mtx_ is taken only inside commit, so
    // the collection runtime keeps reading the previous image while the store
    // writes.
    class RecordTxn;
    void applyRecovered(const std::vector<JobRecord>& adopted,
                        const std::vector<uint64_t>& removed);
    // How one merge treats provenance and the liveness verdict. Observation: a
    // discovery observation — the record's source becomes Discovery and the
    // positive read re-verifies it. Refresh: the periodic eager update of an
    // existing record — execution evidence moves, provenance and the liveness
    // verdict stay with their owners.
    enum class MergeKind { Observation, Refresh };
    // Execution refresh against an existing creation metadata snapshot.
    // Pure decision: no durable write and no publication.
    UpsertResult mergeDiscoveredJob(const JobRecord& stored, const Job& job,
                                  const SchedulerKey& key, uint64_t job_id,
                                  int64_t observed_at, JobRecord& updated_out,
                                  MergeKind kind = MergeKind::Observation);
    // Interpretation-only adapter access. Production assembles the complete
    // adapter set in buildAdapters(), so this resolves through adapters_; the
    // lazy cached build is the test-wiring / late-registration fallback only
    // (never started, facts interpretation only). nullptr for a name no
    // registered adapter answers to.
    ISchedulerAdapter* namespaceAdapterOf(const Job& job) const;
    ISchedulerAdapter* namespaceAdapterNamed(const std::string& name) const;
    mutable std::mutex namespace_adapter_mtx_;
    mutable std::map<std::string, std::unique_ptr<ISchedulerAdapter>>
        namespace_adapters_;
    // The typed-facts duplicate screen. The adapter owns which of its facts
    // identify a scheduled job; an adapter-less row keeps the generic subtype
    // screen only.
    SubAttrMatcher duplicateMatcherOf(const Job& job) const;
    enum class SelectionOperation {
        SetManual,   // replace the manual selection; [] is a persistent stop
        ResetAuto,   // clear manual, keep the automatic selection
        SetAuto,     // record an automatic decision; manual always wins
    };
    struct SelectionMutation {
        uint64_t job_id = 0;
        SelectionOperation operation = SelectionOperation::SetManual;
        // SetManual: manual names. SetAuto: automatic names. ResetAuto: empty.
        std::vector<std::string> collectors;
        std::string automatic_source;                // SetAuto only
        std::optional<uint64_t> expected_revision;   // checked in-transaction
    };
    // Re-read, Closing/CAS checks, transform, one unsynced batch and publication
    // share mutation_mtx_. Failed writes publish and report no accepted state
    // (disk may have landed; recovery owns that uncertainty). Success wakes and
    // samples the runtime only after releasing the registry locks, even for a
    // valid no-op. Targets remain aligned with mutations.
    CollectionOutcome executeSelectionMutations(
        const std::vector<SelectionMutation>& mutations);
    // Restore validation (moved from the store): positive identity checks with
    // the admission probe; adjustments are memory-only, the next durable write
    // of a row persists whatever is current.
    void validateRestoredExecutions(std::vector<JobRecord>& rows);

    // Injected at composition time. Never held across a call into it.
    mutable std::mutex                     runtime_mtx_;
    ICollectionRuntime*                    collection_runtime_ = nullptr;
    // Guards the installed exit hint observer.
    mutable std::mutex                     exit_observer_mtx_;
    std::function<void(pid_t)>             exit_observer_;

    // Owned discovery/maintenance stack (construction order: store -> manager
    // -> heartbeat -> attribution -> RPC; destruction runs deinit()).
    // One adapter per registered scheduler, built from the resolved
    // configuration before the store restores rows; the registry never names a
    // scheduler. Only the `auto_discovery_types` subset is driven as a
    // discovery source.
    std::vector<std::unique_ptr<ISchedulerAdapter>> adapters_;

    // --- registry heartbeat state (guarded by hb_mtx_ unless noted) --------
    // Discovery registration is synchronous on the producing threads: there is
    // no observation queue and no drain step, so a discovered job is durable
    // before the watcher continues with the next event.
    struct AbsenceEntry {
        ProcessIdentity manager;
        int complete_absences = 0;
        std::int64_t last_seen_ms = 0;
        // Monotonic observation sequence of the last accepted observation. It
        // tells a later pass whether the record was observed after that pass
        // began, which wall-clock milliseconds cannot (coarse clock, short
        // passes).
        std::uint64_t last_seen_seq = 0;
    };
    mutable std::mutex hb_mtx_;
    std::condition_variable hb_cv_;
    std::map<uint64_t, AbsenceEntry> absences_;
    std::set<uint64_t> suspect_ids_;
    std::map<std::string, ProcessIdentity> ended_managers_;
    std::vector<std::pair<uint64_t, ProcessIdentity>> closing_managers_;
    bool hb_admitted_ = false;     // false after stopDiscovery()
    bool hb_running_ = false;      // worker thread alive
    bool hb_stopping_ = false;
    // Set by requestRescan() to cut the maintenance wait short (cooldown still
    // limits how often a forced scan actually runs).
    bool hb_wake_ = false;
    bool degraded_ = false;
    bool incomplete_drain_ = false;
    bool status_published_ = false;
    bool last_scan_complete_ = false;
    std::int64_t last_scan_start_ms_ = 0;
    std::int64_t last_scan_success_ms_ = 0;
    std::int64_t last_forced_scan_ms_ = 0;
    std::string hb_last_error_;
    std::thread::id hb_worker_id_;
    std::uint32_t reconcile_interval_sec_ = 30;
    std::uint64_t hb_events_received_ = 0;
    std::uint64_t hb_rescan_requests_ = 0;
    std::uint64_t hb_obsolete_hints_ = 0;
    std::uint64_t hb_incomplete_observations_ = 0;
    std::uint64_t hb_upsert_conflicts_ = 0;
    std::uint64_t hb_not_admitted_capture_all_off_ = 0;
    std::uint64_t hb_not_admitted_by_rule_ = 0;
    std::uint64_t hb_admission_errors_ = 0;
    std::uint64_t hb_decision_repairs_ = 0;
    std::uint64_t hb_decision_failures_ = 0;
    // Eager refreshes this stack could not apply (store write failure or a
    // refused manager/identity merge). Observability only: a record that
    // failed to refresh is retried next pass.
    std::uint64_t hb_refresh_failures_ = 0;
    std::uint64_t hb_removals_ = 0;
    std::uint64_t hb_passes_ = 0;
    std::uint64_t hb_stop_errors_ = 0;
    // Bumped by every noteHeartbeatError(); a pass uses it to tell whether an
    // error was raised while it was running (and therefore must stay visible
    // instead of being cleared by its own "scan complete" branch).
    std::uint64_t hb_error_epoch_ = 0;
    // Bumped by every accepted observation. A pass captures it before scanning
    // so registrations that land during the pass are not read as absences.
    std::uint64_t observation_seq_ = 0;
    std::vector<std::pair<std::string, bool>> source_started_;  // name, started
    std::vector<std::string> source_start_notes_;
    // Serializes passes (worker vs on-demand reconciles). Never held while
    // taking hb_mtx_.
    std::mutex pass_mtx_;
    std::unique_ptr<std::thread> heartbeat_thread_;
    // Liveness probes (production or test-injected). Set once during init,
    // read during passes.
    ProcessLivenessProbe process_probe_;
    CgroupMembershipProbe cgroup_probe_;
    // Every discovery source the heartbeat drives (adapters or test sources),
    // populated by armDiscovery().
    std::vector<std::pair<std::string, IDiscoverySource*>> hb_sources_;
    // The scheduler names whose discovery watcher is armed
    // (job_registry_config.auto_discovery_types). Status/observability only;
    // the adapter set itself is independent of it.
    std::vector<std::string> auto_discovery_types_;
    DiscoverySink heartbeat_sink_;
    // Control-plane validation. Guarded because tests install it after
    // construction; the command path copies the callable under the lock and
    // never calls it while holding a store lock.
    mutable std::mutex selection_validation_mtx_;
    std::function<bool(const std::string&)> collector_validator_;
    // Automatic collector selection keyed by adapter name, armed from the
    // resolved maintenance configuration. A disabled entry selects nothing.
    std::map<std::string, AutoCollectorPolicy> auto_policies_;
    // Admission gate state: the blanket-registration switch and the
    // per-adapter auto_add tri-state, armed from the resolved maintenance
    // configuration in armDiscovery().
    bool capture_all_ = true;
    std::map<std::string, AutoAdd> auto_add_states_;
    bool deinit_done_{false};
    bool deinit_ok_{true};

    // Heartbeat internals.
    void startHeartbeat();
    void heartbeatLoop();
    // The maintenance pass: the phases below, named in order. Everything the
    // phases exchange travels in the PassRun context, so the pass reads as a
    // pipeline instead of one procedure that owns both flow and work.
    bool runPass(bool do_scan, bool wait, bool refresh_facts = true);

    // Result of the one eager refresh of a record: the evidence the removal
    // rule consumes, plus whether any pid identity survived the merge (a
    // record with no pid identity evidence at all is "retry later", not "all dead").
    struct RefreshedEvidence {
        RefreshResult evidence;
        bool has_pid_identity_evidence = false;
    };
    // Stable map key for one manager identity: the triple is what makes a pid
    // a process, so a recycled number cannot alias an old manager. A tuple
    // keeps the per-manager lookup allocation-free.
    using ManagerKey = std::tuple<pid_t, std::uint64_t, std::string>;
    using ManagerIndex = std::map<ManagerKey, std::uint64_t>;
    static ManagerKey managerKey(const ProcessIdentity& identity) {
        return {identity.pid, identity.start_ticks, identity.boot_id};
    }
    // One pass' inputs and ledger: created here, updated by the phases, dead
    // when the pass returns.
    struct PassRun {
        bool enumerate = false;         // this pass enumerated discovery sources
        bool refresh = false;           // this pass re-read the facts of records
        std::uint64_t error_epoch = 0;  // noteHeartbeatError() epoch at entry
        std::uint64_t scan_seq = 0;     // observation sequence at entry
        std::int64_t started_ms = 0;
        std::set<uint64_t> observed;
        std::set<uint64_t> created;
        std::map<std::string, bool> source_complete;
        std::map<uint64_t, RefreshedEvidence> refreshed;
        bool complete = true;
        std::string error;
    };
    // Phase 0: admits the pass (admitted, not stopping) and snapshots the
    // counters the later phases compare against. The caller holds pass_mtx_.
    std::optional<PassRun> beginPass(bool do_scan, bool refresh_facts);
    // Phase 1: enumerates every source's managers, registers the untracked
    // ones, and records which sources covered their inputs.
    void discoverManagers(PassRun& pass);
    // Phase 1b: one untracked manager — observe it, run the admission gate and
    // the upsert, and remember the record so a duplicate entry costs no second
    // read.
    void observeManager(const std::string& source_name, IDiscoverySource& source,
                        const ProcessIdentity& manager, PassRun& pass,
                        ManagerIndex& manager_index);
    // Phase 1c: folds one source's coverage into the pass' completeness and its
    // per-source absence eligibility.
    void noteSourceCoverage(PassRun& pass, const std::string& name,
                            const ManagerSet& managers);
    // Phase 2: re-reads every active record through the same evidence reader
    // and applies it through the same merge.
    void refreshActiveRecords(PassRun& pass);
    // Phase 3: the pass' single fsynced batch.
    void checkpointPass();
    // Phase 4: counters, scan timestamps and the degraded/error verdict.
    void publishPassStatus(const PassRun& pass);
    // Registers the observation of a manager the pass did not track. Runs the
    // creation admission gate (only for unknown identities), the
    // obsolete-manager screen and the store upsert; `observed` receives the
    // record id and `created` receives it only for the CREATE disposition (a
    // fresh record already carries the observe() image and is skipped by this
    // pass' update). The result is the caller's acceptance signal: only an
    // accepted observation may mark the manager as tracked (an identity
    // conflict must keep it unknown so the next pass retries it).
    UpsertResult processObservation(const Job& job, std::set<uint64_t>& observed,
                                    std::set<uint64_t>& created);
    // Liveness evidence for a record with no scheduler adapter (Common /
    // open-world manual rows): the manager identity when it has one and the
    // recorded pid identities, both through the injected probes. There is
    // no cgroup scope to read without an adapter.
    RefreshResult probeRecordEvidence(const Job& job) const;
    // Member half of the liveness evidence: one probe per recorded pid identity,
    // stopping at the first live one. Shared by the legacy path above and by
    // probeSurvivingPidIdentities below.
    void probePidIdentities(const Job& job, RefreshResult& evidence) const;
    // Applies one refreshed image to the current record under mutation_mtx_:
    // the execution-evidence merge, the single durable batch and the runtime
    // wake. Metadata and identity are never rewritten (the record is a creation
    // snapshot). Nullopt means the record moved on (archived, closing, newer
    // facts, refused merge) and the evidence must not be used for removal.
    std::optional<RefreshedEvidence> applyRefreshedRecord(const JobRecord& snapshot,
                                                          Job refreshed,
                                                          RefreshResult evidence);
    // The merge keeps the stored pid identity set when a refresh produced an empty,
    // non-authoritative read (a failed legacy/host read, not a confirmed empty
    // scope). The removal evidence must describe those same survivors:
    // probing only the refreshed image would read "all dead" and remove an
    // execution whose child is still alive.
    void probeSurvivingPidIdentities(const Job& stored, const Job& refreshed,
                                     RefreshResult& evidence) const;
    // Registration admission gate (capture_all=false). Returns
    // nullopt to drop the observation (counted by the caller-visible
    // hb_not_admitted_* counters); otherwise the register decision
    // and, when the type's policy was pre-evaluated, the initial collector
    // list. Evaluated outside every registry lock.
    std::optional<InitialDecision> admissionDecision(const Job& job);
    void requestRescan(const std::string& reason);
    bool rescanRequested() const;
    // Re-evaluates the automatic decision for live instances that never got a
    // durable one. Bounded per pass so the heartbeat keeps its cadence.
    void repairMissingInitialDecisions();
    // Absence accounting plus the removal verdict. The pass context carries
    // both: `scan_seq` is the observation sequence captured before the pass
    // began (a registration accepted on another thread during the pass carries
    // a greater sequence and therefore counts as observed), and `refreshed`
    // carries this pass' evidence per record, so no record is probed twice. A
    // targeted pass (`enumerate` false) has no authoritative coverage and never
    // counts an absence.
    void evaluateRemovals(const PassRun& pass);
    bool removeIfConfirmed(const JobRecord& record, const std::string& reason);
    void rememberEnded(const JobRecord& record);
    bool isObsolete(const SchedulerKey& key, const ProcessIdentity& gen);
    void publishStatusProvider();
    void retractStatusProvider();
    void noteHeartbeatError(const std::string& error);
    nlohmann::json heartbeatStatusJson() const;
    void installHeartbeatSink();
    std::int64_t clockNow() const;

    // Re-arms a drain for a restored Closing record and retries a cleanup that
    // failed after the runtime reported completion.
    void reconcileClosingRemovals();
    // Destructive post-drain step, in this order only: the record is already
    // closing (durable), then kernel ownership is removed with owner checks,
    // then the archive releases the identity index.
    bool cleanupThenArchive(uint64_t jobID);
    // Archival entry point for a record that is already closing. `allow_recovery`
    // is false on the shutdown pass: a single-pass shutdown never retries a
    // write whose outcome is unknown.
    bool finalizeClosingRemoval(uint64_t jobID, bool allow_recovery);
    // Verify every row whose last write lost certainty; false when the store
    // itself cannot recover.
    bool recoverStoreIfNeeded(std::string& error);
    // /proc + scheduler metadata refresh of one job (no persistence).
    void update_job(Job& job);

    // Kernel-side attribution capability (pid2job/cgroup2job). Owned here,
    // driven by lifecycle calls and its own reconcile thread.
    void init_pid_tracker();
    void stop_pid_tracker();
    std::unique_ptr<KernelAttribution> attribution_;
    // 内核登记入口：KernelAttribution 的生命周期调用薄封装（Added/Removed）。
    // Serializes activation against removal so a stale snapshot cannot
    // revive a closing owner.
    bool sync_job_to_kernel(const Job& job);
    // Discovery/startup repair: (re)installs the kernel projection of one
    // instance that is already active. The kernel component owns no queryable
    // "already installed" state, so this is an idempotent re-seed.
    void ensure_kernel_owner(const Job& job);
    std::mutex kernel_owner_mtx_;
    // Removes kernel ownership. The injected test seam may report a failure;
    // the production KernelAttribution API cannot, so it always reports clean.
    void remove_job_from_kernel(const Job& job, bool& ok, std::string& error);
    std::mutex kernel_cleanup_mtx_;
    std::function<bool(uint64_t, const Job&, std::string&)> kernel_cleanup_for_test_;
    // Serializes cleanup+archive so two retry paths cannot clean the same
    // closing record concurrently; the archive is idempotent under it.
    std::mutex closing_mtx_;
    // Instances whose runtime drain completed while cleanup/archive has not yet
    // succeeded. Retried by the maintenance pass; never re-armed.
    mutable std::mutex drain_mtx_;
    std::set<uint64_t> drain_complete_;
    // Fact snapshot of what the collection runtime really had attached when a
    // removal was accepted, taken at the closing transition while the
    // runtime still holds the instance. Runtime state only: never persisted,
    // lost on restart (a restored closing falls back to the durable selection
    // at archive time). Decides archive-versus-discard.
    mutable std::mutex removal_applied_mtx_;
    std::unordered_map<uint64_t, std::vector<std::string>> removal_applied_;
    // KernelAttribution::Host — the registry-side projection updates.
    bool attributionJobExists(uint64_t job_id) override;
    void attributionJobAddPid(uint64_t job_id, pid_t pid) override;
    void attributionJobRemovePid(uint64_t job_id, pid_t pid) override;
    void attributionEnumerateJobs(const std::function<void(const Job&)>& fn) override;
    void attributionExitHint(pid_t pid) override;
    std::string attributionCgroupPath(uint64_t job_id) override;
};
