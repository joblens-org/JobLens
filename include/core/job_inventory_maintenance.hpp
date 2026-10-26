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
// Inventory maintenance controller (doc/job_inventory_collection_decoupling_plan.md
// sections 5 and 8, task 3).
//
// Responsibilities, in the order the plan states them:
//
//   * 5.1 startup  - install the observation sinks, start the discovery
//                    sources, run the initial full scan, merge the buffered
//                    discovery events and the scan through the same
//                    generation-aware upsert path, publish a coherent status.
//   * 5.2 reconcile- periodic full scans (default 30 s) that upsert observed
//                    executions, refresh facts/processes and mark absent ones
//                    suspect. A failed or incomplete pass never counts as
//                    absence.
//   * 5.3 retire   - positive termination evidence (validated manager ended and
//                    the execution cgroup has no live members) or two
//                    consecutive complete-scan absences with no live
//                    generation-matching member. Every retirement is guarded by
//                    an ExecutionGeneration comparison, so a late decision can
//                    never remove a newer execution's ownership.
//
// Deliberate boundaries:
//
//   * No singleton. The service lifecycle owns exactly one controller
//     explicitly (plan 5.1: inventory start/stop is not a side effect of
//     constructing CollectorScheduler), and tests construct as many as they
//     need.
//   * No watcher, eBPF or scheduler dependency. Discovery enters through
//     InventorySource (scan/health/start/stop closure bundle) and process
//     liveness through ProcessLivenessProbe / CgroupMembershipProbe, so the
//     deterministic harness drives fake sources and fake probes. Production
//     wiring binds the scheduler discovery adapters and the /proc probes.
//   * No CollectorScheduler call and no lifecycle callback registration: this
//     component only maintains inventory facts. Binding-driven collection is
//     task 4, automatic policy is task 5.
//   * Event producers (fork/exit/discovery callbacks) only enqueue a bounded
//     hint. Queue overflow counts a drop, degrades the sources and requests an
//     authoritative rescan; it never blocks the producing callback.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <unordered_set>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/job_observation.hpp"
#include "core/job_registry.hpp"

// ---------------------------------------------------------------------------
// Configuration contract (plan section 8)
// ---------------------------------------------------------------------------

enum class InventoryType {
    Condor = 0,
    Slurm,
};

const char* to_string(InventoryType type);
std::optional<InventoryType> inventory_type_from_string(const std::string& raw);

// Resolved settings for one scheduler type. `inventory` mirrors
// job_registry_config.inventory.types membership; `auto_collect` is the
// field-specific resolution described in section 8:
//
//   explicit <type>_job_watcher.auto_collect  wins
//   otherwise   job_registry_config.auto_add_<type>job
//   otherwise   false
//
// Supplying a type in inventory.types never erases an explicit or legacy
// auto-collection choice.
struct InventoryTypeSettings {
    bool inventory = false;
    bool auto_collect = false;
    bool auto_collect_explicit = false;
    bool legacy_auto_collect = false;
    bool use_rules = false;
    std::string rules_dir;
    std::string rules_prefix;
    // Legacy auto_add_* folding: collectors added to every job of this type
    // when automatic collection is on and use_rules is false.
    std::vector<std::string> default_collectors;
    // Explicit cluster namespace fallback for discovery (wins over the
    // environment-derived value, empty = derive from the source).
    std::string cluster_name;
};

struct InventoryMaintenanceConfig {
    // Types that opted into broad discovery. Empty means "inventory off",
    // preserving the pre-decoupling opt-in behavior.
    std::vector<InventoryType> types;
    // Default 30 s; minimum 1 s (validated at startup).
    std::uint32_t reconcile_interval_sec = 30;
    // Always contains both types so the resolved picture is complete.
    std::map<InventoryType, InventoryTypeSettings> settings;

    nlohmann::json to_json() const;
};

// Resolves and validates the configuration from the JSON form of the YAML root
// (what Config::dumpConfig() returns; scalars may therefore be strings).
// Throws std::invalid_argument with a stable message for an unknown inventory
// type, a non-numeric or non-integral reconcile_interval_sec, an interval
// below 1 s, or a malformed section. Startup rejects rather than guessing.
InventoryMaintenanceConfig resolve_inventory_maintenance_config(const nlohmann::json& config);

// ---------------------------------------------------------------------------
// Discovery-source seam
// ---------------------------------------------------------------------------

// Sources are non-owning: JobRegistry::init_inventory constructs exactly one
// watcher per configured scheduler type (implementing IDiscoverySource) and
// hands the pointers to the controller. `start` installs the sink before
// creating its poll thread, exactly like the adapters, so "start before
// callback installation" is unrepresentable here too. `start` returning false
// means "scan-only adapter" (no event source), which is not an error. Any of
// the interface methods may be exercised at any time after construction.

// ---------------------------------------------------------------------------
// Liveness seam (plan 5.2)
// ---------------------------------------------------------------------------

// Unknown is a first-class answer: EPERM, an unreadable procfs entry or an
// unresolvable namespace is never upgraded to "dead". Retirement requires
// positive evidence, so Unknown always blocks it.
enum class LivenessState {
    Unknown = 0,
    Alive,
    Dead,
};

const char* to_string(LivenessState state);

struct LivenessResult {
    LivenessState state = LivenessState::Unknown;
    std::string note;  // e.g. "EPERM reading /proc/1234/stat"
};

// A ProcessIdentity with start_ticks == 0 asks only "does this pid exist and
// is it readable". A complete identity additionally requires the same start
// ticks (and boot id when the probe can see it); reused pids therefore read as
// Dead, not Alive.
struct CgroupMembership {
    bool readable = false;  // false = unknown, never "empty"
    std::vector<pid_t> members;
    std::string error;
};

using ProcessLivenessProbe = std::function<LivenessResult(const ProcessIdentity&)>;
using CgroupMembershipProbe = std::function<CgroupMembership(const std::string&)>;

// Production probes over a (possibly fake) procfs/cgroup tree. Errors are
// classified: ENOENT/ESRCH -> Dead, EACCES/EPERM -> Unknown, anything else ->
// Unknown.
ProcessLivenessProbe make_proc_liveness_probe(const std::string& proc_root = "/proc");
CgroupMembershipProbe make_cgroup_membership_probe();

// ---------------------------------------------------------------------------
// Service lifecycle wiring (plan 5.1 / 5.3)
// ---------------------------------------------------------------------------
//
// Explicit entry points used by src/main.cpp around CollectorScheduler. They
// resolve the configuration, construct the scheduler discovery adapters and the
// controller, and keep them alive for the process lifetime. Implemented in
// src/common/main_utils.cpp.
//
// `start` is idempotent, returns true when inventory is disabled
// (inventory.types is empty) and throws std::invalid_argument on an invalid
// configuration so a service start can reject it instead of guessing.
namespace inventory_service {

bool start();
// Plan 5.3 steps 1-4: stop admission, discovery polling and the maintenance
// producer, completing queued bounded work. Call before draining the scheduler.
void stop_discovery();
// Plan 5.3 step 6: finalize retirements and clear the status provider. Returns
// false when a closing record could not be archived.
bool finalize();

}  // namespace inventory_service

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

class JobInventoryMaintenance {
public:
    struct Options {
        std::size_t queue_capacity = 512;  // bounded hint queue
        // The worker wakes in slices of this length so stop() stays bounded
        // even without an event.
        std::uint32_t wait_slice_ms = 100;
    };

    JobInventoryMaintenance(JobRegistry& registry,
                            InventoryMaintenanceConfig config,
                            std::vector<IDiscoverySource*> sources);
    JobInventoryMaintenance(JobRegistry& registry,
                            InventoryMaintenanceConfig config,
                            std::vector<IDiscoverySource*> sources,
                            Options options);
    ~JobInventoryMaintenance();

    JobInventoryMaintenance(const JobInventoryMaintenance&) = delete;
    JobInventoryMaintenance& operator=(const JobInventoryMaintenance&) = delete;

    // Test seams. Call before start(): passes read them without the mutex.
    void setLivenessProbes(ProcessLivenessProbe process_probe,
                           CgroupMembershipProbe cgroup_probe);
    // Wall-clock injection; also drives the status timestamps.
    void setClock(ScanClock clock);

    // 5.1: installs sinks and starts sources, runs the initial full scan,
    // merges hints buffered during it, publishes the status provider and then
    // starts the reconciliation worker. Returns false only when inventory
    // cannot start at all (store not ready, already running); a source that
    // fails to start leaves the controller degraded but running.
    bool start();

    // 5.3 steps 1-4: stop admission, stop discovery polling, stop the
    // maintenance producer and finish the queued bounded work. Idempotent.
    void stopDiscovery();
    // 5.3 step 6: acknowledge this controller's confirmed retirements. Returns
    // false when a closing record could not be archived (incomplete drain is
    // persisted, never reported as normal completion). Idempotent.
    bool finalize();
    // stopDiscovery() + finalize(). Clears the status provider.
    void stop();

    bool running() const;

    // True once stopDiscovery() ran (admission closed). deinit() uses this to
    // keep the shutdown order without double-stopping.
    bool stoppedDiscovery() const;

    // One full reconciliation pass (scan -> upsert -> hint merge -> retirement).
    // Also used by tests, by the startup path and by on-demand
    // `ensure_registered` refreshes, so all of them run identical code.
    // Reentrant calls return immediately; returns true when a pass ran.
    bool reconcileNow();

    // Event producers. Cheap, bounded and non-blocking with respect to
    // discovery I/O and storage.
    void onObservation(const Observation& observation);  // the ObservationSink
    // One exec event: only the pid. Deduplicated in a bounded FIFO; overflow
    // counts drops, degrades the source and asks for an authoritative rescan.
    void onExecHint(pid_t pid);
    void notifyExit(pid_t pid);
    void notifyRefresh(const std::string& reason);

    nlohmann::json statusJson() const;

    const InventoryMaintenanceConfig& config() const { return config_; }
    bool incompleteDrain() const;

private:
    struct PendingHint {
        enum class Kind {
            Observation = 0,
            Exit,
            Refresh,
        };
        Kind kind = Kind::Observation;
        Observation observation;
        pid_t pid = 0;
        std::string reason;
    };

    struct TrackedInstance {
        ExecutionGeneration generation;
        int complete_absences = 0;
        int64_t last_seen_ms = 0;
    };

    void runWorker();
    // One pass. `do_scan` false is the targeted refresh path used for fork/exit
    // and discovery hints: buffered hints and already-known instances are
    // refreshed without a full source pass. Passes are serialized: `wait` makes
    // the caller block until it can run, otherwise a concurrent pass makes it
    // return false.
    bool runPass(bool do_scan, bool wait);
    // start() phases, in order; each is individually simple and testable.
    bool prepare();          // reset state, build the DiscoverySink
    bool startSources();     // install the sink, start every source
    bool initialPass();      // the first authoritative scan
    void publish();          // status/refresh providers + exit observer
    bool startWorker();      // the reconciliation worker thread
    // stopDiscovery() counterparts.
    void stopSources();
    void retractPublish();   // providers live until finalize (drain visibility)
    void enqueue(PendingHint hint);
    void processObservation(const Observation& observation,
                            std::set<uint64_t>& observed_ids);
    // Targeted observation for one exec-event pid: the first source that can
    // resolve the hint wins; an unresolvable hint is counted, never guessed.
    void processExecHint(pid_t pid, std::set<uint64_t>& observed_ids);
    ObservedExecution to_execution(const Observation& observation) const;
    void evaluateRetirements(const std::set<uint64_t>& observed_ids,
                             const std::map<SchedulerType, bool>& source_complete);
    bool retireIfConfirmed(const InventoryRecord& record, const std::string& reason);
    void rememberEnded(const InventoryRecord& record);
    bool isObsolete(const CanonicalIdentity& identity,
                    const ExecutionGeneration& generation) const;
    void drainHints(std::vector<PendingHint>& out);
    void requestRescan(const std::string& reason);
    // True when a source asked for an authoritative pass (ring-buffer event,
    // queue overflow). Consumed by the worker, never by the producer.
    bool rescanRequested() const;
    // Re-reads process/attribute facts for records no discovery source owns
    // (legacy/manual registrations). Reads used to do this in place; now the
    // maintenance pass is the only place that touches the host for them.
    void refreshLegacyRecords();
    void noteError(const std::string& error);
    std::int64_t clockNow() const;
    std::string stateString() const;
    nlohmann::json sourcesJson() const;
    void pruneTracked(const std::vector<InventoryRecord>& records);
    bool isClosing(uint64_t internal_id) const;

    JobRegistry& registry_;
    InventoryMaintenanceConfig config_;
    // Non-owning: the registry owns exactly one watcher per configured type.
    std::vector<IDiscoverySource*> sources_;
    DiscoverySink sink_;
    Options options_;
    std::vector<bool> source_started_;
    std::vector<std::string> source_start_note_;

    ProcessLivenessProbe process_probe_;
    CgroupMembershipProbe cgroup_probe_;
    ScanClock clock_;

    mutable std::mutex mutex_;
    std::mutex pass_mutex_;  // serialises passes (worker vs explicit reconciles)
    std::condition_variable wake_cv_;
    std::deque<PendingHint> queue_;
    // Exec hints are deduplicated per pid in a bounded FIFO: a job-start burst
    // produces many events for the same execution, and only one targeted
    // observation per pid carries information.
    std::deque<pid_t> exec_hint_queue_;
    std::unordered_set<pid_t> exec_hint_seen_;
    bool exec_hint_overflow_ = false;
    std::map<uint64_t, TrackedInstance> tracked_;
    std::set<uint64_t> suspect_;                        // absent on the last pass
    std::map<std::string, ExecutionGeneration> ended_;   // retired generations
    std::vector<std::pair<uint64_t, ExecutionGeneration>> closing_;

    bool explicit_stop_done_ = false;
    bool admitted_ = false;  // false after stopDiscovery()
    bool running_ = false;   // worker thread started
    bool stopping_ = false;
    bool hint_wakeup_ = false;
    std::thread::id worker_id_;
    bool degraded_ = false;
    bool incomplete_drain_ = false;
    bool status_published_ = false;

    std::int64_t last_scan_start_ms_ = 0;
    std::int64_t last_scan_success_ms_ = 0;
    bool last_scan_complete_ = false;
    std::string last_error_;
    // Throttles hint/event-driven full passes so a fork storm cannot turn into a
    // /proc scan storm.
    std::int64_t last_forced_scan_ms_ = 0;

    std::uint64_t stop_errors_ = 0;
    std::uint64_t events_received_ = 0;
    std::uint64_t events_dropped_ = 0;
    std::uint64_t rescan_requests_ = 0;
    std::uint64_t obsolete_hints_ = 0;
    std::uint64_t unresolved_hints_ = 0;
    std::uint64_t incomplete_observations_ = 0;
    std::uint64_t upsert_conflicts_ = 0;
    std::uint64_t retirements_ = 0;
    std::uint64_t passes_ = 0;

    std::thread worker_;
};
