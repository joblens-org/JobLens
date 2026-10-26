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
// Discovery-adapter observation model for the job-inventory/collection
// decoupling (doc/job_inventory_collection_decoupling_plan.md sections 3.1,
// 4.1 and 5.2).
//
// This header is the contract between the scheduler discovery adapters
// (CondorJobWatcher, SlurmJobWatcher) and the inventory maintenance
// controller:
//
//   * scan()      - one synchronous source pass, returns ScanResult
//   * health()    - SourceHealth of that source (initializing/healthy/degraded)
//   * observations - typed facts the controller resolves into JobRegistry
//                    upserts; never a JobRegistry dependency of its own
//
// Two rules are structural rather than conventional:
//
//   1. An Observation either carries a canonical identity plus a complete
//      execution generation, or it is explicitly incomplete with a reason.
//      There is no "best effort" middle state, and an incomplete observation
//      is not eligible for automatic collection or for retirement decisions.
//   2. A failed or incomplete scan is never interpreted as "everything else
//      ended". ScanResult::complete says whether the pass covered the
//      configured sources; a consumer must check it before treating absence as
//      termination evidence.
#pragma once

#include "core/job_identity.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

// ---------------------------------------------------------------------------
// Clock injection
// ---------------------------------------------------------------------------

// Wall-clock epoch milliseconds. Discovery adapters take a ScanClock so tests
// and the maintenance controller can drive a deterministic clock.
std::int64_t now_epoch_ms();
using ScanClock = std::function<std::int64_t()>;

inline std::int64_t scan_clock_now(const ScanClock& clock)
{
    return clock ? clock() : now_epoch_ms();
}

// ---------------------------------------------------------------------------
// Discovery inputs
// ---------------------------------------------------------------------------

// Where an adapter reads its discovery inputs. The defaults are the real host
// paths; every root is overridable so unprivileged fixtures can present a fake
// procfs/cgroup tree instead of requiring root or live schedulers.
struct DiscoveryEnvironment {
    // procfs root, e.g. "<fixture>/proc". Contains <pid>/{stat,cmdline,environ,cgroup}
    // and sys/kernel/random/boot_id.
    std::string proc_root{"/proc"};
    // cgroup v2 mount root used to turn a procfs "0::/path" line into an
    // absolute cgroup directory ("<cgroup_root>/path/cgroup.procs").
    std::string cgroup_root{"/sys/fs/cgroup"};
    // Optional slurmd spool root ("/var/spool/slurmd"), holding "job<id>"
    // directories the daemon creates per running job on the node. Empty
    // disables the spool source; it can only corroborate presence, never
    // provide process evidence.
    std::string slurm_spool_dir;
    // Resolved scheduler cluster namespaces. Empty means "derive from the
    // source itself" (Condor: COLLECTOR_HOST, Slurm: SLURM_CLUSTER_NAME).
    // Neither value is ever guessed from a native job number.
    std::string condor_cluster_name;
    std::string slurm_cluster_name;
};

// ---------------------------------------------------------------------------
// Observation
// ---------------------------------------------------------------------------

// One candidate local execution seen by a source pass.
//
// `selector` is always filled with whatever native fields the source really
// provided (never synthesized). `canonical_identity` is present only when the
// scheduler namespace and native id both resolved. `identity_complete` also
// requires a complete execution generation (boot id + manager pid + manager
// start ticks); the two-level split lets a consumer show enrichment progress
// without ever indexing a partial identity.
struct Observation {
    SchedulerType scheduler_type = SchedulerType::Unknown;

    NativeSelector selector;
    std::optional<CanonicalIdentity> canonical_identity;
    bool identity_complete = false;
    // Human-readable list of what is missing, joined with "; ". Empty exactly
    // when identity_complete is true.
    std::string incomplete_reason;

    // Manager process of the execution (Condor: condor_starter, Slurm:
    // slurmstepd) plus the execution cgroup as corroborating evidence.
    // cgroup_identity is empty when the cgroup is unavailable.
    ExecutionGeneration generation;
    // Absolute cgroup directory discovered for the execution ("" = unknown).
    std::string cgroup_path;

    // Member processes of the execution, manager excluded. A member whose
    // start ticks could not be read is kept with start_ticks == 0 so the
    // missing evidence stays visible; members_complete is false in that case.
    std::vector<ProcessIdentity> members;
    bool members_complete = true;

    // Scheduler metadata for the registry record.
    std::string owner;
    std::string job_name;
    std::string partition;
    std::string scheduler_host;

    // Time the source data for this observation was read.
    std::int64_t source_timestamp_ms = 0;
    // Per-observation notes, e.g. "cgroup membership unreadable".
    std::vector<std::string> notes;
};

// ---------------------------------------------------------------------------
// Scan result
// ---------------------------------------------------------------------------

// Counts are all "inputs of this source pass", not "jobs in the registry".
struct ScanCounts {
    // Manager-level discovery inputs examined (one Condor starter, one
    // slurmstepd, one spool directory).
    std::size_t candidates = 0;
    std::size_t complete = 0;
    std::size_t incomplete = 0;
    // Process or spool entries that could not be read or attributed at all
    // (permission denied, exited mid-scan, not a job<id> directory). They never
    // become observations and never stand in for the observed absence of a
    // known execution.
    std::size_t skipped = 0;

    std::size_t observed() const { return complete + incomplete; }
};

struct ScanResult {
    std::vector<Observation> observations;
    // True when the pass covered every configured source without an I/O or
    // configuration error. Incomplete *observations* (missing ad, unresolved
    // namespace) do not make a pass incomplete: they were still observed.
    // Consumers must never read absent jobs out of a pass with complete=false.
    bool complete = false;
    std::string error;
    ScanCounts counts;
    // Source-level notes: skipped sources, unavailable eBPF event source, ...
    std::vector<std::string> notes;
    std::int64_t started_at_ms = 0;
    std::int64_t finished_at_ms = 0;

    bool ok() const { return error.empty(); }
};

// ---------------------------------------------------------------------------
// Source health
// ---------------------------------------------------------------------------

// Initializing: no source pass has finished yet (startup barrier; section 5.1
// keeps inventory health here until the first pass completes or degrades).
// Healthy: the last pass covered its sources. Degraded: the last pass failed
// or only covered part of its sources; consumers keep retrying and must not
// treat absence as termination.
enum class SourceState {
    Initializing = 0,
    Healthy,
    Degraded,
};

const char* to_string(SourceState state);

struct SourceHealth {
    SourceState state = SourceState::Initializing;
    // True while the adapter owns a running poll/scan thread.
    bool polling = false;
    // Epoch ms; 0 means "never".
    std::int64_t last_scan_start_ms = 0;
    std::int64_t last_scan_success_ms = 0;
    // Most recent failure text. Kept after recovery for diagnosis; `state` is
    // authoritative.
    std::string last_error;
    // Most recent source-level note (e.g. "eBPF event source unavailable").
    std::string last_note;
    // A consumer asked for an out-of-band pass; cleared when one starts.
    bool rescan_requested = false;
    std::size_t consecutive_failures = 0;
};

// ---------------------------------------------------------------------------
// Adapter runtime options and source interface
// ---------------------------------------------------------------------------

// A discovery adapter never reads Config, never loads eBPF and never starts a
// thread in its constructor: the assembly layer (JobRegistry::init_inventory)
// builds these options explicitly and start() begins operation.
struct DiscoveryOptions {
    // Load the eBPF ring buffer used for incremental discovery hints. It is
    // always started by start(), never by the constructor.
    bool enable_event_source = true;
    // Periodic scan tick of the adapter's poll thread; 0 disables it (the
    // maintenance controller owns pacing and runs the authoritative passes).
    std::uint32_t scan_interval_ms = 0;

    DiscoveryEnvironment discovery;
    ScanClock clock;
};

// Receives observations produced by a scan or a targeted probe. Cheap and
// non-blocking: the plan forbids slow policy/storage work on the producing
// callback path, so the sink belongs to a queue, not to a repository.
using ObservationSink = std::function<void(const Observation&)>;

// Receives discovery output from a source. Both callbacks run on the source's
// poll thread or the maintenance worker and must stay cheap: no policy
// evaluation, no storage writes, no slow disk work.
struct DiscoverySink {
    // Observations produced by a scan (or a periodic scan tick).
    ObservationSink on_observation;
    // An exec event for a process whose parent is the execution manager. Only
    // the pid is handed over; building an observation is the consumer's job.
    std::function<void(pid_t)> on_exec_hint;
};

// One discovery source for one scheduler type. Implemented by the
// Condor/Slurm watchers; the maintenance controller drives sources through
// this interface and owns no adapter details.
class IDiscoverySource {
public:
    virtual ~IDiscoverySource() = default;

    virtual const std::string& name() const = 0;
    virtual SchedulerType schedulerType() const = 0;

    // Installs the sink and then starts polling (the sink exists before any
    // callback can run). Returns false when the adapter has nothing to poll
    // for (no event source and no scan interval); the sink stays installed and
    // scan()/observeForHint() keep working.
    virtual bool start(const DiscoverySink& sink) = 0;
    // Idempotent: stops and joins the poll thread, releases the event source.
    virtual void stop() = 0;
    // One synchronous authoritative source pass.
    virtual ScanResult scan() = 0;
    virtual SourceHealth health() const = 0;
    // Ask for an out-of-band authoritative pass; consumed by the owner.
    virtual void requestRescan() = 0;
    // Targeted observation for one exec-event pid. The BPF filter guarantees
    // the parent is the execution manager, so the manager is the ppid; only
    // this execution's files are read (O(job), not O(node)). Returns nullopt
    // when the hint cannot be resolved (process gone, parent is not the
    // expected manager); the caller then falls back to a full scan.
    virtual std::optional<Observation> observeForHint(pid_t hint_pid) = 0;
};

// ---------------------------------------------------------------------------
// Observation accumulation
// ---------------------------------------------------------------------------

// Merges per-candidate observations into one observation per execution
// generation of a canonical identity. Complete observations with the same
// identity AND the same manager identity are merged (member processes, cgroup
// path, notes union). Incomplete observations are never merged, and two
// different managers of one logical job stay separate so the registry can
// reject the second generation explicitly instead of seeing a blended record.
class ObservationAccumulator {
public:
    void add(Observation observation);
    std::vector<Observation> take();

private:
    std::vector<Observation> observations_;
    std::map<std::string, std::size_t> by_identity_;
};

// ---------------------------------------------------------------------------
// procfs / cgroup readers
// ---------------------------------------------------------------------------
//
// Same layout and semantics as the /proc-based helpers in common/utils.hpp,
// but every root is explicit so fixtures can be unprivileged. Production
// callers pass DiscoveryEnvironment defaults ("/proc", "/sys/fs/cgroup").

// Numeric pid directories of `proc_root`, ascending. Unreadable roots yield an
// empty vector; callers that need the error call ensure_readable_directory().
std::vector<pid_t> list_proc_pids(const std::string& proc_root);

// Returns false with a message when `path` is missing or not a directory.
bool ensure_readable_directory(const std::string& path, std::string& error);

// NUL-separated cmdline turned into a space-separated string.
bool read_proc_cmdline(const std::string& proc_root, pid_t pid, std::string& out,
                       std::string& error);

// Reads one environment variable of a process. false = environ unreadable.
bool read_proc_env(const std::string& proc_root, pid_t pid, const std::string& key,
                   std::string& out, std::string& error);

std::optional<pid_t> read_proc_ppid(const std::string& proc_root, pid_t pid);
std::optional<std::uint64_t> read_proc_start_ticks(const std::string& proc_root, pid_t pid);

// Boot identity for the host that owns `proc_root`. Empty when unavailable, in
// which case no ProcessIdentity/ExecutionGeneration is complete.
std::string read_boot_id(const std::string& proc_root);

// Absolute cgroup v2 directory of a process. Empty for the root cgroup (not
// execution-scoped evidence) or when the file is missing.
std::string read_proc_cgroup_path(const std::string& proc_root, pid_t pid,
                                  const std::string& cgroup_root);

// PIDs listed in <cgroup_dir>/cgroup.procs. Empty when unreadable.
std::vector<pid_t> read_cgroup_members(const std::string& cgroup_dir);

// Error-reporting variant: false means the membership is unknown ("not empty"
// can never be concluded from it); true means the list is authoritative, an
// empty list included.
bool read_cgroup_members(const std::string& cgroup_dir, std::vector<pid_t>& members,
                         std::string& error);

// Process identity from the fake or real procfs. Fields that cannot be read
// stay zero; ProcessIdentity::complete() reports whether the evidence is
// sufficient, and no caller may upgrade a partial identity.
ProcessIdentity read_process_identity(const std::string& proc_root,
                                      const std::string& boot_id, pid_t pid);

// Fills canonical_identity / identity_complete / incomplete_reason from the
// selector and generation an adapter has already filled in. `source_reasons`
// are adapter-level facts (unreadable ad, missing env var) that make the
// observation incomplete on top of unresolved identity/generation.
void finalize_observation(Observation& observation,
                          std::vector<std::string> source_reasons = {});
