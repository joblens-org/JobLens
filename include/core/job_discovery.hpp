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
// Discovery contract (renamed from job_observation when registration became a
// Job-based synchronous path; plan §5.1). The header owns source, scan, the
// proc/cgroup readers and the clock — it must never depend on LevelDB, and the
// registration result type lives in its own contract header.
//
//   * scan()      - one synchronous source pass, returns ScanResult
//   * health()    - SourceHealth of that source (initializing/healthy/degraded)
//   * jobs        - discovered Jobs (typed facts + execution evidence group)
//                   the controller admits through JobRegistry upserts; never a
//                   JobRegistry dependency of its own
//
// Two rules are structural rather than conventional:
//
//   1. A discovered Job either is ready (resolvable identity plus a complete
//      execution generation) or is explicitly incomplete with a reason on the
//      execution group. There is no "best effort" middle state, and an
//      incomplete discovery is not eligible for automatic collection or for
//      retirement decisions.
//   2. A failed or incomplete scan is never interpreted as "everything else
//      ended". ScanResult::complete says whether the pass covered the
//      configured sources; a consumer must check it before treating absence as
//      termination evidence.
#pragma once

#include "core/collector_type.h"
#include "core/job_identity.hpp"
#include "core/registration_contract.hpp"

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
    std::string cluster_name;
};

// ---------------------------------------------------------------------------
// Discovered Job readiness
// ---------------------------------------------------------------------------

// One discovered job IS the registration unit: typed facts on the Job itself
// plus one `Job::execution` evidence group. There is no second envelope — the
// watcher fills the Job where it parses its sources, and the registry derives
// the selector/identity from the Job on admission (selector_of/canonicalize).
//
// Readiness is the old two-level rule: the logical identity resolves AND the
// execution generation is complete (boot id + manager pid + manager start
// ticks). An unresolvable identity stays out of the identity index and is
// retried on the next observation; an incomplete generation is an unverified
// execution, never a guess.
bool discovery_job_ready(const Job& job);

// Fills `job.execution->incomplete_reason` from the identity/generation checks
// plus adapter-level source reasons (unreadable ad, missing env var). Ensures
// the execution group exists so the reasons survive with the evidence.
void finalize_discovery_job(Job& job, std::vector<std::string> source_reasons = {});

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
    // The discovered Jobs: typed facts plus the execution evidence group,
    // built where the source parsed its inputs. JobID, JobPIDs and
    // CollectorNames stay empty: they are store/intent owned.
    std::vector<Job> jobs;
    // True when the pass covered every configured source without an I/O or
    // configuration error. Incomplete *discoveries* (missing ad, unresolved
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

// Receives the discovered Jobs a source's own periodic scan produced,
// delivered on that source's polling thread.
using ObservationSink = std::function<void(const Job&)>;

// Synchronous single-job registration entry (plan §3/§5.1): registers one
// discovered Job and returns the result. It runs on the caller's
// thread (the source's poll thread), never on a worker.
using JobSink = std::function<UpsertResult(const Job&)>;

// Receives discovery output from a source.
struct DiscoverySink {
    // Event path: register one discovered Job immediately on the poll thread. The
    // store writes it without a fence, so the callback never waits for
    // persistence. Empty when this core has no inventory runtime.
    JobSink on_job;
    // Scan path: hand the source's own periodic scan results to the discovery
    // sink, still on the polling thread.
    ObservationSink on_observation;
};

// One discovery source for one scheduler type. Implemented by the
// watchers; the registry drives sources through this interface and owns no
// adapter details. The adapter NAME (see name()) is the scheduler identity —
// there is no per-scheduler enum to implement.
class IDiscoverySource {
public:
    virtual ~IDiscoverySource() = default;

    virtual const std::string& name() const = 0;

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
    virtual std::optional<Job> observeForHint(pid_t hint_pid) = 0;
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

// Process-monotonic read order, assigned before a builder starts reading. The
// store uses it so an older scan can never overwrite a newer event (plan §4.1).
std::uint64_t next_discovery_read_order();

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
