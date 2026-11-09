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
// Liveness probes (thread-free components driven by the registry heartbeat). Production probes read the real procfs/cgroup trees; tests
// install fakes. Error classification is the conservative contract:
//   ENOENT/ESRCH -> Dead, EACCES/EPERM or anything else -> Unknown,
// and Unknown is never death evidence (invariant I3).
#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "core/job_identity.hpp"

enum class LivenessState {
    Alive = 0,
    Dead,
    Unknown,
};

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

// Field 22 of <proc_root>/<pid>/stat (start time in clock ticks), or nullopt
// when it cannot be read.  Used to bind a numeric PID to the process that holds
// it now (manual registration, kernel FORK event): a bare number is not an
// execution identity.
std::optional<std::uint64_t> read_process_start_ticks(pid_t pid,
                                                      const std::string& proc_root = "/proc");

// The boot id this host runs under, or empty when it cannot be read.  One
// implementation for the store, the probes and the admission boundary: a saved
// identity is only replayable when its boot id equals this one.
std::string read_boot_id();

// Production probes over a (possibly fake) procfs/cgroup tree.  The process
// probe also rejects a COMPLETE saved identity whose boot id differs from
// `boot_id` (read once, at factory time, when the argument is empty); a
// bootless identity stays an existence probe, because "pid exists" is not the
// same claim as "this saved execution is live".
ProcessLivenessProbe make_proc_liveness_probe(const std::string& proc_root = "/proc",
                                              std::string boot_id = {});
CgroupMembershipProbe make_cgroup_membership_probe();

// Positive verification of a SAVED ProcessIdentity against this host.  This is
// the only rule an admission/seeding decision may use:
//   * Alive   -- the probe finds the same live process AND both boot ids are
//                known and equal;
//   * Dead    -- the probe finds it gone/reused, or the saved boot id names a
//                different boot (positive proof this is another execution);
//   * Unknown -- the probe could not read the process, or the boot identity of
//                one side is unavailable.  Unknown is never "verified" and
//                never death evidence.

// Verdict for a restored execution generation: only positive evidence counts.
// Stale means the generation names an execution this boot cannot host; Unknown
// means unreadable evidence (neither death nor liveness); Verified means the
// manager process positively matches. Unverified is NOT a lifecycle state.
enum class ExecutionVerdict {
    Verified,
    Stale,
    Unknown,
};

ExecutionVerdict verify_execution(const ExecutionGeneration& generation,
                                  bool execution_observed, const std::string& boot_id,
                                  const ProcessLivenessProbe& probe);

LivenessResult verify_process_identity(const ProcessIdentity& identity,
                                       const std::string& current_boot_id,
                                       const ProcessLivenessProbe& probe);
