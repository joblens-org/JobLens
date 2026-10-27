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
// Liveness probes (v2: thread-free components driven by the registry
// heartbeat). Production probes read the real procfs/cgroup trees; tests
// install fakes. Error classification is the conservative contract:
//   ENOENT/ESRCH -> Dead, EACCES/EPERM or anything else -> Unknown,
// and Unknown is never death evidence (invariant I3).
#pragma once

#include <functional>
#include <string>
#include <vector>

#include "core/job_identity.hpp"

enum class LivenessState {
    Alive = 0,
    Dead,
    Unknown,
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

// Production probes over a (possibly fake) procfs/cgroup tree.
ProcessLivenessProbe make_proc_liveness_probe(const std::string& proc_root = "/proc");
CgroupMembershipProbe make_cgroup_membership_probe();
