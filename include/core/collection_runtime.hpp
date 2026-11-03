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
// The ownership seam between the durable registry (desired state) and the
// collection runtime (actual state).
//
// Division of responsibility, and nothing more:
//
//   JobRegistry / JobStore   owns facts, the automatic and manual selection and
//                            the durable Open/Closing lifecycle. It never keeps
//                            a copy of what is actually attached.
//   CollectorScheduler       owns which collector instance really holds which
//                            job, the monotonic attach token and the drain. It
//                            reads desired state through IJobRecordSource and
//                            reports a completed stop through
//                            IDrainCompletionSink.
//
// Both directions are explicit pointers injected at composition time. Neither
// side reaches for the other's singleton, and neither keeps a shadow copy of
// the other's state.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "core/job_store.hpp"

// What the collection runtime actually holds for one instance. This is the only
// source of "applied" truth; the durable store holds desired state only.
struct ActualCollectionState {
    // The runtime knows this instance (it has an attachment or a drain).
    bool tracked = false;
    // A stop was accepted and its accepted tail is still being released.
    bool draining = false;
    // Monotonic per attachment. A reattach of the same instance gets a strictly
    // greater token; a completion carrying an older token is rejected.
    uint64_t attach_token = 0;
    // The record revision this attachment converged to (0 = never converged).
    uint64_t last_revision = 0;
    // Collectors that really hold the job right now.
    std::vector<std::string> collectors;
    // Last application failure, when the desired set is not fully in effect.
    std::string error;
};

// Registry -> runtime.
class ICollectionRuntime {
public:
    virtual ~ICollectionRuntime() = default;
    // Coalesced "reconcile soon". Only wakes the runtime's own worker: it must
    // not block, run a collector or take a registry lock.
    virtual void wake() = 0;
    // Begin (or continue) stopping one instance. Returns true when the runtime
    // took ownership of the drain; it then calls
    // IDrainCompletionSink::onDrained exactly once for this instance. False
    // means the runtime cannot prove the stopping boundary (an unsupported
    // cutoff capability): the caller must not archive and must not treat the
    // stop as complete.
    virtual bool beginDrain(uint64_t internal_id) = 0;
    // Actual state for status reporting. Honest about an untracked instance.
    virtual ActualCollectionState actual(uint64_t internal_id) = 0;
};

// Runtime -> registry.
class IDrainCompletionSink {
public:
    virtual ~IDrainCompletionSink() = default;
    // The runtime proved the accepted tail was produced and released every
    // collector for this instance. The registry may now run checked kernel
    // cleanup and archive. Idempotent: a repeated report for an instance that
    // is no longer Closing is ignored.
    virtual void onDrained(uint64_t internal_id) = 0;
};

// Runtime -> registry read surface. The runtime converges to what the durable
// records say; it keeps no record mirror of its own.
//
// Both calls report availability separately from content: "the store cannot be
// read" and "there are no records" are different facts. A runtime that treats
// an unavailable source as an empty inventory detaches every collector it
// holds, which is exactly the blanket-detach bug this seam exists to prevent.
class IJobRecordSource {
public:
    virtual ~IJobRecordSource() = default;
    // Every live record, Closing included (the runtime must drain those).
    // Returns false when the desired state cannot be read right now; `out` is
    // then unchanged/empty and the caller must keep its current attachments.
    virtual bool runtimeRecords(std::vector<JobRecord>& out) const = 0;
    // false = the source is unavailable (do not conclude anything).
    // true with an empty `out` = the instance is not tracked by the registry.
    virtual bool runtimeRecord(uint64_t internal_id,
                               std::optional<JobRecord>& out) const = 0;
};
