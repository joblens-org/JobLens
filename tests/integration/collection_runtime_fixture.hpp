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
// Deterministic stand-in for the collection runtime.
//
// Production convergence is asynchronous and owned by CollectorScheduler: the
// registry commits desired state, wakes the runtime and reports actual state
// only from the runtime. These tests need the same separation but synchronous,
// reproducible observation, so this double records its own actual state and is
// driven explicitly by the test. It is used ONLY for registry command tests;
// the runtime/token/cutoff behaviour itself is covered against the real
// CollectorScheduler in the scheduler suites.
#pragma once

#include "core/job_registry.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

// The shape a test reads back. `revision` is the record revision the runtime
// last converged to; it orders the record, it is not runtime proof.
struct AppliedView {
    bool applied = false;
    uint64_t revision = 0;
    uint64_t attach_token = 0;
    std::vector<std::string> collectors;
    std::string error;
};

class TestCollectionRuntime : public ICollectionRuntime {
public:
    using ApplyFn =
        std::function<bool(const Job&, std::vector<std::string>&, std::string&)>;
    using EventFn = std::function<void(JobEvent, const Job&)>;

    explicit TestCollectionRuntime(JobRegistry& registry) : registry_(registry) {}

    void setApply(ApplyFn fn) { apply_ = std::move(fn); }

    // A deferred drain keeps the completion under test control: the command is
    // accepted (record Closing) while the runtime still holds the instance
    // until completeDrain() is called.
    void setDeferredDrain(bool deferred) { auto_drain_ = !deferred; }

    void setEventSink(EventFn fn) { events_ = std::move(fn); }

    // Coalesced wake: these tests drive reconciliation explicitly, so a wake is
    // only counted (a test may assert the command woke the runtime).
    void wake() override { ++wakes_; }
    int wakes() const { return wakes_; }

    bool beginDrain(uint64_t internal_id) override {
        // Idempotent per instance, as the seam promises: the heartbeat may
        // re-arm a restored Closing record and a test may then drive the same
        // instance again. A second call is not a second detach.
        if (draining_.count(internal_id) != 0) return true;
        draining_.insert(internal_id);
        const auto it = actual_.find(internal_id);
        if (it != actual_.end()) {
            it->second.draining = true;
            it->second.collectors.clear();
        }
        if (events_) {
            std::optional<JobRecord> record;
            if (registry_.runtimeRecord(internal_id, record) && record)
                events_(JobEvent::Removed, record->job);
        }
        if (!auto_drain_) return true;
        registry_.onDrained(internal_id);
        return true;
    }

    // The participant reported that its accepted tail is produced.
    bool completeDrain(uint64_t internal_id) {
        draining_.erase(internal_id);
        const auto it = actual_.find(internal_id);
        if (it != actual_.end()) it->second.draining = false;
        // The registry archives the Closing record; a failed cleanup/archive
        // leaves it Closing and the caller can retry.
        registry_.onDrained(internal_id);
        return !registry_.inventoryRecord(internal_id).has_value();
    }

    ActualCollectionState actual(uint64_t internal_id) override {
        const auto it = actual_.find(internal_id);
        if (it == actual_.end()) return ActualCollectionState{};
        return it->second;
    }

    // Test driver: converge one instance to the desired state it currently has.
    bool reconcile(uint64_t internal_id) {
        std::optional<JobRecord> record;
        if (!registry_.runtimeRecord(internal_id, record)) return false;
        if (!record) return false;
        if (record->state == kStateClosing) return beginDrain(internal_id);

        Job desired = record->job;
        desired.CollectorNames = record->selection.effectiveCollectors();

        std::vector<std::string> applied;
        std::string error;
        if (apply_) {
            const bool ok = apply_(desired, applied, error);
            if (!ok && error.empty()) error = "collection runtime rejected the selection";
        } else {
            applied = desired.CollectorNames;
        }

        ActualCollectionState state;
        state.tracked = true;
        const auto previous = actual_.find(internal_id);
        // A facts refresh or an unchanged re-drive keeps the attachment token;
        // a real detach/reattach gets a strictly greater one.
        if (previous != actual_.end() && !previous->second.draining &&
            previous->second.collectors == applied && previous->second.error.empty()) {
            state.attach_token = previous->second.attach_token;
        } else {
            state.attach_token = ++token_;
        }
        state.last_revision = record->revision;
        state.collectors = applied;
        state.error = error;
        // A facts-only record selects nothing, so nothing is attached and no
        // collector hook fires for it.
        if (events_ && !applied.empty()) events_(JobEvent::Added, desired);
        actual_[internal_id] = state;
        return true;
    }

    AppliedView applied(uint64_t internal_id) const {
        AppliedView view;
        const auto it = actual_.find(internal_id);
        if (it == actual_.end()) {
            view.error = "no_collection_runtime";
            return view;
        }
        view.applied = it->second.tracked && it->second.error.empty();
        view.revision = it->second.last_revision;
        view.attach_token = it->second.attach_token;
        view.collectors = it->second.collectors;
        view.error = it->second.error;
        return view;
    }

private:
    JobRegistry& registry_;
    ApplyFn apply_;
    EventFn events_;
    bool auto_drain_ = true;
    std::map<uint64_t, ActualCollectionState> actual_;
    std::set<uint64_t> draining_;
    uint64_t token_ = 0;
    int wakes_ = 0;
};

inline std::mutex& runtime_registry_mutex()
{
    static std::mutex mutex;
    return mutex;
}

inline std::map<JobRegistry*, std::unique_ptr<TestCollectionRuntime>>& runtime_registry()
{
    static std::map<JobRegistry*, std::unique_ptr<TestCollectionRuntime>> runtimes;
    return runtimes;
}

// One double per live registry. A stack address reused by a later test must
// never inherit the previous double's state (or, worse, a reference to a
// registry that has already been destroyed). Installing on the SAME live
// registry is a reconfiguration: the attachment state is preserved.
inline TestCollectionRuntime& install_test_runtime(JobRegistry& registry)
{
    std::lock_guard<std::mutex> lock(runtime_registry_mutex());
    auto& slot = runtime_registry()[&registry];
    if (!slot || registry.collectionRuntime() != slot.get()) {
        slot = std::make_unique<TestCollectionRuntime>(registry);
    }
    return *slot;
}

inline TestCollectionRuntime& test_runtime_for(JobRegistry& registry)
{
    std::lock_guard<std::mutex> lock(runtime_registry_mutex());
    auto& slot = runtime_registry()[&registry];
    if (!slot) slot = std::make_unique<TestCollectionRuntime>(registry);
    return *slot;
}

// Install the double as the registry's collection runtime with an attach
// function (the fake runtime's own convergence step).
inline void install_collection_runtime(JobRegistry& registry,
                                       TestCollectionRuntime::ApplyFn apply)
{
    TestCollectionRuntime& runtime = install_test_runtime(registry);
    registry.setCollectionRuntime(&runtime);
    runtime.setApply(std::move(apply));
}

// Install the double with a deferred drain and an optional event observer.
inline void install_deferred_drain(JobRegistry& registry,
                                   TestCollectionRuntime::EventFn observer = {})
{
    TestCollectionRuntime& runtime = install_test_runtime(registry);
    registry.setCollectionRuntime(&runtime);
    runtime.setDeferredDrain(true);
    runtime.setEventSink(std::move(observer));
}

inline TestCollectionRuntime& test_runtime(JobRegistry& registry)
{
    return test_runtime_for(registry);
}

// The record revision is the one durable counter. Tests that used to read a
// binding revision read this instead; 0 means the record is gone.
inline uint64_t record_revision(JobRegistry& registry, uint64_t internal_id)
{
    const auto record = registry.inventoryRecord(internal_id);
    return record ? record->revision : 0;
}
