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
// KernelAttribution — the optional kernel-side job attribution capability
// (doc/job_inventory_v2_architecture.md §2.6).
//
// Owns the eBPF pid tracker, the cgroup-id cache and the fork/exit
// reconciliation. Invariants (I8):
//   * kernel maps contain only jobs of attribution-capable adapters;
//   * the cgroup-id cache lives HERE, so a removal can always delete the
//     cgroup2job entry even when the cgroup directory is already gone;
//   * every entry is removed by this component.
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "core/collector_type.h"

class JobPidTracker;

class KernelAttribution {
public:
    // The registry-facing surface this component needs. Implementations must
    // be cheap lock-internal operations; none of them may call back into this
    // component while holding a lock it also takes from inside these calls.
    struct Host {
        virtual ~Host() = default;
        // True while the instance is live in the registry (a fork/exit event
        // for an unknown id is ignored).
        virtual bool attributionJobExists(uint64_t internal_id) = 0;
        virtual void attributionJobAddPid(uint64_t internal_id, pid_t pid) = 0;
        virtual void attributionJobRemovePid(uint64_t internal_id, pid_t pid) = 0;
        // Snapshot enumeration for the periodic reconcile (registry copies).
        virtual void attributionEnumerateJobs(
            const std::function<void(const Job&)>& fn) = 0;
        // A pid exited: the heartbeat may schedule a targeted refresh. Must
        // not do I/O on this thread.
        virtual void attributionExitHint(pid_t pid) = 0;
        // The execution cgroup of one instance as told by its adapter
        // (cgroupPathOf); empty = none. Lets this component stay free of
        // scheduler-facts knowledge.
        virtual std::string attributionCgroupPath(uint64_t internal_id)
        {
            (void)internal_id;
            return {};
        }
    };

    explicit KernelAttribution(Host& host);
    ~KernelAttribution();

    KernelAttribution(const KernelAttribution&) = delete;
    KernelAttribution& operator=(const KernelAttribution&) = delete;

    // Capability gate: only jobs whose adapter reports the kernel-attribution
    // trait are registered. Installed before start(); an unset lookup means
    // "every job participates" (legacy/manual registrations).
    void setCapabilityLookup(std::function<bool(uint64_t internal_id)> capable);

    bool start();
    void stop();

    // JobEvent::Added: register the cgroup mapping + seed the root pids. The
    // cgroup id is resolved once here and cached for the later removal.
    void activate(uint64_t internal_id, const Job& job);
    // JobEvent::Removed (after the collector drain): delete the cgroup
    // mapping from the cache and clear known pids.
    void deactivate(uint64_t internal_id, const Job& job);
    // Heartbeat-driven resync of one job (also refreshes the cgroup cache).
    void reconcile(const Job& job);

    bool running() const;
    uint64_t pushedEvents() const;
    uint64_t droppedEvents() const;

private:
    void reconcileLoop();
    // Resolves (and caches) the cgroup id for one job from its typed facts.
    uint64_t resolveCgroupId(const Job& job);
    // Capability gate; callers hold cache_mtx_.
    bool capable(uint64_t internal_id) const;

    Host& host_;
    std::unique_ptr<JobPidTracker> tracker_;
    std::function<bool(uint64_t)> capable_;

    // Guards the cache below; the tracker's ringbuf callbacks and the
    // heartbeat both touch it.
    std::mutex cache_mtx_;
    std::unordered_map<uint64_t, uint64_t> cgroup_ids_;  // internal_id → cgroup inode

    std::atomic<bool> reconcile_running_{false};
    std::unique_ptr<std::thread> reconcile_thread_;
    uint64_t last_dropped_events_{0};
};
