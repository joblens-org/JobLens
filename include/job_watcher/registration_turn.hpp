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
// One poll turn's registration work (plan §6.2/§6.3).
//
// A discovery source owns this object: it batches the Jobs one turn produced,
// registers them synchronously before the turn ends, and keeps the bounded
// re-read table for exec hints whose metadata was not ready yet. No thread, no
// queue, no second discovery model — the source's own poll loop drives it.
#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "core/collector_type.h"
#include "core/job_discovery.hpp"

class RegistrationTurn {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    // Bounded re-read: first retry 5 ms after the failed read, then
    // 25/100/500 ms, then once a second, for at most 30 s. These are design
    // values from the plan, not measured latency guarantees.
    static constexpr std::size_t kMaxPending = 1024;
    static constexpr std::int64_t kMaxAgeMs = 30000;
    // Samples one poll turn may handle, invalid/incomplete ones included
    // (plan §6.2). The 32nd sample is handled and then ends the turn.
    static constexpr std::size_t kSamplesPerTurn = 32;

    void configure(DiscoverySink sink, std::size_t batch_size)
    {
        sink_ = std::move(sink);
        batch_size_ = std::clamp<std::size_t>(batch_size == 0 ? 1 : batch_size, 1, 32);
        pending_.clear();
        batch_.clear();
        // Counters describe one configured epoch (the source's lifetime).
        batches_committed_ = 0;
        pending_added_ = 0;
        pending_expired_ = 0;
        retryable_failures_ = 0;
    }

    void clear()
    {
        pending_.clear();
        batch_.clear();
        sink_ = DiscoverySink{};
    }

    std::size_t batchSize() const { return batch_size_; }
    std::size_t pendingCount() const { return pending_.size(); }
    std::size_t batchesCommitted() const { return batches_committed_; }
    std::size_t pendingExpired() const { return pending_expired_; }
    std::size_t pendingAdded() const { return pending_added_; }
    std::size_t retryableFailures() const { return retryable_failures_; }

    std::int64_t oldestPendingAgeMs(TimePoint now) const
    {
        std::int64_t oldest = 0;
        for (const auto& [pid, entry] : pending_) {
            (void)pid;
            const auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 now - entry.first).count();
            if (age > oldest) oldest = age;
        }
        return oldest;
    }

    // A resolved observation for this turn: convert it to a Job and queue it.
    // Returns false when this core has no registration runtime.
    bool offer(const Observation& observation)
    {
        if (!sink_.on_jobs || !sink_.to_job) return false;
        Job job;
        try {
            job = sink_.to_job(observation);
        } catch (const std::exception&) {
            return false;
        }
        batch_.push_back(std::move(job));
        if (batch_.size() >= batch_size_) flush();
        return true;
    }

    // Registers everything this turn produced. Called before the turn ends, so
    // with the default batch size of 1 the registration completes inside the
    // event that discovered the job.
    void flush()
    {
        if (batch_.empty()) return;
        if (!sink_.on_jobs) {
            batch_.clear();
            return;
        }
        std::vector<Job> pending;
        pending.swap(batch_);
        try {
            const auto results = sink_.on_jobs(pending);
            ++batches_committed_;
            // Count rejected items instead of inventing a per-item retry: a
            // retryable facts failure is covered by the next source pass, and
            // post-commit repairs are record-driven (plan §5.5).
            for (const auto& result : results)
                if (!result.ok && result.retry_registration()) ++retryable_failures_;
        } catch (const std::exception&) {
            batch_.clear();
        }
    }

    // A hint whose metadata was not ready: keep the child pid, its stable
    // identity (when the source could read it) and the deadline. Repeated
    // events never extend the original age limit.
    void rememberPid(pid_t pid, TimePoint now)
    {
        rememberPid(pid, ProcessIdentity{pid, 0, ""}, now);
    }

    void rememberPid(pid_t pid, ProcessIdentity child, TimePoint now)
    {
        if (pid <= 0 || pending_.size() >= kMaxPending) return;
        if (pending_.count(pid) > 0) return;
        Pending entry;
        entry.child = std::move(child);
        entry.first = now;
        entry.due = now + std::chrono::milliseconds(5);
        pending_.emplace(pid, entry);
        ++pending_added_;
    }

    // The stable identity of the hint pid inside one observation. Incomplete
    // when the source could not read it; then no reuse check is possible.
    static ProcessIdentity childIdentity(const Observation& observation, pid_t pid)
    {
        for (const auto& member : observation.members)
            if (member.pid == pid) return member;
        return ProcessIdentity{pid, 0, observation.generation.boot_id};
    }

    // False when the pid now belongs to a different process (PID reuse) or the
    // execution already ended: the re-read must be dropped, not registered.
    static bool sameChild(const ProcessIdentity& expected, const Observation& observation,
                          pid_t pid)
    {
        if (!expected.complete()) return true;   // nothing to compare
        const auto current = childIdentity(observation, pid);
        return current.complete() && current.start_ticks == expected.start_ticks &&
               current.boot_id == expected.boot_id;
    }

    // A re-read that is due: the caller must hand back the ORIGINAL deadline
    // and attempt so the ladder cannot restart the 30 s window on every try.
    struct Due {
        pid_t pid = 0;
        TimePoint first{};
        int attempt = 0;
        ProcessIdentity child;   // stable identity captured when the hint arrived
    };

    // Pops the entries whose re-read is due (bounded), dropping the ones whose
    // age limit expired.
    std::vector<Due> take_due(TimePoint now, std::size_t max_items)
    {
        std::vector<Due> due;
        for (auto it = pending_.begin(); it != pending_.end() && due.size() < max_items;) {
            if (now - it->second.first > std::chrono::milliseconds(kMaxAgeMs)) {
                it = pending_.erase(it);
                ++pending_expired_;
                continue;
            }
            if (it->second.due <= now) {
                due.push_back(Due{it->first, it->second.first, it->second.attempt,
                                  it->second.child});
                it = pending_.erase(it);
                continue;
            }
            ++it;
        }
        return due;
    }

    // Schedules the next attempt of a re-read that is still incomplete. `first`
    // is the original deadline of the entry, never the current instant.
    void reschedule(pid_t pid, TimePoint first, int attempt, TimePoint now)
    {
        if (pending_.size() >= kMaxPending) return;
        if (now - first > std::chrono::milliseconds(kMaxAgeMs)) {
            // The window is over: the hint is dropped and stays visible as an
            // expiry instead of silently disappearing.
            ++pending_expired_;
            return;
        }
        static constexpr std::int64_t kLadderMs[] = {25, 100, 500};
        const std::int64_t delay = attempt < 3 ? kLadderMs[attempt] : 1000;
        Pending entry;
        entry.first = first;
        entry.attempt = attempt;
        entry.due = now + std::chrono::milliseconds(delay);
        pending_[pid] = entry;
    }

    // ---- poll-turn accounting --------------------------------------------
    void begin_turn() { samples_this_turn_ = 0; }
    std::size_t samplesThisTurn() const { return samples_this_turn_; }

    // Called once per sample callback AFTER the sample was handled. True means
    // this sample ends the turn: the budget is spent, a re-read is due, or the
    // source was asked to stop. The sample itself is never dropped.
    bool should_yield(TimePoint now, bool stop_requested)
    {
        ++samples_this_turn_;
        if (stop_requested) return true;
        if (samples_this_turn_ >= kSamplesPerTurn) return true;
        const auto until_due = msUntilNextDue(now);
        return until_due == 0;
    }

    // Time until the next due re-read, for the poll timeout.
    std::int64_t msUntilNextDue(TimePoint now) const
    {
        std::int64_t best = -1;
        for (const auto& [pid, entry] : pending_) {
            (void)pid;
            const auto delta =
                std::chrono::duration_cast<std::chrono::milliseconds>(entry.due - now).count();
            const std::int64_t value = delta < 0 ? 0 : delta;
            if (best < 0 || value < best) best = value;
        }
        return best;
    }

private:
    struct Pending {
        TimePoint first{};
        TimePoint due{};
        int attempt = 0;
        ProcessIdentity child;   // stable child identity, when it was readable
    };

    DiscoverySink sink_;
    std::size_t batch_size_ = 1;
    std::vector<Job> batch_;
    std::map<pid_t, Pending> pending_;
    std::size_t batches_committed_ = 0;
    std::size_t pending_added_ = 0;
    std::size_t pending_expired_ = 0;
    std::size_t retryable_failures_ = 0;
    std::size_t samples_this_turn_ = 0;
};
