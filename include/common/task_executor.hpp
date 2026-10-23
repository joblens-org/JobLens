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
#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>

// One bounded pool executes all collector work: periodic sweeps, event drains,
// lifecycle hooks and source preparation. Callers own their domain state and
// only ask the pool to run a registered task slot.

enum class RequestPolicy {
    // Ordinary periodic tick: a tick that arrives while the slot is queued or
    // running is counted as skipped instead of creating replay work.
    SkipIfBusy,
    // Readiness/continuation/control wakeup: merge with queued work or run one
    // more turn after the current callback completes.
    Remember,
};

enum class WorkResult {
    // The current turn finished all work it knows about.
    Complete,
    // The callback reached its budget and needs another turn. The handle is
    // requeued once at the tail.
    Yield,
};

struct WorkHandle {
    uint64_t id = 0;
    uint64_t generation = 0;
};

inline bool operator==(const WorkHandle& lhs, const WorkHandle& rhs)
{
    return lhs.id == rhs.id && lhs.generation == rhs.generation;
}

inline bool operator!=(const WorkHandle& lhs, const WorkHandle& rhs)
{
    return !(lhs == rhs);
}

struct TaskExecutorOptions {
    size_t workers = 1;
    size_t max_tasks = 1024;
};

class TaskExecutor {
public:
    using Callback = std::function<WorkResult()>;

    explicit TaskExecutor(TaskExecutorOptions options);
    ~TaskExecutor();

    TaskExecutor(const TaskExecutor&) = delete;
    TaskExecutor& operator=(const TaskExecutor&) = delete;

    // Registers one persistent task slot. Returns nullopt when the executor is
    // stopped or all configured slots are in use.
    std::optional<WorkHandle> registerTask(std::string name, Callback callback);

    // Cancels a registration. A queued or idle slot retires immediately; a
    // running callback completes cooperatively and is not requeued. Returns
    // true only when this call changed the slot state; repeated calls are safe.
    bool invalidate(WorkHandle handle);

    // Requests one turn for a handle. Returns true when the work is queued,
    // already running, or merged into an existing request.
    bool request(WorkHandle handle, RequestPolicy policy);

    // Stops accepting work, drops queued requests (counted), waits for running
    // callbacks to finish, then joins the workers. Must be called from a
    // non-worker thread. Idempotent. The owner must also destroy the executor
    // from a non-worker thread.
    void shutdown();

    bool stopped() const;
    nlohmann::json snapshot() const;

private:
    struct Slot;
    struct Worker;

    Slot* findValidLocked(const WorkHandle& handle);
    Callback retireLocked(Slot& slot);
    bool stopLocked();
    void workerLoop(size_t index);
    bool isWorkerThreadLocked() const;
    void clearCallbacksAfterJoin();

    TaskExecutorOptions options_;
    mutable std::mutex mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable ready_cv_;
    bool stopped_ = false;
    bool stopped_from_worker_ = false;
    uint64_t next_generation_ = 0;
    std::deque<size_t> ready_;
    std::vector<std::unique_ptr<Slot>> slots_;
    std::vector<Worker> workers_;
    std::vector<std::thread> threads_;
    std::unordered_set<std::thread::id> worker_ids_;

    uint64_t requests_total_ = 0;
    uint64_t remembered_total_ = 0;
    uint64_t skipped_total_ = 0;
    uint64_t stale_total_ = 0;
    uint64_t stopped_requests_ = 0;
    uint64_t completed_total_ = 0;
    uint64_t failed_total_ = 0;
    uint64_t yielded_total_ = 0;
    uint64_t discarded_on_stop_ = 0;
    size_t max_queue_ = 0;
};
