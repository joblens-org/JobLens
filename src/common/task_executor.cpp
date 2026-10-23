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
#include "common/task_executor.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>
#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

namespace {
using Clock = std::chrono::steady_clock;

double milliseconds(Clock::duration duration)
{
    return std::chrono::duration<double, std::milli>(duration).count();
}

std::string bounded_error(std::string message)
{
    if (message.size() > 512) message.resize(512);
    for (char& value : message) {
        if (static_cast<unsigned char>(value) >= 128) value = '?';
    }
    return message;
}

enum class SlotPhase { Empty, Idle, Queued, Running, Retired };
const char* phase_name(SlotPhase phase);
} // namespace

struct TaskExecutor::Slot {

    uint64_t id = 0;
    uint64_t generation = 0;
    std::string name;
    Callback callback;
    SlotPhase phase = SlotPhase::Empty;
    bool invalid = false;
    bool remembered = false;
    Clock::time_point queued_at{};
    Clock::time_point running_since{};
    uint64_t completed = 0;
    uint64_t failed = 0;
    uint64_t skipped = 0;
    uint64_t yielded = 0;
    uint64_t remembered_requests = 0;
    double last_duration_ms = 0;
    std::string last_error;
};

struct TaskExecutor::Worker {
    bool busy = false;
    uint64_t task_id = 0;
    uint64_t generation = 0;
    std::string task_name;
    Clock::time_point started{};
    uint64_t completed = 0;
    uint64_t failed = 0;
    double total_duration_ms = 0;
    double max_duration_ms = 0;
};

namespace {
const char* phase_name(SlotPhase phase)
{
    switch (phase) {
        case SlotPhase::Empty:   return "empty";
        case SlotPhase::Idle:    return "idle";
        case SlotPhase::Queued:  return "queued";
        case SlotPhase::Running: return "running";
        case SlotPhase::Retired: return "retired";
    }
    return "unknown";
}
} // namespace

TaskExecutor::TaskExecutor(TaskExecutorOptions options)
    : options_(options)
{
    if (options_.workers == 0)
        throw std::invalid_argument("TaskExecutor requires at least one worker");
    if (options_.max_tasks == 0)
        throw std::invalid_argument("TaskExecutor requires at least one task slot");

    slots_.reserve(options_.max_tasks);
    for (size_t index = 0; index < options_.max_tasks; ++index) {
        auto slot = std::make_unique<Slot>();
        slot->id = index;
        slots_.push_back(std::move(slot));
    }
    workers_.resize(options_.workers);

    try {
        threads_.reserve(options_.workers);
        for (size_t index = 0; index < options_.workers; ++index)
            threads_.emplace_back([this, index] { workerLoop(index); });
    } catch (...) {
        {
            std::lock_guard lock(mutex_);
            stopped_ = true;
        }
        ready_cv_.notify_all();
        for (auto& thread : threads_)
            if (thread.joinable()) thread.join();
        throw;
    }
}

TaskExecutor::~TaskExecutor()
{
    shutdown();
}

std::optional<WorkHandle> TaskExecutor::registerTask(std::string name, Callback callback)
{
    if (!callback) return std::nullopt;

    std::lock_guard lock(mutex_);
    if (stopped_) return std::nullopt;

    Slot* target = nullptr;
    for (auto& slot : slots_) {
        if (slot->phase == SlotPhase::Empty || slot->phase == SlotPhase::Retired) {
            target = slot.get();
            break;
        }
    }
    if (!target) return std::nullopt;

    if (next_generation_ == std::numeric_limits<uint64_t>::max())
        throw std::overflow_error("TaskExecutor task generation counter exhausted");

    target->generation = ++next_generation_;
    target->name = std::move(name);
    target->callback = std::move(callback);
    target->phase = SlotPhase::Idle;
    target->invalid = false;
    target->remembered = false;
    target->queued_at = {};
    target->running_since = {};
    target->completed = 0;
    target->failed = 0;
    target->skipped = 0;
    target->yielded = 0;
    target->remembered_requests = 0;
    target->last_duration_ms = 0;
    target->last_error.clear();
    return WorkHandle{target->id, target->generation};
}

TaskExecutor::Slot* TaskExecutor::findValidLocked(const WorkHandle& handle)
{
    if (handle.id >= slots_.size()) return nullptr;
    Slot* slot = slots_[handle.id].get();
    if (slot->phase == SlotPhase::Empty || slot->phase == SlotPhase::Retired)
        return nullptr;
    if (slot->generation != handle.generation) return nullptr;
    return slot;
}

TaskExecutor::Callback TaskExecutor::retireLocked(Slot& slot)
{
    Callback retired = std::move(slot.callback);
    slot.callback = nullptr;
    slot.phase = SlotPhase::Retired;
    slot.invalid = false;
    slot.remembered = false;
    slot.queued_at = {};
    slot.running_since = {};
    return retired;
}

bool TaskExecutor::invalidate(WorkHandle handle)
{
    Callback retired;
    {
        std::lock_guard lock(mutex_);
        Slot* slot = findValidLocked(handle);
        if (!slot) {
            ++stale_total_;
            return false;
        }
        if (slot->phase == SlotPhase::Running) {
            if (slot->invalid) return false;
            slot->invalid = true;
            return true;
        }
        if (slot->phase == SlotPhase::Queued) {
            auto it = std::find(ready_.begin(), ready_.end(), slot->id);
            if (it != ready_.end()) ready_.erase(it);
        }
        retired = retireLocked(*slot);
    }
    // The callback capture is released after the executor lock is dropped.
    (void)retired;
    return true;
}

bool TaskExecutor::request(WorkHandle handle, RequestPolicy policy)
{
    std::lock_guard lock(mutex_);
    if (stopped_) {
        ++stopped_requests_;
        return false;
    }

    Slot* slot = findValidLocked(handle);
    if (!slot) {
        ++stale_total_;
        return false;
    }

    switch (slot->phase) {
        case SlotPhase::Idle: {
            slot->phase = SlotPhase::Queued;
            slot->queued_at = Clock::now();
            ready_.push_back(slot->id);
            ++requests_total_;
            max_queue_ = std::max(max_queue_, ready_.size());
            ready_cv_.notify_one();
            return true;
        }
        case SlotPhase::Queued:
        case SlotPhase::Running: {
            if (policy == RequestPolicy::Remember) {
                slot->remembered = true;
                ++slot->remembered_requests;
                ++remembered_total_;
                return true;
            }
            ++slot->skipped;
            ++skipped_total_;
            return false;
        }
        case SlotPhase::Empty:
        case SlotPhase::Retired:
            break;
    }
    ++stale_total_;
    return false;
}

void TaskExecutor::workerLoop(size_t index)
{
    {
        std::lock_guard lock(mutex_);
        worker_ids_.insert(std::this_thread::get_id());
    }

    for (;;) {
        Slot* slot = nullptr;
        size_t slot_index = 0;
        {
            std::unique_lock lock(mutex_);
            ready_cv_.wait(lock, [this] { return stopped_ || !ready_.empty(); });
            if (stopped_) break;
            if (ready_.empty()) continue;
            slot_index = ready_.front();
            ready_.pop_front();
            Slot& candidate = *slots_[slot_index];
            if (candidate.phase != SlotPhase::Queued || candidate.invalid) continue;

            candidate.phase = SlotPhase::Running;
            candidate.running_since = Clock::now();
            candidate.queued_at = {};
            slot = &candidate;

            Worker& worker = workers_[index];
            worker.busy = true;
            worker.task_id = candidate.id;
            worker.generation = candidate.generation;
            worker.task_name = candidate.name;
            worker.started = candidate.running_since;
        }

        WorkResult result = WorkResult::Complete;
        std::exception_ptr failure;
        try {
            result = slot->callback();
        } catch (...) {
            failure = std::current_exception();
        }

        std::string error;
        if (failure) {
            try {
                std::rethrow_exception(failure);
            } catch (const std::exception& exception) {
                error = exception.what();
            } catch (...) {
                error = "unknown exception";
            }
        }

        Callback retired;
        bool requeued = false;
        {
            std::unique_lock lock(mutex_);
            const auto now = Clock::now();
            const double duration = milliseconds(now - slot->running_since);
            slot->last_duration_ms = duration;

            Worker& worker = workers_[index];
            worker.busy = false;
            worker.task_id = 0;
            worker.generation = 0;
            worker.task_name.clear();
            worker.started = {};
            worker.total_duration_ms += duration;
            worker.max_duration_ms = std::max(worker.max_duration_ms, duration);

            if (failure) {
                ++slot->failed;
                ++failed_total_;
                ++worker.failed;
                slot->last_error = bounded_error(error);
            } else {
                ++slot->completed;
                ++completed_total_;
                ++worker.completed;
                if (result == WorkResult::Yield) {
                    ++slot->yielded;
                    ++yielded_total_;
                }
            }

            if (slot->invalid) {
                retired = retireLocked(*slot);
            } else if (stopped_) {
                slot->phase = SlotPhase::Idle;
                slot->remembered = false;
                slot->queued_at = {};
            } else {
                const bool again = slot->remembered ||
                                   (!failure && result == WorkResult::Yield);
                slot->remembered = false;
                if (again) {
                    slot->phase = SlotPhase::Queued;
                    slot->queued_at = now;
                    ready_.push_back(slot_index);
                    max_queue_ = std::max(max_queue_, ready_.size());
                    requeued = true;
                } else {
                    slot->phase = SlotPhase::Idle;
                }
            }
        }

        if (requeued) ready_cv_.notify_one();
        if (failure) {
            spdlog::error("TaskExecutor: task {} ({}) failed: {}", slot->id, slot->name,
                          bounded_error(error));
        }
        // `retired` releases its captures here, outside the executor lock.
        (void)retired;
    }

    {
        std::lock_guard lock(mutex_);
        worker_ids_.erase(std::this_thread::get_id());
    }
}

bool TaskExecutor::stopLocked()
{
    if (stopped_) return false;
    stopped_ = true;
    discarded_on_stop_ += ready_.size();
    for (size_t index : ready_) {
        Slot& slot = *slots_[index];
        if (slot.phase != SlotPhase::Queued) continue;
        slot.phase = SlotPhase::Idle;
        slot.remembered = false;
        slot.queued_at = {};
    }
    ready_.clear();
    return true;
}

void TaskExecutor::shutdown()
{
    // A callback must never wait for a sibling task or join the pool. Detect
    // this case before taking shutdown_mutex_ so a callback that calls shutdown
    // cannot deadlock with an external thread that is already joining it.
    bool from_worker = false;
    {
        std::lock_guard lock(mutex_);
        if (isWorkerThreadLocked()) {
            stopLocked();
            stopped_from_worker_ = true;
            from_worker = true;
        }
    }
    if (from_worker) {
        ready_cv_.notify_all();
        spdlog::warn("TaskExecutor: shutdown called from a worker; workers were not joined");
        return;
    }

    std::lock_guard shutdown_lock(shutdown_mutex_);
    {
        std::lock_guard lock(mutex_);
        stopLocked();
    }
    ready_cv_.notify_all();

    for (auto& thread : threads_)
        if (thread.joinable()) thread.join();
    clearCallbacksAfterJoin();
}

void TaskExecutor::clearCallbacksAfterJoin()
{
    std::vector<Callback> retired;
    retired.reserve(slots_.size());
    {
        std::lock_guard lock(mutex_);
        for (auto& slot : slots_) {
            if (slot->phase == SlotPhase::Empty) continue;
            retired.push_back(std::move(slot->callback));
            slot->callback = nullptr;
            slot->phase = SlotPhase::Retired;
            slot->invalid = false;
            slot->remembered = false;
            slot->queued_at = {};
            slot->running_since = {};
        }
    }
    // Callback captures are released after the lock is dropped.
}

bool TaskExecutor::isWorkerThreadLocked() const
{
    return worker_ids_.count(std::this_thread::get_id()) > 0;
}

bool TaskExecutor::stopped() const
{
    std::lock_guard lock(mutex_);
    return stopped_;
}

nlohmann::json TaskExecutor::snapshot() const
{
    std::lock_guard lock(mutex_);
    const auto now = Clock::now();

    auto workers = nlohmann::json::array();
    size_t active_workers = 0;
    for (size_t index = 0; index < workers_.size(); ++index) {
        const auto& worker = workers_[index];
        active_workers += worker.busy ? 1 : 0;
        workers.push_back({
            {"worker_index", index},
            {"busy", worker.busy},
            {"task_id", worker.busy ? nlohmann::json(worker.task_id) : nlohmann::json(nullptr)},
            {"generation", worker.busy ? nlohmann::json(worker.generation) : nlohmann::json(nullptr)},
            {"name", worker.task_name},
            {"running_elapsed_ms", worker.busy ? milliseconds(now - worker.started) : 0.0},
            {"completed_turns", worker.completed},
            {"failed_turns", worker.failed},
            {"total_duration_ms", worker.total_duration_ms},
            {"max_duration_ms", worker.max_duration_ms},
        });
    }

    auto tasks = nlohmann::json::array();
    size_t registered = 0;
    size_t retired = 0;
    for (const auto& slot : slots_) {
        if (slot->phase == SlotPhase::Empty) continue;
        if (slot->phase == SlotPhase::Retired) ++retired;
        else ++registered;
        tasks.push_back({
            {"task_id", slot->id},
            {"generation", slot->generation},
            {"name", slot->name},
            {"state", phase_name(slot->phase)},
            {"queued_elapsed_ms", slot->phase == SlotPhase::Queued
                ? milliseconds(now - slot->queued_at) : 0.0},
            {"running_elapsed_ms", slot->phase == SlotPhase::Running
                ? milliseconds(now - slot->running_since) : 0.0},
            {"completed_turns", slot->completed},
            {"failed_turns", slot->failed},
            {"yielded_turns", slot->yielded},
            {"skipped_requests", slot->skipped},
            {"remembered_requests", slot->remembered_requests},
            {"last_duration_ms", slot->last_duration_ms},
            {"last_error", slot->last_error},
        });
    }

    return {
        {"stopped", stopped_},
        {"stopped_from_worker", stopped_from_worker_},
        {"worker_count", workers_.size()},
        {"active_workers", active_workers},
        {"registered_tasks", registered},
        {"retired_tasks", retired},
        {"currentQueuedTasks", ready_.size()},
        {"maxQueueSize", max_queue_},
        {"totalRequests", requests_total_},
        {"totalRememberedRequests", remembered_total_},
        {"totalSkippedRequests", skipped_total_},
        {"totalStaleRequests", stale_total_},
        {"totalStoppedRequests", stopped_requests_},
        {"totalCompleted", completed_total_},
        {"totalFailed", failed_total_},
        {"totalYielded", yielded_total_},
        {"discardedQueuedOnStop", discarded_on_stop_},
        {"workers", std::move(workers)},
        {"tasks", std::move(tasks)},
    };
}
