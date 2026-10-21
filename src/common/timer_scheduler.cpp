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
#include "common/timer_scheduler.hpp"
#include <algorithm>
#include <condition_variable>
#include <deque>
#include <map>
#include <optional>
#include <stdexcept>
#include <spdlog/spdlog.h>

namespace {
double milliseconds(TimerScheduler::Clock::duration duration) {
    return std::chrono::duration<double, std::milli>(duration).count();
}
}

// One short-held mutex protects deadlines, the ready queue and all counters.
// Callbacks execute without it, so observing a blocked callback never waits for it.
struct TimerScheduler::State {
    struct TimerStatus {
        size_t id = 0;
        std::string name, phase = "idle";
        Duration interval{0};
        TimePoint queued_at{}, started_at{};
        size_t skipped = 0, completed = 0, errors = 0;
        double last_wait_ms = 0, last_duration_ms = 0;
        bool cancelled = false;
        std::optional<size_t> worker_index;
    };
    struct Timer {
        TimerStatus status;
        Task callback;
        TimePoint next;
        bool repeat;
    };
    struct Worker {
        std::optional<size_t> task_id;
        std::string name;
        TimePoint started_at{};
        size_t completed = 0, errors = 0;
        double total_ms = 0, max_ms = 0, min_ms = 0;
    };
    std::mutex mutex;
    std::condition_variable timers_changed, work_ready;
    bool stopped = false;
    size_t next_id = 0, registered = 0, completed = 0, errors = 0;
    size_t skipped = 0, max_queue = 0, dispatched = 0, wakeups = 0, started = 0;
    double total_wait_ms = 0, max_wait_ms = 0, min_wait_ms = 0;
    TimePoint created = Clock::now(), stopped_at{};
    std::map<size_t, std::shared_ptr<Timer>> timers;
    std::map<std::pair<TimePoint, size_t>, std::shared_ptr<Timer>> deadlines;
    std::deque<std::shared_ptr<Timer>> ready;
    std::vector<Worker> workers;

    nlohmann::json snapshot() {
        // Copy small observational values only. Never copy callbacks or Job data.
        std::vector<TimerStatus> task_status;
        std::vector<Worker> worker_status;
        TimePoint now;
        size_t pending, queued, registered_copy, completed_copy, errors_copy;
        size_t skipped_copy, max_queue_copy, dispatched_copy, wakeups_copy;
        double runtime_ms, average_wait, max_wait, min_wait;
        bool stopped_copy;
        {
            std::lock_guard lock(mutex);
            now = Clock::now();
            for (const auto& [id, timer] : timers) task_status.push_back(timer->status);
            worker_status = workers;
            pending = deadlines.size(); queued = ready.size();
            registered_copy = registered; completed_copy = completed; errors_copy = errors;
            skipped_copy = skipped; max_queue_copy = max_queue; dispatched_copy = dispatched;
            wakeups_copy = wakeups; stopped_copy = stopped;
            runtime_ms = milliseconds((stopped_at == TimePoint{} ? now : stopped_at) - created);
            average_wait = started ? total_wait_ms / started : 0;
            max_wait = max_wait_ms; min_wait = min_wait_ms;
        }
        auto tasks = nlohmann::json::array();
        size_t active = 0;
        for (const auto& task : task_status) {
            active += task.phase == "running";
            tasks.push_back({{"task_id", task.id}, {"name", task.name}, {"state", task.phase},
                {"cancelled", task.cancelled},
                {"worker_index", task.worker_index ? nlohmann::json(*task.worker_index) : nlohmann::json(nullptr)}, {"period_ms", task.interval.count()},
                {"queued_elapsed_ms", task.phase == "queued" ? milliseconds(now - task.queued_at) : 0},
                {"running_elapsed_ms", task.phase == "running" ? milliseconds(now - task.started_at) : 0},
                {"skipped_runs", task.skipped}, {"completed_runs", task.completed},
                {"error_count", task.errors}, {"last_queue_wait_ms", task.last_wait_ms},
                {"last_duration_ms", task.last_duration_ms}});
        }
        auto worker_json = nlohmann::json::array();
        double total_load = 0, max_load = 0, min_load = 100, total_duration = 0;
        for (size_t i = 0; i < worker_status.size(); ++i) {
            const auto& worker = worker_status[i];
            const double running_ms = worker.task_id ? milliseconds(now - worker.started_at) : 0;
            const double load = runtime_ms > 0 ? (worker.total_ms + running_ms) * 100 / runtime_ms : 0;
            total_load += load; max_load = std::max(max_load, load); min_load = std::min(min_load, load);
            total_duration += worker.total_ms;
            worker_json.push_back({{"workerIndex", i}, {"tasksProcessed", worker.completed},
                {"tasksFailed", worker.errors}, {"maxExecutionTimeMs", worker.max_ms},
                {"averageExecutionTimeMs", worker.completed ? worker.total_ms / worker.completed : 0},
                {"isBusy", worker.task_id.has_value()}, {"name", worker.name},
                {"task_id", worker.task_id ? nlohmann::json(*worker.task_id) : nlohmann::json(nullptr)},
                {"running_elapsed_ms", running_ms},
                {"minExecutionTimeMs", worker.min_ms}, {"loadPercentage", load}});
        }
        return {{"stopped", stopped_copy}, {"worker_count", worker_status.size()},
            {"totalTasksRegistered", registered_copy}, {"totalTasksProcessed", completed_copy},
            {"totalTasksFailed", errors_copy}, {"currentTimerTasks", pending},
            {"currentQueuedTasks", queued}, {"maxQueueSize", max_queue_copy},
            {"totalQueueOperations", dispatched_copy}, {"skippedRuns", skipped_copy},
            {"schedulerRuntime", runtime_ms}, {"schedulerWakeups", wakeups_copy},
            {"schedulerStats", {{"pendingTasks", pending}, {"activeWorkers", active},
                {"averageWaitMs", average_wait}, {"maxWaitMs", max_wait}, {"minWaitMs", min_wait}}},
            {"averageThreadLoad", total_load / worker_status.size()},
            {"maxThreadLoad", max_load}, {"minThreadLoad", min_load},
            {"averageTaskCompletionTimeMs", completed_copy ? total_duration / completed_copy : 0},
            {"workerStats", worker_json}, {"tasks", tasks}};
    }
};

TimerScheduler::TimerScheduler(size_t numWorkers) : state_(std::make_shared<State>()) {
    if (numWorkers == 0) throw std::invalid_argument("TimerScheduler requires at least one worker");
    state_->workers.resize(numWorkers);
    try {
        for (size_t i = 0; i < numWorkers; ++i)
            workers_.emplace_back([this, i] { workerLoop(i); });
        scheduler_thread_ = std::thread([this] { schedulerLoop(); });
    } catch (...) {
        shutdown();
        throw;
    }
}
TimerScheduler::~TimerScheduler() { shutdown(); }

void TimerScheduler::shutdown() {
    std::lock_guard shutdown_lock(shutdown_mutex_);
    {
        std::lock_guard lock(state_->mutex);
        state_->stopped = true;
        state_->deadlines.clear();
        state_->ready.clear();
        for (auto it = state_->timers.begin(); it != state_->timers.end();) {
            if (it->second->status.phase == "running") {
                it->second->status.cancelled = true;
                ++it;
            } else it = state_->timers.erase(it);
        }
    }
    state_->timers_changed.notify_all();
    state_->work_ready.notify_all();
    if (scheduler_thread_.joinable()) scheduler_thread_.join();
    for (auto& worker : workers_) if (worker.joinable()) worker.join();
    std::lock_guard lock(state_->mutex);
    if (state_->stopped_at == TimePoint{}) state_->stopped_at = Clock::now();
}

size_t TimerScheduler::registerTimer(Duration delay, Task task, std::string name) {
    return addTask(std::move(task), delay, false, std::move(name));
}
size_t TimerScheduler::registerRepeatingTimer(Duration interval, Task task, std::string name) {
    return addTask(std::move(task), interval, true, std::move(name));
}
size_t TimerScheduler::addTask(Task callback, Duration interval, bool repeat, std::string name) {
    if (!callback || interval.count() < 0 || (repeat && interval.count() == 0))
        throw std::invalid_argument("TimerScheduler requires a callback and a positive repeat interval");
    std::lock_guard lock(state_->mutex);
    if (state_->stopped) throw std::logic_error("TimerScheduler is stopped");
    auto task = std::make_shared<State::Timer>();
    task->status.id = state_->next_id++;
    task->status.name = std::move(name);
    task->status.interval = interval;
    task->callback = std::move(callback);
    task->next = Clock::now() + (repeat ? Duration(0) : interval);
    task->repeat = repeat;
    state_->timers.emplace(task->status.id, task);
    state_->deadlines.emplace(std::make_pair(task->next, task->status.id), task);
    ++state_->registered;
    state_->timers_changed.notify_one();
    return task->status.id;
}

bool TimerScheduler::cancelTimer(size_t id) {
    std::lock_guard lock(state_->mutex);
    const auto it = state_->timers.find(id);
    if (it == state_->timers.end() || it->second->status.cancelled) return false;
    auto task = it->second;
    task->status.cancelled = true;
    state_->deadlines.erase({task->next, id});
    state_->ready.erase(std::remove(state_->ready.begin(), state_->ready.end(), task), state_->ready.end());
    if (task->status.phase != "running") state_->timers.erase(it);
    state_->timers_changed.notify_one();
    return true;
}

void TimerScheduler::schedulerLoop() {
    std::unique_lock lock(state_->mutex);
    while (!state_->stopped) {
        if (state_->deadlines.empty()) {
            state_->timers_changed.wait(lock, [this] { return state_->stopped || !state_->deadlines.empty(); });
            continue;
        }
        const auto next = state_->deadlines.begin()->first.first;
        const auto now = Clock::now();
        if (next > now) {
            state_->timers_changed.wait_until(lock, next);
            continue;
        }
        auto task = state_->deadlines.begin()->second;
        state_->deadlines.erase(state_->deadlines.begin());
        ++state_->wakeups;
        if (task->status.phase == "idle") {
            task->status.phase = "queued";
            task->status.queued_at = now;
            state_->ready.push_back(task);
            ++state_->dispatched;
            state_->max_queue = std::max(state_->max_queue, state_->ready.size());
            state_->work_ready.notify_one();
        } else {
            ++task->status.skipped;
            ++state_->skipped;
        }
        if (task->repeat) {
            task->next = now + task->status.interval;
            state_->deadlines.emplace(std::make_pair(task->next, task->status.id), task);
        }
        // Under overload every deadline can remain overdue. Give workers and
        // status/cancellation callers a chance to acquire the mutex each tick.
        lock.unlock();
        std::this_thread::yield();
        lock.lock();
    }
}

void TimerScheduler::workerLoop(size_t index) {
    std::unique_lock lock(state_->mutex);
    auto& worker = state_->workers[index];
    for (;;) {
        state_->work_ready.wait(lock, [this] { return state_->stopped || !state_->ready.empty(); });
        if (state_->stopped) return;
        auto task = state_->ready.front();
        state_->ready.pop_front();
        task->status.phase = "running";
        task->status.started_at = Clock::now();
        task->status.last_wait_ms = milliseconds(task->status.started_at - task->status.queued_at);
        const auto wait = task->status.last_wait_ms;
        state_->total_wait_ms += wait;
        state_->max_wait_ms = std::max(state_->max_wait_ms, wait);
        state_->min_wait_ms = state_->started ? std::min(state_->min_wait_ms, wait) : wait;
        ++state_->started;
        worker.task_id = task->status.id;
        worker.name = task->status.name;
        worker.started_at = task->status.started_at;
        task->status.worker_index = index;
        lock.unlock();
        bool failed = false;
        try { task->callback(); }
        catch (const std::exception& error) {
            failed = true;
            spdlog::error("TimerScheduler: task {} ({}) failed: {}", task->status.id, task->status.name, error.what());
        } catch (...) {
            failed = true;
            spdlog::error("TimerScheduler: task {} ({}) failed with unknown exception", task->status.id, task->status.name);
        }
        lock.lock();
        const auto duration = milliseconds(Clock::now() - task->status.started_at);
        task->status.last_duration_ms = duration;
        task->status.phase = "idle";
        ++task->status.completed; task->status.errors += failed;
        worker.total_ms += duration;
        worker.max_ms = std::max(worker.max_ms, duration);
        worker.min_ms = worker.completed ? std::min(worker.min_ms, duration) : duration;
        ++worker.completed; worker.errors += failed;
        ++state_->completed; state_->errors += failed;
        worker.task_id.reset();
        worker.name.clear();
        task->status.worker_index.reset();
        if (!task->repeat || task->status.cancelled) state_->timers.erase(task->status.id);
    }
}

TimerScheduler::StatusReader TimerScheduler::statusReader() const {
    return [state = state_] { return state->snapshot(); };
}
nlohmann::json TimerScheduler::getPerformanceStats() { return state_->snapshot(); }
