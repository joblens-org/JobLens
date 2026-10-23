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
#include <chrono>
#include <exception>
#include <map>
#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

namespace {
using Clock = std::chrono::steady_clock;

double milliseconds(Clock::duration duration)
{
    return std::chrono::duration<double, std::milli>(duration).count();
}
} // namespace

struct TimerScheduler::State {
    struct Timer {
        size_t id = 0;
        std::string name;
        bool callback_timer = true;
        bool repeat = false;
        bool canceled = false;
        Duration interval{0};
        TimePoint next{};
        Task task;
        WorkHandle work{};
        RequestPolicy policy = RequestPolicy::SkipIfBusy;
        std::string phase = "idle";
        TimePoint queued_at{};
        TimePoint started_at{};
        size_t skipped = 0;
        size_t completed = 0;
        size_t errors = 0;
        double last_wait_ms = 0;
        double last_duration_ms = 0;
    };

    std::mutex mutex;
    std::condition_variable changed;
    std::condition_variable drained;
    TaskExecutor* executor = nullptr;
    bool stopped = false;
    size_t next_id = 0;
    size_t registered = 0;
    size_t completed = 0;
    size_t errors = 0;
    size_t skipped = 0;
    size_t wakeups = 0;
    size_t dispatched = 0;
    size_t canceled = 0;
    size_t inflight = 0;
    double total_duration_ms = 0;
    TimePoint created = Clock::now();
    TimePoint stopped_at{};
    std::map<size_t, std::shared_ptr<Timer>> timers;
    std::map<std::pair<TimePoint, size_t>, std::shared_ptr<Timer>> deadlines;

    nlohmann::json snapshot();
};

nlohmann::json TimerScheduler::State::snapshot()
{
    struct TimerView {
        size_t id = 0;
        std::string name;
        std::string phase;
        bool canceled = false;
        bool repeat = false;
        Duration interval{0};
        TimePoint queued_at{};
        TimePoint started_at{};
        size_t skipped = 0;
        size_t completed = 0;
        size_t errors = 0;
        double last_wait_ms = 0;
        double last_duration_ms = 0;
    };

    std::vector<TimerView> views;
    size_t pending = 0, registered_copy = 0, completed_copy = 0, errors_copy = 0;
    size_t skipped_copy = 0, wakeups_copy = 0, dispatched_copy = 0, canceled_copy = 0;
    double total_duration = 0;
    TimePoint now{};
    bool stopped_copy = false;
    double runtime_ms = 0;
    {
        std::lock_guard lock(mutex);
        now = Clock::now();
        for (const auto& [id, timer] : timers) {
            (void)id;
            views.push_back({timer->id, timer->name, timer->phase, timer->canceled,
                             timer->repeat, timer->interval, timer->queued_at,
                             timer->started_at, timer->skipped, timer->completed,
                             timer->errors, timer->last_wait_ms, timer->last_duration_ms});
        }
        pending = deadlines.size();
        registered_copy = registered;
        completed_copy = completed;
        errors_copy = errors;
        skipped_copy = skipped;
        wakeups_copy = wakeups;
        dispatched_copy = dispatched;
        canceled_copy = canceled;
        total_duration = total_duration_ms;
        stopped_copy = stopped;
        runtime_ms = milliseconds((stopped ? stopped_at : now) - created);
    }

    const nlohmann::json executor_status = executor
        ? executor->snapshot() : nlohmann::json::object();

    auto tasks = nlohmann::json::array();
    double wait_total = 0, wait_max = 0, wait_min = 0;
    size_t wait_count = 0;
    for (const auto& view : views) {
        const double wait = view.last_wait_ms;
        if (wait > 0) {
            wait_total += wait;
            wait_max = std::max(wait_max, wait);
            wait_min = wait_count ? std::min(wait_min, wait) : wait;
            ++wait_count;
        }
        tasks.push_back({
            {"task_id", view.id},
            {"name", view.name},
            {"state", view.phase},
            {"cancelled", view.canceled},
            {"period_ms", view.interval.count()},
            {"queued_elapsed_ms", view.phase == "queued"
                ? milliseconds(now - view.queued_at) : 0.0},
            {"running_elapsed_ms", view.phase == "running"
                ? milliseconds(now - view.started_at) : 0.0},
            {"skipped_runs", view.skipped},
            {"completed_runs", view.completed},
            {"error_count", view.errors},
            {"last_queue_wait_ms", wait},
            {"last_duration_ms", view.last_duration_ms},
        });
    }

    auto worker_stats = nlohmann::json::array();
    double total_load = 0, max_load = 0, min_load = 100;
    double total_worker_duration = 0;
    size_t worker_count = 0;
    if (executor_status.is_object() && executor_status.contains("workers")) {
        for (const auto& worker : executor_status.at("workers")) {
            const auto completed_turns = worker.value("completed_turns", 0ull);
            const double worker_total = worker.value("total_duration_ms", 0.0);
            const double running = worker.value("running_elapsed_ms", 0.0);
            const double load = runtime_ms > 0 ? (worker_total + running) * 100 / runtime_ms : 0;
            total_load += load;
            max_load = std::max(max_load, load);
            min_load = std::min(min_load, load);
            total_worker_duration += worker_total;
            ++worker_count;
            worker_stats.push_back({
                {"workerIndex", worker.value("worker_index", 0ull)},
                {"tasksProcessed", completed_turns},
                {"tasksFailed", worker.value("failed_turns", 0ull)},
                {"maxExecutionTimeMs", worker.value("max_duration_ms", 0.0)},
                {"averageExecutionTimeMs", completed_turns
                    ? worker_total / static_cast<double>(completed_turns) : 0.0},
                {"isBusy", worker.value("busy", false)},
                {"name", worker.value("name", std::string{})},
                {"task_id", worker.contains("task_id") ? worker.at("task_id") : nlohmann::json(nullptr)},
                {"running_elapsed_ms", running},
                {"minExecutionTimeMs", 0.0},
                {"loadPercentage", load},
            });
        }
    }

    const size_t active_workers = executor_status.value("active_workers", 0ull);
    const size_t current_queued = executor_status.value("currentQueuedTasks", 0ull);
    const size_t max_queue = executor_status.value("maxQueueSize", 0ull);
    const size_t worker_count_value = executor_status.value("worker_count", 0ull);

    return {
        {"stopped", stopped_copy},
        {"worker_count", worker_count_value},
        {"totalTasksRegistered", registered_copy},
        {"totalTasksProcessed", completed_copy},
        {"totalTasksFailed", errors_copy},
        {"currentTimerTasks", pending},
        {"currentQueuedTasks", current_queued},
        {"maxQueueSize", max_queue},
        {"totalQueueOperations", dispatched_copy},
        {"skippedRuns", skipped_copy},
        {"canceledTimers", canceled_copy},
        {"schedulerRuntime", runtime_ms},
        {"schedulerWakeups", wakeups_copy},
        {"scheduling_semantics_version", 2},
        {"schedulerStats", {
            {"pendingTasks", pending},
            {"activeWorkers", active_workers},
            {"averageWaitMs", wait_count ? wait_total / wait_count : 0.0},
            {"maxWaitMs", wait_max},
            {"minWaitMs", wait_count ? wait_min : 0.0},
        }},
        {"averageThreadLoad", worker_count ? total_load / worker_count : 0.0},
        {"maxThreadLoad", max_load},
        {"minThreadLoad", worker_count ? min_load : 0.0},
        {"averageTaskCompletionTimeMs", completed_copy
            ? total_duration / static_cast<double>(completed_copy) : 0.0},
        {"workerStats", std::move(worker_stats)},
        {"tasks", std::move(tasks)},
        {"executor", executor_status},
    };
}

TimerScheduler::TimerScheduler(TaskExecutor& executor)
    : state_(std::make_shared<State>())
{
    state_->executor = &executor;
    service_thread_ = std::thread([this] { serviceLoop(); });
}

TimerScheduler::~TimerScheduler()
{
    shutdown();
}

size_t TimerScheduler::addTimer(Duration delay, bool repeat, Task task,
                                WorkHandle work, RequestPolicy policy,
                                std::string name, bool callback_timer)
{
    if (repeat && delay.count() <= 0)
        throw std::invalid_argument("TimerScheduler requires a positive repeat interval");
    if (!repeat && delay.count() < 0)
        throw std::invalid_argument("TimerScheduler requires a non-negative delay");
    if (callback_timer && !task)
        throw std::invalid_argument("TimerScheduler requires a callback");

    auto timer = std::make_shared<State::Timer>();
    timer->name = std::move(name);
    timer->callback_timer = callback_timer;
    timer->repeat = repeat;
    timer->interval = delay;
    timer->task = std::move(task);
    timer->policy = policy;
    timer->next = Clock::now() + (repeat ? Duration(0) : delay);

    std::lock_guard lock(state_->mutex);
    if (state_->stopped) throw std::logic_error("TimerScheduler is stopped");

    timer->id = state_->next_id++;
    if (callback_timer) {
        auto weak = std::weak_ptr<State::Timer>(timer);
        auto state = state_;
        auto handle = state->executor->registerTask(timer->name, [state, weak] {
            auto timer = weak.lock();
            if (!timer) return WorkResult::Complete;

            bool canceled = false;
            {
                std::lock_guard lock(state->mutex);
                if (timer->canceled) {
                    canceled = true;
                } else {
                    timer->phase = "running";
                    timer->started_at = Clock::now();
                    timer->last_wait_ms = timer->queued_at == TimePoint{}
                        ? 0.0 : milliseconds(timer->started_at - timer->queued_at);
                    timer->queued_at = {};
                    ++state->inflight;
                }
            }
            if (canceled) return WorkResult::Complete;

            std::exception_ptr failure;
            try {
                timer->task();
            } catch (...) {
                failure = std::current_exception();
            }

            bool retire = false;
            {
                std::lock_guard lock(state->mutex);
                const auto now = Clock::now();
                timer->phase = "idle";
                timer->last_duration_ms = milliseconds(now - timer->started_at);
                ++timer->completed;
                ++state->completed;
                state->total_duration_ms += timer->last_duration_ms;
                if (failure) {
                    ++timer->errors;
                    ++state->errors;
                }
                if (!timer->repeat || timer->canceled) {
                    retire = true;
                    state->timers.erase(timer->id);
                    state->deadlines.erase({timer->next, timer->id});
                    timer->phase = "cancelled";
                }
                --state->inflight;
                state->drained.notify_all();
            }

            if (retire) state->executor->invalidate(timer->work);
            if (failure) std::rethrow_exception(failure);
            return WorkResult::Complete;
        });
        if (!handle)
            throw std::runtime_error("TimerScheduler: shared executor has no free task slot");
        timer->work = *handle;
    } else {
        timer->work = work;
    }

    state_->timers.emplace(timer->id, timer);
    state_->deadlines.emplace(std::make_pair(timer->next, timer->id), timer);
    ++state_->registered;
    state_->changed.notify_all();
    return timer->id;
}

size_t TimerScheduler::registerTimer(Duration delay, Task task, std::string name)
{
    return addTimer(delay, false, std::move(task), {}, RequestPolicy::Remember,
                    std::move(name), true);
}

size_t TimerScheduler::registerRepeatingTimer(Duration interval, Task task, std::string name)
{
    return addTimer(interval, true, std::move(task), {}, RequestPolicy::SkipIfBusy,
                    std::move(name), true);
}

size_t TimerScheduler::registerWorkTimer(Duration delay, WorkHandle work,
                                         RequestPolicy policy, std::string name)
{
    return addTimer(delay, false, {}, work, policy, std::move(name), false);
}

size_t TimerScheduler::registerRepeatingWorkTimer(Duration interval,
                                                  WorkHandle work,
                                                  RequestPolicy policy, std::string name)
{
    return addTimer(interval, true, {}, work, policy, std::move(name), false);
}

bool TimerScheduler::triggerTimer(size_t id)
{
    return rescheduleTimer(id, Duration(0));
}

bool TimerScheduler::rescheduleTimer(size_t id, Duration delay)
{
    if (delay.count() < 0) return false;
    std::lock_guard lock(state_->mutex);
    const auto it = state_->timers.find(id);
    if (it == state_->timers.end() || it->second->canceled) return false;
    auto& timer = it->second;
    state_->deadlines.erase({timer->next, id});
    timer->next = Clock::now() + delay;
    state_->deadlines.emplace(std::make_pair(timer->next, id), timer);
    state_->changed.notify_all();
    return true;
}

bool TimerScheduler::cancelTimer(size_t id)
{
    std::shared_ptr<State::Timer> timer;
    bool has_slot = false;
    bool running = false;
    {
        std::lock_guard lock(state_->mutex);
        const auto it = state_->timers.find(id);
        if (it == state_->timers.end() || it->second->canceled) return false;
        timer = it->second;
        timer->canceled = true;
        state_->deadlines.erase({timer->next, id});
        ++state_->canceled;
        running = timer->phase == "running";
        has_slot = timer->callback_timer;
        if (!running) state_->timers.erase(it);
        state_->changed.notify_all();
    }

    if (has_slot) {
        state_->executor->invalidate(timer->work);
    } else {
        state_->executor->cancelPending(timer->work, RequestOrigin::Timer);
    }
    return true;
}

void TimerScheduler::serviceLoop()
{
    std::unique_lock lock(state_->mutex);
    while (!state_->stopped) {
        if (state_->deadlines.empty()) {
            state_->changed.wait(lock);
            continue;
        }
        const auto next = state_->deadlines.begin()->first.first;
        const auto now = Clock::now();
        if (next > now) {
            state_->changed.wait_until(lock, next);
            continue;
        }

        auto timer = state_->deadlines.begin()->second;
        state_->deadlines.erase(state_->deadlines.begin());
        ++state_->wakeups;
        if (timer->canceled) continue;

        const bool accepted = state_->executor->request(
            timer->work, timer->policy, RequestOrigin::Timer);
        if (accepted) {
            timer->phase = "queued";
            timer->queued_at = now;
            ++state_->dispatched;
        } else {
            ++timer->skipped;
            ++state_->skipped;
        }

        if (timer->repeat) {
            timer->next = now + timer->interval;
            state_->deadlines.emplace(std::make_pair(timer->next, timer->id), timer);
        } else if (!accepted) {
            state_->timers.erase(timer->id);
        }

        // Under overload every deadline can remain due. Release the timing
        // lock once per dispatch so registration, cancellation and status
        // queries are not starved.
        lock.unlock();
        std::this_thread::yield();
        lock.lock();
    }
}

void TimerScheduler::shutdown()
{
    std::lock_guard shutdown_lock(shutdown_mutex_);
    std::vector<WorkHandle> slots;
    std::vector<WorkHandle> work_timers;
    {
        std::lock_guard lock(state_->mutex);
        if (!state_->stopped) {
            state_->stopped = true;
            state_->deadlines.clear();
            for (auto& [id, timer] : state_->timers) {
                (void)id;
                timer->canceled = true;
                if (timer->callback_timer) slots.push_back(timer->work);
                else work_timers.push_back(timer->work);
            }
        }
    }
    state_->changed.notify_all();

    if (service_thread_.joinable()) service_thread_.join();
    for (const auto& handle : slots) state_->executor->invalidate(handle);
    for (const auto& handle : work_timers)
        state_->executor->cancelPending(handle, RequestOrigin::Timer);

    std::unique_lock lock(state_->mutex);
    state_->drained.wait(lock, [this] { return state_->inflight == 0; });
    state_->timers.clear();
    state_->deadlines.clear();
    if (state_->stopped_at == TimePoint{}) state_->stopped_at = Clock::now();
}

nlohmann::json TimerScheduler::getPerformanceStats()
{
    return state_->snapshot();
}

TimerScheduler::StatusReader TimerScheduler::statusReader() const
{
    return [state = state_] { return state->snapshot(); };
}
