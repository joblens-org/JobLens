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
#include "common/timer_scheduler.hpp"

#include <atomic>
#include <chrono>
#include <fstream>
#include <future>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::chrono_literals;

static void check(bool value, const std::string& message)
{
    if (!value) throw std::runtime_error(message);
}

struct Gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool opened = false;

    void release()
    {
        std::lock_guard lock(mutex);
        if (opened) return;
        opened = true;
        cv.notify_all();
    }

    void open() { release(); }

    void wait()
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return opened; });
    }

    bool wait_for(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [this] { return opened; });
    }

    ~Gate() { release(); }
};

struct Finish {
    TimerScheduler& scheduler;
    Gate& gate;
    ~Finish()
    {
        gate.release();
        scheduler.shutdown();
    }
};

template <class Predicate>
static void eventually(Predicate predicate, const std::string& message,
                       std::chrono::milliseconds timeout = 2s)
{
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        check(std::chrono::steady_clock::now() < end, message);
        std::this_thread::sleep_for(2ms);
    }
}

static nlohmann::json task_status(TimerScheduler& scheduler, const std::string& name)
{
    const auto status = scheduler.getPerformanceStats();
    for (const auto& task : status.at("tasks"))
        if (task.at("name") == name) return task;
    throw std::runtime_error("missing task: " + name);
}

static size_t thread_count()
{
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0)
            return static_cast<size_t>(std::stoul(line.substr(8)));
    }
    return 0;
}

// T2: retain the existing repeating-timer serialization and busy-tick
// suppression while the callback runs on the injected executor.
static void repeating_callbacks_do_not_overlap()
{
    TaskExecutor executor({3, 128});
    Gate gate;
    TimerScheduler scheduler(executor);
    std::atomic<unsigned> entered{0};
    Gate started;
    Finish finish{scheduler, gate};
    const auto slow = scheduler.registerRepeatingTimer(10ms, [&] {
        if (++entered == 1) started.open();
        gate.wait();
    }, "slow");
    check(started.wait_for(2s), "slow callback did not start");
    std::this_thread::sleep_for(100ms);
    const auto overlapping = entered.load();
    scheduler.cancelTimer(slow);
    gate.release();
    scheduler.shutdown();
    check(overlapping == 1, "repeating timer dispatched overlapping callbacks");
}

static void queued_ticks_are_skipped_and_cancellation_removes_work()
{
    TaskExecutor executor({1, 128});
    Gate gate;
    TimerScheduler scheduler(executor);
    Gate blocker_started;
    std::atomic<unsigned> cancelled_calls{0};
    Finish finish{scheduler, gate};
    scheduler.registerTimer(0ms, [&] { blocker_started.open(); gate.wait(); }, "blocker");
    check(blocker_started.wait_for(2s), "blocker did not start");
    const auto pending = scheduler.registerRepeatingTimer(5ms, [&] { ++cancelled_calls; }, "queued");
    eventually([&] {
        const auto status = scheduler.getPerformanceStats();
        for (const auto& task : status.at("tasks"))
            if (task.at("name") == "queued") return task.at("skipped_runs").get<size_t>() >= 3;
        return false;
    }, "queued timer never skipped a tick");
    const auto status = scheduler.getPerformanceStats();
    check(status.at("currentQueuedTasks") == 1, "repeating ticks grew the ready queue");
    check(status.at("schedulerStats").at("activeWorkers") == 1, "active workers incorrect");
    check(task_status(scheduler, "queued").at("queued_elapsed_ms").get<double>() >= 10,
          "queue age does not measure time waiting for a worker");
    check(task_status(scheduler, "blocker").at("state") == "running", "missing blocker identity");
    check(status.at("workerStats").at(0).at("name") == "blocker", "missing worker identity");
    check(scheduler.cancelTimer(pending), "cancel queued timer failed");
    check(!scheduler.cancelTimer(pending), "repeat cancellation must report false");
    check(scheduler.getPerformanceStats().at("currentQueuedTasks") == 0, "cancel left queued work");
    auto sentinel = std::make_shared<Gate>();
    scheduler.registerTimer(0ms, [sentinel] { sentinel->open(); }, "sentinel");
    gate.release();
    check(sentinel->wait_for(2s), "queue did not recover");
    check(cancelled_calls == 0, "cancelled queued callback executed");
}

static void slow_timer_does_not_starve_another_timer()
{
    TaskExecutor executor({2, 128});
    Gate gate;
    TimerScheduler scheduler(executor);
    std::atomic<unsigned> fast{0};
    Gate slow_started;
    Finish finish{scheduler, gate};
    scheduler.registerRepeatingTimer(5ms, [&] { slow_started.open(); gate.wait(); }, "slow");
    check(slow_started.wait_for(2s), "slow timer did not start");
    scheduler.registerRepeatingTimer(5ms, [&] { ++fast; }, "fast");
    eventually([&] { return fast >= 5; }, "slow timer exhausted workers needed by another timer");
    // A skipped tick is observable, but *when* the overlap is noticed depends on
    // scheduler jitter; asserting it at one instant made this case fail under
    // load. Wait for the evidence with a deadline instead of assuming it is
    // already there.
    eventually([&] { return task_status(scheduler, "slow").at("skipped_runs").get<size_t>() > 0; },
               "running timer did not expose skipped ticks");
    const auto slow = task_status(scheduler, "slow");
    check(slow.at("state") == "running", "a timer that skipped a tick must stay running");
    check(slow.at("running_elapsed_ms").get<double>() > 0, "running elapsed time missing");
}

static void exceptions_release_inflight_and_status_survives_destruction()
{
    TaskExecutor executor({1, 128});
    TimerScheduler::StatusReader reader;
    {
        Gate gate;
        TimerScheduler scheduler(executor);
        std::atomic<unsigned> calls{0};
        reader = scheduler.statusReader();
        const auto id = scheduler.registerRepeatingTimer(5ms, [&] {
            const auto count = ++calls;
            if (count == 1) throw std::runtime_error("expected failure");
            if (count == 2) throw 42;
            gate.wait();
        }, "throws");
        eventually([&] { return calls >= 3; }, "throwing callback permanently blocked future ticks");
        check(task_status(scheduler, "throws").at("error_count") == 2, "exception count incorrect");
        scheduler.cancelTimer(id);
        gate.release();
        scheduler.shutdown();
    }
    const auto stopped = reader();
    check(stopped.at("stopped") == true && stopped.at("tasks").empty(), "stale task after destruction");
    check(stopped.at("totalTasksFailed") == 2, "failed runs were lost after shutdown");
    check(stopped.at("schedulerStats").at("activeWorkers") == 0, "worker stayed active after shutdown");
    executor.shutdown();
}

static void overdue_deadlines_allow_control_and_worker_progress()
{
    TaskExecutor executor({2, 4096});
    std::atomic<size_t> completed{0};
    TimerScheduler scheduler(executor);
    for (size_t i = 0; i < 4096; ++i)
        scheduler.registerRepeatingTimer(1ms, [&] { ++completed; }, "overload");
    const auto before = completed.load();
    eventually([&] { return completed > before + 10; }, "overdue timers monopolized the worker queue");
    const auto status = scheduler.getPerformanceStats();
    check(status.at("tasks").size() == 4096, "overload status omitted registered timers");
    check(status.at("currentQueuedTasks").get<size_t>() <= 4096, "overload grew duplicate callbacks");
    scheduler.shutdown();
    executor.shutdown();
}

// T1: the timer service uses the caller's executor and does not create a second
// worker pool.
static void injected_executor_ownership()
{
    TaskExecutor executor({2, 64});
    const size_t before = thread_count();
    TimerScheduler scheduler(executor);
    const size_t after = thread_count();
    check(after <= before + 1, "TimerScheduler created an extra worker pool");

    std::atomic<int> runs{0};
    scheduler.registerRepeatingTimer(5ms, [&] { ++runs; }, "owned");
    eventually([&] { return runs.load() > 0; }, "timer callback did not run on injected executor");

    const auto timer = scheduler.getPerformanceStats();
    const auto pool = executor.snapshot();
    check(timer.at("worker_count") == 2, "timer status lost injected worker count");
    check(pool.at("worker_count") == 2, "executor worker count changed");
    check(timer.at("executor").at("worker_count") == 2, "executor observation missing");

    scheduler.shutdown();
    executor.shutdown();
}

// T3: withdrawing a timer request must not remove caller/event work queued on
// the same lane.
static void timer_cancellation_preserves_caller_work()
{
    Gate blocker_entered;
    Gate release_blocker;
    TaskExecutor executor({1, 64});
    std::atomic<int> lane_runs{0};

    auto blocker = executor.registerTask("blocker", [&] {
        blocker_entered.open();
        release_blocker.wait();
        return WorkResult::Complete;
    });
    auto lane = executor.registerTask("lane", [&] {
        ++lane_runs;
        return WorkResult::Complete;
    });
    check(blocker.has_value() && lane.has_value(), "T3 register");
    check(executor.request(*blocker, RequestPolicy::Remember), "T3 blocker request");
    check(blocker_entered.wait_for(3s), "T3 blocker entered");

    TimerScheduler scheduler(executor);
    const auto timer = scheduler.registerRepeatingWorkTimer(
        5ms, *lane, RequestPolicy::SkipIfBusy, "lane-timer");
    eventually([&] {
        const auto status = executor.snapshot();
        for (const auto& task : status.at("tasks"))
            if (task.at("name") == "lane") return task.at("state") == "queued";
        return false;
    }, "T3 timer request was not queued on the lane");
    check(executor.request(*lane, RequestPolicy::Remember), "T3 caller request");
    check(scheduler.cancelTimer(timer), "T3 cancel timer");

    bool still_queued = false;
    const auto lane_status = executor.snapshot();
    for (const auto& task : lane_status.at("tasks"))
        if (task.at("name") == "lane") still_queued = task.at("state") == "queued";
    check(still_queued, "T3 timer cancellation removed caller work");

    release_blocker.open();
    eventually([&] { return lane_runs.load() == 1; }, "T3 caller work did not run");
    std::this_thread::sleep_for(30ms);
    check(lane_runs.load() == 1, "T3 canceled timer ran again");
    check(scheduler.getPerformanceStats().at("canceledTimers") == 1,
          "T3 canceled timer not observed");
    scheduler.shutdown();
    executor.shutdown();
}

// T4: repeatedly changing a deadline keeps one active deadline per timer.
static void reschedule_replaces_deadline()
{
    Gate blocker_entered;
    Gate release_blocker;
    TaskExecutor executor({1, 64});
    auto blocker = executor.registerTask("blocker", [&] {
        blocker_entered.open();
        release_blocker.wait();
        return WorkResult::Complete;
    });
    check(blocker.has_value(), "T4 register blocker");
    check(executor.request(*blocker, RequestPolicy::Remember), "T4 blocker request");
    check(blocker_entered.wait_for(3s), "T4 blocker entered");

    TimerScheduler scheduler(executor);
    const auto timer = scheduler.registerRepeatingTimer(1000ms, [] {}, "reschedule");
    for (int index = 0; index < 100; ++index)
        check(scheduler.triggerTimer(timer), "T4 trigger timer");

    const auto status = scheduler.getPerformanceStats();
    check(status.at("currentTimerTasks") == 1, "T4 accumulated stale deadlines");
    check(status.at("tasks").size() == 1, "T4 duplicated timer entries");

    release_blocker.open();
    scheduler.shutdown();
    executor.shutdown();
}

// T5: cancelling a running callback lets it finish, but never revives its
// deadline or releases the callback while it is still executing.
static void running_cancellation_does_not_revive()
{
    Gate entered;
    Gate release;
    TaskExecutor executor({1, 64});
    std::atomic<int> runs{0};
    auto capture = std::make_shared<int>(7);
    std::weak_ptr<int> observer = capture;

    TimerScheduler scheduler(executor);
    const auto timer = scheduler.registerRepeatingTimer(5ms, [&, capture] {
        ++runs;
        entered.open();
        release.wait();
    }, "running");
    check(entered.wait_for(3s), "T5 running callback did not start");
    check(scheduler.cancelTimer(timer), "T5 cancel running timer");
    check(!scheduler.cancelTimer(timer), "T5 repeated cancellation");
    capture.reset();
    check(!observer.expired(), "T5 callback capture released before completion");

    release.open();
    eventually([&] { return observer.expired(); }, "T5 callback capture was not released");
    std::this_thread::sleep_for(30ms);
    check(runs.load() == 1, "T5 canceled running timer revived");
    check(scheduler.getPerformanceStats().at("tasks").empty(), "T5 cancelled timer status remained");
    scheduler.shutdown();
    executor.shutdown();
}

// T6: deadline due time is observable separately from worker start under pool
// saturation.
static void deadline_versus_execution_start()
{
    Gate blocker_entered;
    Gate release_blocker;
    TaskExecutor executor({1, 64});
    auto blocker = executor.registerTask("blocker", [&] {
        blocker_entered.open();
        release_blocker.wait();
        return WorkResult::Complete;
    });
    check(blocker.has_value(), "T6 register blocker");
    check(executor.request(*blocker, RequestPolicy::Remember), "T6 blocker request");
    check(blocker_entered.wait_for(3s), "T6 blocker entered");

    TimerScheduler scheduler(executor);
    Gate fired;
    const auto waiting = scheduler.registerRepeatingTimer(
        1000ms, [&] { fired.open(); }, "waiting");
    eventually([&] {
        const auto status = task_status(scheduler, "waiting");
        return status.at("state") == "queued" &&
               status.at("queued_elapsed_ms").get<double>() >= 10;
    }, "T6 deadline did not wait independently of execution");

    release_blocker.open();
    check(fired.wait_for(3s), "T6 queued timer did not run");
    eventually([&] {
        return task_status(scheduler, "waiting").at("completed_runs") == 1;
    }, "T6 timer completion was not observed");
    check(task_status(scheduler, "waiting").at("last_queue_wait_ms").get<double>() >= 10,
          "T6 queue wait was not measured");
    scheduler.cancelTimer(waiting);
    scheduler.shutdown();
    executor.shutdown();
}

// T7: ordinary sampling can stop while a finish deadline still runs and the
// shared executor remains available.
static void finish_timers_survive_sampling_cancel()
{
    TaskExecutor executor({1, 64});
    TimerScheduler scheduler(executor);
    std::atomic<int> sampling{0};
    Gate finished;

    const auto sampling_timer = scheduler.registerRepeatingTimer(
        5ms, [&] { ++sampling; }, "sampling");
    scheduler.registerTimer(30ms, [&] { finished.open(); }, "finish");
    check(scheduler.cancelTimer(sampling_timer), "T7 cancel sampling timer");
    const int sampling_before = sampling.load();

    check(finished.wait_for(3s),
          "T7 finish deadline did not run after sampling stopped");
    check(sampling.load() == sampling_before, "T7 sampling timer ran after cancellation");
    check(scheduler.getPerformanceStats().at("stopped") == false,
          "T7 scheduler stopped before finish completed");
    scheduler.shutdown();
    executor.shutdown();
}

int main()
{
    try {
        repeating_callbacks_do_not_overlap();
        queued_ticks_are_skipped_and_cancellation_removes_work();
        slow_timer_does_not_starve_another_timer();
        exceptions_release_inflight_and_status_survives_destruction();
        overdue_deadlines_allow_control_and_worker_progress();
        injected_executor_ownership();
        timer_cancellation_preserves_caller_work();
        reschedule_replaces_deadline();
        running_cancellation_does_not_revive();
        deadline_versus_execution_start();
        finish_timers_survive_sampling_cancel();
        std::cout << "PASS timer injection, deadlines, suppression, cancellation, "
                     "rescheduling, finish deadlines and status\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
