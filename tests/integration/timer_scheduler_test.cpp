#include "common/timer_scheduler.hpp"
#include <atomic>
#include <future>
#include <iostream>
#include <stdexcept>

using namespace std::chrono_literals;

static void check(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

// Release callbacks before destroying the scheduler, including on assertion failure.
struct Gate {
    std::promise<void> promise;
    std::shared_future<void> released = promise.get_future().share();
    bool open = false;
    void release() { if (!open) { open = true; promise.set_value(); } }
    ~Gate() { release(); }
};

struct Finish {
    TimerScheduler& scheduler;
    Gate& gate;
    ~Finish() { gate.release(); scheduler.shutdown(); }
};

template<class Predicate>
static void eventually(Predicate predicate, const char* message) {
    const auto end = std::chrono::steady_clock::now() + 2s;
    while (!predicate()) {
        check(std::chrono::steady_clock::now() < end, message);
        std::this_thread::sleep_for(2ms);
    }
}

static nlohmann::json task_status(TimerScheduler& scheduler, const std::string& name) {
    const auto status = scheduler.getPerformanceStats();
    for (const auto& task : status.at("tasks"))
        if (task.at("name") == name) return task;
    throw std::runtime_error("missing task: " + name);
}

static void repeating_callbacks_do_not_overlap() {
    TimerScheduler scheduler(3);
    Gate gate;
    std::atomic<unsigned> entered{0};
    std::promise<void> first;
    auto started = first.get_future();
    Finish finish{scheduler, gate};
    const auto slow = scheduler.registerRepeatingTimer(10ms, [&] {
        if (++entered == 1) first.set_value();
        gate.released.wait();
    }, "slow");
    check(started.wait_for(2s) == std::future_status::ready, "slow callback did not start");
    // Several deadlines must elapse while the callback is deliberately blocked.
    std::this_thread::sleep_for(100ms);
    const auto overlapping = entered.load();
    scheduler.cancelTimer(slow);
    gate.release();
    scheduler.shutdown();
    check(overlapping == 1, "repeating timer dispatched overlapping callbacks");
}

static void queued_ticks_are_skipped_and_cancellation_removes_work() {
    TimerScheduler scheduler(1);
    Gate gate;
    std::promise<void> first;
    auto started = first.get_future();
    std::atomic<unsigned> cancelled_calls{0};
    Finish finish{scheduler, gate};
    scheduler.registerTimer(0ms, [&] { first.set_value(); gate.released.wait(); }, "blocker");
    check(started.wait_for(2s) == std::future_status::ready, "blocker did not start");
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
    auto sentinel = std::make_shared<std::promise<void>>();
    auto ran = sentinel->get_future();
    scheduler.registerTimer(0ms, [sentinel] { sentinel->set_value(); }, "sentinel");
    gate.release();
    check(ran.wait_for(2s) == std::future_status::ready, "queue did not recover");
    check(cancelled_calls == 0, "cancelled queued callback executed");
}

static void slow_timer_does_not_starve_another_timer() {
    TimerScheduler scheduler(2);
    Gate gate;
    std::atomic<unsigned> fast{0};
    std::promise<void> first;
    auto started = first.get_future();
    Finish finish{scheduler, gate};
    scheduler.registerRepeatingTimer(5ms, [&] { first.set_value(); gate.released.wait(); }, "slow");
    check(started.wait_for(2s) == std::future_status::ready, "slow timer did not start");
    scheduler.registerRepeatingTimer(5ms, [&] { ++fast; }, "fast");
    eventually([&] { return fast >= 5; }, "slow timer exhausted workers needed by another timer");
    const auto slow = task_status(scheduler, "slow");
    check(slow.at("state") == "running" && slow.at("skipped_runs").get<size_t>() > 0,
          "running timer did not expose skipped ticks");
    check(slow.at("running_elapsed_ms").get<double>() > 0, "running elapsed time missing");
}

static void exceptions_release_inflight_and_status_survives_destruction() {
    TimerScheduler::StatusReader reader;
    {
        TimerScheduler scheduler(1);
        Gate gate;
        std::atomic<unsigned> calls{0};
        Finish finish{scheduler, gate};
        reader = scheduler.statusReader();
        const auto id = scheduler.registerRepeatingTimer(5ms, [&] {
            const auto count = ++calls;
            if (count == 1) throw std::runtime_error("expected failure");
            if (count == 2) throw 42;
            gate.released.wait();
        }, "throws");
        eventually([&] { return calls >= 3; }, "throwing callback permanently blocked future ticks");
        check(task_status(scheduler, "throws").at("error_count") == 2, "exception count incorrect");
        scheduler.cancelTimer(id);
        gate.release();
    }
    const auto stopped = reader();
    check(stopped.at("stopped") == true && stopped.at("tasks").empty(), "stale task after destruction");
    check(stopped.at("totalTasksFailed") == 2, "failed runs were lost after shutdown");
    check(stopped.at("schedulerStats").at("activeWorkers") == 0, "worker stayed active after shutdown");
}

static void overdue_deadlines_allow_control_and_worker_progress() {
    std::atomic<size_t> completed{0};
    TimerScheduler scheduler(2);
    for (size_t i = 0; i < 4096; ++i)
        scheduler.registerRepeatingTimer(1ms, [&] { ++completed; }, "overload");
    const auto before = completed.load();
    eventually([&] { return completed > before + 10; }, "overdue timers monopolized the worker queue");
    const auto status = scheduler.getPerformanceStats();
    check(status.at("tasks").size() == 4096, "overload status omitted registered timers");
    check(status.at("currentQueuedTasks").get<size_t>() <= 4096, "overload grew duplicate callbacks");
    scheduler.shutdown();
}

int main() {
    try {
        repeating_callbacks_do_not_overlap();
        queued_ticks_are_skipped_and_cancellation_removes_work();
        slow_timer_does_not_starve_another_timer();
        exceptions_release_inflight_and_status_survives_destruction();
        overdue_deadlines_allow_control_and_worker_progress();
        std::cout << "PASS timer deduplication, bounded pending work, progress, cancellation and status\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
