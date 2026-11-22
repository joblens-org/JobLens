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
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

void check(bool condition, const std::string& message)
{
    if (!condition) throw std::runtime_error(message);
}

template <typename Predicate>
void eventually(Predicate predicate, const std::string& message,
                std::chrono::milliseconds timeout = 3000ms)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline)
            throw std::runtime_error("timeout: " + message);
        std::this_thread::sleep_for(1ms);
    }
}

class Gate {
public:
    void open()
    {
        std::lock_guard lock(mutex_);
        open_ = true;
        cv_.notify_all();
    }

    bool wait_for(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return open_; });
    }

    void wait()
    {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return open_; });
    }

    ~Gate() { open(); }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool open_ = false;
};

nlohmann::json task_status(TaskExecutor& executor, const std::string& name)
{
    const auto status = executor.snapshot();
    for (const auto& task : status.at("tasks")) {
        if (task.at("name") == name) return task;
    }
    throw std::runtime_error("missing task status: " + name);
}

bool task_idle(TaskExecutor& executor, const std::string& name)
{
    const auto status = task_status(executor, name);
    return status.at("state") == "idle";
}

// X1: init/hooks/collect/reads/finalize all share the configured worker cap.
void x1_global_worker_cap()
{
    for (size_t workers : {1u, 2u, 4u}) {
        TaskExecutor executor({workers, 32});
        Gate release;
        std::mutex mutex;
        std::condition_variable entered_cv;
        int active = 0;
        int peak = 0;
        int completed = 0;

        std::vector<WorkHandle> handles;
        for (int index = 0; index < 8; ++index) {
            auto handle = executor.registerTask(
                "cap-" + std::to_string(workers) + "-" + std::to_string(index),
                [&] {
                    {
                        std::lock_guard lock(mutex);
                        ++active;
                        peak = std::max(peak, active);
                        entered_cv.notify_all();
                    }
                    release.wait();
                    {
                        std::lock_guard lock(mutex);
                        --active;
                        ++completed;
                    }
                    return WorkResult::Complete;
                });
            check(handle.has_value(), "X1 register task");
            handles.push_back(*handle);
        }

        for (const auto& handle : handles)
            check(executor.request(handle, RequestPolicy::Remember), "X1 request task");

        {
            std::unique_lock lock(mutex);
            check(entered_cv.wait_for(lock, 3s, [&] { return active == static_cast<int>(workers); }),
                  "X1 expected workers to run concurrently");
            check(peak <= static_cast<int>(workers), "X1 worker cap exceeded");
        }
        release.open();
        eventually([&] {
            std::lock_guard lock(mutex);
            return completed == 8;
        }, "X1 all tasks completed");

        const auto status = executor.snapshot();
        check(status.at("worker_count") == workers, "X1 reported worker count");
        check(status.at("totalCompleted") == 8, "X1 reported completed turns");
        check(status.at("maxQueueSize") <= 8, "X1 ready queue bounded by registrations");
    }
}

// X2: one lane per registration, and Remember while running grants exactly one
// additional turn without overlapping the callback.
void x2_no_overlap_and_remember()
{
    TaskExecutor executor({2, 8});
    Gate first_entered;
    Gate release_first;
    std::atomic<int> inside{0};
    std::atomic<int> overlap{0};
    std::atomic<int> runs{0};

    auto handle = executor.registerTask("no-overlap", [&] {
        if (++inside > 1) ++overlap;
        const int run = ++runs;
        if (run == 1) {
            first_entered.open();
            release_first.wait();
        }
        --inside;
        return WorkResult::Complete;
    });
    check(handle.has_value(), "X2 register");

    check(executor.request(*handle, RequestPolicy::Remember), "X2 first request");
    check(first_entered.wait_for(3s), "X2 first callback entered");
    for (int index = 0; index < 100; ++index)
        check(executor.request(*handle, RequestPolicy::Remember), "X2 remember while running");
    release_first.open();

    eventually([&] {
        return task_status(executor, "no-overlap").at("completed_turns") == 2;
    }, "X2 second turn completed");
    check(runs.load() == 2, "X2 Remember must coalesce into one extra turn");
    check(overlap.load() == 0, "X2 callbacks overlapped on one registration");
    check(task_idle(executor, "no-overlap"), "X2 task returned to idle");
}

// X3: a flood of notification requests must not allocate one queue entry each.
void x3_notification_merging()
{
    TaskExecutor executor({1, 4});
    Gate entered;
    Gate release;
    std::atomic<int> runs{0};

    auto handle = executor.registerTask("merge", [&] {
        const int run = ++runs;
        if (run == 1) {
            entered.open();
            release.wait();
        }
        return WorkResult::Complete;
    });
    check(handle.has_value(), "X3 register");
    check(executor.request(*handle, RequestPolicy::Remember), "X3 first request");
    check(entered.wait_for(3s), "X3 first callback entered");

    constexpr int notifications = 1000000;
    for (int index = 0; index < notifications; ++index)
        check(executor.request(*handle, RequestPolicy::Remember), "X3 remember flood");

    check(executor.snapshot().at("currentQueuedTasks") == 0,
          "X3 running handle must not create queued copies");
    check(executor.snapshot().at("maxQueueSize") <= 1,
          "X3 ready queue must stay bounded by registrations");

    release.open();
    eventually([&] { return runs.load() == 2; }, "X3 coalesced second turn");
    eventually([&] { return task_idle(executor, "merge"); }, "X3 idle after second turn");

    const auto status = executor.snapshot();
    check(status.at("maxQueueSize") <= 1, "X3 queue grew with notification count");
    check(status.at("totalRememberedRequests") >= notifications,
          "X3 remembered requests were counted");
}

// X4: readiness arriving after the consumer's empty check must still produce a
// second turn without another notification.
void x4_remember_after_empty_check()
{
    TaskExecutor executor({1, 4});
    Gate first_entered;
    Gate release_first;
    std::atomic<bool> event{false};
    std::atomic<bool> consumed{false};
    std::atomic<int> pass{0};

    auto handle = executor.registerTask("two-phase", [&] {
        if (++pass == 1) {
            first_entered.open();
            release_first.wait();
            return WorkResult::Complete;
        }
        if (event.load()) consumed = true;
        return WorkResult::Complete;
    });
    check(handle.has_value(), "X4 register");
    check(executor.request(*handle, RequestPolicy::Remember), "X4 first request");
    check(first_entered.wait_for(3s), "X4 first pass entered");

    event = true;
    check(executor.request(*handle, RequestPolicy::Remember), "X4 remember after empty check");
    release_first.open();

    eventually([&] { return consumed.load(); }, "X4 event consumed by remembered turn");
    check(pass.load() == 2, "X4 exactly one remembered turn");
}

// X5: periodic ticks while queued or running are counted as skipped and do not
// create replay work.
void x5_periodic_suppression()
{
    {
        TaskExecutor executor({1, 4});
        Gate entered;
        Gate release;
        std::atomic<int> runs{0};

        auto handle = executor.registerTask("running-skip", [&] {
            if (++runs == 1) {
                entered.open();
                release.wait();
            }
            return WorkResult::Complete;
        });
        check(handle.has_value(), "X5 register running task");
        check(executor.request(*handle, RequestPolicy::SkipIfBusy), "X5 first tick");
        check(entered.wait_for(3s), "X5 callback entered");

        for (int index = 0; index < 1000; ++index)
            check(!executor.request(*handle, RequestPolicy::SkipIfBusy),
                  "X5 running tick must be skipped");
        release.open();
        eventually([&] { return task_idle(executor, "running-skip"); }, "X5 running task idle");
        check(runs.load() == 1, "X5 skipped ticks produced replay work");
        check(task_status(executor, "running-skip").at("skipped_requests") == 1000,
              "X5 skipped ticks counted");
    }

    {
        TaskExecutor executor({1, 4});
        Gate blocker_entered;
        Gate release_blocker;
        std::atomic<int> second_runs{0};

        auto blocker = executor.registerTask("blocker", [&] {
            blocker_entered.open();
            release_blocker.wait();
            return WorkResult::Complete;
        });
        auto second = executor.registerTask("queued-skip", [&] {
            ++second_runs;
            return WorkResult::Complete;
        });
        check(blocker.has_value() && second.has_value(), "X5 register queued tasks");
        check(executor.request(*blocker, RequestPolicy::Remember), "X5 request blocker");
        check(blocker_entered.wait_for(3s), "X5 blocker entered");
        check(executor.request(*second, RequestPolicy::SkipIfBusy), "X5 queue second task");
        for (int index = 0; index < 100; ++index)
            check(!executor.request(*second, RequestPolicy::SkipIfBusy),
                  "X5 queued tick must be skipped");
        check(executor.snapshot().at("currentQueuedTasks") == 1,
              "X5 queued tick must not duplicate work");

        release_blocker.open();
        eventually([&] { return second_runs.load() == 1; }, "X5 queued task ran once");
        check(task_status(executor, "queued-skip").at("skipped_requests") == 100,
              "X5 queued skips counted");
    }
}

// X6: continuation returns to the tail so another collector can progress.
void x6_fair_continuation()
{
    TaskExecutor executor({1, 4});
    Gate first_turn;
    Gate release_first;
    std::atomic<int> busy_runs{0};
    std::atomic<bool> other_ran{false};
    std::atomic<bool> order_violation{false};

    auto busy = executor.registerTask("continuation", [&] {
        const int run = ++busy_runs;
        if (run == 1) {
            first_turn.open();
            release_first.wait();
        } else if (!other_ran.load()) {
            order_violation = true;
        }
        std::this_thread::sleep_for(1ms);
        return WorkResult::Yield;
    });
    auto other = executor.registerTask("other", [&] {
        other_ran = true;
        return WorkResult::Complete;
    });
    check(busy.has_value() && other.has_value(), "X6 register");
    check(executor.request(*busy, RequestPolicy::Remember), "X6 request busy");
    check(first_turn.wait_for(3s), "X6 busy first turn entered");
    check(executor.request(*other, RequestPolicy::Remember), "X6 request other");
    release_first.open();

    eventually([&] { return other_ran.load(); }, "X6 other task starved by continuation");
    eventually([&] { return busy_runs.load() >= 2; }, "X6 busy continuation resumed");
    check(!order_violation.load(), "X6 continuation did not return to the tail");
    check(executor.invalidate(*busy), "X6 invalidate busy task");
    eventually([&] {
        return task_status(executor, "continuation").at("state") == "retired";
    }, "X6 invalidated task retired");
}

// X7: a multi-turn sweep processes each item once and keeps one sweep identity.
void x7_sweep_continuation()
{
    TaskExecutor executor({1, 4});
    constexpr int items = 8;
    Gate first_turn;
    Gate release_first;
    std::atomic<int> cursor{0};
    std::vector<int> processed;

    auto handle = executor.registerTask("sweep", [&] {
        const int item = cursor.fetch_add(1);
        if (item < items) processed.push_back(item);
        if (item == 0) {
            first_turn.open();
            release_first.wait();
        }
        return item + 1 < items ? WorkResult::Yield : WorkResult::Complete;
    });
    check(handle.has_value(), "X7 register");
    check(executor.request(*handle, RequestPolicy::SkipIfBusy), "X7 start sweep");
    check(first_turn.wait_for(3s), "X7 first turn entered");
    for (int index = 0; index < 50; ++index)
        check(!executor.request(*handle, RequestPolicy::SkipIfBusy),
              "X7 new tick during sweep must be skipped");
    release_first.open();

    eventually([&] { return task_idle(executor, "sweep"); }, "X7 sweep finished");
    check(static_cast<int>(processed.size()) == items, "X7 processed item count");
    for (int index = 0; index < items; ++index)
        check(processed[static_cast<size_t>(index)] == index, "X7 duplicate or reordered item");

    const auto status = task_status(executor, "sweep");
    check(status.at("yielded_turns") == items - 1, "X7 continuation count");
    check(status.at("skipped_requests") == 50, "X7 skipped sweep ticks");
}

// X8: with one worker, dynamic registration, self-request, invalidation and a
// finish-like cleanup must not wait for a task that needs the same worker.
void x8_one_worker_dependency()
{
    TaskExecutor executor({1, 8});
    std::atomic<bool> child_ran{false};
    std::atomic<bool> registered{false};
    std::atomic<int> self_runs{0};
    std::atomic<int> cleanup_runs{0};

    auto cleanup = executor.registerTask("cleanup", [&] {
        ++cleanup_runs;
        return WorkResult::Complete;
    });
    check(cleanup.has_value(), "X8 register cleanup task");

    WorkHandle self_handle{};
    auto self = executor.registerTask("self", [&] {
        const int run = ++self_runs;
        if (run == 1) {
            auto child = executor.registerTask("dynamic-child", [&] {
                child_ran = true;
                return WorkResult::Complete;
            });
            check(child.has_value(), "X8 dynamic registration");
            check(executor.request(*child, RequestPolicy::Remember), "X8 child request");
            check(executor.invalidate(*cleanup), "X8 cleanup from callback");
            check(executor.request(self_handle, RequestPolicy::Remember), "X8 self request");
            registered = true;
        }
        return WorkResult::Complete;
    });
    check(self.has_value(), "X8 register self task");
    self_handle = *self;

    check(executor.request(*self, RequestPolicy::Remember), "X8 start self task");
    eventually([&] { return registered.load() && child_ran.load(); },
               "X8 one-worker progress");
    eventually([&] { return self_runs.load() == 2; }, "X8 self-request turn");
    eventually([&] { return task_idle(executor, "self"); }, "X8 self idle");
    check(cleanup_runs.load() == 0, "X8 invalidated cleanup ran");
    check(task_status(executor, "cleanup").at("state") == "retired",
          "X8 cleanup retired");
}

// X9: an exception restores state; an independent Remember still runs once more.
void x9_exception_recovery()
{
    {
        TaskExecutor executor({1, 4});
        std::atomic<int> runs{0};
        auto handle = executor.registerTask("throws", [&] {
            if (++runs == 1) throw std::runtime_error("expected failure");
            return WorkResult::Complete;
        });
        check(handle.has_value(), "X9 register");
        check(executor.request(*handle, RequestPolicy::Remember), "X9 first request");
        eventually([&] {
            const auto status = task_status(executor, "throws");
            return status.at("failed_turns") == 1 && status.at("state") == "idle";
        }, "X9 failure restored idle state");

        const auto failed = executor.snapshot();
        check(failed.at("totalFailed") == 1, "X9 failure counter");
        check(executor.request(*handle, RequestPolicy::Remember), "X9 request after failure");
        eventually([&] { return runs.load() == 2; }, "X9 recovery turn");
        eventually([&] { return task_idle(executor, "throws"); }, "X9 recovery idle");
        check(executor.invalidate(*handle), "X9 cancel after recovery");
        check(!executor.request(*handle, RequestPolicy::Remember), "X9 cancelled handle rejected");
    }

    {
        TaskExecutor executor({1, 4});
        Gate entered;
        Gate release;
        std::atomic<int> runs{0};
        auto handle = executor.registerTask("throw-with-remember", [&] {
            const int run = ++runs;
            if (run == 1) {
                entered.open();
                release.wait();
                throw std::runtime_error("expected remembered failure");
            }
            return WorkResult::Complete;
        });
        check(handle.has_value(), "X9 register remembered failure");
        check(executor.request(*handle, RequestPolicy::Remember), "X9 start remembered failure");
        check(entered.wait_for(3s), "X9 failure entered");
        check(executor.request(*handle, RequestPolicy::Remember), "X9 remember during failure");
        release.open();

        eventually([&] { return runs.load() == 2; }, "X9 remembered work after exception");
        eventually([&] { return task_idle(executor, "throw-with-remember"); },
                   "X9 remembered failure idle");
        const auto status = task_status(executor, "throw-with-remember");
        check(status.at("failed_turns") == 1, "X9 remembered failure counted");
        check(status.at("completed_turns") == 1, "X9 remembered recovery counted");
    }
}

// X10: registration bounds, generation rejection and callback release.
void x10_generation_and_registration_bound()
{
    TaskExecutor executor({1, 2});
    auto capture = std::make_shared<int>(7);
    std::weak_ptr<int> observer = capture;
    std::atomic<int> runs{0};

    auto first = executor.registerTask("one", [&, capture] {
        ++runs;
        return WorkResult::Complete;
    });
    auto second = executor.registerTask("two", [&] { return WorkResult::Complete; });
    check(first.has_value() && second.has_value(), "X10 register two slots");
    check(!executor.registerTask("three", [&] { return WorkResult::Complete; }).has_value(),
          "X10 excess registration must fail");

    check(executor.invalidate(*first), "X10 invalidate first slot");
    capture.reset();
    check(observer.expired(), "X10 callback capture released on retire");

    auto third = executor.registerTask("three", [&] {
        ++runs;
        return WorkResult::Complete;
    });
    check(third.has_value(), "X10 retired slot reused");
    check(!executor.request(*first, RequestPolicy::Remember),
          "X10 stale generation must be rejected");
    check(executor.request(*third, RequestPolicy::Remember), "X10 reused slot request");
    eventually([&] { return runs.load() == 1; }, "X10 reused slot ran");

    executor.shutdown();
    check(executor.stopped(), "X10 stopped state");
    check(!executor.request(*third, RequestPolicy::Remember), "X10 request after stop");
    check(!executor.registerTask("after-stop", [&] { return WorkResult::Complete; }).has_value(),
          "X10 registration after stop");
    check(executor.snapshot().at("currentQueuedTasks") == 0, "X10 queue empty after stop");
}

// External stop/join, queued discard accounting and responsive snapshots.
void stop_and_snapshot()
{
    {
        TaskExecutor executor({2, 4});
        Gate entered;
        Gate release;
        auto handle = executor.registerTask("blocked", [&] {
            entered.open();
            release.wait();
            return WorkResult::Complete;
        });
        check(handle.has_value(), "stop register");
        check(executor.request(*handle, RequestPolicy::Remember), "stop request");
        check(entered.wait_for(3s), "stop callback entered");

        const auto started = std::chrono::steady_clock::now();
        const auto status = executor.snapshot();
        const auto elapsed = std::chrono::steady_clock::now() - started;
        check(elapsed < 200ms, "snapshot waited for a blocked callback");
        check(status.at("active_workers") == 1, "snapshot active worker");
        check(status.at("tasks").at(0).at("state") == "running", "snapshot running state");

        release.open();
        executor.shutdown();
        check(executor.stopped(), "stop state");
        check(executor.snapshot().at("currentQueuedTasks") == 0, "stop queue empty");
        executor.shutdown();
    }

    {
        TaskExecutor executor({1, 4});
        Gate blocker_entered;
        Gate release_blocker;
        auto blocker = executor.registerTask("blocker", [&] {
            blocker_entered.open();
            release_blocker.wait();
            return WorkResult::Complete;
        });
        auto queued = executor.registerTask("queued", [&] { return WorkResult::Complete; });
        check(blocker.has_value() && queued.has_value(), "discard register");
        check(executor.request(*blocker, RequestPolicy::Remember), "discard blocker request");
        check(blocker_entered.wait_for(3s), "discard blocker entered");
        check(executor.request(*queued, RequestPolicy::Remember), "discard queued request");
        check(executor.snapshot().at("currentQueuedTasks") == 1, "discard queued observed");

        release_blocker.open();
        executor.shutdown();
        check(executor.snapshot().at("discardedQueuedOnStop") == 1,
              "discard counted on stop");
    }
}

} // namespace

int main()
{
    try {
        x1_global_worker_cap();
        x2_no_overlap_and_remember();
        x3_notification_merging();
        x4_remember_after_empty_check();
        x5_periodic_suppression();
        x6_fair_continuation();
        x7_sweep_continuation();
        x8_one_worker_dependency();
        x9_exception_recovery();
        x10_generation_and_registration_bound();
        stop_and_snapshot();
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
    std::cout << "PASS task executor: worker cap, serial lanes, coalescing, continuation, "
                 "suppression, exception recovery, generations and shutdown\n";
    return 0;
}
