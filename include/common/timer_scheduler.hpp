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

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <nlohmann/json.hpp>

#include "common/task_executor.hpp"

// TimerScheduler owns deadlines and one timer-wait thread. It never creates
// collection workers: every callback or work request is executed by the
// injected TaskExecutor.
class TimerScheduler {
public:
    using Task = std::function<void()>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = std::chrono::milliseconds;
    using StatusReader = std::function<nlohmann::json()>;

    explicit TimerScheduler(TaskExecutor& executor);
    ~TimerScheduler();
    void shutdown();

    // Callback timers own one executor registration per timer. Repeating
    // timers use SkipIfBusy, so a slow callback suppresses missed ticks instead
    // of accumulating callbacks.
    size_t registerTimer(Duration delay, Task task, std::string name = {});
    size_t registerRepeatingTimer(Duration interval, Task task, std::string name = {});

    // Work timers request an existing executor registration. They do not own a
    // worker or a callback closure per tick.
    size_t registerWorkTimer(Duration delay, WorkHandle work,
                             RequestPolicy policy, std::string name = {});
    size_t registerRepeatingWorkTimer(Duration interval, WorkHandle work,
                                      RequestPolicy policy, std::string name = {});

    // Replaces the outstanding deadline instead of adding a stale heap entry.
    bool triggerTimer(size_t id);
    bool rescheduleTimer(size_t id, Duration delay);

    // Removes the timing source. An unstarted timer request is withdrawn;
    // caller/event work on the same lane is not. A running callback completes.
    bool cancelTimer(size_t id);

    nlohmann::json getPerformanceStats();
    // The reader owns the observational state and is safe while the executor
    // outlives the scheduler.
    StatusReader statusReader() const;

private:
    struct State;
    std::shared_ptr<State> state_;
    std::mutex shutdown_mutex_;
    std::thread service_thread_;

    void serviceLoop();
    size_t addTimer(Duration delay, bool repeat, Task task,
                    WorkHandle work, RequestPolicy policy,
                    std::string name, bool callback_timer);
};
