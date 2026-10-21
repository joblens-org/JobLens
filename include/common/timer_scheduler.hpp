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

class TimerScheduler {
public:
    using Task = std::function<void()>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = std::chrono::milliseconds;
    using StatusReader = std::function<nlohmann::json()>;

    explicit TimerScheduler(size_t numWorkers = 8);
    ~TimerScheduler();
    void shutdown();

    size_t registerTimer(Duration delay, Task task, std::string name = {});
    // At most one queued or running callback per timer. Missed ticks are skipped.
    size_t registerRepeatingTimer(Duration interval, Task task, std::string name = {});
    // Removes queued work; an already running callback completes normally.
    bool cancelTimer(size_t id);

    nlohmann::json getPerformanceStats();
    // The reader owns the status state, and is safe after scheduler destruction.
    StatusReader statusReader() const;

private:
    struct State;
    std::shared_ptr<State> state_;
    std::mutex shutdown_mutex_;
    std::vector<std::thread> workers_;
    std::thread scheduler_thread_;

    size_t addTask(Task task, Duration interval, bool repeat, std::string name);
    void schedulerLoop();
    void workerLoop(size_t index);
};
