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
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LOGGER_TRACE


#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>
#include <nlohmann/json.hpp>


class TimerScheduler {
public:
    using Task      = std::function<void()>;
    using Clock     = std::chrono::steady_clock;
    using TimePoint = std::chrono::time_point<Clock>;
    using Duration  = std::chrono::milliseconds;

    struct TimerTask {
        TimePoint nextRun;
        Duration  interval;
        Task      task;
        bool      repeat;
        size_t    id;
        TimePoint scheduledTime;  // 任务被调度到队列的时间，用于计算等待时间

        bool operator<(const TimerTask& other) const {
            return nextRun > other.nextRun;
        }
    };

    struct SchedulerStatus {
        size_t pendingTasks;
        size_t activeWorkers;

        double averageWaitMs;
        double maxWaitMs;
        double minWaitMs;
    };

    struct WorkThreadStats {
        // mutable std::mutex mtx;
        size_t workerIndex;
        std::thread::id threadId;
        std::atomic<size_t> tasksProcessed = 0;
        std::atomic<size_t> tasksFailed = 0;
        std::atomic<double> averageExecutionTimeMs = 0.0;
        std::atomic<double> maxExecutionTimeMs = 0.0;
        std::atomic<double> minExecutionTimeMs = std::numeric_limits<double>::max();
        std::atomic<double> loadPercentage = 0.0;  // 线程负载百分比
        std::chrono::milliseconds totalBusyTime{0};
        std::chrono::milliseconds totalIdleTime{0};
        TimePoint lastIdleStartTime;
        TimePoint lastTaskStartTime;
        std::atomic<bool> isBusy = false;
    };

    struct PerformanceStats {
        // 任务统计
        size_t totalTasksRegistered = 0;
        size_t totalTasksProcessed = 0;
        size_t totalTasksFailed = 0;
        size_t currentTimerTasks = 0;
        size_t currentQueuedTasks = 0;
        
        // 队列统计
        size_t maxQueueSize = 0;
        size_t totalQueueOperations = 0;
        
        // 调度器统计
        std::chrono::milliseconds schedulerRuntime{0};
        size_t schedulerWakeups = 0;
        
        // 线程统计
        std::vector<WorkThreadStats> workerStats;
        SchedulerStatus schedulerStats;
        
        // 系统负载
        double averageThreadLoad = 0.0;
        double maxThreadLoad = 0.0;
        double minThreadLoad = 100.0;
        
        // 时间窗口统计（最近1分钟）
        size_t tasksProcessedLastMinute = 0;
        double averageTaskCompletionTimeMs = 0.0;
        
        // 获取统计信息的字符串表示
        nlohmann::json toJSON() const;
    };



    explicit TimerScheduler(size_t numWorkers = 8);
    ~TimerScheduler();

    void shutdown();

    // 注册单次定时任务
    size_t registerTimer(Duration delay, Task task);

    // 注册重复定时任务
    size_t registerRepeatingTimer(Duration interval, Task task);

    // 取消任务
    bool cancelTimer(size_t id);

    // 获取性能统计
    nlohmann::json getPerformanceStats();

private:
    size_t addTask(Task task, Duration interval, bool repeat);

    void schedulerLoop();
    void workerLoop(size_t workerIndex);

    std::vector<std::thread> workers;
    std::thread              schedulerThread;

    struct TimerTaskComparator {
        bool operator()(const std::shared_ptr<TimerTask>& a,
                       const std::shared_ptr<TimerTask>& b) const {
            return a->nextRun > b->nextRun;
        }
    };
    std::priority_queue<
        std::shared_ptr<TimerTask>,
        std::vector<std::shared_ptr<TimerTask>>,
        TimerTaskComparator> tasks;
    std::unordered_map<size_t, std::shared_ptr<TimerTask>> taskMap;

    std::queue<Task> taskQueue;

    std::mutex              mtx;
    std::mutex              queueMtx;
    std::condition_variable cv;
    std::condition_variable schedulerCv;

    std::atomic<bool>  stop;
    std::atomic<size_t> nextId;

    // 统计信息 - 任务等待时间跟踪
    double totalWaitMs{0.0};      // 所有任务的累计等待时间
    double maxWaitMs{0.0};        // 最大等待时间
    double minWaitMs{std::numeric_limits<double>::max()};  // 最小等待时间
    size_t tasksWithWaitData{0};  // 计算过等待时间的任务数

    // 统计信息
    inline void startTaskExecution(std::shared_ptr<TimerScheduler::WorkThreadStats> stat);
    inline void endTaskExecution(std::shared_ptr<TimerScheduler::WorkThreadStats> stat, bool success);
    void updateSchedulerLoad(bool isBusy);
    void updateAllThreadLoad();

    mutable std::mutex statsMutex;
    PerformanceStats stats;
    std::vector<std::shared_ptr<TimerScheduler::WorkThreadStats>> workerThreadStats;
    SchedulerStatus schedulerThreadStats;

    TimePoint schedulerStartTime;
    std::vector<TimePoint> workerStartTimes;

    std::atomic<size_t> tasksProcessedCounter{0};
    std::atomic<size_t> queueSizeCounter{0};

};
