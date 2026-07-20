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
#include <spdlog/spdlog.h>

TimerScheduler::TimerScheduler(size_t numWorkers)
    : stop(false), nextId(0) {
    workerThreadStats.resize(numWorkers);
    for (size_t i = 0; i < numWorkers; ++i) {
        workers.emplace_back([this, i] { 
            workerLoop(i); });
    }
    schedulerThread = std::thread([this] { schedulerLoop(); });
}

TimerScheduler::~TimerScheduler() {
    stop = true;
    cv.notify_all();
    schedulerCv.notify_all();
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    if (schedulerThread.joinable()) {
        schedulerThread.join();
    }
}

void TimerScheduler::shutdown() {
    stop = true;
    cv.notify_all();
    schedulerCv.notify_all();
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    if (schedulerThread.joinable()) {
        schedulerThread.join();
    }
}

size_t TimerScheduler::registerTimer(Duration delay, Task task) {
    return addTask(std::move(task), delay, false);
}

size_t TimerScheduler::registerRepeatingTimer(Duration interval, Task task) {
    return addTask(std::move(task), interval, true);
}

bool TimerScheduler::cancelTimer(size_t id) {
    std::lock_guard<std::mutex> lock(mtx);
    return taskMap.erase(id) > 0;
}

bool TimerScheduler::rescheduleTimer(size_t id, Duration delay) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = taskMap.find(id);
    if (it == taskMap.end()) {
        return false;
    }

    auto oldTask = it->second;
    auto now = Clock::now();
    auto newTask = std::make_shared<TimerTask>(TimerTask{
        now + delay,
        oldTask->interval,
        oldTask->task,
        oldTask->repeat,
        oldTask->id,
        now
    });

    taskMap[id] = newTask;
    tasks.push(newTask);
    cv.notify_one();
    return true;
}

bool TimerScheduler::triggerTimer(size_t id) {
    return rescheduleTimer(id, Duration{0});
}

size_t TimerScheduler::addTask(Task task, Duration interval, bool repeat) {
    std::lock_guard<std::mutex> lock(mtx);
    auto id = nextId++;
    auto now = Clock::now();
    std::shared_ptr<TimerTask> timerTask;
    if (repeat) {
        timerTask = std::make_shared<TimerTask>(TimerTask{now, interval, std::move(task), repeat, id, now});
    } else {
        timerTask = std::make_shared<TimerTask>(TimerTask{now + interval, interval, std::move(task), repeat, id, now});
    }

    taskMap[id] = timerTask;
    tasks.push(timerTask);

    {
        std::lock_guard<std::mutex> statsLock(statsMutex);
        stats.totalTasksRegistered++;
        stats.currentTimerTasks = tasks.size();
        stats.maxQueueSize = std::max(stats.maxQueueSize, stats.currentTimerTasks);
        stats.totalQueueOperations++;
    }

    cv.notify_one();
    return id;
}

void TimerScheduler::schedulerLoop() {
    schedulerStartTime = Clock::now();
    
    while (!stop) {
        std::unique_lock<std::mutex> lock(mtx);
        if (tasks.empty()) {           
            cv.wait(lock, [this] { return stop || !tasks.empty(); });
            if (stop) break;
        }

        auto now = Clock::now();
        if (!tasks.empty() && tasks.top()->nextRun <= now) {
            auto task = tasks.top();
            tasks.pop();
            auto it = taskMap.find(task->id);
            if (it == taskMap.end() || it->second != task) {
                continue; 
            }
            
            auto waitTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - task->nextRun);
            double waitMs = waitTime.count();
            
            totalWaitMs += waitMs;
            tasksWithWaitData++;
            
            if (waitMs > maxWaitMs) {
                maxWaitMs = waitMs;
            }
            if (waitMs < minWaitMs) {
                minWaitMs = waitMs;
            }
            
            task->scheduledTime = now; // 更新任务的调度时间
            if (task->repeat) {
                task->nextRun = now + task->interval;
                tasks.push(task);
            } else {
                taskMap.erase(task->id);
            }
            lock.unlock();

            {
                std::lock_guard<std::mutex> queueLock(queueMtx);
                taskQueue.push(task->task);
                queueSizeCounter++;
            }
            schedulerCv.notify_one();
        } else {
            cv.wait_until(lock, tasks.top()->nextRun);
        }
    }
}


inline void TimerScheduler::startTaskExecution(std::shared_ptr<TimerScheduler::WorkThreadStats> stat) {
    // 这里不用加锁，因为不存在对共享数据的修改冲突
    auto now = Clock::now();
    auto idleTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - stat->lastIdleStartTime);
    stat->totalIdleTime += idleTime;
    stat->lastTaskStartTime = now;
    stat->isBusy = true;
}


inline void TimerScheduler::endTaskExecution(std::shared_ptr<TimerScheduler::WorkThreadStats> stat, bool success) {
    // 使用原子操作更新统计数据，避免加锁开销
    stat->tasksProcessed++;
    stat->isBusy = false;
    
    if (!success) {
        stat->tasksFailed++;
        stats.totalTasksFailed++;
    }
    auto now = Clock::now();
    auto executionTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - stat->lastTaskStartTime);
    // 更新执行时间统计
    double execTimeMs = executionTime.count();
    stat->totalBusyTime += executionTime;
    stat->lastIdleStartTime = now;
    stat->loadPercentage.store(
        (stat->totalBusyTime.count() * 100.0) /
        (stat->totalBusyTime.count() + stat->totalIdleTime.count()));
    
    if (execTimeMs < stat->minExecutionTimeMs.load()) {
        stat->minExecutionTimeMs.store(execTimeMs);
    }
    if (execTimeMs > stat->maxExecutionTimeMs.load()) {
        stat->maxExecutionTimeMs.store(execTimeMs);
    }
    
    // 更新平均执行时间
    stat->averageExecutionTimeMs.store( 
        (stat->averageExecutionTimeMs.load() * (stat->tasksProcessed - 1) + execTimeMs) /
        stat->tasksProcessed);

}


void TimerScheduler::workerLoop(size_t workerIndex) {
    thread_local static std::shared_ptr<TimerScheduler::WorkThreadStats> stat;
    stat = std::make_shared<TimerScheduler::WorkThreadStats>();
    stat->threadId = std::this_thread::get_id();
    stat->workerIndex = workerIndex;
    workerThreadStats[workerIndex] = stat;
    while (!stop) {
        std::unique_lock<std::mutex> lock(queueMtx);
        schedulerCv.wait(lock, [this] { return stop || !taskQueue.empty(); });
        if (stop) break;

        if (!taskQueue.empty()) {
            bool success = true;
            auto task = std::move(taskQueue.front());
            taskQueue.pop();
            queueSizeCounter--;
            lock.unlock();

            startTaskExecution(stat);
            try {
                task();
            } catch (const std::exception& e) {
                success = false;
                spdlog::error("TimerScheduler: Task execution failed: {}",e.what());
            }
            endTaskExecution(stat, success);
        }
    }
}


// 统计要快，渲染可以慢一点
void TimerScheduler::updateAllThreadLoad() {
    size_t i = 0;
    stats.totalTasksProcessed = 0;
    stats.totalTasksFailed = 0;
    stats.averageThreadLoad = 0;
    stats.averageTaskCompletionTimeMs = 0.0;

    for (auto& stat : workerThreadStats) {
        auto& cp = stats.workerStats[i++];
        cp.tasksProcessed = stat->tasksProcessed.load();
        cp.tasksFailed = stat->tasksFailed.load();
        cp.averageExecutionTimeMs = stat->averageExecutionTimeMs.load();
        cp.maxExecutionTimeMs = stat->maxExecutionTimeMs.load();
        cp.minExecutionTimeMs = stat->minExecutionTimeMs.load();
        cp.loadPercentage = stat->loadPercentage.load();
        cp.isBusy = stat->isBusy.load();

        stats.totalTasksProcessed += cp.tasksProcessed;
        stats.totalTasksFailed += cp.tasksFailed;
        
        stats.averageThreadLoad += cp.loadPercentage;
        stats.averageTaskCompletionTimeMs += cp.averageExecutionTimeMs;

        if (cp.loadPercentage > stats.maxThreadLoad) {
            stats.maxThreadLoad = cp.loadPercentage;
        }
        if (cp.loadPercentage < stats.minThreadLoad) {
            stats.minThreadLoad = cp.loadPercentage;
        }

    }
    if (!workerThreadStats.empty()) {
        stats.averageThreadLoad /= workerThreadStats.size();
        stats.averageTaskCompletionTimeMs /= workerThreadStats.size();
    }

    // 计算调度器统计信息
    {
        std::lock_guard<std::mutex> lockMtx(mtx);
        
        // 1. 待调度任务数 = 定时任务队列中的任务数
        stats.schedulerStats.pendingTasks = tasks.size();
        stats.currentTimerTasks = tasks.size();
        stats.currentQueuedTasks = queueSizeCounter.load();
        
        // 2. 活跃的工作线程数（正在执行任务的线程）
        stats.schedulerStats.activeWorkers = 0;
        for (const auto& stat : workerThreadStats) {
            if (stat && stat->isBusy.load()) {
                stats.schedulerStats.activeWorkers++;
            }
        }
    }
    
    // 3. 任务等待时间统计
    if (tasksWithWaitData > 0) {
        stats.schedulerStats.averageWaitMs = totalWaitMs / tasksWithWaitData;
        stats.schedulerStats.maxWaitMs = maxWaitMs;
        stats.schedulerStats.minWaitMs = minWaitMs;
    } else {
        stats.schedulerStats.averageWaitMs = 0.0;
        stats.schedulerStats.maxWaitMs = 0.0;
        stats.schedulerStats.minWaitMs = 0.0;
    }
}


nlohmann::json TimerScheduler::getPerformanceStats() {
    std::lock_guard<std::mutex> lock(statsMutex);
    PerformanceStats cp;
    updateAllThreadLoad();
    return stats.toJSON();
}

nlohmann::json TimerScheduler::PerformanceStats::toJSON() const {
    nlohmann::json j;
    j["totalTasksRegistered"] = totalTasksRegistered;
    j["totalTasksProcessed"] = totalTasksProcessed;
    j["totalTasksFailed"] = totalTasksFailed;
    j["currentTimerTasks"] = currentTimerTasks;
    j["currentQueuedTasks"] = currentQueuedTasks;

    j["maxQueueSize"] = maxQueueSize;
    j["totalQueueOperations"] = totalQueueOperations;

    j["schedulerRuntime"] = schedulerRuntime.count();
    j["schedulerWakeups"] = schedulerWakeups;

    // 工作线程统计
    j["workerStats"] = nlohmann::json::array();
    for (const auto& ws : workerStats) {
        nlohmann::json jws;
        jws["workerIndex"] = ws.workerIndex;
        jws["tasksProcessed"] = ws.tasksProcessed.load();
        jws["tasksFailed"] = ws.tasksFailed.load();
        jws["averageExecutionTimeMs"] = ws.averageExecutionTimeMs.load();
        jws["maxExecutionTimeMs"] = ws.maxExecutionTimeMs.load();
        jws["minExecutionTimeMs"] = ws.minExecutionTimeMs.load();
        jws["loadPercentage"] = ws.loadPercentage.load();
        jws["isBusy"] = ws.isBusy.load();
        j["workerStats"].push_back(jws);
    }
    // 调度器线程统计
    nlohmann::json jss;
    jss["pendingTasks"] = schedulerStats.pendingTasks;
    jss["activeWorkers"] = schedulerStats.activeWorkers;
    jss["averageWaitMs"] = schedulerStats.averageWaitMs;
    jss["maxWaitMs"] = schedulerStats.maxWaitMs;
    jss["minWaitMs"] = schedulerStats.minWaitMs;
    j["schedulerStats"] = jss;

    j["averageThreadLoad"] = averageThreadLoad;
    j["maxThreadLoad"] = maxThreadLoad;
    j["minThreadLoad"] = minThreadLoad;
    j["tasksProcessedLastMinute"] = tasksProcessedLastMinute;
    j["averageTaskCompletionTimeMs"] = averageTaskCompletionTimeMs;
    return j;
}
