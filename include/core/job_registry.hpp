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
// job_registry.h
#pragma once
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <functional>
#include <optional>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <thread>
#include <atomic>


#include "common/streamer_watcher.hpp"
#include "core/collector_type.h"
#include "job_lifecycle_event.h"
#include "ebpf/trace_condor_starter.h"
#include "common/ebpf_common.hpp"
#include "core/job_pid_tracker.hpp"

#include "job_watcher/condor_job_watcher.hpp"
#include "job_watcher/slurm_job_watcher.hpp"


class JobRegistry {
public:
    static JobRegistry& instance();          // 仍保留单例，方便迁移；也可由 main() 构造
    ~JobRegistry(){
        stop_pid_tracker();
        if (db_running){
            deinit_job_db();
        }
    };

    // 禁止拷贝
    JobRegistry(const JobRegistry&)            = delete;
    JobRegistry& operator=(const JobRegistry&) = delete;

    // Job 增删
    bool addJob(const Job& job);
    void delJob(uint64_t jobID);
    // 通过 sub_attr 匹配删除作业，返回删除的作业数量
    size_t delJob(const SubAttrMatcher& matcher);
    std::optional<Job> findJob(uint64_t jobID);
    // 通过 sub_attr 匹配查找作业，返回匹配的作业列表
    std::vector<Job> findJob(const SubAttrMatcher& matcher) const;
    std::vector<Job> snapshot() const;

    // 生命周期回调注册
    void addLifecycleCb(JobLifecycleCb cb);

    // 获取下一个全局唯一递增的JobID
    uint64_t getNextJobID();

    // 统一删除接口，通过 JSON 请求解析删除条件
    // 支持通过 JobID 或 sub_attr 匹配删除
    // 返回删除的作业数量
    size_t delJobByRequest(const nlohmann::json& req);

private:
    JobRegistry();
    void init_job_watcher();
    void regRPChandle();
    inline void update_job(Job& job);
    mutable std::shared_mutex              mtx_;
    std::unordered_map<uint64_t, Job>           jobs_;   // key = JobID
    std::vector<Job>                       restore_jobs_;
    std::vector<JobLifecycleCb>            cbs_;

    // 采集JobInfo持久化
    bool addJob(const Job& job, bool from_db);
    bool init_job_db();
    void deinit_job_db();
    void restore_jobs_from_db();
    void persist_new_job(const Job& job);
    void update_job_info(const Job& job);
    void end_job_in_db(uint64_t jobID);
    bool db_running{false};
    leveldb::DB* job_db = nullptr;    // LevelDB 实例，通过 delete 释放
    std::mutex db_mtx_;                 // 序列化job_db的并发事务操作

    // JobID计数器相关
    void init_job_id_counter();      // 初始化计数器（从数据库加载最大值）
    void persist_job_id_counter();   // 持久化当前计数器值
    uint64_t next_job_id_{1};        // 下一个可用的JobID，从1开始
    std::mutex job_id_mtx_;          // JobID计数器的互斥锁

    // Condor作业自动添加
    bool enable_auto_add_condor_job{false};
    std::unique_ptr<CondorJobWatcher> condor_job_watcher_;
    
    // Slurm作业自动添加
    bool enable_auto_add_slurm_job{false};
    std::unique_ptr<SlurmJobWatcher> slurm_job_watcher_;

    // 共享 pid2job / cgroup2job 维护: JobPidTracker 加载生命周期追踪 bpf,
    // JobRegistry 通过它向内核登记 Job 归属并消费 fork/exit 事件。
    void init_pid_tracker();
    void stop_pid_tracker();
    void sync_job_to_kernel(Job& job);            // Added: 写 cgroup2job + 种子 pid2job
    void remove_job_from_kernel(const Job& job);  // Removed: 删 cgroup2job + 清 pid2job 残留
    void on_kernel_fork(uint32_t pid, uint64_t job_id);  // ringbuf FORK: 给 Job 加 pid
    void on_kernel_exit(uint32_t pid, uint64_t job_id);  // ringbuf EXIT: 从 Job 删 pid
    void reconcile_loop();                         // 低频对账, 防 ringbuf 丢事件导致漂移
    std::unique_ptr<JobPidTracker> pid_tracker_;
    std::atomic<bool> reconcile_running_{false};
    std::unique_ptr<std::thread> reconcile_thread_;
};