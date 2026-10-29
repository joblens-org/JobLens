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
#include "core/kernel_attribution.hpp"

#include <spdlog/spdlog.h>

#include <chrono>
#include <thread>

#include "common/ebpf_common.hpp"
#include "core/job_pid_tracker.hpp"

namespace {

// （job_cgroup_path 已移除：cgroup 路径由 Host::attributionCgroupPath 提供，
// registry 在那里按 record.adapter 分派到 adapter 的 cgroupPathOf。）

}  // namespace

KernelAttribution::KernelAttribution(Host& host) : host_(host) {}

KernelAttribution::~KernelAttribution()
{
    stop();
}

void KernelAttribution::setCapabilityLookup(
    std::function<bool(uint64_t internal_id)> capable)
{
    std::lock_guard<std::mutex> lock(cache_mtx_);
    capable_ = std::move(capable);
}

bool KernelAttribution::capable(uint64_t internal_id) const
{
    // guarded by cache_mtx_ (callers hold it)
    return !capable_ || capable_(internal_id);
}

bool KernelAttribution::start()
{
    tracker_ = std::make_unique<JobPidTracker>();
    tracker_->set_fork_cb([this](uint32_t pid, uint64_t job_id) {
        // 内核已将 child 写入 pid2job, 这里同步更新用户态 Job.JobPIDs。
        if (!host_.attributionJobExists(job_id)) {
            spdlog::debug("KernelAttribution: fork event for unknown job_id={} pid={}, "
                          "ignored (job may have just been removed)",
                          static_cast<unsigned long long>(job_id), pid);
            return;
        }
        host_.attributionJobAddPid(job_id, static_cast<pid_t>(pid));
    });
    tracker_->set_exit_cb([this](uint32_t pid, uint64_t job_id) {
        // 内核已从 pid2job 删除, 这里同步移除用户态 Job.JobPIDs。
        if (host_.attributionJobExists(job_id))
            host_.attributionJobRemovePid(job_id, static_cast<pid_t>(pid));
        // Maintenance/heartbeat owns fact refresh and retirement; the kernel
        // callback only hands over a cheap hint, never discovery I/O.
        host_.attributionExitHint(static_cast<pid_t>(pid));
    });
    if (!tracker_->start()) {
        spdlog::warn("KernelAttribution: pid tracker start failed, kernel pid2job "
                     "maintenance disabled");
        tracker_.reset();
        return false;
    }
    reconcile_running_ = true;
    reconcile_thread_ = std::make_unique<std::thread>([this]() { reconcileLoop(); });
    spdlog::info("KernelAttribution: pid tracker enabled");
    return true;
}

void KernelAttribution::stop()
{
    if (reconcile_running_) {
        reconcile_running_ = false;
        if (reconcile_thread_ && reconcile_thread_->joinable()) reconcile_thread_->join();
        reconcile_thread_.reset();
    }
    if (tracker_) {
        tracker_->stop();
        tracker_.reset();
    }
}

uint64_t KernelAttribution::resolveCgroupId(const Job& job)
{
    {
        std::lock_guard<std::mutex> lock(cache_mtx_);
        const auto cached = cgroup_ids_.find(job.JobID);
        if (cached != cgroup_ids_.end()) return cached->second;
    }
    const auto cg = host_.attributionCgroupPath(job.JobID);
    if (cg.empty()) return 0;
    const auto id = EbpfCommon::cgroup_path_to_id(cg).value_or(0);
    if (id != 0) {
        std::lock_guard<std::mutex> lock(cache_mtx_);
        cgroup_ids_[job.JobID] = id;
    }
    return id;
}

void KernelAttribution::activate(uint64_t internal_id, const Job& job)
{
    if (!tracker_ || internal_id == 0) return;
    {
        std::lock_guard<std::mutex> lock(cache_mtx_);
        if (!capable(internal_id)) return;
    }

    const uint64_t cgid = resolveCgroupId(job);
    if (cgid != 0) {
        if (tracker_->set_cgroup_job(cgid, internal_id)) {
            spdlog::debug("KernelAttribution: cgroup2job registered job_id={} cgroup_id={}",
                          internal_id, cgid);
        } else {
            spdlog::warn("KernelAttribution: cgroup2job write failed job_id={} cgroup_id={}",
                         internal_id, cgid);
        }
    }
    for (pid_t pid : job.JobPIDs) {
        if (!tracker_->set_pid_job(static_cast<uint32_t>(pid), internal_id)) {
            spdlog::warn("KernelAttribution: pid2job seed write failed job_id={} pid={}",
                         internal_id, pid);
        }
    }
    spdlog::debug("KernelAttribution: activated job_id={} seed_pids={} has_cgroup={}",
                  internal_id, job.JobPIDs.size(), cgid != 0);
}

void KernelAttribution::deactivate(uint64_t internal_id, const Job& job)
{
    if (!tracker_ || internal_id == 0) return;

    uint64_t cgid = 0;
    {
        std::lock_guard<std::mutex> lock(cache_mtx_);
        const auto cached = cgroup_ids_.find(internal_id);
        if (cached != cgroup_ids_.end()) {
            cgid = cached->second;
            cgroup_ids_.erase(cached);
        }
    }
    if (cgid != 0) {
        tracker_->del_cgroup_job(cgid);
        spdlog::debug("KernelAttribution: cgroup2job removed job_id={} cgroup_id={}",
                      internal_id, cgid);
    }
    for (pid_t pid : job.JobPIDs) {
        tracker_->del_pid_job(static_cast<uint32_t>(pid));
    }
    spdlog::debug("KernelAttribution: deactivated job_id={} cleared_pids={} had_cgroup={}",
                  internal_id, job.JobPIDs.size(), cgid != 0);
}

void KernelAttribution::reconcile(const Job& job)
{
    if (!tracker_ || job.JobID == 0) return;
    {
        std::lock_guard<std::mutex> lock(cache_mtx_);
        if (!capable(job.JobID)) return;
    }

    const uint64_t cgid = resolveCgroupId(job);
    if (cgid != 0) tracker_->set_cgroup_job(cgid, job.JobID);
    for (pid_t pid : job.JobPIDs) {
        tracker_->set_pid_job(static_cast<uint32_t>(pid), job.JobID);
    }
}

uint64_t KernelAttribution::droppedEvents() const
{
    return tracker_ ? tracker_->dropped_events() : 0;
}

bool KernelAttribution::running() const
{
    return tracker_ != nullptr;
}

uint64_t KernelAttribution::pushedEvents() const
{
    return tracker_ ? tracker_->pushed_events() : 0;
}

// 低频对账: 周期性把当前所有 Job 的归属重新写回内核, 校正 ringbuf 丢事件导致的漂移。
void KernelAttribution::reconcileLoop()
{
    while (reconcile_running_) {
        for (int i = 0; i < 300 && reconcile_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (!reconcile_running_) break;

        size_t resynced = 0;
        host_.attributionEnumerateJobs([this, &resynced](const Job& job) {
            reconcile(job);
            ++resynced;
        });
        spdlog::debug("KernelAttribution: reconcile pass done, resynced {} jobs to kernel",
                      resynced);

        // ringbuf 溢出是静默的: 一旦发生, JobPIDs 会悄悄失真。按对账周期上报增量。
        const uint64_t dropped = droppedEvents();
        if (dropped > last_dropped_events_) {
            spdlog::warn("KernelAttribution: job_event ringbuf overflow, {} new pid "
                         "attribution events dropped (cumulative {}); JobPIDs may be "
                         "incomplete until the next cgroup resync",
                         dropped - last_dropped_events_, dropped);
            last_dropped_events_ = dropped;
        }
    }
}
