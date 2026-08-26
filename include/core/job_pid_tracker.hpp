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

#include <bpf/libbpf.h>
#include <spdlog/spdlog.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/ebpf_common.hpp"
#include "common/utils.hpp"
#include "ebpf/job_pid_track.h"

// JobPidTracker: 共享 pid2job/cgroup2job/job_event_rb 三张 map 的宿主。
//   - 加载 job_pid_track.bpf.o(内含 sched_process_fork/exit hook), 首个加载者
//     创建并 pin 三张 map 到 bpffs, 其余采集器的 .bpf.o 通过 pinning 复用;
//   - 起 poll 线程消费 ringbuf, 把 FORK/EXIT 事件通过回调上报给 JobRegistry;
//   - 对外暴露 cgroup2job / pid2job 的写接口, 供 JobRegistry 维护归属关系。
class JobPidTracker {
public:
    // FORK/EXIT 事件回调: (pid, job_id)。由 JobRegistry 注入。
    using PidEventCb = std::function<void(uint32_t pid, uint64_t job_id)>;

    JobPidTracker() = default;
    ~JobPidTracker() { stop(); }

    JobPidTracker(const JobPidTracker&) = delete;
    JobPidTracker& operator=(const JobPidTracker&) = delete;

    void set_fork_cb(PidEventCb cb) { fork_cb_ = std::move(cb); }
    void set_exit_cb(PidEventCb cb) { exit_cb_ = std::move(cb); }

    // 加载 bpf 对象(create-or-reuse pinned map)、取回 map fd、起 ringbuf poll 线程。
    bool start() {
        if (running_) return true;
        auto path = Utils::JobLensRootDir() + bpf_o_path_;
        bpf_obj_ = EbpfCommon::load_bpf_obj_pinned(path, bpf_links_, JOBLENS_BPF_PIN_ROOT);
        if (!bpf_obj_) {
            spdlog::error("JobPidTracker: load bpf {} failed", path);
            return false;
        }

        cgroup2job_fd_ = bpf_object__find_map_fd_by_name(bpf_obj_, JOBLENS_CGROUP2JOB_MAP_NAME);
        pid2job_fd_ = bpf_object__find_map_fd_by_name(bpf_obj_, JOBLENS_PID2JOB_MAP_NAME);
        if (cgroup2job_fd_ < 0 || pid2job_fd_ < 0) {
            spdlog::error("JobPidTracker: find shared map fd failed (cgroup2job={}, pid2job={})",
                          cgroup2job_fd_, pid2job_fd_);
            deinit();
            return false;
        }

        int rb_fd = bpf_object__find_map_fd_by_name(bpf_obj_, JOBLENS_JOB_EVENT_RB_NAME);
        if (rb_fd < 0) {
            spdlog::error("JobPidTracker: find ringbuf {} failed", JOBLENS_JOB_EVENT_RB_NAME);
            deinit();
            return false;
        }
        rb_ = ring_buffer__new(rb_fd, &JobPidTracker::handle_event, this, nullptr);
        if (!rb_) {
            spdlog::error("JobPidTracker: ring_buffer__new failed");
            deinit();
            return false;
        }

        running_ = true;
        poll_thread_ = std::make_unique<std::thread>([this]() {
            spdlog::debug("JobPidTracker: ringbuf poll thread started");
            while (running_) {
                int err = ring_buffer__poll(rb_, 100 /* ms */);
                if (err == -EINTR) break;
                if (err < 0) {
                    spdlog::error("JobPidTracker: ring_buffer__poll error {}", err);
                    break;
                }
            }
            spdlog::debug("JobPidTracker: ringbuf poll thread exited");
        });
        spdlog::info("JobPidTracker: started (pid2job_fd={}, cgroup2job_fd={}, pin_root={})",
                     pid2job_fd_, cgroup2job_fd_, JOBLENS_BPF_PIN_ROOT);
        return true;
    }

    void stop() {
        if (running_) {
            running_ = false;
            if (poll_thread_ && poll_thread_->joinable()) poll_thread_->join();
            poll_thread_.reset();
            spdlog::info("JobPidTracker: stopped");
        }
        deinit();
    }

    // 用户态写 cgroup2job: Job 新增时登记 cgroup 归属。
    bool set_cgroup_job(uint64_t cgroup_id, uint64_t job_id) {
        return EbpfCommon::update_map_by_fd_u64(cgroup2job_fd_, cgroup_id, job_id);
    }
    bool del_cgroup_job(uint64_t cgroup_id) {
        return EbpfCommon::delete_map_by_fd_u64(cgroup2job_fd_, cgroup_id);
    }

    // 用户态写 pid2job: 登记"根 pid"种子; 内核 fork hook 据此繁衍子进程。
    bool set_pid_job(uint32_t pid, uint64_t job_id) {
        return EbpfCommon::update_map_by_fd(pid2job_fd_, pid, job_id);
    }
    bool del_pid_job(uint32_t pid) {
        return EbpfCommon::delete_map_by_fd(pid2job_fd_, pid);
    }

    bool running() const { return running_; }

private:
    static int handle_event(void* ctx, void* data, size_t size) {
        auto* self = static_cast<JobPidTracker*>(ctx);
        if (size < sizeof(job_pid_event)) {
            spdlog::warn("JobPidTracker: dropped malformed ringbuf event, size={} expected>={}",
                         size, sizeof(job_pid_event));
            return 0;
        }
        const auto* e = static_cast<const job_pid_event*>(data);
        if (e->type == JOB_PID_EVENT_FORK) {
            spdlog::trace("JobPidTracker: FORK event pid={} job_id={}", e->pid, e->job_id);
            if (self->fork_cb_) self->fork_cb_(e->pid, e->job_id);
        } else if (e->type == JOB_PID_EVENT_EXIT) {
            spdlog::trace("JobPidTracker: EXIT event pid={} job_id={}", e->pid, e->job_id);
            if (self->exit_cb_) self->exit_cb_(e->pid, e->job_id);
        } else {
            spdlog::warn("JobPidTracker: unknown ringbuf event type={} pid={} job_id={}",
                         e->type, e->pid, e->job_id);
        }
        return 0;
    }

    void deinit() {
        if (rb_) {
            ring_buffer__free(rb_);
            rb_ = nullptr;
        }
        if (bpf_obj_) {
            // 卸载前 unpin: 作为共享 map 宿主, 清理 bpffs pin 项, 避免下次启动时
            // 旧 map 定义残留与新定义冲突导致 reuse 校验失败。
            bpf_object__unpin_maps(bpf_obj_, JOBLENS_BPF_PIN_ROOT);
            EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
            bpf_obj_ = nullptr;
        }
        cgroup2job_fd_ = -1;
        pid2job_fd_ = -1;
    }

    std::string bpf_o_path_ = JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/job_pid_track.bpf.o";
    bpf_object* bpf_obj_{nullptr};
    std::vector<bpf_link*> bpf_links_;
    ring_buffer* rb_{nullptr};
    int cgroup2job_fd_{-1};
    int pid2job_fd_{-1};

    std::atomic<bool> running_{false};
    std::unique_ptr<std::thread> poll_thread_;
    PidEventCb fork_cb_;
    PidEventCb exit_cb_;
};
