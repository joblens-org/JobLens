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
#include "job_watcher/execve_event_source.hpp"
#include "common/ebpf_common.hpp"
#include "common/utils.hpp"
#include "ebpf/trace_execve_job.h"
#include <spdlog/spdlog.h>

ExecveEventSource::ExecveEventSource()
    : bpf_o_path_(JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/trace_execve_job.bpf.o"),
      rb_name_("execve_job_events")
{
}

ExecveEventSource::~ExecveEventSource()
{
    stop();
}

bool ExecveEventSource::start()
{
    if (running_) {
        return true;
    }
    if (!init_ebpf()) {
        deinit_ebpf();
        return false;
    }
    running_ = true;
    worker_thread_ = std::make_unique<std::thread>([this] { worker_loop(); });
    poll_thread_ = std::make_unique<std::thread>([this] {
        while (running_) {
            int err = ring_buffer__poll(bpf_rb_, 100);
            if (err == -EINTR) {
                break;
            }
            if (err < 0) {
                spdlog::error("ExecveEventSource: ring_buffer__poll error {}", err);
            }
        }
    });
    spdlog::info("ExecveEventSource: started");
    return true;
}

void ExecveEventSource::stop()
{
    if (!running_) {
        deinit_ebpf();
        return;
    }
    running_ = false;
    spdlog::info("ExecveEventSource: stopping");
    queue_cv_.notify_all();
    if (poll_thread_ && poll_thread_->joinable()) {
        poll_thread_->join();
    }
    if (worker_thread_ && worker_thread_->joinable()) {
        worker_thread_->join();
    }
    deinit_ebpf();
}

void ExecveEventSource::register_callback(TriggerEventCallback cb)
{
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    callbacks_.push_back(std::move(cb));
}

bool ExecveEventSource::init_ebpf()
{
    auto path = Utils::JobLensRootDir() + bpf_o_path_;
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    if (!bpf_obj_) {
        return false;
    }
    ring_buffer_sample_fn fn = [](void* ctx, void* data, size_t) {
        auto* self = static_cast<ExecveEventSource*>(ctx);
        auto* event = static_cast<execve_job_event*>(data);
        ExecveTriggerEvent e;
        e.pid = event->pid;
        e.tgid = event->tgid;
        e.ppid = event->ppid;
        e.ptgid = event->ptgid;
        e.comm = std::string(event->comm);
        e.parent_comm = std::string(event->parent_comm);
        e.scheduler_hint = static_cast<SchedulerHint>(event->scheduler_hint);
        spdlog::debug("ExecveEventSource: enqueued execve event pid={} comm={} parent_comm={} hint={}",
                      e.pid, e.comm, e.parent_comm, static_cast<int>(e.scheduler_hint));
        self->enqueue_event(std::move(e));
        return 0;
    };
    bpf_rb_ = EbpfCommon::new_rb(bpf_obj_, rb_name_, fn, this);
    return bpf_rb_ != nullptr;
}

void ExecveEventSource::deinit_ebpf()
{
    EbpfCommon::free_rb(bpf_rb_);
    bpf_rb_ = nullptr;
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
    bpf_obj_ = nullptr;
}

void ExecveEventSource::enqueue_event(ExecveTriggerEvent event)
{
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        pending_events_.push_back(std::move(event));
    }
    queue_cv_.notify_one();
}

void ExecveEventSource::worker_loop()
{
    while (running_) {
        ExecveTriggerEvent event;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return !running_ || !pending_events_.empty(); });
            if (!running_) {
                break;
            }
            event = std::move(pending_events_.front());
            pending_events_.pop_front();
        }
        std::vector<TriggerEventCallback> callbacks;
        {
            std::lock_guard<std::mutex> lock(callbacks_mutex_);
            callbacks = callbacks_;
        }
        spdlog::debug("ExecveEventSource: dispatching pid={} comm={} to {} callbacks",
                      event.pid, event.comm, callbacks.size());
        TriggerEvent trigger_event = std::move(event);
        for (auto& cb : callbacks) {
            try {
                cb(trigger_event);
            } catch (const std::exception& e) {
                spdlog::warn("ExecveEventSource: callback exception: {}", e.what());
            }
        }
    }
}
