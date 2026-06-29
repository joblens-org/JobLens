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
#include "core/cgroup_mkdir_event_source.hpp"
#include "common/ebpf_common.hpp"
#include "common/utils.hpp"
#include "ebpf/trace_cgroup_mkdir.h"
#include <spdlog/spdlog.h>
#include <filesystem>

namespace fs = std::filesystem;

CgroupMkdirEventSource::CgroupMkdirEventSource()
    : bpf_o_path_(JOBLENS_INSTALL_LIBDIR "/joblens/bpf_obj/trace_cgroup_mkdir.bpf.o"),
      rb_name_("cgroup_mkdir_events")
{
}

CgroupMkdirEventSource::~CgroupMkdirEventSource()
{
    stop();
}

bool CgroupMkdirEventSource::start()
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
                spdlog::error("CgroupMkdirEventSource: ring_buffer__poll error {}", err);
            }
        }
    });
    spdlog::info("CgroupMkdirEventSource: started, mount={}", cgroup2_mount_);
    return true;
}

void CgroupMkdirEventSource::stop()
{
    if (!running_) {
        deinit_ebpf();
        return;
    }
    running_ = false;
    spdlog::info("CgroupMkdirEventSource: stopping");
    queue_cv_.notify_all();
    if (poll_thread_ && poll_thread_->joinable()) {
        poll_thread_->join();
    }
    if (worker_thread_ && worker_thread_->joinable()) {
        worker_thread_->join();
    }
    deinit_ebpf();
}

void CgroupMkdirEventSource::register_callback(Callback cb)
{
    std::lock_guard<std::mutex> lock(callbacks_mutex_);
    callbacks_.push_back(std::move(cb));
}

bool CgroupMkdirEventSource::init_ebpf()
{
    cgroup2_mount_ = Utils::discover_cgroup2_mount();
    if (cgroup2_mount_.empty()) {
        spdlog::warn("CgroupMkdirEventSource: cgroup v2 mount not found");
        return false;
    }
    auto path = Utils::JobLensRootDir() + bpf_o_path_;
    bpf_obj_ = EbpfCommon::load_bpf_obj(path, bpf_links_);
    if (!bpf_obj_) {
        return false;
    }
    ring_buffer_sample_fn fn = [](void* ctx, void* data, size_t) {
        auto* self = static_cast<CgroupMkdirEventSource*>(ctx);
        auto* event = static_cast<cgroup_mkdir_event*>(data);
        auto normalized = self->normalize_event_path(event->path);
        if (!normalized.empty()) {
            spdlog::debug("CgroupMkdirEventSource: enqueued mkdir event path={}", normalized);
            self->enqueue_path(std::move(normalized));
        }
        return 0;
    };
    bpf_rb_ = EbpfCommon::new_rb(bpf_obj_, rb_name_, fn, this);
    return bpf_rb_ != nullptr;
}

void CgroupMkdirEventSource::deinit_ebpf()
{
    EbpfCommon::free_rb(bpf_rb_);
    bpf_rb_ = nullptr;
    EbpfCommon::unload_bpf_obj(bpf_obj_, bpf_links_);
    bpf_obj_ = nullptr;
}

std::string CgroupMkdirEventSource::normalize_event_path(const char* path) const
{
    if (path == nullptr || path[0] == '\0') {
        spdlog::debug("CgroupMkdirEventSource: event dropped, empty cgroup path");
        return {};
    }
    std::string raw(path);
    if (cgroup2_mount_.empty()) {
        spdlog::debug("CgroupMkdirEventSource: event dropped, cgroup v2 mount unknown path={}", raw);
        return {};
    }
    if (raw.rfind(cgroup2_mount_, 0) == 0) {
        return fs::path(raw).lexically_normal().string();
    }
    fs::path relative(raw);
    if (relative.is_absolute()) {
        relative = relative.relative_path();
    }
    return (fs::path(cgroup2_mount_) / relative).lexically_normal().string();
}

void CgroupMkdirEventSource::enqueue_path(std::string cgroup_path)
{
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        pending_paths_.push_back(std::move(cgroup_path));
    }
    queue_cv_.notify_one();
}

void CgroupMkdirEventSource::worker_loop()
{
    while (running_) {
        std::string path;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return !running_ || !pending_paths_.empty(); });
            if (!running_) {
                break;
            }
            path = std::move(pending_paths_.front());
            pending_paths_.pop_front();
        }
        std::vector<Callback> callbacks;
        {
            std::lock_guard<std::mutex> lock(callbacks_mutex_);
            callbacks = callbacks_;
        }
        spdlog::debug("CgroupMkdirEventSource: dispatching cgroup={} to {} callbacks", path, callbacks.size());
        for (auto& cb : callbacks) {
            cb(path);
        }
    }
}
