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

#include <atomic>
#include <bpf/libbpf.h>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class CgroupMkdirEventSource {
public:
    using Callback = std::function<void(const std::string& cgroup_path)>;

    CgroupMkdirEventSource();
    ~CgroupMkdirEventSource();

    bool start();
    void stop();
    void register_callback(Callback cb);

private:
    bool init_ebpf();
    void deinit_ebpf();
    void enqueue_path(std::string cgroup_path);
    void worker_loop();
    std::string normalize_event_path(const char* path) const;

    std::atomic<bool> running_{false};
    std::unique_ptr<std::thread> poll_thread_;
    std::unique_ptr<std::thread> worker_thread_;
    bpf_object* bpf_obj_{nullptr};
    ring_buffer* bpf_rb_{nullptr};
    std::vector<bpf_link*> bpf_links_;

    std::mutex callbacks_mutex_;
    std::vector<Callback> callbacks_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<std::string> pending_paths_;

    std::string bpf_o_path_;
    std::string rb_name_;
    std::string cgroup2_mount_;
};
