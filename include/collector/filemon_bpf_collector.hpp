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
// #pragma once
// #include "collector/ibpf_collector.hpp"
// #include <bpf/bpf.h>
// #include <bpf/libbpf.h>
// #include <thread>
// #include <vector>
// #include <atomic>
// #include "bpf_datastruct/bpf_types.h"
// #include "nlohmann/json.hpp"

// using json = nlohmann::json;

// #define MAX_PATH 256
// #define TASK_COMM_LEN 16

// struct event {
//     u32  pid;
//     u32  tgid;
//     char comm[TASK_COMM_LEN];
//     u64  op;          // 0=read 1=write 2=open 3=close
//     char path[MAX_PATH];
//     size_t cnt;
//     u64 pos;
// };


// class FileMonCollector : public IBpfCollector {
// public:
//     FileMonCollector() = default;
//     ~FileMonCollector() { stop(); }

//     CollectResult collect(const Job& job) override;

// protected:
    
//     bool load_and_attach() override;   // 只做 attach，不写 PID
//     CollectResult fill_result(const Job&) override { return {}; }

//     void do_deinit() noexcept override {
//         pids_filter_cache.clear();
//         tl_events.clear();
//     }
    
// private:
//     void stop() noexcept;

//     /* 把 job.pids 刷进 pid_target map */
//     void update_pid_filter(const int jobid, const std::vector<int>& pids);
    

//     int        rb_map_fd_   = -1;
//     int        pid_map_fd_  = -1;
//     struct ring_buffer* rb_ = nullptr;

//     std::atomic<bool> attached_{false};
    

//     std::unordered_map<int, std::vector<int>> pids_filter_cache;
//     std::vector<event> tl_events;
    
//     ring_buffer_sample_fn event_cb = [](void *ctx, void *data, size_t len) -> int {
//         auto ev = (struct event*)data;
//         auto events = (std::vector<event>*)ctx;
//         events->push_back(*ev);
//         return 0;
//     };
// };