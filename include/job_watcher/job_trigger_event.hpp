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

#include <string>
#include <variant>
#include <cstdint>

// BPF 侧传递的调度器类型提示
enum class SchedulerHint {
    Unknown = 0,
    Condor  = 1,
    Slurm   = 2,
};

// 来自 cgroup_mkdir 触发器的 cgroup 路径事件
struct CgroupMkdirTriggerEvent {
    std::string cgroup_path;
};

// 来自 execve 触发器的进程启动事件
struct ExecveTriggerEvent {
    uint32_t pid;
    uint32_t tgid;
    uint32_t ppid;
    uint32_t ptgid;
    std::string comm;
    std::string parent_comm;
    SchedulerHint scheduler_hint{SchedulerHint::Unknown};
};

// 统一的触发事件类型：一个变体包含两种事件
using TriggerEvent = std::variant<CgroupMkdirTriggerEvent, ExecveTriggerEvent>;
