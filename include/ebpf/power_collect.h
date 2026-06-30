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
#ifndef POWER_COLLECT_H
#define POWER_COLLECT_H

// 用于给用户态程序共享数据结构
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#endif

/* Per-CPU state: what is currently running on each core */
struct cpu_state {
    u64 pid_tgid;   // tgid<<32 | pid
    u64 start_ns;   // timestamp when this task started on this CPU
};

/* Key for per-task per-CPU runtime map */
struct task_cpu_key {
    u64 pid_tgid;   // tgid<<32 | pid
    u32 cpu;
} __attribute__((packed));

/* Per-task per-CPU accumulated runtime (user-space readout) */
struct task_cpu_runtime {
    u64 pid_tgid;
    u32 cpu;
    u64 runtime_ns;
};

#endif
