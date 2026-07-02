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
 * limitations under the License.
 *
 * power_collect.h — eBPF 功耗采集共享数据结构
 * 内核态(eBPF程序)和用户态(PowerCollector)共同引用的结构体定义。
 * 通过 #include 同一份头文件保证二进制布局一致。
 */
#ifndef POWER_COLLECT_H
#define POWER_COLLECT_H

// 用于给用户态程序共享数据结构
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#endif

/*
 * 每核当前运行任务追踪
 * BPF_MAP_TYPE_PERCPU_ARRAY 类型，每个CPU核心有独立的一份副本。
 * 每次sched_switch时记录: 谁正在这个核心上跑、从什么时候开始的。
 */
struct cpu_state {
    u64 pid_tgid;   // tgid<<32 | pid (高32位进程组ID，低32位线程ID)
    u64 start_ns;   // 这个任务在这个核心上开始运行的时刻 (bpf_ktime_get_ns)
};

/*
 * 每任务、每核心累计运行时间的key
 * key={pid_tgid, cpu}，value=runtime_ns
 * 每个(进程,CPU)对有一条独立记录，24核并发写入无锁竞争。
 * __attribute__((packed)) 消除结构体尾部padding，保证BPF map key大小匹配。
 */
struct task_cpu_key {
    u64 pid_tgid;   // tgid<<32 | pid
    u32 cpu;        // CPU核心编号 (bpf_get_smp_processor_id)
} __attribute__((packed));

/*
 * 用户态 dump 时使用的值类型
 * 每次采集周期遍历BPF map，把key+value解析成这个结构体返回
 */
struct task_cpu_runtime {
    u64 pid_tgid;       // 进程标识
    u32 cpu;            // CPU编号
    u64 runtime_ns;     // 自上次clear以来的累计纳秒运行时间
};

#endif
