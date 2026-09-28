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
#ifndef JOB_PID_TRACK_H
#define JOB_PID_TRACK_H
// 用于给内核态 BPF 程序与用户态程序共享数据结构。
// 内核态: 先 include vmlinux.h(定义 __VMLINUX_H__)后再 include 本头。
// 用户态: 走 bpf_types.h 提供 u32/u64 别名。
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#include <linux/bpf.h>
#endif

// ============================================================================
// 三个共享 map 的权威规格。所有引用这些 map 的 .bpf.c 必须逐字节使用相同
// 的 type / key / value / max_entries, 否则 libbpf 通过 pinning reuse 时
// 内核会做定义校验并失败。为保证一致性, 统一用下列宏声明。
// ============================================================================

// pin 根目录: 所有共享 map 都 pin 到 bpffs 的该目录下。
#define JOBLENS_BPF_PIN_ROOT "/sys/fs/bpf/joblens"

// pid2job: pid(tgid) -> job_id。
//   - 用户态 JobRegistry 在 Job 新增时写入"根 pid"作为种子;
//   - 内核态 sched_process_fork hook 在子进程继承 parent 归属时繁衍写入;
//   - 内核态 sched_process_exit hook 在进程退出时删除。
//   LRU_HASH: pid 回卷/漏删时由内核自动淘汰脏项兜底。
#define JOBLENS_PID2JOB_MAX_ENTRIES 65536

// cgroup2job: cgroup_id(kernfs ino) -> job_id。
//   - 用户态 JobRegistry 在 Job 新增/删除时维护;
//   - 常规作业调度器同一 Job 只用一个 cgroup, 故容量 4096 足够。
#define JOBLENS_CGROUP2JOB_MAX_ENTRIES 4096

// job_event ring buffer 大小(字节)。
#define JOBLENS_JOB_EVENT_RB_SIZE (256 * 1024)

// map 名(内核态 SEC 名与用户态查找名必须一致)。
#define JOBLENS_PID2JOB_MAP_NAME     "pid2job"
#define JOBLENS_CGROUP2JOB_MAP_NAME  "cgroup2job"
#define JOBLENS_JOB_EVENT_RB_NAME    "job_event_rb"

// job_event ringbuf 统计 map 名。每 CPU 一份 u64, 用户态按 CPU 求和后暴露,
// 使溢出丢弃与应用侧真实入队速率都可直接观测(而不是只能靠推算)。
// 与上述三张共享 map 不同, 该 map 不 pin: 它是本程序私有的运行时诊断数据,
// 随进程重启清零, 不参与跨 .bpf.o 的 create-or-reuse 校验。
#define JOBLENS_JOB_EVENT_STATS_MAP_NAME "job_event_stats"
#define JOBLENS_JOB_EVENT_STAT_DROPPED   0  // ringbuf 满, reserve 失败而丢弃
#define JOBLENS_JOB_EVENT_STAT_PUSHED    1  // 成功入队
#define JOBLENS_JOB_EVENT_STAT_SLOTS     2

// ringbuf 事件类型。
enum job_pid_event_type {
    JOB_PID_EVENT_FORK = 0,  // 子进程继承了某 Job 归属, pid 已加入 pid2job
    JOB_PID_EVENT_EXIT = 1,  // 属于某 Job 的进程退出, pid 已从 pid2job 删除
};

// 内核态 -> 用户态的进程生命周期事件。
struct job_pid_event {
    u32 type;     // enum job_pid_event_type
    u32 pid;      // 发生变化的进程 pid(tgid)
    u64 job_id;   // 归属的 Job ID
};

#endif // JOB_PID_TRACK_H
