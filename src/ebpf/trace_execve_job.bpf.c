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
#include "ebpf/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "ebpf/trace_execve_job.h"

// 内核中初筛父子进程 comm，返回调度器类型
static inline __u32 get_scheduler_hint(const char *comm, __u32 len)
{
    // 检查是否以 "condor_starter" 开头
    if (len >= 14) {
        if (comm[0] == 'c' && comm[1] == 'o' && comm[2] == 'n' &&
            comm[3] == 'd' && comm[4] == 'o' && comm[5] == 'r' &&
            comm[6] == '_' && comm[7] == 's' && comm[8] == 't' &&
            comm[9] == 'a' && comm[10] == 'r' && comm[11] == 't' &&
            comm[12] == 'e' && comm[13] == 'r')
            return SCHEDULER_HINT_CONDOR;
    }
    // 检查是否以 "slurmstepd" 开头
    if (len >= 10) {
        if (comm[0] == 's' && comm[1] == 'l' && comm[2] == 'u' &&
            comm[3] == 'r' && comm[4] == 'm' && comm[5] == 's' &&
            comm[6] == 't' && comm[7] == 'e' && comm[8] == 'p' &&
            comm[9] == 'd')
            return SCHEDULER_HINT_SLURM;
    }
    return SCHEDULER_HINT_UNKNOWN;
}

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} execve_job_events SEC(".maps");

SEC("tracepoint/syscalls/sys_exit_execve")
int trace_execve_job(struct trace_event_raw_sys_exit *ctx)
{
    struct task_struct *task;
    struct task_struct *parent;
    struct execve_job_event *e;
    __u64 pid_tgid;
    __u64 parent_pid_tgid;
    __u32 scheduler_hint;
    char parent_comm[TASK_COMM_LEN];

    task = (struct task_struct *)bpf_get_current_task();
    if (!task)
        return 0;

    // 读取真实父进程
    if (BPF_CORE_READ_INTO(&parent, task, real_parent) != 0)
        return 0;
    if (!parent)
        return 0;

    // 读取父进程的 comm 名称
    if (bpf_probe_read_kernel_str(parent_comm, TASK_COMM_LEN, parent->comm) <= 0)
        return 0;

    // 内核侧初筛：只接受 condor_starter / slurmstepd 子进程
    scheduler_hint = get_scheduler_hint(parent_comm, TASK_COMM_LEN);
    if (scheduler_hint == SCHEDULER_HINT_UNKNOWN)
        return 0;

    e = bpf_ringbuf_reserve(&execve_job_events, sizeof(*e), 0);
    if (!e)
        return 0;

    pid_tgid = bpf_get_current_pid_tgid();
    e->pid  = (__u32)pid_tgid;
    e->tgid = (__u32)(pid_tgid >> 32);

    // ppid/ptgid 必须从真实父进程读取，不能从当前进程的 pid_tgid 推导
    parent_pid_tgid = BPF_CORE_READ(parent, pid);
    e->ppid  = (__u32)parent_pid_tgid;
    e->ptgid = (__u32)BPF_CORE_READ(parent, tgid);

    e->scheduler_hint = scheduler_hint;
    bpf_get_current_comm(&e->comm, sizeof(e->comm));

    // 手动复制父 comm
    for (int i = 0; i < TASK_COMM_LEN; i++) {
        e->parent_comm[i] = parent_comm[i];
        if (parent_comm[i] == '\0')
            break;
    }

    bpf_ringbuf_submit(e, 0);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
