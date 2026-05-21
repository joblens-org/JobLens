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
#include "ebpf/trace_slurm_stepd.h"

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);   /* 256 KB */
} slurm_exec_events SEC(".maps");

#define TARGET "slurmstepd"

/* 检查字符串是否以prefix开头 */
static __always_inline int str_starts_with(const char *str, const char *prefix, int len)
{
    int i = 0;
    while (prefix[i] != '\0' && i < len) {
        if (str[i] != prefix[i])
            return 0;
        i++;
    }
    return (prefix[i] == '\0') ? 1 : 0;
}

SEC("tracepoint/syscalls/sys_exit_execve")
int trace_slurm_exit_execve(struct trace_event_raw_sys_exit *ctx)
{
    struct task_struct *task = (struct task_struct *)bpf_get_current_task();
    struct task_struct *the_parent;
    struct slurm_event *e;
    char parent_comm[TASK_COMM_LEN];
    
    BPF_CORE_READ_INTO(&the_parent, task, real_parent);
    BPF_CORE_READ_STR_INTO(&parent_comm, the_parent, comm);
    
    /* 检查父进程是否是 slurmstepd */
    if (bpf_strncmp(parent_comm, sizeof(parent_comm), TARGET) != 0) {
        return 0;
    }
    
    e = bpf_ringbuf_reserve(&slurm_exec_events, sizeof(*e), 0);
    if (!e)
        return 0;
    
    e->pid = bpf_get_current_pid_tgid() >> 32;
    e->ppid = BPF_CORE_READ(task, parent, tgid);
    bpf_get_current_comm(&e->comm, TASK_COMM_LEN);
        
    bpf_ringbuf_submit(e, 0);
    
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
