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
 * job_pid_track.bpf.c — 进程生命周期追踪, 内核态自维护 pid2job 映射
 *
 * 权威共享 map 的宿主: 本程序创建并 pin pid2job / cgroup2job / job_event_rb
 * 三张 map 到 bpffs, 其余采集器的 .bpf.o 通过 LIBBPF_PIN_BY_NAME 复用同一份。
 *
 * 工作原理:
 *   - 用户态 JobRegistry 在 Job 新增时写入 cgroup2job 与"根 pid"种子 pid2job;
 *   - sched_process_fork: 子进程继承 parent 的 Job 归属(child 优先按 parent
 *     的 pid2job), 命中则繁衍写入 pid2job 并通过 ringbuf 通知用户态;
 *   - sched_process_exit: 属于某 Job 的进程退出时删除 pid2job 并通知用户态。
 */
#include "ebpf/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include "ebpf/job_pid_track.h" // 一定要放在最后面

/* pid(tgid) -> job_id。LRU_HASH: pid 回卷或漏删时内核自动淘汰脏项。 */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u32);
    __type(value, u64);
    __uint(max_entries, JOBLENS_PID2JOB_MAX_ENTRIES);
    __uint(pinning, LIBBPF_PIN_BY_NAME);
} pid2job SEC(".maps");

/* cgroup_id(kernfs ino) -> job_id。用户态 stat().st_ino 写入, 与
 * bpf_get_current_cgroup_id() 同值。 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u64);
    __type(value, u64);
    __uint(max_entries, JOBLENS_CGROUP2JOB_MAX_ENTRIES);
    __uint(pinning, LIBBPF_PIN_BY_NAME);
} cgroup2job SEC(".maps");

/* 进程生命周期事件 ringbuf: 内核态推送 FORK/EXIT, 用户态 JobRegistry 消费。 */
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, JOBLENS_JOB_EVENT_RB_SIZE);
    __uint(pinning, LIBBPF_PIN_BY_NAME);
} job_event_rb SEC(".maps");

/* 向用户态推送一条进程生命周期事件。 */
static __always_inline void push_event(u32 type, u32 pid, u64 job_id)
{
    struct job_pid_event *e = bpf_ringbuf_reserve(&job_event_rb, sizeof(*e), 0);
    if (!e)
        return;
    e->type = type;
    e->pid = pid;
    e->job_id = job_id;
    bpf_ringbuf_submit(e, 0);
}

/* sched_process_fork: 上下文为 parent 进程, ctx 携带 parent/child pid。
 * fork 在 child pid 分配后触发, parent_pid 与 child_pid 均有效。
 * child 优先继承 parent 的 Job 归属(pid2job)。 */
SEC("tp/sched/sched_process_fork")
int on_sched_process_fork(struct trace_event_raw_sched_process_fork *ctx)
{
    u32 parent_pid = (u32)ctx->parent_pid;
    u32 child_pid = (u32)ctx->child_pid;

    u64 *job_id = bpf_map_lookup_elem(&pid2job, &parent_pid);
    if (!job_id)
        return 0; // parent 不属于任何 Job, 忽略(种子根 pid 由用户态写)

    u64 jid = *job_id;
    bpf_map_update_elem(&pid2job, &child_pid, &jid, BPF_ANY);
    push_event(JOB_PID_EVENT_FORK, child_pid, jid);
    return 0;
}

/* sched_process_exit: 使用 sched_process_template 上下文(无 pid 字段),
 * 通过 bpf_get_current_pid_tgid() 取 pid/tid, 仅处理进程级退出。 */
SEC("tp/sched/sched_process_exit")
int on_sched_process_exit(struct trace_event_raw_sched_process_template *ctx)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 pid = id >> 32;   // tgid
    u32 tid = (u32)id;
    if (pid != tid)
        return 0; // 线程退出, 非进程退出, 忽略

    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0; // 不属于任何 Job

    u64 jid = *job_id;
    push_event(JOB_PID_EVENT_EXIT, pid, jid);
    bpf_map_delete_elem(&pid2job, &pid);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
