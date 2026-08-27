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
#include <bpf/bpf_core_read.h>
#include "ebpf/job_pid_track.h" // 一定要放在最后面

/* kfunc: 按 pid 取 task 引用, 用于在 fork 上下文读 child 的 tgid。
 * 需要内核 5.15+(本项目 CO-RE 目标内核均满足)。 */
extern struct task_struct *bpf_task_from_pid(int pid) __ksym;
extern void bpf_task_release(struct task_struct *p) __ksym;

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

/* sched_process_fork: 上下文为 parent, ctx 携带 parent/child 的 pid(线程级 tid)。
 * 本追踪统一为进程(tgid)语义: pid2job 只存 tgid。
 *   - child 是新线程(child_tgid == parent 所属进程 tgid): 其 tgid 已作为进程种子
 *     在 pid2job 中, 无需处理;
 *   - child 是新进程(child_tgid == child_pid): 继承 parent 进程的 Job 归属,
 *     以 child_tgid 为 key 写入 pid2job 并通知用户态。
 * 用 bpf_task_from_pid 读 child 的 tgid 以区分二者。 */
SEC("tp/sched/sched_process_fork")
int on_sched_process_fork(struct trace_event_raw_sched_process_fork *ctx)
{
    u32 child_pid = (u32)ctx->child_pid;

    struct task_struct *child = bpf_task_from_pid(child_pid);
    if (!child)
        return 0;
    u32 child_tgid = (u32)BPF_CORE_READ(child, tgid);
    bpf_task_release(child);

    if (child_tgid != child_pid)
        return 0; // child 是新线程, 其进程 tgid 已被追踪, 忽略

    // child 是新进程: 用 parent 的 tgid 查归属(parent 上下文 tgid = current tgid)
    u32 parent_tgid = bpf_get_current_pid_tgid() >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &parent_tgid);
    if (!job_id)
        return 0; // parent 进程不属于任何 Job

    u64 jid = *job_id;
    bpf_map_update_elem(&pid2job, &child_tgid, &jid, BPF_ANY);
    push_event(JOB_PID_EVENT_FORK, child_tgid, jid);
    return 0;
}

/* sched_process_exit: 按 tid 触发。pid2job 只存 tgid, 故仅在进程组代表
 * (tid == tgid)退出时回收该 tgid 条目; 普通线程退出无需处理。 */
SEC("tp/sched/sched_process_exit")
int on_sched_process_exit(struct trace_event_raw_sched_process_template *ctx)
{
    u64 id = bpf_get_current_pid_tgid();
    u32 tgid = id >> 32;
    u32 tid = (u32)id;
    if (tgid != tid)
        return 0; // 普通线程退出, pid2job 只存 tgid, 无需处理

    u64 *job_id = bpf_map_lookup_elem(&pid2job, &tgid);
    if (!job_id)
        return 0; // 不属于任何 Job

    u64 jid = *job_id;
    push_event(JOB_PID_EVENT_EXIT, tgid, jid);
    bpf_map_delete_elem(&pid2job, &tgid);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
