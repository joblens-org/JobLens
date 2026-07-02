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
 * power_collect.bpf.c — eBPF sched_switch 功耗追踪
 *
 * 钩住内核调度器的上下文切换事件，以纳秒精度记录每个(tgid,pid)
 * 在每个CPU核心上的实际运行时间。
 *
 * 工作原理:
 *   每次Linux调度器切换进程时 (~10,000次/秒):
 *     step 1. 通过BPF_CORE_READ读prev进程的pid和tgid (CO-RE: 跨内核兼容)
 *     step 2. delta = bpf_ktime_get_ns() - cpu_state[本核].start_ns
 *     step 3. task_cpu_time[{prev_pid_tgid, cpu}] += delta
 *     step 4. cpu_state[本核] = {next_pid_tgid, now}  (为下一次切换做准备)
 *
 * 三个BPF Maps:
 *   cpu_state     - PERCPU_ARRAY: 每核当前运行状态 (天然隔离，无锁)
 *   task_cpu_time - HASH: 每(任务,CPU)的累计运行纳秒
 *   pid2job       - HASH: PID→JobID映射 (用户态批量写入)
 *
 * 用户态每个采集周期dump并清空task_cpu_time，结合RAPL ΔE_pkg和
 * 每核心频率，用频率加权公式将整机能耗归因到各个作业。
 */
#include "ebpf/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "ebpf/power_collect.h"

/* ── BPF Maps ───────────────────────────────────────────────────────────── */

/*
 * 每核心当前运行任务追踪
 * BPF_MAP_TYPE_PERCPU_ARRAY: 每个CPU有独立的1元素数组。
 * bpf_get_smp_processor_id() 自动返回当前CPU的副本，无需手动索引。
 * key=0固定，value是struct cpu_state。
 */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, u32);
    __type(value, struct cpu_state);
} cpu_state SEC(".maps");

/*
 * 每任务、每核心累计运行时间 (纳秒)
 * key = {pid_tgid, cpu} (struct task_cpu_key)
 * value = u64 runtime_ns
 * 262144条目的容量足够追踪数千个进程×128核心
 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 262144);
    __type(key, struct task_cpu_key);
    __type(value, u64);
} task_cpu_time SEC(".maps");

/*
 * PID → JobID 映射表
 * 由用户态 PowerCollector::update_pid2job_map() 在每个采集周期批量写入。
 * eBPF侧仅用于查询——但当前版本的归因计算在用户态完成，
 * 此map可在eBPF侧做预过滤以减小map遍历开销。
 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u32);       /* tid (线程ID) */
    __type(value, u64);     /* JobID */
    __uint(max_entries, 65536);
} pid2job SEC(".maps");

/* ── sched_switch tracepoint (BTF-enabled) ──────────────────────────────── */

/*
 * tp_btf/sched_switch: 使用内核BTF类型信息提供有类型的tracepoint参数。
 * BPF_PROG宏展开为: 接收(void *ctx)，自动提取preempt/prev/next。
 * 相比传统raw tracepoint (tp/sched/switch)，tp_btf不需要手动从
 * u64 ctx[]数组取task_struct指针，更可靠且编译时类型安全。
 */
SEC("tp_btf/sched_switch")
int BPF_PROG(on_sched_switch, bool preempt,
             struct task_struct *prev, struct task_struct *next)
{
    /* ── 通过CO-RE读取prev和next的pid/tgid ──
     * BPF_CORE_READ_INTO 使用内核BTF自动解析task_struct字段偏移，
     * 同一份eBPF字节码可跨内核版本运行 (5.5+)
     */
    pid_t prev_pid = 0, prev_tgid = 0, next_pid = 0, next_tgid = 0;
    int ret;

    ret = BPF_CORE_READ_INTO(&prev_pid, prev, pid);
    if (ret) return 0;
    ret = BPF_CORE_READ_INTO(&prev_tgid, prev, tgid);
    if (ret) return 0;
    ret = BPF_CORE_READ_INTO(&next_pid, next, pid);
    if (ret) return 0;
    ret = BPF_CORE_READ_INTO(&next_tgid, next, tgid);
    if (ret) return 0;

    /* ── 获取当前时间和CPU编号 ── */
    u64 now = bpf_ktime_get_ns();          // CLOCK_MONOTONIC 纳秒时间戳
    u32 cpu  = bpf_get_smp_processor_id(); // 当前执行的CPU核心
    u32 zero = 0;

    /* ── 读取本核当前运行状态 ──
     * PERCPU_ARRAY + key=0 → 自动返回本CPU的副本，不需要显式索引cpu编号
     */
    struct cpu_state *cs = bpf_map_lookup_elem(&cpu_state, &zero);
    if (!cs)
        return 0;

    /* ── 为刚离开CPU的prev进程累加运行时间 ── */
    u64 prev_pid_tgid = cs->pid_tgid;
    if (prev_pid_tgid != 0 && cs->start_ns != 0) {
        u64 delta = now - cs->start_ns;
        /* 安全检查: delta>0 且小于 ~36.5小时 (1ULL<<60 ns)，
         * 过滤时钟异常和空闲任务意外溢出 */
        if (delta > 0 && delta < (1ULL << 60)) {
            struct task_cpu_key key = {
                .pid_tgid = prev_pid_tgid,
                .cpu      = cpu,
            };
            /* 累加: 尝试原地更新(已有记录)，不存在则插入新记录。
             * in-place update安全——每个(pid_tgid,cpu)对是唯一的，
             * 24核并发写入不同的key，无竞争条件。 */
            u64 *existing = bpf_map_lookup_elem(&task_cpu_time, &key);
            if (existing) {
                *existing += delta;
            } else {
                bpf_map_update_elem(&task_cpu_time, &key, &delta, BPF_ANY);
            }
        }
    }

    /* ── 开始追踪next进程 ──
     * 更新本核状态: 把下一个进程设为"当前运行"，记录开始时刻
     */
    cs->pid_tgid = ((u64)next_tgid << 32) | (u32)next_pid;
    cs->start_ns = now;

    return 0;
}

char LICENSE[] SEC("license") = "GPL";
