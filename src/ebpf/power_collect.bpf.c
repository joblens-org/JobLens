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
 * power_collect.bpf.c — eBPF sched_switch tracer for per-task per-CPU runtime
 *
 * Hooks sched_switch via BTF-enabled tracepoint to track, at nanosecond
 * precision, the wall-clock time each (tgid,pid) spends on every CPU core.
 * User space reads the accumulated map and clears it on each sampling interval,
 * then combines the data with RAPL  E_pkg and per-core frequency to attribute
 * package energy to individual jobs.
 */
#include "ebpf/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "ebpf/power_collect.h"

/* ── BPF Maps ───────────────────────────────────────────────────────────── */

/*
 * Per-CPU array (1 element) tracking the currently-running task on each core.
 * BPF_MAP_TYPE_PERCPU_ARRAY gives each CPU its own private copy of the 1 entry.
 */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, u32);
    __type(value, struct cpu_state);
} cpu_state SEC(".maps");

/*
 * Per-task per-CPU accumulated runtime (ns).
 * Key = {pid_tgid, cpu}  Value = runtime_ns
 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 262144);
    __type(key, struct task_cpu_key);
    __type(value, u64);
} task_cpu_time SEC(".maps");

/*
 * PID → JobID mapping.
 * Populated from user space on every collection cycle so the collector
 * can group per-pid runtime by JobID.
 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u32);       /* tid */
    __type(value, u64);     /* JobID */
    __uint(max_entries, 65536);
} pid2job SEC(".maps");

/* ── sched_switch tracepoint (BTF-enabled) ──────────────────────────────── */

/*
 * tp_btf/sched_switch uses the kernel's BTF information to provide typed
 * arguments directly: (bool preempt, struct task_struct *prev,
 *                       struct task_struct *next)
 *
 * The BPF_PROG macro handles unwrapping these from the raw tracepoint context.
 */
SEC("tp_btf/sched_switch")
int BPF_PROG(on_sched_switch, bool preempt,
             struct task_struct *prev, struct task_struct *next)
{
    /* --- read pid & tgid via CO-RE from task_struct --- */
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

    u64 now = bpf_ktime_get_ns();
    u32 cpu  = bpf_get_smp_processor_id();
    u32 zero = 0;

    /* --- read per-CPU state for THIS cpu --- */
    struct cpu_state *cs = bpf_map_lookup_elem(&cpu_state, &zero);
    if (!cs)
        return 0;

    /* --- account runtime for the task that just left this CPU --- */
    u64 prev_pid_tgid = cs->pid_tgid;
    if (prev_pid_tgid != 0 && cs->start_ns != 0) {
        u64 delta = now - cs->start_ns;
        if (delta > 0 && delta < (1ULL << 60)) {  /* sanity cap: ~36.5 hours */
            struct task_cpu_key key = {
                .pid_tgid = prev_pid_tgid,
                .cpu      = cpu,
            };
            u64 *existing = bpf_map_lookup_elem(&task_cpu_time, &key);
            if (existing) {
                /* In-place update is safe — unique (pid_tgid,cpu) per CPU */
                *existing += delta;
            } else {
                bpf_map_update_elem(&task_cpu_time, &key, &delta, BPF_ANY);
            }
        }
    }

    /* --- start tracking the next task on this CPU --- */
    cs->pid_tgid = ((u64)next_tgid << 32) | (u32)next_pid;
    cs->start_ns = now;

    return 0;
}

char LICENSE[] SEC("license") = "GPL";
