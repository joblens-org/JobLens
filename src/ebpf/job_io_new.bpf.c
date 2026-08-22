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
#include "ebpf/job_io_new.h" //一定要放在最后面

#define TP_ARGS(dst, idx, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int) + idx * (sizeof(long unsigned int)); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define TP_RET(dst, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define MAX_ENTRIES 65536

/* pid → job_id。LRU_HASH：resolve_job 命中 cgroup 后回写短命/子进程 pid，需内核自动淘汰脏项 */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u32);
    __type(value, u64);
    __uint(max_entries, MAX_ENTRIES);
} pid2job SEC(".maps");

/* cgroup_id(kernfs ino) → job_id。用户态 stat().st_ino 写入，与 bpf_get_current_cgroup_id() 同值 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u64);
    __type(value, u64);
    __uint(max_entries, 4096);
} cgroup2job SEC(".maps");

/* Job 级 I/O 累加器：key=job_id，含短命进程，summary 按 JobID O(1) 读 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u64);
    __type(value, struct rw_stat);
    __uint(max_entries, 4096);
} job_stat SEC(".maps");

/* 明细累加器：key={job_id,pid,fd}，用户态遍历发现短命进程 + 按文件/进程聚合 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct job_pid_fd_key);
    __type(value, struct rw_stat);
    __uint(max_entries, MAX_ENTRIES);
} job_fd_stat SEC(".maps");

/* 时延直方图：key={job_id,bucket,is_write}，value=count（累积） */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct latency_key);
    __type(value, u64);
    __uint(max_entries, 4096 * 64);
} latency_hist SEC(".maps");

/* 逐级解析归属 JobID：先查 pid2job；miss 则查 cgroup2job 并把 pid 回写 pid2job 做缓存 */
static __always_inline u64* resolve_job(u32 pid)
{
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (job_id)
        return job_id;

    u64 cgid = bpf_get_current_cgroup_id();
    job_id = bpf_map_lookup_elem(&cgroup2job, &cgid);
    if (job_id) {
        u64 jid = *job_id;
        bpf_map_update_elem(&pid2job, &pid, &jid, BPF_ANY);
        return bpf_map_lookup_elem(&pid2job, &pid);
    }
    return NULL;
}

/* 低端线性 + 高端 log2 混合分桶（微秒基准）：
 *   <1us → 0；1-100us 10us 步长；100us-1ms 100us 步长；1-10ms 1ms 步长；
 *   10-100ms 10ms 步长；100ms-1s 100ms 步长；>=1s → 47 catch-all */
static __always_inline u32 latency_bucket(u64 ns)
{
    u64 us = ns / 1000;
    if (us < 1)          return 0;
    if (us < 100)        return 1 + (us - 1) / 10;
    if (us < 1000)       return 11 + (us - 100) / 100;
    if (us < 10000)      return 20 + (us - 1000) / 1000;
    if (us < 100000)     return 29 + (us - 10000) / 10000;
    if (us < 1000000)    return 38 + (us - 100000) / 100000;
    return 47;
}

static __always_inline s8 sign_and_abs_s64(s64* x)
{
    if (*x >= 0) return 1;
    *x = -*x;
    return -1;
}

static __always_inline u64 div_u64(u64 x, u64 d)
{
    if (d == 0) return 0;
    return x / d;
}

/* 对一个 rw_stat 累加器施加单次读/写：字节累计 + Welford 在线均值/方差 */
static __always_inline void apply_rw(struct rw_stat *stat, ssize_t ret, bool is_write)
{
    s64 diff = ret, diff2 = 0;
    s8 sign_diff = 1;
    if (is_write){
        __sync_fetch_and_add(&stat->write_bytes, ret);
        __sync_fetch_and_add(&stat->write_count, 1);
        __sync_fetch_and_sub(&diff, stat->write_mean);
        sign_diff = sign_and_abs_s64(&diff);
        u64 r = div_u64(diff, stat->write_count);
        __sync_fetch_and_add(&stat->write_mean, sign_diff * r);
        __sync_fetch_and_sub(&diff2, stat->write_mean);
        __sync_fetch_and_add(&stat->write_variance, diff2 * diff * r);
        stat->write_ktimestamp = bpf_ktime_get_ns();
    } else {
        __sync_fetch_and_add(&stat->read_bytes, ret);
        __sync_fetch_and_add(&stat->read_count, 1);
        __sync_fetch_and_sub(&diff, stat->read_mean);
        sign_diff = sign_and_abs_s64(&diff);
        u64 r = div_u64(diff, stat->read_count);
        __sync_fetch_and_add(&stat->read_mean, sign_diff * r);
        __sync_fetch_and_sub(&diff2, stat->read_mean);
        __sync_fetch_and_add(&stat->read_variance, diff2 * diff * r);
        stat->read_ktimestamp = bpf_ktime_get_ns();
    }
}

/* 明细 + Job 级双累加：短命进程无法被用户态按 pid 枚举，但在内核侧按 job_id 汇总 */
static __always_inline void account_rw(u64 job_id, u32 pid, u32 fd, ssize_t ret, bool is_write)
{
    if (ret <= 0) return;

    struct job_pid_fd_key key = {.job_id = job_id, .pid = pid, .fd = fd};
    struct rw_stat *stat = bpf_map_lookup_elem(&job_fd_stat, &key);
    if (!stat) {
        struct rw_stat init = {};
        bpf_map_update_elem(&job_fd_stat, &key, &init, BPF_ANY);
        stat = bpf_map_lookup_elem(&job_fd_stat, &key);
        if (!stat) return;
    }
    apply_rw(stat, ret, is_write);

    struct rw_stat *jstat = bpf_map_lookup_elem(&job_stat, &job_id);
    if (!jstat) {
        struct rw_stat init = {};
        bpf_map_update_elem(&job_stat, &job_id, &init, BPF_ANY);
        jstat = bpf_map_lookup_elem(&job_stat, &job_id);
        if (!jstat) return;
    }
    apply_rw(jstat, ret, is_write);
}

static __always_inline void record_latency(u64 job_id, u64 enter_ns, bool is_write)
{
    u64 now = bpf_ktime_get_ns();
    struct latency_key lk = {
        .job_id = job_id,
        .bucket = latency_bucket(now - enter_ns),
        .is_write = is_write ? 1 : 0,
    };
    u64 one = 1;
    u64 *cnt = bpf_map_lookup_elem(&latency_hist, &lk);
    if (cnt) {
        __sync_fetch_and_add(cnt, 1);
    } else {
        bpf_map_update_elem(&latency_hist, &lk, &one, BPF_ANY);
    }
}

/* 4 个 enter map：tid → {fd, ts} */
#define DEFINE_ENTER_MAP(name) \
struct { \
    __uint(type, BPF_MAP_TYPE_LRU_HASH); \
    __type(key, u64); \
    __type(value, struct enter_ctx); \
    __uint(max_entries, 4096); \
} name SEC(".maps");

DEFINE_ENTER_MAP(read_enter_args)
DEFINE_ENTER_MAP(write_enter_args)
DEFINE_ENTER_MAP(read64_enter_args)
DEFINE_ENTER_MAP(write64_enter_args)

#define DEFINE_ENTER(name, map) \
SEC("tracepoint/syscalls/sys_enter_" #name) \
int trace_##name##_enter(struct trace_event_raw_sys_enter* ctx) { \
    u64 tid = bpf_get_current_pid_tgid(); \
    u32 pid = tid >> 32; \
    u64 *job_id = resolve_job(pid); \
    if (!job_id) return 0; \
    struct enter_ctx e = {.ts = bpf_ktime_get_ns()}; \
    TP_ARGS(&e.fd, 0, ctx); \
    bpf_map_update_elem(&map, &tid, &e, BPF_ANY); \
    return 0; \
}

#define DEFINE_EXIT(name, map, is_write) \
SEC("tracepoint/syscalls/sys_exit_" #name) \
int trace_##name##_exit(struct trace_event_raw_sys_exit* ctx) { \
    u64 tid = bpf_get_current_pid_tgid(); \
    u32 pid = tid >> 32; \
    u64 *job_id = resolve_job(pid); \
    if (!job_id) return 0; \
    struct enter_ctx *e = bpf_map_lookup_elem(&map, &tid); \
    if (!e) return 0; \
    ssize_t ret; \
    TP_RET(&ret, ctx); \
    account_rw(*job_id, pid, e->fd, ret, is_write); \
    record_latency(*job_id, e->ts, is_write); \
    bpf_map_delete_elem(&map, &tid); \
    return 0; \
}

DEFINE_ENTER(read, read_enter_args)
DEFINE_EXIT(read, read_enter_args, false)
DEFINE_ENTER(write, write_enter_args)
DEFINE_EXIT(write, write_enter_args, true)
DEFINE_ENTER(pread64, read64_enter_args)
DEFINE_EXIT(pread64, read64_enter_args, false)
DEFINE_ENTER(pwrite64, write64_enter_args)
DEFINE_EXIT(pwrite64, write64_enter_args, true)

char LICENSE[] SEC("license") = "GPL";
