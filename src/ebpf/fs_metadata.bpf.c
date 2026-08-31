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
#include "ebpf/job_pid_track.h" // 共享 pid2job / cgroup2job 规格
#include "ebpf/fs_metadata.h"   //一定要放在最后面

#define TP_ARGS(dst, idx, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int) + idx * (sizeof(long unsigned int)); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define TP_RET(dst, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

/* ── map 容量推导 ───────────────────────────────────────────────────────
 * 单节点 Job 上限 ~300(取决于 CPU 核数, 通常更小)。据此按实际规模精算容量,
 * 避免过度预留内核内存。共享 map(pid2job/cgroup2job)容量由 job_pid_track.h
 * 宏统一控制, 本处不得改动(否则 pinning reuse 校验失败)。 */
#define FS_META_MAX_JOBS      384   /* Job 上限余量(~300 实际 + 冗余)          */
#define FS_META_PROCS_PER_JOB 128   /* 单 Job 做元数据操作的进程/线程规模上限   */
#define FS_META_OP_KINDS      21    /* 元数据操作类型数(= FS_META_MAX)         */
#define FS_META_USED_BUCKETS  48    /* latency_bucket() 实际使用的桶数(0..47)  */
/* 明细 map 的稀疏系数: 单进程通常只触发少数几种元数据 op(open/close/getattr/
 * read...), 而非全部 21 种。按每进程活跃 op 种类上限精算, 避免为不存在的
 * {pid,op} 笛卡尔组合过度预分配内核内存(HASH map 按 max_entries 预分配)。 */
#define FS_META_OPS_PER_PROC  8

/* AT_REMOVEDIR 标志位定义 */
#define AT_REMOVEDIR 0x200

/* O_CREAT 标志位定义 (八进制 0100 = 0x40) */
#define O_CREAT 0100

/* pid → job_id。共享 map(由 job_pid_track.bpf.o 维护, pin 复用)。
 * 本对象只读: fork 繁衍/exit 回收统一由 job_pid_track 负责, 避免双写竞争。 */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u32);
    __type(value, u64);
    __uint(max_entries, JOBLENS_PID2JOB_MAX_ENTRIES);
    __uint(pinning, LIBBPF_PIN_BY_NAME);
} pid2job SEC(".maps");

/* cgroup_id(kernfs ino) → job_id。共享 map, pin 复用。用户态 stat().st_ino 写入,
 * 与 bpf_get_current_cgroup_id() 同值。 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u64);
    __type(value, u64);
    __uint(max_entries, JOBLENS_CGROUP2JOB_MAX_ENTRIES);
    __uint(pinning, LIBBPF_PIN_BY_NAME);
} cgroup2job SEC(".maps");

/* 明细累加器：key={job_id,pid,op}，用户态遍历发现短命进程 + 按进程聚合 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct fs_meta_key);
    __type(value, struct fs_meta_stat);
    __uint(max_entries, FS_META_MAX_JOBS * FS_META_PROCS_PER_JOB * FS_META_OPS_PER_PROC);
} fs_meta_stat SEC(".maps");

/* Job 级累加器：key={job_id,op}，含短命进程，summary 按 JobID O(1) 读 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct fs_meta_job_key);
    __type(value, struct fs_meta_stat);
    __uint(max_entries, FS_META_MAX_JOBS * FS_META_OP_KINDS);
} fs_meta_job_stat SEC(".maps");

/* 时延直方图：key={job_id,op,bucket}，value=count（累积） */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct fs_meta_latency_key);
    __type(value, u64);
    __uint(max_entries, FS_META_MAX_JOBS * FS_META_OP_KINDS * FS_META_USED_BUCKETS);
} fs_meta_latency_hist SEC(".maps");

/* enter 时间戳 LRU map，用于延迟计算 (tid → enter_ns) */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);
    __type(value, u64);
    __uint(max_entries, 1024 * 4);
} meta_enter_ts SEC(".maps");

/* enter 操作类型 LRU map，用于在 exit 时确定操作类型 (tid → op) */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);
    __type(value, u32);
    __uint(max_entries, 1024 * 4);
} meta_enter_op SEC(".maps");

/* 逐级解析归属 JobID：先查 pid2job；miss 则查 cgroup2job。
 * 注意: pid2job 的繁衍(回写)统一由 job_pid_track 的 fork hook 负责, 本处只读, 不回写。 */
static __always_inline u64* resolve_job(u32 pid)
{
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (job_id)
        return job_id;

    u64 cgid = bpf_get_current_cgroup_id();
    return bpf_map_lookup_elem(&cgroup2job, &cgid);
}

/* 低端线性 + 高端 log2 混合分桶（微秒基准，对标 job_io_new）：
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

/* 对一个 fs_meta_stat 累加器施加单次操作：次数 + 成功/错误 + 时延累计 */
static __always_inline void apply_meta(struct fs_meta_stat *stat, ssize_t ret, u64 lat_ns)
{
    __sync_fetch_and_add(&stat->calls, 1);
    if (ret >= 0) {
        __sync_fetch_and_add(&stat->success, 1);
    } else {
        __sync_fetch_and_add(&stat->errors, 1);
        stat->last_errno = ret; /* 保存错误码（负数） */
    }
    if (lat_ns > 0) {
        __sync_fetch_and_add(&stat->total_latency_ns, lat_ns);
        if (lat_ns > stat->max_latency_ns) {
            stat->max_latency_ns = lat_ns;
        }
    }
    stat->last_timestamp_ns = bpf_ktime_get_ns();
}

/* 记录时延桶：key={job_id,op,bucket}，计数累加 */
static __always_inline void record_meta_latency(u64 job_id, u32 op, u64 lat_ns)
{
    if (lat_ns == 0) return;
    struct fs_meta_latency_key lk = {
        .job_id = job_id,
        .op     = op,
        .bucket = latency_bucket(lat_ns),
    };
    u64 one = 1;
    u64 *cnt = bpf_map_lookup_elem(&fs_meta_latency_hist, &lk);
    if (cnt) {
        __sync_fetch_and_add(cnt, 1);
    } else {
        bpf_map_update_elem(&fs_meta_latency_hist, &lk, &one, BPF_ANY);
    }
}

/* 明细 + Job 级双累加 + 时延桶：短命进程无法被用户态按 pid 枚举，
 * 但在内核侧按 {job_id,op} 汇总，summary 仍可 O(1) 读取。 */
static __always_inline void account_meta(u64 job_id, u32 pid, u32 op, ssize_t ret, u64 enter_ns)
{
    u64 lat_ns = 0;
    if (enter_ns > 0) {
        u64 now = bpf_ktime_get_ns();
        lat_ns = now - enter_ns;
    }

    /* 1. 明细累加器 {job_id,pid,op} */
    struct fs_meta_key key = {.job_id = job_id, .pid = pid, .op = op};
    struct fs_meta_stat *stat = bpf_map_lookup_elem(&fs_meta_stat, &key);
    if (!stat) {
        struct fs_meta_stat init = {};
        bpf_map_update_elem(&fs_meta_stat, &key, &init, BPF_ANY);
        stat = bpf_map_lookup_elem(&fs_meta_stat, &key);
        if (!stat) return;
    }
    apply_meta(stat, ret, lat_ns);

    /* 2. Job 级累加器 {job_id,op} */
    struct fs_meta_job_key jkey = {.job_id = job_id, .op = op, ._pad = 0};
    struct fs_meta_stat *jstat = bpf_map_lookup_elem(&fs_meta_job_stat, &jkey);
    if (!jstat) {
        struct fs_meta_stat init = {};
        bpf_map_update_elem(&fs_meta_job_stat, &jkey, &init, BPF_ANY);
        jstat = bpf_map_lookup_elem(&fs_meta_job_stat, &jkey);
        if (!jstat) return;
    }
    apply_meta(jstat, ret, lat_ns);

    /* 3. 时延直方图 {job_id,op,bucket} */
    record_meta_latency(job_id, op, lat_ns);
}

/* ============================================================================
 * 通用 enter/exit 宏定义 —— 消除逐 syscall 的样板代码。
 * 三种模式:
 *   SIMPLE : 操作类型固定, enter 无需读 flag (如 close/fsync/getattr...)。
 *   FLAG   : enter 时读某个参数(flag), 据此选择 op_a/op_b (如 open/unlinkat...)。
 *   FIXED  : 操作类型固定但 enter 侧显式写 meta_enter_op (如 creat)。
 * 所有 exit 侧统一走 account_meta, 时延/成功/错误由内核统一记账。
 * ==========================================================================*/

/* 通用 enter：仅记录进入时间戳 (op 在 exit 侧由固定值给出) */
#define DEFINE_META_ENTER_SIMPLE(name) \
SEC("tracepoint/syscalls/sys_enter_" #name) \
int trace_enter_##name(struct trace_event_raw_sys_enter* ctx) { \
    u64 tid = bpf_get_current_pid_tgid(); \
    u32 pid = tid >> 32; \
    if (!resolve_job(pid)) return 0; \
    u64 ts = bpf_ktime_get_ns(); \
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY); \
    return 0; \
}

/* 通用 exit：固定 op。读时间戳 → account_meta → 清理时间戳 */
#define DEFINE_META_EXIT_SIMPLE(name, fixed_op) \
SEC("tracepoint/syscalls/sys_exit_" #name) \
int trace_exit_##name(struct trace_event_raw_sys_exit* ctx) { \
    u64 tid = bpf_get_current_pid_tgid(); \
    u32 pid = tid >> 32; \
    u64 *job_id = resolve_job(pid); \
    if (!job_id) return 0; \
    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid); \
    u64 ts = enter_ts ? *enter_ts : 0; \
    ssize_t ret; \
    TP_RET(&ret, ctx); \
    account_meta(*job_id, pid, (fixed_op), ret, ts); \
    bpf_map_delete_elem(&meta_enter_ts, &tid); \
    return 0; \
}

/* 固定 op 的完整探针对 (enter + exit) */
#define DEFINE_META_SIMPLE(name, fixed_op) \
    DEFINE_META_ENTER_SIMPLE(name) \
    DEFINE_META_EXIT_SIMPLE(name, fixed_op)

/* enter 读 flag 决定 op：flag_idx 为参数下标, 命中 flag_bit 用 op_set 否则 op_unset。
 * 同时写 meta_enter_op 供 exit 侧还原。 */
#define DEFINE_META_ENTER_FLAG(name, flag_idx, flag_bit, op_set, op_unset) \
SEC("tracepoint/syscalls/sys_enter_" #name) \
int trace_enter_##name(struct trace_event_raw_sys_enter* ctx) { \
    u64 tid = bpf_get_current_pid_tgid(); \
    u32 pid = tid >> 32; \
    if (!resolve_job(pid)) return 0; \
    u64 ts = bpf_ktime_get_ns(); \
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY); \
    u32 flags = 0; \
    TP_ARGS(&flags, (flag_idx), ctx); \
    u32 op = (flags & (flag_bit)) ? (op_set) : (op_unset); \
    bpf_map_update_elem(&meta_enter_op, &tid, &op, BPF_ANY); \
    return 0; \
}

/* exit 侧从 meta_enter_op 还原 op (配合 DEFINE_META_ENTER_FLAG)。
 * default_op 为找不到记录时的兜底 op。 */
#define DEFINE_META_EXIT_DYNOP(name, default_op) \
SEC("tracepoint/syscalls/sys_exit_" #name) \
int trace_exit_##name(struct trace_event_raw_sys_exit* ctx) { \
    u64 tid = bpf_get_current_pid_tgid(); \
    u32 pid = tid >> 32; \
    u64 *job_id = resolve_job(pid); \
    if (!job_id) return 0; \
    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid); \
    u64 ts = enter_ts ? *enter_ts : 0; \
    u32 *op = bpf_map_lookup_elem(&meta_enter_op, &tid); \
    u32 op_val = op ? *op : (default_op); \
    ssize_t ret; \
    TP_RET(&ret, ctx); \
    account_meta(*job_id, pid, op_val, ret, ts); \
    bpf_map_delete_elem(&meta_enter_ts, &tid); \
    bpf_map_delete_elem(&meta_enter_op, &tid); \
    return 0; \
}

/* flag 型完整探针对 (enter 读 flag + exit 还原 op) */
#define DEFINE_META_FLAG(name, flag_idx, flag_bit, op_set, op_unset, default_op) \
    DEFINE_META_ENTER_FLAG(name, flag_idx, flag_bit, op_set, op_unset) \
    DEFINE_META_EXIT_DYNOP(name, default_op)

/* ==================== open/openat/creat 系列 ==================== */
/* openat: dirfd, pathname, flags(arg2), mode → O_CREAT 决定 create/open */
DEFINE_META_FLAG(openat, 2, O_CREAT, FS_META_CREATE, FS_META_OPEN, FS_META_OPEN)
/* open: pathname, flags(arg1), mode → O_CREAT 决定 create/open */
DEFINE_META_FLAG(open,   1, O_CREAT, FS_META_CREATE, FS_META_OPEN, FS_META_OPEN)
/* creat: pathname, mode (等价 open with O_CREAT|O_WRONLY|O_TRUNC) → 固定 create */
DEFINE_META_SIMPLE(creat, FS_META_CREATE)

/* ==================== close 系列 ==================== */
DEFINE_META_SIMPLE(close, FS_META_CLOSE)

/* ==================== stat 系列 (getattr) ==================== */
DEFINE_META_SIMPLE(newfstatat, FS_META_GETATTR)
DEFINE_META_SIMPLE(newfstat,   FS_META_GETATTR)

/* ==================== getdents64 (readdir) ==================== */
DEFINE_META_SIMPLE(getdents64, FS_META_READDIR)

/* ==================== mkdirat (mkdir) / mknodat (mknod) ==================== */
DEFINE_META_SIMPLE(mkdirat, FS_META_MKDIR)
DEFINE_META_SIMPLE(mknodat, FS_META_MKNOD)

/* ==================== unlinkat / unlink (unlink/rmdir) ==================== */
/* unlinkat: dirfd, pathname, flags(arg2) → AT_REMOVEDIR 决定 rmdir/unlink */
DEFINE_META_FLAG(unlinkat, 2, AT_REMOVEDIR, FS_META_RMDIR, FS_META_UNLINK, FS_META_UNLINK)
/* unlink: 非 *at 变体, glibc unlink() 实际调用本 syscall → 纯 unlink */
DEFINE_META_SIMPLE(unlink, FS_META_UNLINK)

/* ==================== renameat2 / rename / linkat / symlinkat / readlinkat ==================== */
DEFINE_META_SIMPLE(renameat2,  FS_META_RENAME)
/* rename: 非 *at 变体, glibc rename() 实际调用本 syscall → rename */
DEFINE_META_SIMPLE(rename,     FS_META_RENAME)
DEFINE_META_SIMPLE(linkat,     FS_META_LINK)
DEFINE_META_SIMPLE(symlinkat,  FS_META_SYMLINK)
DEFINE_META_SIMPLE(readlinkat, FS_META_READLINK)

/* ==================== setattr 系列 (chmod/chown/utime/truncate) ==================== */
DEFINE_META_SIMPLE(fchmodat,   FS_META_SETATTR)
DEFINE_META_SIMPLE(fchownat,   FS_META_SETATTR)
DEFINE_META_SIMPLE(utimensat,  FS_META_SETATTR)
DEFINE_META_SIMPLE(ftruncate,  FS_META_SETATTR)

/* ==================== xattr 系列 ==================== */
DEFINE_META_SIMPLE(getxattr,    FS_META_GETXATTR)
DEFINE_META_SIMPLE(setxattr,    FS_META_SETXATTR)
DEFINE_META_SIMPLE(listxattr,   FS_META_LISTXATTR)
DEFINE_META_SIMPLE(removexattr, FS_META_REMOVEXATTR)

/* ==================== statfs 系列 ==================== */
DEFINE_META_SIMPLE(fstatfs, FS_META_STATFS)

/* ==================== sync 系列 ==================== */
DEFINE_META_SIMPLE(sync,  FS_META_SYNC)
DEFINE_META_SIMPLE(fsync, FS_META_FSYNC)

char LICENSE[] SEC("license") = "GPL";
