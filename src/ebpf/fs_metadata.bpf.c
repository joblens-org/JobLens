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
#include "ebpf/fs_metadata.h" //一定要放在最后面

#define TP_ARGS(dst, idx, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int) + idx * (sizeof(long unsigned int)); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define TP_RET(dst, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define MAX_ENTRIES 10000

/* AT_REMOVEDIR 标志位定义 */
#define AT_REMOVEDIR 0x200

/* O_CREAT 标志位定义 (八进制 0100 = 0x40) */
#define O_CREAT 0100

/* PID → JobID 过滤表 */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u32);
    __type(value, u64);
    __uint(max_entries, MAX_ENTRIES);
} pid2job SEC(".maps");

/* 累加器：(pid, op) → fs_meta_stat */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct fs_meta_key);
    __type(value, struct fs_meta_stat);
    __uint(max_entries, MAX_ENTRIES * 21);
} fs_meta_stat SEC(".maps");

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

/* 统计更新辅助函数 */
static __always_inline void account_meta(u32 pid, u32 op, ssize_t ret, u64 enter_ns)
{
    struct fs_meta_key key = {.pid = pid, .op = op};
    struct fs_meta_stat *stat = bpf_map_lookup_elem(&fs_meta_stat, &key);

    if (!stat) {
        struct fs_meta_stat init = {};
        bpf_map_update_elem(&fs_meta_stat, &key, &init, BPF_ANY);
        stat = bpf_map_lookup_elem(&fs_meta_stat, &key);
        if (!stat)
            return;
    }

    /* 总调用次数 */
    __sync_fetch_and_add(&stat->calls, 1);

    /* 成功/错误统计 */
    if (ret >= 0) {
        __sync_fetch_and_add(&stat->success, 1);
    } else {
        __sync_fetch_and_add(&stat->errors, 1);
        stat->last_errno = ret; /* 保存错误码（负数） */
    }

    /* 延迟计算 */
    if (enter_ns > 0) {
        u64 now = bpf_ktime_get_ns();
        u64 lat_ns = now - enter_ns;
        __sync_fetch_and_add(&stat->total_latency_ns, lat_ns);
        if (lat_ns > stat->max_latency_ns) {
            stat->max_latency_ns = lat_ns;
        }
    }

    /* 更新时间戳 */
    stat->last_timestamp_ns = bpf_ktime_get_ns();
}

/* ==================== open/openat/openat2/creat 系列 ==================== */

/* sys_enter_openat: dirfd, pathname, flags, mode */
SEC("tracepoint/syscalls/sys_enter_openat")
int trace_enter_openat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    /* 保存进入时间戳 */
    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    /* 读取 flags (arg2) 判断是否为创建操作 */
    u32 flags;
    TP_ARGS(&flags, 2, ctx);
    u32 op = (flags & O_CREAT) ? FS_META_CREATE : FS_META_OPEN;
    bpf_map_update_elem(&meta_enter_op, &tid, &op, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_openat")
int trace_exit_openat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    /* 获取进入时间戳 */
    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    /* 获取操作类型 */
    u32 *op = bpf_map_lookup_elem(&meta_enter_op, &tid);
    u32 op_val = op ? *op : FS_META_OPEN;

    /* 读取返回值 */
    ssize_t ret;
    TP_RET(&ret, ctx);

    /* 统计 */
    account_meta(pid, op_val, ret, ts);

    /* 清理 map */
    bpf_map_delete_elem(&meta_enter_ts, &tid);
    bpf_map_delete_elem(&meta_enter_op, &tid);

    return 0;
}

/* sys_enter_open: pathname, flags, mode */
SEC("tracepoint/syscalls/sys_enter_open")
int trace_enter_open(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    u32 flags;
    TP_ARGS(&flags, 1, ctx);
    u32 op = (flags & O_CREAT) ? FS_META_CREATE : FS_META_OPEN;
    bpf_map_update_elem(&meta_enter_op, &tid, &op, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_open")
int trace_exit_open(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    u32 *op = bpf_map_lookup_elem(&meta_enter_op, &tid);
    u32 op_val = op ? *op : FS_META_OPEN;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, op_val, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);
    bpf_map_delete_elem(&meta_enter_op, &tid);

    return 0;
}

/* sys_enter_creat: pathname, mode (等价于 open with O_CREAT|O_WRONLY|O_TRUNC) */
SEC("tracepoint/syscalls/sys_enter_creat")
int trace_enter_creat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);
    u32 op = FS_META_CREATE;
    bpf_map_update_elem(&meta_enter_op, &tid, &op, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_creat")
int trace_exit_creat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_CREATE, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);
    bpf_map_delete_elem(&meta_enter_op, &tid);

    return 0;
}

/* ==================== close 系列 ==================== */

SEC("tracepoint/syscalls/sys_enter_close")
int trace_enter_close(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_close")
int trace_exit_close(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_CLOSE, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== stat 系列 (getattr) ==================== */

SEC("tracepoint/syscalls/sys_enter_newfstatat")
int trace_enter_newfstatat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_newfstatat")
int trace_exit_newfstatat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_GETATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_fstat: fd, statbuf */
SEC("tracepoint/syscalls/sys_enter_fstat")
int trace_enter_fstat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_fstat")
int trace_exit_fstat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_GETATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== getdents64 (readdir) ==================== */

SEC("tracepoint/syscalls/sys_enter_getdents64")
int trace_enter_getdents64(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_getdents64")
int trace_exit_getdents64(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_READDIR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== mkdirat (mkdir) ==================== */

SEC("tracepoint/syscalls/sys_enter_mkdirat")
int trace_enter_mkdirat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_mkdirat")
int trace_exit_mkdirat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_MKDIR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== mknodat (mknod) ==================== */

SEC("tracepoint/syscalls/sys_enter_mknodat")
int trace_enter_mknodat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_mknodat")
int trace_exit_mknodat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_MKNOD, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== unlinkat (unlink/rmdir) ==================== */

/* sys_enter_unlinkat: dirfd, pathname, flags */
SEC("tracepoint/syscalls/sys_enter_unlinkat")
int trace_enter_unlinkat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    /* 读取 flags (arg2) 判断是 unlink 还是 rmdir */
    u32 flags;
    TP_ARGS(&flags, 2, ctx);
    u32 op = (flags & AT_REMOVEDIR) ? FS_META_RMDIR : FS_META_UNLINK;
    bpf_map_update_elem(&meta_enter_op, &tid, &op, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_unlinkat")
int trace_exit_unlinkat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    u32 *op = bpf_map_lookup_elem(&meta_enter_op, &tid);
    u32 op_val = op ? *op : FS_META_UNLINK;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, op_val, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);
    bpf_map_delete_elem(&meta_enter_op, &tid);

    return 0;
}

/* ==================== renameat2 (rename) ==================== */

/* sys_enter_renameat2: olddirfd, oldname, newdirfd, newname, flags */
SEC("tracepoint/syscalls/sys_enter_renameat2")
int trace_enter_renameat2(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_renameat2")
int trace_exit_renameat2(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_RENAME, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== linkat (link) ==================== */

/* sys_enter_linkat: olddirfd, oldname, newdirfd, newname, flags */
SEC("tracepoint/syscalls/sys_enter_linkat")
int trace_enter_linkat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_linkat")
int trace_exit_linkat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_LINK, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== symlinkat (symlink) ==================== */

/* sys_enter_symlinkat: target, newdirfd, linkpath */
SEC("tracepoint/syscalls/sys_enter_symlinkat")
int trace_enter_symlinkat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_symlinkat")
int trace_exit_symlinkat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_SYMLINK, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== readlinkat (readlink) ==================== */

/* sys_enter_readlinkat: dirfd, pathname, buf, bufsiz */
SEC("tracepoint/syscalls/sys_enter_readlinkat")
int trace_enter_readlinkat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_readlinkat")
int trace_exit_readlinkat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_READLINK, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== setattr 系列 (chmod/chown/utime/truncate) ==================== */

/* sys_enter_fchmodat: dirfd, pathname, mode, flags */
SEC("tracepoint/syscalls/sys_enter_fchmodat")
int trace_enter_fchmodat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_fchmodat")
int trace_exit_fchmodat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_SETATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_fchownat: dirfd, pathname, user, group, flags */
SEC("tracepoint/syscalls/sys_enter_fchownat")
int trace_enter_fchownat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_fchownat")
int trace_exit_fchownat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_SETATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_utimensat: dirfd, pathname, times, flags */
SEC("tracepoint/syscalls/sys_enter_utimensat")
int trace_enter_utimensat(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_utimensat")
int trace_exit_utimensat(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_SETATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_ftruncate: fd, length */
SEC("tracepoint/syscalls/sys_enter_ftruncate")
int trace_enter_ftruncate(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_ftruncate")
int trace_exit_ftruncate(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_SETATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== xattr 系列 ==================== */

/* sys_enter_getxattr: pathname, name, value, size */
SEC("tracepoint/syscalls/sys_enter_getxattr")
int trace_enter_getxattr(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_getxattr")
int trace_exit_getxattr(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_GETXATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_setxattr: pathname, name, value, size, flags */
SEC("tracepoint/syscalls/sys_enter_setxattr")
int trace_enter_setxattr(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_setxattr")
int trace_exit_setxattr(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_SETXATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_listxattr: pathname, list, size */
SEC("tracepoint/syscalls/sys_enter_listxattr")
int trace_enter_listxattr(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_listxattr")
int trace_exit_listxattr(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_LISTXATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_removexattr: pathname, name */
SEC("tracepoint/syscalls/sys_enter_removexattr")
int trace_enter_removexattr(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_removexattr")
int trace_exit_removexattr(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_REMOVEXATTR, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== statfs 系列 ==================== */

/* sys_enter_fstatfs: fd, buf */
SEC("tracepoint/syscalls/sys_enter_fstatfs")
int trace_enter_fstatfs(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_fstatfs")
int trace_exit_fstatfs(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_STATFS, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* ==================== sync 系列 ==================== */

/* sys_enter_sync: 无参数 */
SEC("tracepoint/syscalls/sys_enter_sync")
int trace_enter_sync(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_sync")
int trace_exit_sync(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_SYNC, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

/* sys_enter_fsync: fd */
SEC("tracepoint/syscalls/sys_enter_fsync")
int trace_enter_fsync(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&meta_enter_ts, &tid, &ts, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_fsync")
int trace_exit_fsync(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id)
        return 0;

    u64 *enter_ts = bpf_map_lookup_elem(&meta_enter_ts, &tid);
    u64 ts = enter_ts ? *enter_ts : 0;

    ssize_t ret;
    TP_RET(&ret, ctx);

    account_meta(pid, FS_META_FSYNC, ret, ts);

    bpf_map_delete_elem(&meta_enter_ts, &tid);

    return 0;
}

char LICENSE[] SEC("license") = "GPL";
