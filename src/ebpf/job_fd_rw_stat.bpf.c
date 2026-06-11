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
#include "ebpf/job_fd_rw_stat.h" //一定要放在最后面

// #include <bpf/bpf_core_read.h>
// #include <linux/bpf.h>

#define TP_ARGS(dst, idx, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int) + idx * (sizeof(long unsigned int)); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define TP_RET(dst, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define MAX_ENTRIES 10000
#define MAX_FD_PER_JOB 1024


static __always_inline u64 div_u64(u64 x, u64 d)
{
    u64 q, _d = d;
    /* 让验证器看到 d>0 */
    if (_d == 0)
        return 0;          /* 走不到，但能让 verifier 闭嘴 */
    /* 用 BPF 支持的 64×64→128 的 mul64 指令序列倒算 */
    q = x / _d;            /* 5.10+ 在 x86_64/arm64 上已生成真实 DIV 指令 */
    return q;
}

static __always_inline s8 sign_and_abs_s64(s64* x)
{
    if (*x >= 0){
        return 1;
    }else{
        *x = -*x;
        return -1;
    }
}


/* 过滤表：pid → job_id */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u32);
    __type(value, u64);
    __uint(max_entries, MAX_ENTRIES);
} pid2job SEC(".maps");



// 如果追求更高性能，可以将这里改为PERCPU，之后在用户态进行聚合
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, struct pid_fd_key);
    __type(value, struct rw_stat);
    __uint(max_entries, MAX_ENTRIES);
} job_fd_stat SEC(".maps");


struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1024 * 1024 * sizeof(struct event));
} event_rb SEC(".maps");

/* 采样阈值：每 256 KB 发一次 */
#define SAMPLE_THRESH 0x3FFFF

static __always_inline void account_rw(u32 pid, u32 fd, ssize_t ret, bool is_write)
{
    if (ret <= 0) return;
    bpf_printk("account_rw called for pid:%d, fd:%d, ret:%d, is_write:%d\n", pid, fd, ret, is_write);

    struct pid_fd_key key = {.pid=pid, .fd = fd};
    struct rw_stat *stat;
    stat = bpf_map_lookup_elem(&job_fd_stat, &key);
    if (!stat) {
        struct rw_stat init = {};
        bpf_map_update_elem(&job_fd_stat, &key, &init, BPF_ANY);
        stat = bpf_map_lookup_elem(&job_fd_stat, &key);
        if (!stat) return;
    }
    
    s64 diff = ret;
    s64 diff2 = 0;
    s8  sign_diff = 1;
    if (is_write){
        __sync_fetch_and_add(&stat->write_bytes, ret);
        // 更新平均值
        __sync_fetch_and_add(&stat->write_count, 1);
        __sync_fetch_and_sub(&diff, stat->write_mean);
        sign_diff = sign_and_abs_s64(&diff);
        u64 r = div_u64(diff, stat->write_count);
        __sync_fetch_and_add(&stat->write_mean, sign_diff * r);
        // 更新方差
        __sync_fetch_and_sub(&diff2, stat->write_mean);
        __sync_fetch_and_add(&stat->write_variance, diff2 * diff * r);
        // 更新时间
        stat->write_ktimestamp = bpf_ktime_get_ns();
    }else{
        __sync_fetch_and_add(&stat->read_bytes, ret);
        // 更新平均值
        __sync_fetch_and_add(&stat->read_count, 1);
        __sync_fetch_and_sub(&diff, stat->read_mean);
        sign_diff = sign_and_abs_s64(&diff);
        u64 r = div_u64(diff, stat->read_count);
        __sync_fetch_and_add(&stat->read_mean, sign_diff * r);
        // 更新方差
        __sync_fetch_and_sub(&diff2, stat->read_mean);
        __sync_fetch_and_add(&stat->read_variance, diff2 * diff * r);
        // 更新时间
        stat->read_ktimestamp = bpf_ktime_get_ns();
    }
    // char fmt[] = "account_rw update one msg for fd:%d, data: write_bytes:%d, read_bytes:%d";
    // bpf_trace_printk(fmt, sizeof(fmt),fd, stat->write_bytes, stat->read_bytes);
    

    /* 采样通知 */
    // u64 thresh = is_write ? stat->write_bytes : stat->read_bytes;
    // if ((thresh & SAMPLE_THRESH) == 0) {
    //     struct event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    //     if (e) {
    //         e->job_id = 0;
    //         e->pid    = pid;
    //         e->fd     = fd;
    //         e->read_bytes  = stat->read_bytes;
    //         e->write_bytes = stat->write_bytes;
    //         // e->mmap_bytes = stat->mmap_bytes;
    //         e->read_variance = stat->read_variance;
    //         e->write_variance = stat->write_variance;
    //         e->ktimestamp = bpf_ktime_get_ns();
    //         bpf_ringbuf_submit(e, 0);
    //     }
    // }

    return;
}

/* ------ read/write/pread/pwrite ------ */
struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);       // tid
    __type(value, u32);     // fd
    __uint(max_entries, 1024 * 4);
} read_enter_args SEC(".maps");


SEC("tracepoint/syscalls/sys_enter_read")
int trace_read_enter(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    // char fmt[] = "read get one jobid: %d";
    // bpf_trace_printk(fmt, sizeof(fmt),*job_id);
    u32 fd;
    TP_ARGS(&fd, 0, ctx);
    bpf_map_update_elem(&read_enter_args, &tid, &fd, BPF_ANY);
    return 0;
}


SEC("tracepoint/syscalls/sys_exit_read")
int trace_read_exit(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    u32* pfd = bpf_map_lookup_elem(&read_enter_args, &tid);
    if (!pfd) {
        return 0;
    }
    ssize_t ret;
    TP_RET(&ret, ctx);
    account_rw(pid, *pfd, ret, false);
    bpf_map_delete_elem(&read_enter_args, &tid);
    return 0;
}

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);       // tid
    __type(value, u32);     // fd
    __uint(max_entries, 1024 * 4);
} write_enter_args SEC(".maps");

SEC("tracepoint/syscalls/sys_enter_write")
int trace_write_enter(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    // bpf_trace_printk("test print",11);
    u64 fake = 1;
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    // bpf_map_update_elem(&pid2job, &pid, &fake, 0);
    if (!job_id){
        // bpf_trace_printk("test print",11);
        return 0;
    } 
    // char fmt[] = "write get one jobid: %d";
    // bpf_trace_printk(fmt, sizeof(fmt),*job_id);
    u32 fd;
    TP_ARGS(&fd, 0, ctx);
    bpf_map_update_elem(&write_enter_args, &tid, &fd, BPF_ANY);
    return 0;
}

SEC("tracepoint/syscalls/sys_exit_write")
int trace_write_exit(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    // char fmt[] = "write get one jobid: %d";
    // bpf_trace_printk(fmt, sizeof(fmt),*job_id);
    u32* pfd = bpf_map_lookup_elem(&write_enter_args, &tid);
    if (!pfd) {
        return 0;
    }
    ssize_t ret;
    TP_RET(&ret, ctx);
    account_rw(pid, *pfd, ret, true);
    bpf_map_delete_elem(&write_enter_args, &tid);
    return 0;
}


struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);       // tid
    __type(value, u32);     // fd
    __uint(max_entries, 1024 * 4);
} read64_enter_args SEC(".maps");

SEC("tracepoint/syscalls/sys_enter_pread64")
int trace_pread64_enter(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    // char fmt[] = "write get one jobid: %d";
    // bpf_trace_printk(fmt, sizeof(fmt),*job_id);
    u32 fd;
    TP_ARGS(&fd, 0, ctx);
    bpf_map_update_elem(&read64_enter_args, &tid, &fd, BPF_ANY);
    return 0;
}


SEC("tracepoint/syscalls/sys_exit_pread64")
int trace_pread64_exit(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    u32* pfd = bpf_map_lookup_elem(&read64_enter_args, &tid);
    if (!pfd) {
        return 0;
    }
    ssize_t ret;
    TP_RET(&ret, ctx);
    account_rw(pid, *pfd, ret, false);
    bpf_map_delete_elem(&read64_enter_args, &tid);
    return 0;
}


struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);       // tid
    __type(value, u32);     // fd
    __uint(max_entries, 1024 * 4);
} write64_enter_args SEC(".maps");


SEC("tracepoint/syscalls/sys_enter_pwrite64")
int trace_pwrite64_enter(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    u32 fd;
    TP_ARGS(&fd, 0, ctx);
    bpf_map_update_elem(&write64_enter_args, &tid, &fd, BPF_ANY);
    return 0;
}


SEC("tracepoint/syscalls/sys_exit_pwrite64")
int trace_pwrite64_exit(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    u32* pfd = bpf_map_lookup_elem(&write64_enter_args, &tid);
    if (!pfd) {
        return 0;
    }
    ssize_t ret;
    TP_RET(&ret, ctx);
    account_rw(pid, *pfd, ret, true);
    bpf_map_delete_elem(&write64_enter_args, &tid);
    return 0;
}

/* ------ mmap ------ */
// 目前需求不高，之后再说
// SEC("tracepoint/syscalls/sys_exit_mmap")
// int trace_mmap_exit(struct trace_event_raw_sys_exit* ctx)
// {
//     ssize_t ret = ctx->ret;
//     if (ret < 0) return 0;
//     u32 pid = bpf_get_current_pid_tgid() >> 32;
//     u32 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
//     if (!job_id) return 0;

//     struct syscall_mmap_args {
//         unsigned long addr, len, prot, flags, fd, off;
//     } *args = (void*)ctx->args;
//     int prot = BPF_CORE_READ(args, prot);
//     if (!(prot & PROT_WRITE)) return 0;

//     u64 len = BPF_CORE_READ(args, len);
//     u32 fd  = (u32)BPF_CORE_READ(args, fd);
//     struct pid_fd_key key = {.job_id = *job_id, .fd = fd};
//     struct rw_stat *stat = bpf_map_lookup_elem(&job_fd_stat, &key);
//     if (!stat) {
//         struct rw_stat init = {};
//         bpf_map_update_elem(&job_fd_stat, &key, &init, BPF_NOEXIST);
//         stat = bpf_map_lookup_elem(&job_fd_stat, &key);
//         if (!stat) return 0;
//     }
//     __sync_fetch_and_add(&stat->mmap_bytes, len);
//     return 0;
// }

char LICENSE[] SEC("license") = "GPL";
