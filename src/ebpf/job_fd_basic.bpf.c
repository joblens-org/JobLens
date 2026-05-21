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
#include "ebpf/job_fd_basic.h" //一定要放在最后面

#define TP_ARGS(dst, idx, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int) + idx * (sizeof(long unsigned int)); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

#define TP_RET(dst, ctx) \
{void *__p = (void*)ctx + sizeof(struct trace_entry) + sizeof(long int); \
bpf_probe_read_kernel(dst, sizeof(*dst), __p);}

/* 过滤表：pid → job_id */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, u32);
    __type(value, u64);
    __uint(max_entries, 8192);
} pid2job SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1024 * 8);
} event_rb SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);       // tid
    __type(value, u32);     // fd
    __uint(max_entries, 1024 * 4);
} read_enter_args SEC(".maps");

// 使用最简单的写法，一次调用推送一次，没活了
SEC("tracepoint/syscalls/sys_enter_read")
int trace_read_enter(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    bpf_printk("read enter get jobid: %d\n", *job_id);

    u32 fd;
    size_t cnt;
    TP_ARGS(&fd, 0, ctx);
    TP_ARGS(&cnt, 2, ctx);

    bpf_printk("read enter get fd: %d cnt: %llu\n", fd, cnt);
    // long unsigned int pfd;
    // u32 fd;
    // long unsigned int pcnt;
    // size_t cnt;
    // int common_pid;
    // BPF_CORE_READ_INTO(&common_pid, ctx, ent.pid);
    // bpf_printk("read enter common pid: %d\n", common_pid);
    // BPF_CORE_READ_INTO(&pfd, ctx, args);
    // bpf_probe_read_kernel(&fd, sizeof(u32), (void*)pfd);
    // BPF_CORE_READ_INTO(&pcnt, ctx, args[2]);
    // bpf_probe_read_kernel(&cnt, sizeof(size_t), (void*)((long unsigned int*)ctx->args + 2));
    // bpf_printk("read enter get ptr fd: %llu, cnt: %llu\n", pfd, pcnt);
    // bpf_printk("read enter get fd: %d, cnt: %llu\n", fd, cnt);
    
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
    if (!pfd) return 0;
    u32 fd = *pfd;
    bpf_printk("read exit get fd: %d\n", fd);
    bpf_map_delete_elem(&read_enter_args, &tid);
    ssize_t func_ret;
    TP_RET(&func_ret, ctx);
    bpf_printk("read exit func ret: %d\n", func_ret);
    if (func_ret <= 0) return 0;
    size_t cnt = (size_t)func_ret;


    struct event *e = bpf_ringbuf_reserve(&event_rb, sizeof(*e), 0);
    if (!e) return 0;
    
    e->fd = fd;
    e->pid = pid;
    e->is_write = 0;
    e->cnt = cnt;
    bpf_ringbuf_submit(e, 0);
    
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
    u32 pid = tid >> 32;
    bpf_printk("write enter pid: %d\n", pid);
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    bpf_printk("write enter get jobid: %d\n", *job_id);
    u64 fd;
    size_t cnt;
    BPF_CORE_READ_INTO(&fd, ctx, args[0]);
    BPF_CORE_READ_INTO(&cnt, ctx, args[2]);
    u32 u_fd = (u32)fd;
    bpf_map_update_elem(&write_enter_args, &tid, &u_fd, BPF_ANY);
    return 0;
}


SEC("tracepoint/syscalls/sys_exit_write")
int trace_write_exit(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    u32* pfd = bpf_map_lookup_elem(&write_enter_args, &tid);
    if (!pfd) return 0;
    u32 fd = *pfd;
    bpf_printk("write exit get fd: %d\n", fd);
    bpf_map_delete_elem(&write_enter_args, &tid);
    ssize_t func_ret;
    BPF_CORE_READ_INTO(&func_ret, ctx, ret);
    if (func_ret <= 0) return 0;
    size_t cnt = (size_t)func_ret;

    struct event *e = bpf_ringbuf_reserve(&event_rb, sizeof(*e), 0);
    if (!e) return 0;
    
    e->fd = fd;
    e->pid = pid;
    e->is_write = 1;
    e->cnt = cnt;
    bpf_ringbuf_submit(e, 0);
    
    
    return 0;
}

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __type(key, u64);       // tid
    __type(value, u32);     // fd
    __uint(max_entries, 1024 * 4);
} read64_enter_args SEC(".maps");

SEC("tracepoint/syscalls/sys_enter_pread64")
int trace_read64_enter(struct trace_event_raw_sys_enter* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    u64 fd;
    size_t cnt;

    BPF_CORE_READ_INTO(&fd, ctx, args[0]);
    BPF_CORE_READ_INTO(&cnt, ctx, args[2]);
    u32 u_fd = (u32)fd;
    bpf_map_update_elem(&read64_enter_args, &tid, &u_fd, BPF_ANY);
    return 0;
}


SEC("tracepoint/syscalls/sys_exit_pread64")
int trace_read64_exit(struct trace_event_raw_sys_exit* ctx)
{
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    u64 *job_id = bpf_map_lookup_elem(&pid2job, &pid);
    if (!job_id) return 0;
    u32* pfd = bpf_map_lookup_elem(&read64_enter_args, &tid);
    if (!pfd) return 0;
    u32 fd = *pfd;
    bpf_map_delete_elem(&read64_enter_args, &tid);
    ssize_t func_ret;
    BPF_CORE_READ_INTO(&func_ret, ctx, ret);
    if (func_ret <= 0) return 0;
    size_t cnt = (size_t)func_ret;

    struct event *e = bpf_ringbuf_reserve(&event_rb, sizeof(*e), 0);
    if (!e) return 0;
    
    e->fd = fd;
    e->pid = pid;
    e->is_write = 0;
    e->cnt = cnt;
    bpf_ringbuf_submit(e, 0);
    
    
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
    u64 fd;
    size_t cnt;

    BPF_CORE_READ_INTO(&fd, ctx, args[0]);
    BPF_CORE_READ_INTO(&cnt, ctx, args[2]);
    u32 u_fd = (u32)fd;
    bpf_map_update_elem(&write64_enter_args, &tid, &u_fd, BPF_ANY);
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
    if (!pfd) return 0;
    u32 fd = *pfd;
    bpf_map_delete_elem(&write64_enter_args, &tid);
    ssize_t func_ret;
    BPF_CORE_READ_INTO(&func_ret, ctx, ret);
    if (func_ret <= 0) return 0;
    size_t cnt = (size_t)func_ret;

    struct event *e = bpf_ringbuf_reserve(&event_rb, sizeof(*e), 0);
    if (!e) return 0;
    
    e->fd = fd;
    e->pid = pid;
    e->is_write = 1;
    e->cnt = cnt;
    bpf_ringbuf_submit(e, 0);
    
    return 0;
}


char LICENSE[] SEC("license") = "GPL";
