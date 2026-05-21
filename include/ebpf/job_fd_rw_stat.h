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
#ifndef JOB_FD_RW_STAT
#define JOB_FD_RW_STAT
// 用于给用户态程序共享数据结构
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#include <linux/bpf.h>
#endif

struct rw_stat {
    // struct bpf_spin_lock lock; // 防止多线程读写同一文件导致统计出问题
    // u32  _pad;
    u64  read_ktimestamp;
    u64  write_ktimestamp;
    u64  read_bytes;
    u64  write_bytes;
    u64  read_mean;
    u64  write_mean;
    s64  read_variance;
    s64  write_variance;
    u64  write_count;
    u64  read_count;
    // u64 mmap_bytes;
};

/* ringbuf 采样事件 */
struct event {
    u32 job_id;
    u32 pid;
    u32 fd;
    u64 read_bytes;
    u64 write_bytes;
    // u64 mmap_bytes;
    s64 read_variance;
    s64 write_variance;
    u64 ktimestamp; //ns
};

/* 累加器 key：job + fd */
struct pid_fd_key {
    u32 pid;
    u32 fd;
};


#endif