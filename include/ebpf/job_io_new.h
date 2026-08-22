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
#ifndef JOB_IO_NEW_H
#define JOB_IO_NEW_H
// 用于给用户态程序共享数据结构
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#include <linux/bpf.h>
#endif

struct rw_stat {
    u64  read_ktimestamp;
    u64  write_ktimestamp;
    u64  read_bytes;      // syscall 读字节 ≈ /proc rchar
    u64  write_bytes;     // syscall 写字节 ≈ /proc wchar
    u64  read_mean;
    u64  write_mean;
    s64  read_variance;
    s64  write_variance;
    u64  write_count;     // ≈ /proc syscw
    u64  read_count;      // ≈ /proc syscr
};

// job_fd_stat 明细 map 的 key：带 job_id 可反查
struct job_pid_fd_key {
    u64 job_id;
    u32 pid;
    u32 fd;
};

// enter 侧记录的 fd + 进入时间戳（时延 = exit_ns - enter_ns）
struct enter_ctx {
    u32 fd;
    u64 ts;
};

// 时延直方图 key
struct latency_key {
    u64 job_id;
    u32 bucket;
    u32 is_write;
};

#define LATENCY_BUCKETS 64

#endif
