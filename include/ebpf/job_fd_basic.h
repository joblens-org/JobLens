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
#ifndef JOB_FD_BASIC_STAT
#define JOB_FD_BASIC_STAT
// 用于给用户态程序共享数据结构
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#include <linux/bpf.h>
#endif

/* ringbuf 采样事件 */
struct event {
    // u32 job_id;
    u32 pid;
    u32 fd;
    u32 is_write;
    size_t cnt;
    // u64 ktimestamp; //ns
};


#endif