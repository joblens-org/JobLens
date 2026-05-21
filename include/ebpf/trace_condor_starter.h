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
#ifndef TRACE_CONDOR_STARTER
#define TRACE_CONDOR_STARTER
// 用于给用户态程序共享数据结构
#ifndef __VMLINUX_H__
#include "bpf_types.h"
#include <linux/bpf.h>
#endif

#define TASK_COMM_LEN 16

struct event {
    __u32 pid;
    __u32 ppid;
    char  comm[TASK_COMM_LEN];
};

#endif