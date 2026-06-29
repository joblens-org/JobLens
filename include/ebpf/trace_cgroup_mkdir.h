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
#ifndef TRACE_CGROUP_MKDIR
#define TRACE_CGROUP_MKDIR

#ifndef __VMLINUX_H__
#include <linux/bpf.h>
#endif

#define TASK_COMM_LEN 16
#define CGROUP_MKDIR_PATH_LEN 256

struct cgroup_mkdir_event {
    __u32 pid;
    __u32 tgid;
    __u32 hierarchy_id;
    __u32 level;
    __u64 cgroup_id;
    char comm[TASK_COMM_LEN];
    char path[CGROUP_MKDIR_PATH_LEN];
};

#endif
