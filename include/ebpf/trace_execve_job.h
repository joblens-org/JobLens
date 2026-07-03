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
#ifndef TRACE_EXECVE_JOB
#define TRACE_EXECVE_JOB

#ifndef __VMLINUX_H__
#include <linux/bpf.h>
#endif

#define TASK_COMM_LEN   16
#define EXECVE_JOB_PATH_LEN 256

// 调度器提示，与 userspace SchedulerHint 保持同步
enum scheduler_hint {
    SCHEDULER_HINT_UNKNOWN = 0,
    SCHEDULER_HINT_CONDOR  = 1,
    SCHEDULER_HINT_SLURM   = 2,
};

struct execve_job_event {
    __u32 pid;
    __u32 tgid;
    __u32 ppid;
    __u32 ptgid;
    __u32 scheduler_hint;
    char comm[TASK_COMM_LEN];
    char parent_comm[TASK_COMM_LEN];
};

#endif
