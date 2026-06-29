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
#include "ebpf/trace_cgroup_mkdir.h"

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} cgroup_mkdir_events SEC(".maps");

SEC("raw_tracepoint/cgroup_mkdir")
int trace_cgroup_mkdir(struct bpf_raw_tracepoint_args *ctx)
{
    struct cgroup *cgrp = (struct cgroup *)ctx->args[0];
    const char *path = (const char *)ctx->args[1];
    struct cgroup_mkdir_event *e;
    __u32 hierarchy_id = 0;
    __u64 pid_tgid;

    if (!cgrp || !path)
        return 0;

    BPF_CORE_READ_INTO(&hierarchy_id, cgrp, root, hierarchy_id);
    if (hierarchy_id != 0)
        return 0;

    e = bpf_ringbuf_reserve(&cgroup_mkdir_events, sizeof(*e), 0);
    if (!e)
        return 0;

    pid_tgid = bpf_get_current_pid_tgid();
    e->pid = (__u32)pid_tgid;
    e->tgid = (__u32)(pid_tgid >> 32);
    e->hierarchy_id = hierarchy_id;
    BPF_CORE_READ_INTO(&e->level, cgrp, level);
    BPF_CORE_READ_INTO(&e->cgroup_id, cgrp, kn, id);
    bpf_get_current_comm(&e->comm, sizeof(e->comm));

    if (bpf_probe_read_kernel_str(e->path, sizeof(e->path), path) <= 0)
        e->path[0] = '\0';

    bpf_ringbuf_submit(e, 0);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
