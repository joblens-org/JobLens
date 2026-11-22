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
/* Dedicated test probe for collector-owned eBPF sources: a private ring and
 * stats map plus one tracepoint program. It never touches deployment maps;
 * every loaded instance gets its own private map set. Compiled by
 * run_event_bpf_test.sh with a generated vmlinux.h. */
#include "vmlinux.h"

#include <bpf/bpf_helpers.h>

#include "collector_event_fixture.h"

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 16);
} cev_events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct cev_stats);
} cev_stats SEC(".maps");

/* getpid is trivially triggerable from the test process, so the fixture has a
 * deterministic producer without sharing any state with deployment probes. */
SEC(CEV_PROG_SEC)
int cev_on_getpid(void *ctx)
{
    (void)ctx;
    __u32 key = 0;
    struct cev_stats *stats = bpf_map_lookup_elem(&cev_stats, &key);
    if (!stats)
        return 0;
    struct cev_sample *sample = bpf_ringbuf_reserve(&cev_events, sizeof(*sample), 0);
    if (!sample) {
        __sync_fetch_and_add(&stats->dropped, 1);
        return 0;
    }
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    sample->seq = __sync_fetch_and_add(&stats->seq, 1) + 1;
    sample->kernel_ts = bpf_ktime_get_ns();
    sample->pid = (__u32)pid_tgid;
    sample->tgid = (__u32)(pid_tgid >> 32);
    bpf_ringbuf_submit(sample, 0);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
