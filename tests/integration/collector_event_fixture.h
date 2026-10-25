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
/* Shared reference-protocol definitions for collector-owned event sources.
 *
 * This header is included by both userspace tests and the BPF probe
 * (collector_event_probe.bpf.c). It must stay BPF-safe: include it after
 * vmlinux.h / <linux/types.h>, never include C++ headers here, and only use
 * fixed-width kernel types. */

#ifndef COLLECTOR_EVENT_FIXTURE_H
#define COLLECTOR_EVENT_FIXTURE_H

/* ------------------------- Unix datagram protocol ------------------------- */
/* One message per datagram. send(2) is message-oriented, so a datagram is
 * never a partial frame; oversize input is detected with MSG_TRUNC. */

#define CEV_DGRAM_MAGIC 0x4a4c4556u /* "JLEV" */
#define CEV_DGRAM_MAX 512u          /* hard datagram bound, bytes */
#define CEV_PAYLOAD_MAX (CEV_DGRAM_MAX - 24u)

#define CEV_FLAG_END 0x1u /* producer-declared end marker for this tag */

struct cev_datagram {
    __u32 magic;      /* CEV_DGRAM_MAGIC */
    __u32 seq;        /* producer sequence */
    __u64 tag;        /* Job scope: intended JobID; System scope: ignored */
    __u32 payload_len;
    __u32 flags;      /* CEV_FLAG_* */
    char payload[CEV_PAYLOAD_MAX];
};

/* ------------------------------ BPF sample -------------------------------- */
/* Layout shared by the test probe and its userspace consumer. Records are
 * copied out of the sample callback before further processing. */

struct cev_sample {
    __u64 seq;
    __u64 kernel_ts;
    __u32 pid;
    __u32 tgid;
};

struct cev_stats {
    __u64 seq;     /* last assigned producer sequence */
    __u64 dropped; /* reserve failures: exact kernel-loss telemetry */
};

#define CEV_RING_MAP "cev_events"
#define CEV_STATS_MAP "cev_stats"
#define CEV_PROG_SEC "tp/syscalls/sys_enter_getpid"

/* Default userspace per-turn bound used by the reference collectors when the
 * configuration does not override it. */
#define CEV_DEFAULT_TURN_LIMIT 64u

#endif /* COLLECTOR_EVENT_FIXTURE_H */
