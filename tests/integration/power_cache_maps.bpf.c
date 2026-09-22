#include "ebpf/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include "ebpf/trace_sched_runtime.h"

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 64);
    __type(key, struct task_cpu_key);
    __type(value, u64);
} task_cpu_time SEC(".maps");

char LICENSE[] SEC("license") = "GPL";
