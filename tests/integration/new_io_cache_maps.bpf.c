#include "ebpf/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include "ebpf/job_io_new.h"

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, struct job_pid_fd_key);
    __type(value, struct rw_stat);
} job_fd_stat SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 256);
    __type(key, u64);
    __type(value, struct rw_stat);
} job_stat SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 256);
    __type(key, struct latency_key);
    __type(value, u64);
} latency_hist SEC(".maps");

char LICENSE[] SEC("license") = "GPL";
