#include "ebpf/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include "ebpf/job_io_new.h"
#include "ebpf/fs_metadata.h"

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, struct job_pid_fd_key);
    __type(value, struct rw_stat);
} io_detail SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, struct fs_meta_key);
    __type(value, struct fs_meta_stat);
} fs_detail SEC(".maps");

char LICENSE[] SEC("license") = "GPL";
