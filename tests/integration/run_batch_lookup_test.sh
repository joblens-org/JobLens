#!/bin/bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/ebpf"
bpftool btf dump file /sys/kernel/btf/vmlinux format c > "$work/ebpf/vmlinux.h"
clang -g -O2 -target bpf -I "$work" -I "$root/include" \
    -c "$root/tests/integration/batch_maps.bpf.c" -o "$work/maps.bpf.o"
g++ -std=c++17 -O2 -I "$root/include" \
    "$root/tests/integration/batch_lookup_test.cpp" \
    -lbpf -lfmt -lspdlog -lyaml-cpp -o "$work/batch_lookup_test"
timeout 30 "$work/batch_lookup_test" "$work/maps.bpf.o"
