#!/usr/bin/env bash
# Privileged BPF source acceptance for collector-owned eBPF sources (Tier B).
#
# Compiles the dedicated test probe against generated vmlinux headers and runs
# collector_event_bpf_test through the real scheduler. Requires root (or
# CAP_BPF/CAP_PERFMON and tracepoint attach permission).
#
# Usage:
#   bash tests/integration/run_event_bpf_test.sh [build-dir]
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)

build=${1:-"$root/build"}
build=$(cd "$build" && pwd)

if [[ ${SKIP_BUILD:-0} != 1 ]]; then
    cmake --build "$build" -j 2
fi

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    echo "SKIP collector event bpf: not running as root (use sudo on a prepared host)" >&2
    exit 77
fi
if ! command -v bpftool >/dev/null 2>&1 && [[ ! -x /usr/sbin/bpftool ]]; then
    echo "SKIP collector event bpf: bpftool is not available" >&2
    exit 77
fi
BPFTOOL=$(command -v bpftool || echo /usr/sbin/bpftool)
if [[ ! -r /sys/kernel/btf/vmlinux ]]; then
    echo "SKIP collector event bpf: BTF at /sys/kernel/btf/vmlinux is not readable" >&2
    exit 77
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/ebpf"

"$BPFTOOL" btf dump file /sys/kernel/btf/vmlinux format c > "$work/ebpf/vmlinux.h"
clang -g -O2 -target bpf -D__TARGET_ARCH_x86 -I "$work/ebpf" -I "$root/include" \
    -c "$root/tests/integration/collector_event_probe.bpf.c" -o "$work/collector_event_probe.bpf.o"

# Build the test against the production objects using the Ninja link command.
link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
[[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }
c++ -std=c++17 -O0 -g -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    -c "$root/tests/integration/collector_event_bpf_test.cpp" -o "$work/bpf_source_test.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/bpf_source_test.o\"}
command=${command/ -o JobLens / -o \"$work\/bpf_source_test\" }
(cd "$build"; bash -c "$command")

timeout 120 "$work/bpf_source_test" "$work/collector_event_probe.bpf.o" "$work/bpf_sources"
