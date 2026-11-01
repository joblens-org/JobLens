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

# Test TUs must see the same feature defines as the production objects: spdlog
# may be packaged with an external fmt, and a sanitizer build needs its flags
# here too. Derive them from the Ninja command for main.cpp.o instead of
# hardcoding a set that only matches one distribution.
joblens_test_defines=$(ninja -C "$build" -t commands JobLens 2>/dev/null \
    | grep -m1 'src/main.cpp.o' | sed 's/ -o .*//' \
    | tr ' ' '\n' | grep -E '^-D[A-Za-z0-9_]+(=[A-Za-z0-9_.:+-]+)?$' | tr '\n' ' ')

# Optional sanitizer run (plan P5): the test TU must be instrumented with the
# same flags as the core, or the report is meaningless. JOBLENS_TEST_SANITIZER
# carries the flag list (e.g. "address,undefined").
joblens_test_werror="-Werror"
joblens_test_extra_flags=""
if [ -n "${JOBLENS_TEST_SANITIZER:-}" ]; then
    joblens_test_werror=""
    # An instrumented run is not a lint gate: clang reports warnings gcc does
    # not, and the point is the sanitizer report, not -Werror.
    joblens_test_extra_flags="-fsanitize=$JOBLENS_TEST_SANITIZER -fno-omit-frame-pointer -g -Wno-error"
fi

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
${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    $joblens_test_extra_flags -c "$root/tests/integration/collector_event_bpf_test.cpp" -o "$work/bpf_source_test.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/bpf_source_test.o\"}
command=${command/ -o JobLens / -o \"$work\/bpf_source_test\" }
(cd "$build"; bash -c "$command")

timeout 120 "$work/bpf_source_test" "$work/collector_event_probe.bpf.o" "$work/bpf_sources"
