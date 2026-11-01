#!/usr/bin/env bash
# Discovery-adapter scan/health/lifecycle acceptance (Task 3, items 1-2):
# CondorJobWatcher::scan()/health()/start()/stop() against a fixture procfs,
# cgroup, job-ad and slurmd-spool tree.
#
# Usage:
#   bash tests/integration/run_watcher_scan_test.sh [build-dir]
#
# The build dir defaults to build-watcher/ when it exists (a private build tree
# used while other agents hold build/), otherwise build/. The test TU is
# compiled and linked against the production object set by reusing the Ninja
# JobLens link command, so the sources are exercised exactly as shipped.
#
# No root and no eBPF are required: the fixture supplies the discovery roots and
# an unavailable event source is reported as an explicit scan note.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)

if [[ $# -ge 1 ]]; then
    build=$1
elif [[ -d "$root/build-watcher" ]]; then
    build=$root/build-watcher
else
    build=$root/build
fi
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

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
[[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }

# JOBLENS_INSTALL_LIBDIR is a compile-time macro of the production target; the
# test TU includes ebpf_common.hpp through the watcher headers.
${CXX:-c++} -std=c++17 -O0 -g -Wall -Wextra $joblens_test_werror -pthread \
    $joblens_test_defines \
    -DJOBLENS_INSTALL_LIBDIR=\"lib64\" \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    $joblens_test_extra_flags -c "$root/tests/integration/job_watcher_scan_test.cpp" -o "$work/job_watcher_scan_test.o"

command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/job_watcher_scan_test.o\"}
command=${command/ -o JobLens / -o \"$work\/job_watcher_scan_test\" }
(cd "$build"; bash -c "$command")

# The test creates its own fixture root below the given prefix and removes it.
timeout 180 "$work/job_watcher_scan_test" "$work/fixture"
