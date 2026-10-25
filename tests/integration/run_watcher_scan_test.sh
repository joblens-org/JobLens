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
c++ -std=c++17 -O0 -g -Wall -Wextra -Werror -pthread \
    -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
    -DJOBLENS_INSTALL_LIBDIR=\"lib64\" \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    -c "$root/tests/integration/job_watcher_scan_test.cpp" -o "$work/job_watcher_scan_test.o"

command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/job_watcher_scan_test.o\"}
command=${command/ -o JobLens / -o \"$work\/job_watcher_scan_test\" }
(cd "$build"; bash -c "$command")

# The test creates its own fixture root below the given prefix and removes it.
timeout 180 "$work/job_watcher_scan_test" "$work/fixture"
