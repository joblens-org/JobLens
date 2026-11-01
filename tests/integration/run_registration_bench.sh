#!/usr/bin/env bash
# Synchronous-registration cost measurement (plan: register the discovered Job
# inside the discovery callback instead of the exec ingest worker).
#
# Compiles tests/integration/job_registration_bench.cpp against the production
# object set (the Ninja link line) and runs it: real /proc reads, real
# _CONDOR_JOB_AD files, real per-job cgroup membership, real LevelDB with
# sync=true on the target host's filesystem.
#
# Usage:
#   bash tests/integration/run_registration_bench.sh [build-dir] [-- extra args]
#
# Everything after `--` is passed to the benchmark. The benchmark itself spawns
# the fixture processes and cleans them up unless --keep-fixture is given.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)

if [[ $# -ge 1 && $1 != --* ]]; then
    build=$1
    shift
else
    if [[ -d "$root/build-watcher" ]]; then build=$root/build-watcher; else build=$root/build; fi
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

if [[ ${1:-} == -- ]]; then shift; fi

if [[ ${SKIP_BUILD:-0} != 1 ]]; then
    cmake --build "$build" -j "${BUILD_JOBS:-4}"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
[[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }

${CXX:-c++} -std=c++17 -O2 -g -Wall -Wextra \
    $joblens_test_defines \
    -DJOBLENS_INSTALL_LIBDIR=\"lib64\" \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    $joblens_test_extra_flags -c "$root/tests/integration/job_registration_bench.cpp" -o "$work/job_registration_bench.o"

command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/job_registration_bench.o\"}
command=${command/ -o JobLens / -o \"$work\/job_registration_bench\" }
(cd "$build"; bash -c "$command")

# Keep the binary next to the build tree: it is what a remote host runs.
cp "$work/job_registration_bench" "$build/job_registration_bench"
printf 'binary: %s\n' "$build/job_registration_bench"

status=0
"$build/job_registration_bench" "$@" || status=$?
exit "$status"
