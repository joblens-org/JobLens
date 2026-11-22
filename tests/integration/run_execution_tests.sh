#!/usr/bin/env bash
# Unified collector execution suites.
#
# Usage:
#   bash tests/integration/run_execution_tests.sh [build-dir] [--suite NAME]
#
# Suites: executor, matrix, poller, lifecycle, sources, all. Unknown suites
# fail. Later implementation tasks add timers and finalization here.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)

suite="all"
build=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --suite)
            [[ $# -ge 2 ]] || { printf '%s\n' '--suite requires a name' >&2; exit 2; }
            suite=$2
            shift 2
            ;;
        --suite=*)
            suite=${1#*=}
            shift
            ;;
        -h|--help)
            sed -n '2,8p' "$0"
            exit 0
            ;;
        *)
            if [[ -n "$build" ]]; then
                printf 'Unexpected argument: %s\n' "$1" >&2
                exit 2
            fi
            build=$1
            shift
            ;;
    esac
done

build=${build:-"$root/build"}
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

run_executor() {
    ${CXX:-c++} -std=c++17 -O0 -g -Wall -Wextra $joblens_test_werror $joblens_test_extra_flags -I"$root/include" -pthread \
        "$root/tests/integration/task_executor_test.cpp" \
        "$root/src/common/task_executor.cpp" \
        -lspdlog -lfmt -o "$work/task_executor_test"
    timeout 120 "$work/task_executor_test"
}

run_poller() {
    ${CXX:-c++} -std=c++17 -O0 -g -Wall -Wextra $joblens_test_werror $joblens_test_extra_flags -I"$root/include" -pthread \
        "$root/tests/integration/event_poller_test.cpp" \
        "$root/src/common/event_poller.cpp" \
        "$root/src/common/task_executor.cpp" \
        -lspdlog -lfmt -o "$work/event_poller_test"
    timeout 120 "$work/event_poller_test"
}

run_lifecycle() {
    link=""
    while IFS= read -r command; do
        if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
    done < <(ninja -C "$build" -t commands JobLens)
    [[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }
    ${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        $joblens_test_extra_flags -c "$root/tests/integration/collector_execution_lifecycle_test.cpp" -o "$work/lifecycle_test.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/lifecycle_test.o\"}
    command=${command/ -o JobLens / -o \"$work\/lifecycle_test\" }
    (cd "$build"; bash -c "$command")
    timeout 120 "$work/lifecycle_test" "$work/lifecycle"
}

run_matrix() {
    link=""
    while IFS= read -r command; do
        if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
    done < <(ninja -C "$build" -t commands JobLens)
    [[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }
    ${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        $joblens_test_extra_flags -c "$root/tests/integration/collector_execution_matrix_test.cpp" -o "$work/matrix_test.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/matrix_test.o\"}
    command=${command/ -o JobLens / -o \"$work\/matrix_test\" }
    (cd "$build"; bash -c "$command")
    timeout 60 "$work/matrix_test" "$work/matrix"
}

run_sources() {
    link=""
    while IFS= read -r command; do
        if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
    done < <(ninja -C "$build" -t commands JobLens)
    [[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }
    ${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        $joblens_test_extra_flags -c "$root/tests/integration/collector_event_source_test.cpp" -o "$work/source_test.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/source_test.o\"}
    command=${command/ -o JobLens / -o \"$work\/source_test\" }
    (cd "$build"; bash -c "$command")
    timeout 120 "$work/source_test" "$work/sources"
}

case "$suite" in
    executor)
        run_executor
        ;;
    matrix)
        run_matrix
        ;;
    poller)
        run_poller
        ;;
    lifecycle)
        run_lifecycle
        ;;
    sources)
        run_sources
        ;;
    all)
        run_executor
        run_matrix
        run_poller
        run_lifecycle
        run_sources
        ;;
    *)
        printf 'Unknown execution suite: %s\n' "$suite" >&2
        exit 2
        ;;
esac
