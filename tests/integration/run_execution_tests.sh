#!/usr/bin/env bash
# Unified collector execution suites.
#
# Usage:
#   bash tests/integration/run_execution_tests.sh [build-dir] [--suite NAME]
#
# Suites: executor, all. Unknown suites fail. Later implementation tasks add
# timers, matrix, poller, lifecycle, sources and finalization here.
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

if [[ ${SKIP_BUILD:-0} != 1 ]]; then
    cmake --build "$build" -j 2
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

run_executor() {
    c++ -std=c++17 -O0 -g -Wall -Wextra -Werror -I"$root/include" -pthread \
        "$root/tests/integration/task_executor_test.cpp" \
        "$root/src/common/task_executor.cpp" \
        -lspdlog -lfmt -o "$work/task_executor_test"
    timeout 120 "$work/task_executor_test"
}

run_poller() {
    c++ -std=c++17 -O0 -g -Wall -Wextra -Werror -I"$root/include" -pthread \
        "$root/tests/integration/event_poller_test.cpp" \
        "$root/src/common/event_poller.cpp" \
        "$root/src/common/task_executor.cpp" \
        -lspdlog -lfmt -o "$work/event_poller_test"
    timeout 120 "$work/event_poller_test"
}

run_matrix() {
    link=""
    while IFS= read -r command; do
        if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
    done < <(ninja -C "$build" -t commands JobLens)
    [[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }
    c++ -std=c++17 -O0 -g -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        -c "$root/tests/integration/collector_execution_matrix_test.cpp" -o "$work/matrix_test.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/matrix_test.o\"}
    command=${command/ -o JobLens / -o \"$work\/matrix_test\" }
    (cd "$build"; bash -c "$command")
    timeout 60 "$work/matrix_test" "$work/matrix"
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
    all)
        run_executor
        run_matrix
        run_poller
        ;;
    *)
        printf 'Unknown execution suite: %s\n' "$suite" >&2
        exit 2
        ;;
esac
