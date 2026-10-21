#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
build=${1:-"$root/build"}
build=$(cd "$build" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
c++ -std=c++17 -O0 -g -Wall -Wextra -Werror -I"$root/include" -pthread \
    "$root/tests/integration/timer_scheduler_test.cpp" "$root/src/common/timer_scheduler.cpp" \
    -lspdlog -lfmt -o "$work/timer_test"
timeout 20 "$work/timer_test"
if [[ ${SKIP_BUILD:-0} != 1 ]]; then cmake --build "$build" -j 2; fi
link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
[[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }
c++ -std=c++17 -O0 -g -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    -c "$root/tests/integration/collector_scheduler_status_test.cpp" -o "$work/status_test.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/status_test.o\"}
command=${command/ -o JobLens / -o \"$work\/status_test\" }
(cd "$build"; bash -c "$command")
timeout 20 "$work/status_test" "$work/status"
