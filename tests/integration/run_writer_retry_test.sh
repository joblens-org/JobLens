#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
build=${1:-"$root/build"}
build=$(cd "$build" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cmake --build "$build" -j 2
link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
if [[ -z "$link" ]]; then
    printf '%s\n' '未找到 Ninja JobLens 链接命令' >&2
    exit 1
fi
for test in es_writer_retry_test perf_counter_call_count_test; do
    c++ -std=c++17 -O0 -g -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        -c "$root/tests/integration/$test.cpp" -o "$work/$test.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/$test.o\"}
    command=${command/ -o JobLens / -o \"$work\/$test\" }
    (cd "$build"; bash -c "$command")
    timeout 45 "$work/$test" "$root/tests/integration/es_writer_retry_test.yaml"
done
