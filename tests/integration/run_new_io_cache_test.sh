#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
build=${1:-"$root/build"}
build=$(cd "$build" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
c++ -std=c++17 -O2 -Wall -Wextra -Werror -I"$root/include" \
    "$root/tests/integration/new_io_fd_index_test.cpp" -o "$work/index_test"
"$work/index_test"
cmake --build "$build" -j 2
link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
[[ -n "$link" ]] || { printf '%s\n' '未找到 Ninja 链接命令' >&2; exit 1; }
c++ -std=c++17 -O0 -g -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    -c "$root/tests/integration/new_io_cache_test.cpp" -o "$work/test.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/test.o\"}
command=${command/ -o JobLens / -o \"$work\/test\" }
(cd "$build"; bash -c "$command")
clang -g -O2 -target bpf -I"$build/include" -I"$root/include" \
    -c "$root/tests/integration/new_io_cache_maps.bpf.c" -o "$work/maps.bpf.o"
timeout 30 "$work/test" "$work/maps.bpf.o"
