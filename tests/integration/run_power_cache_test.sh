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
[[ -n "$link" ]] || { printf '%s\n' 'Ninja JobLens link command not found' >&2; exit 1; }
c++ -std=c++17 -O0 -g -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    -c "$root/tests/integration/power_cache_test.cpp" -o "$work/test.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/test.o\"}
wraps="-Wl,--wrap=stat -Wl,--wrap=opendir -Wl,--wrap=bpf_map__fd"
wraps+=" -Wl,--wrap=bpf_map_lookup_batch -Wl,--wrap=bpf_map_delete_batch"
wraps+=" -Wl,--wrap=bpf_map_get_next_key -Wl,--wrap=bpf_map_lookup_elem -Wl,--wrap=bpf_map_delete_elem"
wraps+=" -Wl,--wrap=popen -Wl,--wrap=pclose"
command=${command/ -o JobLens / $wraps -o \"$work\/test\" }
(cd "$build"; bash -c "$command")
clang -g -O2 -target bpf -I"$build/include" -I"$root/include" \
    -c "$root/tests/integration/power_cache_maps.bpf.c" -o "$work/maps.bpf.o"
timeout 30 "$work/test" "$work/maps.bpf.o" "$work/runtime"
