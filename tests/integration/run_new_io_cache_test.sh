#!/usr/bin/env bash
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
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
${CXX:-c++} -std=c++17 -O2 -Wall -Wextra $joblens_test_werror -I"$root/include" \
    "$root/tests/integration/new_io_fd_index_test.cpp" -o "$work/index_test"
"$work/index_test"
cmake --build "$build" -j 2
link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
[[ -n "$link" ]] || { printf '%s\n' '未找到 Ninja 链接命令' >&2; exit 1; }
${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    $joblens_test_extra_flags -c "$root/tests/integration/new_io_cache_test.cpp" -o "$work/test.o"
${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    $joblens_test_extra_flags -c "$root/tests/integration/io_no_stat_test.cpp" -o "$work/legacy.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/test.o\"}
wraps="\"$work/legacy.o\" -Wl,--wrap=stat -Wl,--wrap=kill -Wl,--wrap=bpf_map_lookup_batch -Wl,--wrap=bpf_object__load"
wraps+=" -Wl,--wrap=bpf_object__find_map_fd_by_name -Wl,--wrap=bpf_map__fd"
wraps+=" -Wl,--wrap=bpf_map_update_elem -Wl,--wrap=bpf_map_lookup_elem -Wl,--wrap=bpf_map_delete_elem"
wraps+=" -Wl,--wrap=bpf_map__lookup_elem -Wl,--wrap=bpf_map__lookup_and_delete_elem"
command=${command/ -o JobLens / $wraps -o \"$work\/test\" }
(cd "$build"; bash -c "$command")
clang -g -O2 -target bpf -I"$build/include" -I"$root/include" \
    -c "$root/tests/integration/new_io_cache_maps.bpf.c" -o "$work/maps.bpf.o"
timeout 30 "$work/test" "$work/maps.bpf.o" "${@:2}"
