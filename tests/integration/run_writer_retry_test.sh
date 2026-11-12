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
    ${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        $joblens_test_extra_flags -c "$root/tests/integration/$test.cpp" -o "$work/$test.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/$test.o\"}
    command=${command/ -o JobLens / -o \"$work\/$test\" }
    if [[ "$test" == es_writer_retry_test ]]; then command=${command/ -o / -Wl,--wrap=curl_easy_perform -o }; fi
    (cd "$build"; bash -c "$command")
    timeout 45 "$work/$test" "$root/tests/integration/es_writer_retry_test.yaml"
done
