#!/usr/bin/env bash
# RuleEngine 沙箱回归：io/package/require/load 不可达，无限循环被预算中断，
# 规则目录之外的路径与超大规则被拒绝。
#
# Usage:
#   bash tests/integration/run_rule_engine_sandbox_test.sh [build-dir]
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)

build=${1:-"$root/build"}
build=$(cd "$build" && pwd)

joblens_test_defines=$(ninja -C "$build" -t commands JobLens 2>/dev/null \
    | grep -m1 'src/main.cpp.o' | sed 's/ -o .*//' \
    | tr ' ' '\n' | grep -E '^-D[A-Za-z0-9_]+(=[A-Za-z0-9_.:+-]+)?$' | tr '\n' ' ')

joblens_test_werror="-Werror"
joblens_test_extra_flags=""
if [ -n "${JOBLENS_TEST_SANITIZER:-}" ]; then
    joblens_test_werror=""
    joblens_test_extra_flags="-fsanitize=$JOBLENS_TEST_SANITIZER -fno-omit-frame-pointer -g -Wno-error"
fi

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

${CXX:-c++} -std=c++17 -O0 -g -Wall -Wextra $joblens_test_werror $joblens_test_defines \
    -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    $joblens_test_extra_flags -c "$root/tests/integration/rule_engine_sandbox_test.cpp" -o "$work/test.o"

command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/test.o\"}
command=${command/ -o JobLens / -o \"$work\/test\" }
(cd "$build"; bash -c "$command")
timeout 60 "$work/test"
