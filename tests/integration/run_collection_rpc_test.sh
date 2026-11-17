#!/usr/bin/env bash
# Core half of the section 9 job/collection RPC contract.
#
# Usage:
#   bash tests/integration/run_collection_rpc_test.sh [build-dir]
#
# The test compiles one TU and links it against the production object set by
# reusing the Ninja JobLens link command, then drives every new RPC method
# through a temporary AF_UNIX socket.
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

if [[ ${SKIP_BUILD:-0} != 1 ]]; then
    cmake --build "$build" -j 2
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

link_test() {
    local source=$1 name=$2
    local link=""
    while IFS= read -r command; do
        if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
    done < <(ninja -C "$build" -t commands JobLens)
    [[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }
    ${CXX:-c++} -std=c++17 -O0 -g -Wall -Wextra $joblens_test_werror $joblens_test_defines \
        -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        $joblens_test_extra_flags -c "$source" -o "$work/$name.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/$name.o\"}
    command=${command/ -o JobLens / -o \"$work\/$name\" }
    (cd "$build"; bash -c "$command")
    timeout 120 "$work/$name" "$work/$name-data"
}

link_test "$root/tests/integration/collection_rpc_test.cpp" collection_rpc_test
