#!/usr/bin/env bash
# Core half of the section 9 inventory/collection RPC contract
# (doc/job_inventory_collection_decoupling_plan.md, cases A30-A33).
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
    c++ -std=c++17 -O0 -g -Wall -Wextra -Werror -DSPDLOG_COMPILED_LIB \
        -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        -c "$source" -o "$work/$name.o"
    command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/$name.o\"}
    command=${command/ -o JobLens / -o \"$work\/$name\" }
    (cd "$build"; bash -c "$command")
    timeout 120 "$work/$name" "$work/$name-data"
}

link_test "$root/tests/integration/collection_rpc_test.cpp" collection_rpc_test
