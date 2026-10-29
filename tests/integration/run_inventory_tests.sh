#!/usr/bin/env bash
# Deterministic inventory/collection-decoupling suites.
#
# Usage:
#   bash tests/integration/run_inventory_tests.sh [build-dir] [--suite NAME]
#
# Suites: identity, store, intent, maintenance, lifecycle, all. Unknown suites
# fail. Later
# implementation tasks add binding and policy suites here.
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

# Compiles one test TU and links it against the production object set by
# reusing the Ninja JobLens link command.
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

run_identity() {
    link_test "$root/tests/integration/job_identity_test.cpp" job_identity_test
}

run_store() {
    link_test "$root/tests/integration/job_inventory_store_test.cpp" job_inventory_store_test
}

run_intent() {
    link_test "$root/tests/integration/job_collection_intent_test.cpp" job_collection_intent_test
}

run_maintenance() {
    link_test "$root/tests/integration/job_inventory_maintenance_test.cpp" \
        job_inventory_maintenance_test
}

run_lifecycle() {
    link_test "$root/tests/integration/job_registry_lifecycle_test.cpp" \
        job_registry_lifecycle_test
}

case "$suite" in
    identity)
        run_identity
        ;;
    store)
        run_store
        ;;
    intent)
        run_intent
        ;;
    maintenance)
        run_maintenance
        ;;
    lifecycle)
        run_lifecycle
        ;;
    all)
        run_identity
        run_store
        run_intent
        run_maintenance
        run_lifecycle
        ;;
    *)
        printf 'Unknown inventory suite: %s\n' "$suite" >&2
        exit 2
        ;;
esac
