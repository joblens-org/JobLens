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
[[ -n "$link" ]] || { printf '%s\n' 'Ninja JobLens link command not found' >&2; exit 1; }
${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
    -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
    $joblens_test_extra_flags -c "$root/tests/integration/cpumem_collector_test.cpp" -o "$work/test.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/test.o\"}
command=${command/ -o JobLens / -Wl,--wrap=sysconf -o \"$work\/test\" }
(cd "$build"; bash -c "$command")
timeout 30 strace -f -s 256 -e trace=openat,write -o "$work/trace" "$work/test"
python3 - "$work/trace" <<'PY'
import collections
import re
import sys

phases = collections.defaultdict(collections.Counter)
phase = None
for line in open(sys.argv[1]):
    begin = re.search(r'CPUMEM_BEGIN (\w+)', line)
    if begin:
        phase = begin[1]
    elif 'CPUMEM_END ' in line:
        phase = None
    elif phase and 'openat(' in line:
        if '"/proc/stat"' in line:
            phases[phase]['system_stat'] += 1
        match = re.search(r'"/proc/\d+/(stat|status|comm)"', line)
        if match:
            phases[phase][match[1]] += 1
for phase in ('first', 'second'):
    expected = dict(system_stat=1, stat=2, status=2)
    assert dict(phases[phase]) == expected, (phase, dict(phases[phase]), expected)
expected = dict(system_stat=1, stat=1, status=2, comm=1)
assert dict(phases['fallback']) == expected, (dict(phases['fallback']), expected)
print('Verified one /proc/stat read per Job, no redundant comm reads and stat-failure fallback')
PY
