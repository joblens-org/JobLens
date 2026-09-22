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
    -c "$root/tests/integration/cpumem_collector_test.cpp" -o "$work/test.o"
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
