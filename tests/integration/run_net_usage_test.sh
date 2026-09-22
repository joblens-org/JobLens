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
    -c "$root/tests/integration/net_usage_collector_test.cpp" -o "$work/test.o"
command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/test.o\"}
command=${command/ -o JobLens / -Wl,--wrap=stat -Wl,--wrap=send -Wl,--wrap=recv -o \"$work\/test\" }
(cd "$build"; bash -c "$command")
timeout 30 strace -f -s 256 -e trace=openat,sendto,write -o "$work/trace" "$work/test"
python3 - "$work/trace" <<'PY'
import collections
import re
import sys

phases = collections.defaultdict(collections.Counter)
phase = None
for line in open(sys.argv[1]):
    begin = re.search(r'NET_BEGIN (\w+)', line)
    if begin:
        phase = begin[1]
    elif 'NET_END ' in line:
        phase = None
    elif phase:
        if 'openat(' in line and re.search(r'"/proc/\d+/fd/?"', line):
            phases[phase]['fd_scans'] += 1
        if 'openat(' in line and re.search(r'"/proc/\d+/net/(tcp6?|udp6?)"', line):
            phases[phase]['net_tables'] += 1
        if 'sendto(' in line:
            phases[phase]['tcp_queries'] += 1
for phase in ('shared', 'updated'):
    expected = dict(fd_scans=2, net_tables=4, tcp_queries=2)
    assert dict(phases[phase]) == expected, (phase, dict(phases[phase]), expected)
assert dict(phases['empty']) == dict(fd_scans=1), dict(phases['empty'])
assert dict(phases['unknown_namespace']) == dict(fd_scans=2, net_tables=8, tcp_queries=4), dict(phases['unknown_namespace'])
assert dict(phases['without_netlink']) == dict(fd_scans=1, net_tables=4), dict(phases['without_netlink'])
print('Verified one FD scan/PID, one table set/namespace and one query/shared TCP socket')
PY
if command -v unshare >/dev/null 2>&1 && unshare --user --map-root-user --net true 2>/dev/null; then
    timeout 30 unshare --user --map-root-user "$work/test" --namespace-race
else
    printf '%s\n' 'SKIP namespace switch regression: user/network namespaces are unavailable'
fi
