#!/usr/bin/env bash
# 原生输出契约 / 路由 / 调度集成测试。
# 需要先构建成功；SKIP_BUILD=1 可跳过重新构建。
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
if [[ ${SKIP_BUILD:-0} != 1 ]]; then cmake --build "$build" -j 2; fi
link=""
while IFS= read -r command; do
    if [[ "$command" == *" -o JobLens "* ]]; then link=$command; fi
done < <(ninja -C "$build" -t commands JobLens)
[[ -n "$link" ]] || { printf '%s\n' 'Could not find the Ninja JobLens link command' >&2; exit 1; }

cat > "$work/contract.yaml" <<EOF
writers_config:
  buffer_capacity: 256
  max_pending_records: 4096
  max_pending_bytes: 67108864
  max_record_bytes: 8388608
  writers: []
collectors_config:
  enable_collector_perf: false
  default_freq: 1
  default_use_writers: []
sink_cfg: {}
job_registry_config:
  job_db_path: $work/contract.db
EOF

cat > "$work/routing.yaml" <<EOF
writers_config:
  buffer_capacity: 256
  writers:
    - {name: w_doc, type: CaptureWriter, config: cap_doc}
    - {name: w_text, type: CaptureWriter, config: cap_text}
    - {name: w_metrics, type: CaptureWriter, config: cap_metrics}
    - {name: w_all, type: CaptureWriter, config: cap_all}
collectors_config:
  enable_collector_perf: false
  default_freq: 1
  default_use_writers: []
cap_doc: {kind: document}
cap_text: {kind: text}
cap_metrics: {kind: metrics}
cap_all: {kind: all}
rt_explicit:
  outputs: {document: [w_doc], text: [w_text], metrics: [w_metrics]}
rt_legacy:
  use_writers: [w_text]
rt_disable_map:
  outputs: {}
rt_disable_list:
  use_writers: []
rt_unknown_writer:
  outputs: {text: [missing_writer]}
rt_unknown_channel:
  outputs: {nope: [w_text]}
rt_bad_kind:
  outputs: {text: [w_doc]}
rt_both:
  outputs: {text: [w_text]}
  use_writers: [w_text]
rt_duplicate:
  outputs: {text: [w_text, w_text]}
rt_two_channels:
  outputs: {document: [w_all], text: [w_all]}
job_registry_config:
  job_db_path: $work/routing.db
EOF

run_test() {
    local name=$1
    local config=$2
    ${CXX:-c++} -std=c++17 -O0 -g $joblens_test_defines \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        $joblens_test_extra_flags -c "$root/tests/integration/$name.cpp" -o "$work/$name.o"
    local command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/$name.o\"}
    command=${command/ -o JobLens / -o \"$work\/$name\" }
    (cd "$build"; bash -c "$command")
    timeout 120 "$work/$name" "$config"
}

cat > "$work/wm_lifecycle.yaml" <<EOF
writers_config:
  buffer_capacity: 256
  max_pending_records: 4096
  max_pending_bytes: 67108864
  max_record_bytes: 8388608
  writers:
EOF
for i in $(seq 0 99); do
    printf '    - {name: wm_probe_%s, type: ProbeWriter, config: wm_cfg}\n' "$i" >> "$work/wm_lifecycle.yaml"
done
cat >> "$work/wm_lifecycle.yaml" <<EOF
wm_cfg: {}
job_registry_config:
  job_db_path: $work/lifecycle.db
EOF

cat > "$work/limits_override.yaml" <<EOF
limits_test: {scenario: overrides}
writers_config:
  buffer_capacity: 256
  max_pending_records: 4096
  max_pending_bytes: 67108864
  max_record_bytes: 8388608
  writers: []
ctl_cfg: {}
neg_records_cfg: {max_pending_records: -5}
zero_records_cfg: {max_pending_records: 0}
neg_bytes_cfg: {max_pending_bytes: -1}
zero_bytes_cfg: {max_pending_bytes: 0}
neg_record_bytes_cfg: {max_record_bytes: -3}
zero_record_bytes_cfg: {max_record_bytes: 0}
huge_records_cfg: {max_pending_records: 4294967296}
huge_bytes_cfg: {max_pending_bytes: 18446744073709551615}
EOF
cat > "$work/limits_global_buffer.yaml" <<EOF
limits_test: {scenario: global_buffer}
writers_config:
  buffer_capacity: -9
  max_pending_records: 4096
  max_pending_bytes: 67108864
  max_record_bytes: 8388608
  writers: []
ctl_cfg: {}
EOF
cat > "$work/limits_global_records.yaml" <<EOF
limits_test: {scenario: global_records}
writers_config:
  buffer_capacity: 256
  max_pending_records: -7
  max_pending_bytes: 67108864
  max_record_bytes: 8388608
  writers: []
ctl_cfg: {}
EOF

run_test output_contract_test "$work/contract.yaml"
run_test output_routing_test "$work/routing.yaml"
run_test collector_output_test "$work/collector_output"
run_test writer_manager_lifecycle_test "$work/wm_lifecycle.yaml"
run_test writer_queue_limits_test "$work/limits_override.yaml"
run_test writer_queue_limits_test "$work/limits_global_buffer.yaml"
run_test writer_queue_limits_test "$work/limits_global_records.yaml"
