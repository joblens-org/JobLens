#!/usr/bin/env bash
# 原生输出契约 / 路由 / 调度集成测试。
# 需要先构建成功；SKIP_BUILD=1 可跳过重新构建。
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
build=${1:-"$root/build"}
build=$(cd "$build" && pwd)
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
    c++ -std=c++17 -O0 -g -DSPDLOG_COMPILED_LIB -DYAML_CPP_STATIC_DEFINE -DHAS_STRING_VIEW=1 \
        -I"$build/include" -I"$root/include" -I/usr/include/libnl3 \
        -c "$root/tests/integration/$name.cpp" -o "$work/$name.o"
    local command=${link/CMakeFiles\/JobLens.dir\/src\/main.cpp.o/\"$work\/$name.o\"}
    command=${command/ -o JobLens / -o \"$work\/$name\" }
    (cd "$build"; bash -c "$command")
    timeout 120 "$work/$name" "$config"
}

run_test output_contract_test "$work/contract.yaml"
run_test output_routing_test "$work/routing.yaml"
run_test collector_output_test "$work/collector_output"
