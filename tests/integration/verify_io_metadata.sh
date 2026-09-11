#!/bin/bash
set -euo pipefail
job=$1
binary=$2
details=${3:-true}
work=$(mktemp -d /tmp/jl-io-meta.XXXXXX)
chmod 777 "$work"
pid=''
runner=''
cleanup() {
    curl -fsS http://127.0.0.1:7592/joblens/job -H 'Content-Type: application/json' \
        -d "{\"opt\":\"remove\",\"type\":\"job.condor\",\"JobID\":$job}" >/dev/null || true
    if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; fi
    if [[ -n "$runner" ]]; then wait "$runner" || true; fi
    rm -rf "$work"
}
trap cleanup EXIT
curl -fsS http://127.0.0.1:7592/joblens/rpc/health
start=$(date '+%Y-%m-%d %H:%M:%S')
runuser -u wzycc -- "$binary" "$work/data" > "$work/pid" &
runner=$!
sleep 1
pid=$(cat "$work/pid")
[[ "$pid" =~ ^[0-9]+$ ]]
curl -fsS http://127.0.0.1:7592/joblens/job -H 'Content-Type: application/json' \
    -d "{\"opt\":\"add\",\"type\":\"job.condor\",\"JobID\":$job,\"JobPIDs\":[$pid],\"Lens\":[\"new_io_usage_collector\",\"fs_metadata_collector\"],\"sub_attr\":{\"cluster_id\":$job,\"proc_id\":0,\"auto_update_child\":false}}"
sleep 12
kill -0 "$pid"
journalctl -u joblens --since "$start" --no-pager > "$work/journal"
sed -n 's/^.*document to index: //p' "$work/journal" | \
    jq -c --argjson job "$job" 'select(.data.job_id == $job)' > "$work/documents"
jq -s -e --argjson details "$details" --argjson pid "$pid" '
  [.[] | .data | select(has("job_total"))] as $io |
  [.[] | .data | select(has("job_ops"))] as $fs |
  ($io|length) >= 3 and ($fs|length) >= 3 and
  $io[-1].job_total.rchar > $io[0].job_total.rchar and
  $io[-1].job_total.wchar > $io[0].job_total.wchar and
  $io[-1].job_total.rchar_speed > 0 and
  $fs[-1].job_metadata_ops_total > $fs[0].job_metadata_ops_total and
  $fs[-1].job_metadata_ops_rate > 0 and
  any($fs[-1].job_ops[]; .errors > 0 and .last_errno == -2) and
  all($fs[-1].job_ops[]; .calls == (.success + .errors)) and
  (if $details then
    any($io[-1].processes[]; .pid == $pid and .rchar > 0) and
    ($io[-1].files|length) > 0 and
    any($fs[-1].processes[]; .pid == $pid and .metadata_ops_total > 0 and
      any(.ops[]; .errors > 0 and .last_errno == -2))
   else
    all($io[]; (.processes|length)==0 and (.files|length)==0) and
    all($fs[]; (.processes|length)==0)
   end)
' "$work/documents"
grep -q 'ESWriter: post ret code: 200' "$work/journal"
jq -s '{io:([.[]|.data|select(has("job_total"))][-1]|{job_id,processes:(.processes|length),files:(.files|length),job_total}),fs:([.[]|.data|select(has("job_ops"))][-1]|{job_id,processes:(.processes|length),job_metadata_ops_total,job_metadata_ops_rate,job_ops})}' "$work/documents"
printf 'PASS job=%s details=%s pid=%s\n' "$job" "$details" "$pid"
