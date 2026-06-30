#!/usr/bin/env bash
# ============================================================================
# power_bench.sh — Production-grade server power benchmarking tool
# Version: 1.1.0
#
# Hardware-adaptive RAPL + IPMI dual-source power measurement.
# Pluggable calibration model (Kavanagh 2019 default).
# Output: JSON (structured), CSV, Markdown table, terminal report.
#
# Usage: ./power_bench.sh [-d SEC] [-r N] [-c CONFIG] [--dry-run] [--debug]
# ============================================================================
set -euo pipefail

# ── P0: Hardware Detection Layer ───────────────────────────────────────────
detect_cpu_vendor() {
    local vendor="unknown"
    if grep -q "GenuineIntel" /proc/cpuinfo 2>/dev/null; then vendor="intel"
    elif grep -q "AuthenticAMD" /proc/cpuinfo 2>/dev/null; then vendor="amd"
    elif grep -q "ARM" /proc/cpuinfo 2>/dev/null; then vendor="arm"
    fi
    echo "$vendor"
}

detect_rapl_paths() {
    local vendor="$1"
    case "$vendor" in
        intel) RAPL_BASE="/sys/class/powercap/intel-rapl" ;;
        amd)   RAPL_BASE="/sys/class/powercap/amd_energy" ;;
        arm)   RAPL_BASE="" ;;
        *)     RAPL_BASE="" ;;
    esac
    RAPL_BASE="${POWER_RAPL_BASE:-$RAPL_BASE}"
}

detect_rapl_max() {
    local max=0; local v
    shopt -s nullglob
    for f in "${RAPL_BASE}":*/max_energy_range_uj; do
        [ -f "$f" ] || continue
        v=$(cat "$f" 2>/dev/null || echo 0)
        max=$((max > v ? max : v))
    done
    shopt -u nullglob
    echo "${max:-0}"
}

detect_ipmi_tool() {
    if command -v ipmi-dcmi &>/dev/null; then
        echo "ipmi-dcmi|ipmi-dcmi --get-system-power-statistics"
    elif command -v ipmitool &>/dev/null; then
        echo "ipmitool|ipmitool dcmi power reading"
    else
        echo "none|"
    fi
}

hw_probe() {
    HW_CPU_VENDOR=$(detect_cpu_vendor)
    detect_rapl_paths "$HW_CPU_VENDOR"
    HW_RAPL_MAX=$(detect_rapl_max)
    HW_CORES=$(nproc)

    local ipmi_info; ipmi_info=$(detect_ipmi_tool)
    HW_IPMI_TOOL="${ipmi_info%%|*}"
    HW_IPMI_CMD="${ipmi_info##*|}"

    HW_IPMI_AVAILABLE=true
    [ "$HW_IPMI_TOOL" = "none" ] && HW_IPMI_AVAILABLE=false

    HW_LOAD_TOOL="stress-ng"
    command -v stress-ng &>/dev/null || HW_LOAD_TOOL="none"

    debug "CPU: $HW_CPU_VENDOR | Cores: $HW_CORES | RAPL: ${RAPL_BASE:-none} (max=$HW_RAPL_MAX) | IPMI: $HW_IPMI_TOOL | Load: $HW_LOAD_TOOL"
}

# ── P0: IPMI Sensor Fuzzy Match ───────────────────────────────────────────
sensor_value() {
    local name_hint="$1" type_hint="${2:-}"
    ipmitool sensor list 2>/dev/null | awk -F'|' -v n="$name_hint" -v t="$type_hint" '
        BEGIN { IGNORECASE=1 }
        $1 ~ n && $3 ~ t { gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit }
    '
}

# ── P0: Core Measurement Functions ────────────────────────────────────────
read_rapl_pkg() {
    [ -z "${RAPL_BASE:-}" ] && { echo "0"; return; }
    local total=0
    for p in "${RAPL_BASE}":*; do
        [ -d "$p" ] || continue
        local f="$p/energy_uj"
        [ -f "$f" ] && total=$((total + $(cat "$f")))
    done
    echo "$total"
}

# read_ipmi → power in W (with timeout, robust digit extraction)
read_ipmi() {
    local w=0 out
    if [ "$HW_IPMI_TOOL" = "ipmi-dcmi" ]; then
        out=$(timeout 5 ipmi-dcmi --get-system-power-statistics 2>/dev/null || true)
        w=$(echo "$out" | grep -oP 'Current Power[^0-9]*\K[0-9]+' | head -1)
    elif [ "$HW_IPMI_TOOL" = "ipmitool" ]; then
        out=$(timeout 5 ipmitool dcmi power reading 2>/dev/null || true)
        w=$(echo "$out" | grep -oP 'Instantaneous power reading[^0-9]*\K[0-9]+' | head -1)
    fi
    echo "${w:-0}"
}

read_ipmi_dense_bg() {
    local duration="$1" tmpfile="$2"
    for ((s=1; s<=duration; s++)); do read_ipmi; sleep 1; done > "$tmpfile" 2>/dev/null
}

ipmi_median() { sort -n "$1" | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }

# safe_delta cur prev → handles counter wraparound
# Correct overflow formula: (max - prev) + cur = cur + max - prev
safe_delta() {
    local cur="$1" prev="$2" max="${3:-$HW_RAPL_MAX}"
    if [ "$cur" -ge "$prev" ]; then echo $((cur - prev))
    elif [ "$max" -gt 0 ]; then echo $((max - prev + cur))
    else echo $((cur))  # no max available, best-effort
    fi
}

# ── P0: Temperature & Sensors ──────────────────────────────────────────────
read_temps() {
    local raw cpu1 cpu2 inlet
    raw=$(ipmitool sensor list 2>/dev/null)
    # Match by sensor name only; type column ("degrees C") doesn't contain "temp"
    cpu1=$(echo "$raw" | grep -iE "(02-)?CPU[- ]*1" | head -1 | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}')
    cpu2=$(echo "$raw" | grep -iE "(03-)?CPU[- ]*2" | head -1 | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}')
    inlet=$(echo "$raw" | grep -iE "Inlet.*Ambient|Ambient.*Inlet|Inlet Temp|System Temp" | head -1 | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2}')
    echo "${cpu1:-N/A},${cpu2:-N/A},${inlet:-N/A}"
}

# ── P0: Output Formatting ─────────────────────────────────────────────────
adaptive_unit() {
    if awk -v x="$1" 'BEGIN {exit(x<1?0:1)}'; then
        printf "%.3f W" "$1"
    elif awk -v x="$1" 'BEGIN {exit(x<100?0:1)}'; then
        printf "%.1f W" "$1"
    else
        printf "%.0f W" "$1"
    fi
}
# ── P1: Configuration System ──────────────────────────────────────────────
CONFIG_FILE="${POWER_CONFIG:-./config.ini}"

load_config() {
    [ -f "$CONFIG_FILE" ] || return 0
    while IFS='=' read -r key value; do
        [[ "$key" =~ ^[[:space:]]*# ]] && continue
        [ -z "$key" ] && continue
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        export "POWER_${key}=${value}"
    done < "$CONFIG_FILE"
    debug "config loaded from $CONFIG_FILE"
}

# ── P1: Calibration Model (Pluggable) ─────────────────────────────────────
CAL_MODEL="${POWER_CAL_MODEL:-kavanagh2019}"

calibrate_kavanagh2019() {
    local cal_tmp; cal_tmp=$(mktemp)
    log "Calibrating (Kavanagh 2019, 60s)..."
    read_ipmi_dense_bg 60 "$cal_tmp" &
    local bg_pid=$!
    local r1; r1=$(read_rapl_pkg)
    sleep 60
    local r2; r2=$(read_rapl_pkg)
    wait $bg_pid 2>/dev/null || true

    CAL_IPMI_IDLE=$(sort -n "$cal_tmp" | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
    local delta; delta=$(safe_delta "$r2" "$r1")
    CAL_RAPL_IDLE=$(awk -v e="$delta" 'BEGIN {printf "%.1f", e/60/1000000}')
    CAL_STATIC=$(awk -v i="$CAL_IPMI_IDLE" -v r="$CAL_RAPL_IDLE" 'BEGIN {printf "%.1f", i - r}')

    # Validity: warn if static overhead outside [30,120]W (typical server range)
    if awk -v s="$CAL_STATIC" 'BEGIN {exit(s>=30 && s<=120?1:0)}'; then
        warn "Calibration static overhead=${CAL_STATIC}W is out of expected range [30,120]W"
        warn "  Model may be unsuitable for this hardware. Check IPMI or use --cal-model=none"
    fi

    rm -f "$cal_tmp"
    log "Calibration: IPMI_idle=${CAL_IPMI_IDLE}W  RAPL_idle=${CAL_RAPL_IDLE}W  Static=${CAL_STATIC}W"
}

compute_cal() {
    local rapl="$1"; local dram="${2:-0}"
    case "$CAL_MODEL" in
        kavanagh2019) awk -v b="$CAL_IPMI_IDLE" -v r="$rapl" -v ri="$CAL_RAPL_IDLE" -v d="$dram" \
            'BEGIN {printf "%.1f", b + (r - ri) + d}' ;;
        none) echo "$rapl" ;;
        *)    echo "$rapl" ;;
    esac
}

# ── P1: Sample One Round ──────────────────────────────────────────────────
sample_one() {
    local label="$1" round="$2" cores="$3" duration="$4"

    local ipmi_tmp; ipmi_tmp=$(mktemp)
    local ipmi_pid=""
    if $HW_IPMI_AVAILABLE; then
        read_ipmi_dense_bg "$duration" "$ipmi_tmp" &
        ipmi_pid=$!
    fi

    local stress_pid=""
    if [ "$cores" -gt 0 ] && [ "$HW_LOAD_TOOL" != "none" ]; then
        stress-ng --cpu "$cores" --timeout $((duration + 5))s --quiet &
        stress_pid=$!
        sleep 2
    fi

    local r1; r1=$(read_rapl_pkg)
    sleep "$duration"
    local r2; r2=$(read_rapl_pkg)

    [ -n "$ipmi_pid" ] && wait $ipmi_pid 2>/dev/null || true

    local delta; delta=$(safe_delta "$r2" "$r1")
    local pkg_w; pkg_w=$(awk -v e="$delta" -v dt="$duration" 'BEGIN {printf "%.1f", e/dt/1000000}')

    # IPMI median
    local ipmi_w="N/A"
    if $HW_IPMI_AVAILABLE && [ -s "$ipmi_tmp" ]; then
        ipmi_w=$(sort -n "$ipmi_tmp" | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}')
    fi

    # Physical constraint: IPMI < PKG → expired (DCMI stale)
    local valid="✓"
    [ "$ipmi_w" = "N/A" ] && valid="N/A"
    [ "$valid" = "✓" ] && awk -v i="$ipmi_w" -v p="$pkg_w" 'BEGIN {exit(i>=p?0:1)}' || valid="✗"

    # Outlier: flag if PKG > 500W (physically impossible for this class of server)
    local flag="-"
    awk -v p="$pkg_w" 'BEGIN {exit(p>500?0:1)}' && flag="OUTLIER"

    # Calibration
    local cal_w; cal_w=$(compute_cal "$pkg_w" 0)

    # Temperature
    local temps; temps=$(read_temps)

    echo "$label,$round,$pkg_w,$ipmi_w,$cal_w,$valid,$flag,$temps"

    local ipmi_display="$ipmi_w"
    [ "$valid" = "✗" ] && ipmi_display="${ipmi_w}[expired]"
    printf "    %s#%d: PKG=%s  IPMI=%s  CAL=%s  %s\n" \
        "$label" "$round" "$(adaptive_unit "$pkg_w")" "$ipmi_display" "$(adaptive_unit "$cal_w")" "${valid}${flag:+(flag)}"

    rm -f "$ipmi_tmp"
    [ -n "$stress_pid" ] && wait $stress_pid 2>/dev/null || true
    sleep 3
}

# ── Run Full Test ─────────────────────────────────────────────────────────
run_test() {
    local label="$1" cores="$2" n="$3" duration="$4"
    log "Testing: $label ($n rounds × ${duration}s)"
    for ((i=1; i<=n; i++)); do
        printf "  [%2d/%2d] " "$i" "$n"
        local result; result=$(sample_one "$label" "$i" "$cores" "$duration")
        echo "$result" >> "$OUT_CSV"
    done
    echo ""
}

# ── P2: Stats Computation ─────────────────────────────────────────────────
# Pass CSV path via env var to avoid shell injection in Python heredoc
compute_stats() {
    local csv="$1"
    OUT_CSV_PATH="$csv" python3 << 'PYEOF'
import csv, statistics, json, os

csv_path = os.environ.get('OUT_CSV_PATH', '')
if not csv_path:
    print('{}')
    exit(0)

rows = list(csv.DictReader(open(csv_path)))

def stats(vals):
    clean = [v for v in vals if v is not None and str(v).upper() not in ('N/A', '')]
    if not clean: return {'avg': 0, 'std': 0, 'n': 0}
    m = statistics.mean(clean)
    s = statistics.stdev(clean) if len(clean) > 1 else 0.0
    return {'avg': round(m, 1), 'std': round(s, 1), 'n': len(clean)}

by_label = {}
for r in rows:
    lbl = r.get('label', '')
    if lbl not in by_label:
        by_label[lbl] = {'pkg': [], 'ipmi': [], 'cal': []}
    for k in ('pkg_w', 'ipmi_w', 'cal_w'):
        v = r.get(k, 'N/A')
        short_key = k.split('_')[0]
        try:
            by_label[lbl][short_key].append(float(v))
        except (ValueError, TypeError):
            pass

out = {}
for lbl, data in by_label.items():
    out[lbl] = {k: stats(data[k]) for k in ('pkg', 'ipmi', 'cal')}

print(json.dumps(out))
PYEOF
}

# ── P2: Markdown Report Generation ─────────────────────────────────────────
generate_report() {
    local stats_json="$1"
    local temps="N/A"
    $HW_IPMI_AVAILABLE && temps="$(read_temps)" || true

    cat > "$OUT_MD" << MDEOF
# Power Bench Report — $TIMESTAMP

**Hardware**: $HW_CPU_VENDOR, $HW_CORES cores, RAPL max=$HW_RAPL_MAX
**IPMI**: $HW_IPMI_TOOL | **Calibration**: $CAL_MODEL (static=${CAL_STATIC}W)
**Test**: ${DURATION}s × $ROUNDS rounds per load level

## Results

| Load   | RAPL Pkg (W) | IPMI Med (W) | CAL (W) | Valid |
|--------|-------------|-------------|---------|-------|
MDEOF
    python3 -c "
import json, os
d = json.loads(open('$OUT_JSON').read())
for lbl in ['idle', 'half', 'full']:
    if lbl not in d: continue
    p = d[lbl].get('pkg', {})
    i = d[lbl].get('ipmi', {})
    c = d[lbl].get('cal', {})
    print(f\"| {lbl:6s} | {p.get('avg',0):5.1f} ± {p.get('std',0):.1f} | {i.get('avg',0):5.0f} ± {i.get('std',0):.0f} | {c.get('avg',0):5.1f} | {i.get('n',0)}/{p.get('n',0)} |\")
" >> "$OUT_MD"

    cat >> "$OUT_MD" << MDEOF

## Delta Analysis

| Metric | Value |
|--------|-------|
| Per-core (full load) | $WATT_PER_CORE W/core |
| Idle→Full ΔPKG | $DELTA_PKG W |
| Idle→Full ΔIPMI | $DELTA_IPMI W |
| CPU temps (latest) | $temps |

## Files

- Raw data: \`$OUT_CSV\`
- JSON: \`$OUT_JSON\`
MDEOF
    log "report.md generated"
}

# ── Main Entry ────────────────────────────────────────────────────────────
main() {
    DURATION=30; ROUNDS=10; DRY_RUN=false; DEBUG_MODE=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -d) DURATION="$2"; shift 2 ;;
            -r) ROUNDS="$2"; shift 2 ;;
            -o) OUT_DIR="$2"; shift 2 ;;
            -c) CONFIG_FILE="$2"; shift 2 ;;
            --dry-run) DRY_RUN=true; shift ;;
            --debug) DEBUG_MODE=true; shift ;;
            -h|--help) head -16 "$0" | grep '^#' | sed 's/^# \?//'; exit 0 ;;
            *) shift ;;
        esac
    done

    TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
    OUT_DIR="${OUT_DIR:-./${TIMESTAMP}_bench}"
    mkdir -p "$OUT_DIR"
    OUT_CSV="$OUT_DIR/data.csv"
    OUT_JSON="$OUT_DIR/summary.json"
    OUT_MD="$OUT_DIR/report.md"

    load_config
    hw_probe

    if $DRY_RUN; then
        echo "=== Power Bench Dry Run ==="
        echo "CPU: $HW_CPU_VENDOR | Cores: $HW_CORES"
        echo "RAPL: ${RAPL_BASE:-none} (max=$HW_RAPL_MAX)"
        echo "IPMI: $HW_IPMI_TOOL ($HW_IPMI_AVAILABLE)"
        echo "Load tool: $HW_LOAD_TOOL"
        [ "$HW_LOAD_TOOL" = "none" ] && echo "WARNING: No load generation tool found (install stress-ng)"
        echo "Cal model: $CAL_MODEL"
        echo "Duration: ${DURATION}s × $ROUNDS rounds"
        echo "Output: $OUT_DIR"
        exit 0
    fi

    log "=== Power Bench v1.1 ==="
    log "CPU=$HW_CPU_VENDOR cores=$HW_CORES | RAPL=${RAPL_BASE:-none} | IPMI=$HW_IPMI_TOOL | Model=$CAL_MODEL"
    log "Test: ${DURATION}s × $ROUNDS rounds | Output: $OUT_DIR"

    if [ "$HW_LOAD_TOOL" = "none" ]; then
        warn "No load generation tool available (install stress-ng). Only idle measurements will be valid."
    fi

    # CSV header (flag is separate column now)
    echo "label,round,pkg_w,ipmi_w,cal_w,valid,flag,cpu1_temp,cpu2_temp,inlet_temp" > "$OUT_CSV"

    if $HW_IPMI_AVAILABLE && [ "$CAL_MODEL" != "none" ]; then
        calibrate_kavanagh2019
    else
        CAL_IPMI_IDLE=0; CAL_RAPL_IDLE=0; CAL_STATIC=0
        warn "IPMI unavailable or calibration disabled — CAL column will mirror PKG"
    fi

    # half cores: minimum 1 (avoid 0 cores = idle)
    local half_cores=$((HW_CORES/2))
    [ "$half_cores" -lt 1 ] && half_cores=1

    run_test "idle"  0            "$ROUNDS" "$DURATION"
    run_test "half"  "$half_cores" "$ROUNDS" "$DURATION"
    run_test "full"  "$HW_CORES"   "$ROUNDS" "$DURATION"

    local stats_json; stats_json=$(compute_stats "$OUT_CSV")
    echo "$stats_json" > "$OUT_JSON"

    # Extract key metrics for report
    WATT_PER_CORE=$(python3 -c "
import json
d=json.loads(open('$OUT_JSON').read())
dpkg=d.get('full',{}).get('pkg',{}).get('avg',0)-d.get('idle',{}).get('pkg',{}).get('avg',0)
print(round(dpkg/$HW_CORES,1))
" 2>/dev/null || echo "N/A")
    DELTA_PKG=$(python3 -c "
import json
d=json.loads(open('$OUT_JSON').read())
print(round(d.get('full',{}).get('pkg',{}).get('avg',0)-d.get('idle',{}).get('pkg',{}).get('avg',0),1))
" 2>/dev/null || echo "N/A")
    DELTA_IPMI=$(python3 -c "
import json
d=json.loads(open('$OUT_JSON').read())
print(round(d.get('full',{}).get('ipmi',{}).get('avg',0)-d.get('idle',{}).get('ipmi',{}).get('avg',0),1))
" 2>/dev/null || echo "N/A")

    generate_report "$stats_json"

    log "=== Complete: $OUT_DIR ==="
    log "  data.csv | summary.json | report.md"
}

# ── Utilities ─────────────────────────────────────────────────────────────
log()   { echo "[$(date +%H:%M:%S)] $*"; }
warn()  { echo "[$(date +%H:%M:%S)] WARN: $*" >&2; }
debug() { if $DEBUG_MODE; then echo "[$(date +%H:%M:%S)] DEBUG: $*" >&2; fi; }

main "$@"
