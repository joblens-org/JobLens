#!/usr/bin/env bash
# JobLens 集成测试 — 快速烟气测试 (仅健康检查)
# 用法: bash quick_smoke.sh
# 流程: up → provision → health check → clean
# 比 run_all.sh 更快: 跳过 condor/slurm discovery、filewriter、API、性能测试
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "  JobLens 集成测试 — 烟气测试"
echo "=========================================="
echo ""

# ---- 阶段 1: 启动 VM ----
echo "=== [1/4] 启动 VM ==="
vagrant up --provider=libvirt
echo ""

# ---- 阶段 2: 部署服务 ----
echo "=== [2/4] 部署服务 ==="
make provision
echo ""

# ---- 阶段 3: 健康检查 (仅 curl, 不装 venv) ----
echo "=== [3/4] 健康检查 ==="

check_endpoint() {
    local name="$1"
    local url="$2"
    local expect="${3:-200}"

    printf "  %-40s " "${name} ..."
    local code
    code=$(vagrant ssh worker -c "curl -s -o /dev/null -w '%{http_code}' --max-time 10 '${url}'" 2>/dev/null || echo "000")
    # 去掉 vagrant ssh 可能带出的 Connection closed 噪音
    code=$(echo "$code" | tail -1)
    if [ "$code" = "$expect" ]; then
        echo "OK (HTTP $code)"
    else
        echo "FAIL (HTTP $code, expected $expect)"
        return 1
    fi
}

check_endpoint "/joblens/healthy"    "http://localhost:7592/joblens/healthy"    200 || exit 1
check_endpoint "/joblens/rpc/health"  "http://localhost:7592/joblens/rpc/health"  200 || exit 1
check_endpoint "/trigger/health"      "http://localhost:7592/trigger/health"      200 || exit 1
check_endpoint "/joblens/jobs/count"  "http://localhost:7592/joblens/jobs/count"  200 || exit 1

echo ""
echo "  全部健康检查通过"
echo ""

# ---- 阶段 4: 清理 ----
echo "=== [4/4] 清理环境 ==="
make clean
echo ""

echo "=========================================="
echo "  烟气测试通过"
echo "=========================================="
