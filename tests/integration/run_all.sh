#!/usr/bin/env bash
# JobLens 集成测试 — 完整一键流水线
# 用法: bash run_all.sh
# 流程: up → provision → test → clean
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "  JobLens 集成测试 — 完整流水线"
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

# ---- 阶段 3: 运行测试 ----
echo "=== [3/4] 运行集成测试 ==="
make test
echo ""

# ---- 阶段 4: 清理 ----
echo "=== [4/4] 清理环境 ==="
make clean
echo ""

echo "=========================================="
echo "  集成测试流水线完成"
echo "=========================================="
