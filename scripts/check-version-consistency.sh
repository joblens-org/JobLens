#!/bin/bash
# ============================================================
# check-version-consistency.sh — 构建期版本一致性校验
#
# 校验 trigger/version.py 与 CMakeLists.txt 中 project() 声明的版本号一致。
# 版本不一致时以非零退出码退出，阻断构建。
#
# 用法:
#   ./scripts/check-version-consistency.sh          # 校验版本一致性
#   ./scripts/check-version-consistency.sh --help   # 显示此帮助
#
# 被 unified build script 和 CI workflow 调用。
# ============================================================

set -euo pipefail

# —— 颜色定义 ——
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# —— 路径计算：脚本所在目录的父目录即项目根 ——
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

CMAKE_FILE="${PROJECT_ROOT}/CMakeLists.txt"
VERSION_PY="${PROJECT_ROOT}/trigger/version.py"

# —— --help ——
show_help() {
    echo "用法: $(basename "$0") [--help]"
    echo ""
    echo "校验 trigger/version.py 与 CMakeLists.txt 中 project() 声明的版本号一致。"
    echo ""
    echo "选项:"
    echo "  --help    显示此帮助信息"
    echo ""
    echo "版本不一致时以非零退出码退出，适用于 CI / 构建脚本。"
    echo ""
    echo "版本来源:"
    echo "  CMakeLists.txt   → project(JobLens VERSION X.Y.Z ...)"
    echo "  trigger/version.py → __version__ = \"X.Y.Z\""
    exit 0
}

if [[ "${1:-}" == "--help" ]]; then
    show_help
fi

# —— 提取 CMakeLists.txt 版本 ——
# project() 声明跨两行，用 tr 合并后再 grep
CMAKE_VERSION=$(tr '\n' ' ' < "$CMAKE_FILE" | grep -oP 'project\(\s*JobLens\s+VERSION\s+\K[\d.]+' | head -1)
if [[ -z "$CMAKE_VERSION" ]]; then
    echo -e "${RED}✗ 无法从 CMakeLists.txt 提取版本号${NC}"
    exit 1
fi

# —— 提取 trigger/version.py 版本 ——
PY_VERSION=$(grep -oP '__version__\s*=\s*"\K[\d.]+(?=")' "$VERSION_PY" | head -1)
if [[ -z "$PY_VERSION" ]]; then
    echo -e "${RED}✗ 无法从 trigger/version.py 提取版本号${NC}"
    exit 1
fi

# —— 对比 ——
if [[ "$CMAKE_VERSION" == "$PY_VERSION" ]]; then
    echo -e "${GREEN}✓ 版本一致: CMakeLists.txt → ${CMAKE_VERSION}, trigger/version.py → ${PY_VERSION}${NC}"
    exit 0
else
    echo -e "${RED}✗ 版本不一致${NC}"
    echo "  CMakeLists.txt   → ${CMAKE_VERSION}"
    echo "  trigger/version.py → ${PY_VERSION}"
    exit 1
fi
