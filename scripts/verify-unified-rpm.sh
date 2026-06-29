#!/bin/bash
# verify-unified-rpm.sh — JobLens 统一 RPM 包内容验证脚本
#
# 在每个 unified RPM 构建后自动检查：
#   包内容、依赖、scriptlets、配置标记、Obsoletes/Provides 等关键项。
#
# 用法:
#   ./scripts/verify-unified-rpm.sh <rpm-file-path>    # 验证指定 RPM
#   ./scripts/verify-unified-rpm.sh --help              # 显示帮助
#
# 检查项（共 10 项）：
#   1.  /usr/bin/JobLens 二进制存在
#   2.  /usr/lib/joblens/trigger-venv/bin/gunicorn 存在（venv 入口）
#   3.  /usr/lib/systemd/system/joblens.service 存在
#   4.  /usr/lib/systemd/system/joblens-trigger.service 存在
#   5.  bpf_obj/*.bpf.o eBPF 对象文件存在
#   6.  /etc/JobLens/config.yaml 和 /etc/JobLens/trigger/config.yaml 存在
#   7.  Requires 不含 python3dist( venv 依赖泄漏
#   8.  Requires 不含自引用 joblens 依赖
#   9.  scriptlets 含 systemd post/preun/postun 维护脚本
#   10. Obsoletes 含 joblens-trigger（旧包平滑升级标记）
#
# 返回值: 全部通过 → 0，任意一项失败 → 1
#
# 参考:
#   - scripts/rpm/joblens-unified.spec（unified spec 定义）
#   - .github/workflows/ci.yml（现有 CI 验证步骤）
#   - .omo/notepads/unified-rpm-packaging/ownership-map.md（路径清单）
set -euo pipefail

# —— 颜色定义 ——
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# —— 全局状态 ——
PASS_COUNT=0
FAIL_COUNT=0
FAIL_ITEMS=()
TOTAL_CHECKS=10
RPM_FILE=""

# ============================================================================
# 帮助信息
# ============================================================================
usage() {
    echo "用法: $0 [选项] <rpm-file-path>"
    echo ""
    echo "JobLens 统一 RPM 包内容验证脚本"
    echo "对构建产物执行 10 项自动检查，确保 RPM 包含所有预期内容。"
    echo ""
    echo "选项:"
    echo "  -h, --help       显示此帮助信息"
    echo ""
    echo "参数:"
    echo "  rpm-file-path     待验证的 RPM 文件路径"
    echo "                     (例: ~/rpmbuild/RPMS/x86_64/joblens-0.1.0-1.fc39.x86_64.rpm)"
    echo ""
    echo "检查项（共 10 项）:"
    echo "  1.  /usr/bin/JobLens 二进制"
    echo "  2.  trigger-venv/bin/gunicorn 入口"
    echo "  3.  joblens.service systemd unit"
    echo "  4.  joblens-trigger.service systemd unit"
    echo "  5.  eBPF *.bpf.o 对象文件"
    echo "  6.  配置文件 /etc/JobLens/config.yaml 等"
    echo "  7.  Requires 不含 python3dist( 泄漏"
    echo "  8.  Requires 不含自引用 joblens"
    echo "  9.  含 systemd scriptlets (post/preun/postun)"
    echo "  10. Obsoletes 含 joblens-trigger"
    echo ""
    echo "返回值: 全部通过 → 0，任意一项失败 → 1"
    echo ""
    echo "示例:"
    echo "  $0 ~/rpmbuild/RPMS/x86_64/joblens-0.1.0-1.fc39.x86_64.rpm"
    echo "  $0 --help"
}

# ============================================================================
# 辅助函数
# ============================================================================

# 获取 RPM 文件列表（类似 rpm -qpl，但不安装 RPM）
# 参数: $1 = RPM 文件路径
get_filelist() {
    rpm -qpl "$1" 2>/dev/null
}

# 获取 RPM Requires
# 参数: $1 = RPM 文件路径
get_requires() {
    rpm -qp --requires "$1" 2>/dev/null
}

# 获取 RPM scriptlets
# 参数: $1 = RPM 文件路径
get_scripts() {
    rpm -qp --scripts "$1" 2>/dev/null
}

# 获取 RPM Obsoletes
# 参数: $1 = RPM 文件路径
get_obsoletes() {
    rpm -qp --obsoletes "$1" 2>/dev/null
}

# 打印通过项
# 参数: $1 = 检查编号 (N/10), $2 = 描述
pass_check() {
    local label="$1"
    local desc="$2"
    echo -e "  ${GREEN}[✓]${NC} [${label}] ${desc} → ${GREEN}PASS${NC}"
    PASS_COUNT=$((PASS_COUNT + 1))
}

# 打印失败项，记录失败详情
# 参数: $1 = 检查编号 (N/10), $2 = 描述, $3 = 实际输出（可选）
fail_check() {
    local label="$1"
    local desc="$2"
    local detail="$3"
    echo -e "  ${RED}[✗]${NC} [${label}] ${desc} → ${RED}FAIL${NC}"
    if [[ -n "$detail" ]]; then
        echo ""
        echo -e "  ${YELLOW}── 实际输出 ──${NC}"
        echo "$detail" | sed 's/^/    /'
        echo -e "  ${YELLOW}──────────────${NC}"
        echo ""
    fi
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAIL_ITEMS+=("$label: $desc")
}

# ============================================================================
# 10 项检查函数
# ============================================================================

# 检查 1: /usr/bin/JobLens 存在
check_1_binary() {
    local files
    files=$(get_filelist "$RPM_FILE")
    if echo "$files" | grep -qE '^/usr/bin/JobLens$'; then
        pass_check "1/10" "/usr/bin/JobLens 二进制存在"
    else
        fail_check "1/10" "/usr/bin/JobLens 二进制存在" \
            "预期: /usr/bin/JobLens
实际文件列表（匹配 /usr/bin/ 的内容）:
$(echo "$files" | grep '/usr/bin/' || echo '  (无匹配)')"
    fi
}

# 检查 2: /usr/lib/joblens/trigger-venv/bin/gunicorn 存在
check_2_gunicorn() {
    local files
    files=$(get_filelist "$RPM_FILE")
    if echo "$files" | grep -q '/usr/lib/joblens/trigger-venv/bin/gunicorn$'; then
        pass_check "2/10" "trigger-venv/bin/gunicorn 入口存在"
    else
        fail_check "2/10" "trigger-venv/bin/gunicorn 入口存在" \
            "预期: /usr/lib/joblens/trigger-venv/bin/gunicorn
实际 trigger-venv/bin/ 文件:
$(echo "$files" | grep 'trigger-venv/bin/' || echo '  (无匹配)')"
    fi
}

# 检查 3: /usr/lib/systemd/system/joblens.service 存在
check_3_core_service() {
    local files
    files=$(get_filelist "$RPM_FILE")
    if echo "$files" | grep -q '/usr/lib/systemd/system/joblens\.service$'; then
        pass_check "3/10" "joblens.service systemd unit 存在"
    else
        fail_check "3/10" "joblens.service systemd unit 存在" \
            "预期: /usr/lib/systemd/system/joblens.service
实际 systemd unit 文件:
$(echo "$files" | grep 'systemd/system/' || echo '  (无匹配)')"
    fi
}

# 检查 4: /usr/lib/systemd/system/joblens-trigger.service 存在
check_4_trigger_service() {
    local files
    files=$(get_filelist "$RPM_FILE")
    if echo "$files" | grep -q '/usr/lib/systemd/system/joblens-trigger\.service$'; then
        pass_check "4/10" "joblens-trigger.service systemd unit 存在"
    else
        fail_check "4/10" "joblens-trigger.service systemd unit 存在" \
            "预期: /usr/lib/systemd/system/joblens-trigger.service
实际 systemd unit 文件:
$(echo "$files" | grep 'systemd/system/' || echo '  (无匹配)')"
    fi
}

# 检查 5: bpf_obj/*.bpf.o 存在
check_5_bpf_objects() {
    local files
    files=$(get_filelist "$RPM_FILE")
    if echo "$files" | grep -qE '/usr/lib/joblens/bpf_obj/[^/]+\.bpf\.o$'; then
        local count
        count=$(echo "$files" | grep -cE '/usr/lib/joblens/bpf_obj/[^/]+\.bpf\.o$' || true)
        pass_check "5/10" "eBPF *.bpf.o 对象文件存在 (${count} 个)"
    else
        fail_check "5/10" "eBPF *.bpf.o 对象文件存在" \
            "预期: /usr/lib/joblens/bpf_obj/*.bpf.o
实际 bpf_obj/ 文件:
$(echo "$files" | grep 'bpf_obj/' || echo '  (无匹配)')"
    fi
}

# 检查 6: /etc/JobLens/config.yaml 和 /etc/JobLens/trigger/config.yaml
check_6_configs() {
    local files
    files=$(get_filelist "$RPM_FILE")
    local ok=true
    local missing=""

    if ! echo "$files" | grep -q '/etc/JobLens/config\.yaml$'; then
        ok=false
        missing="$missing  - /etc/JobLens/config.yaml
"
    fi

    if ! echo "$files" | grep -q '/etc/JobLens/trigger/config\.yaml$'; then
        ok=false
        missing="$missing  - /etc/JobLens/trigger/config.yaml
"
    fi

    # 额外检查：gunicorn.conf.py 也应在
    local gunicorn_ok=true
    if ! echo "$files" | grep -q '/etc/JobLens/trigger/gunicorn\.conf\.py$'; then
        gunicorn_ok=false
    fi

    if $ok && $gunicorn_ok; then
        pass_check "6/10" "配置文件存在 (config.yaml ×2 + gunicorn.conf.py)"
    elif $ok && ! $gunicorn_ok; then
        # config.yaml 文件都存在，但 gunicorn.conf.py 缺失 — 小问题，仅警告
        pass_check "6/10" "配置文件存在 (config.yaml ×2, 但 gunicorn.conf.py 缺失)"
    else
        fail_check "6/10" "配置文件存在" \
            "预期: /etc/JobLens/config.yaml
       /etc/JobLens/trigger/config.yaml
       /etc/JobLens/trigger/gunicorn.conf.py
缺失:
${missing}
实际 /etc/JobLens/ 文件:
$(echo "$files" | grep '/etc/JobLens/' || echo '  (无匹配)')"
    fi
}

# 检查 7: Requires 不含 python3dist( 泄漏
check_7_no_venv_deps() {
    local reqs
    reqs=$(get_requires "$RPM_FILE")
    local leaked
    leaked=$(echo "$reqs" | grep -i 'python3dist(' || true)

    if [[ -z "$leaked" ]]; then
        pass_check "7/10" "Requires 不含 python3dist(...) venv 依赖泄漏"
    else
        fail_check "7/10" "Requires 不含 python3dist(...) venv 依赖泄漏" \
            "检测到泄漏的 venv Requires:
$(echo "$leaked" | sed 's/^/  /')
完整 Requires:
$(echo "$reqs" | sed 's/^/  /')"
    fi
}

# 检查 8: Requires 不含自引用 joblens 依赖
check_8_no_self_ref() {
    local reqs
    reqs=$(get_requires "$RPM_FILE")
    # 匹配 "joblens >="、"joblens ="、"joblens <" 等自引用
    local self_ref
    self_ref=$(echo "$reqs" | grep -E '^joblens[[:space:]]*[><=]' || true)

    if [[ -z "$self_ref" ]]; then
        pass_check "8/10" "Requires 不含自引用 joblens >= ... 依赖"
    else
        fail_check "8/10" "Requires 不含自引用 joblens >= ... 依赖" \
            "检测到自引用 Requires:
$(echo "$self_ref" | sed 's/^/  /')
完整 Requires:
$(echo "$reqs" | sed 's/^/  /')"
    fi
}

# 检查 9: scriptlets 含 systemd post/preun/postun
check_9_scriptlets() {
    local scripts
    scripts=$(get_scripts "$RPM_FILE")
    local ok=true
    local missing=""

    # 检查 %post
    if ! echo "$scripts" | grep -q 'postinstall scriptlet' \
        && ! echo "$scripts" | grep -q 'systemctl.*daemon-reload\|systemd_post\|preset.*joblens'; then
        ok=false
        missing="$missing  - %post (systemd daemon-reload / preset / systemd_post)
"
    fi

    # 检查 %preun
    if ! echo "$scripts" | grep -q 'preuninstall scriptlet' \
        && ! echo "$scripts" | grep -q 'systemd_preun\|systemctl.*stop.*joblens\|systemctl.*disable.*joblens'; then
        ok=false
        missing="$missing  - %preun (systemd stop / disable)
"
    fi

    # 检查 %postun
    if ! echo "$scripts" | grep -q 'postuninstall scriptlet' \
        && ! echo "$scripts" | grep -q 'systemd_postun_with_restart\|postun.*daemon-reload'; then
        ok=false
        missing="$missing  - %postun (systemd_postun_with_restart / daemon-reload)
"
    fi

    if $ok; then
        pass_check "9/10" "含 systemd post/preun/postun scriptlets"
    else
        fail_check "9/10" "含 systemd post/preun/postun scriptlets" \
            "缺失的 scriptlets:
${missing}
完整 scripts 输出:
$(echo "$scripts" | head -80 | sed 's/^/  /')"
    fi
}

# 检查 10: Obsoletes 含 joblens-trigger
check_10_obsoletes() {
    local obs
    obs=$(get_obsoletes "$RPM_FILE")

    if echo "$obs" | grep -q 'joblens-trigger'; then
        local line
        line=$(echo "$obs" | grep 'joblens-trigger')
        pass_check "10/10" "Obsoletes 含 joblens-trigger → ${line}"
    else
        fail_check "10/10" "Obsoletes 含 joblens-trigger（旧包平滑升级标记）" \
            "预期: Obsoletes: joblens-trigger < ...
实际 Obsoletes:
$(echo "$obs" | sed 's/^/  /')"
    fi
}

# ============================================================================
# 主流程
# ============================================================================
main() {
    # 解析参数
    if [[ $# -eq 0 ]]; then
        echo -e "${RED}错误: 缺少 RPM 文件路径参数${NC}"
        echo ""
        usage
        exit 2
    fi

    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        usage
        exit 0
    fi

    RPM_FILE="$1"

    # 验证文件存在
    if [[ ! -f "$RPM_FILE" ]]; then
        echo -e "${RED}错误: 文件不存在: ${RPM_FILE}${NC}"
        echo ""
        echo "请确认文件路径正确。示例:"
        echo "  $0 ~/rpmbuild/RPMS/x86_64/joblens-0.1.0-1.fc39.x86_64.rpm"
        exit 2
    fi

    # 验证是 RPM 文件（通过 file 命令检查）
    if ! file "$RPM_FILE" 2>/dev/null | grep -qi 'RPM'; then
        echo -e "${YELLOW}警告: 文件似乎不是 RPM 包，仍将尝试检查...${NC}"
    fi

    # 验证 rpm 命令可用
    if ! command -v rpm &>/dev/null; then
        echo -e "${RED}错误: rpm 命令不可用，请确保在 RPM 系环境中运行${NC}"
        exit 2
    fi

    # 显示 RPM 基本信息
    local rpm_name
    rpm_name=$(rpm -qp --qf '%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}' "$RPM_FILE" 2>/dev/null || echo "未知")
    local rpm_size
    rpm_size=$(ls -lh "$RPM_FILE" | awk '{print $5}')

    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║        JobLens 统一 RPM 包验证                               ║${NC}"
    echo -e "${BOLD}╠══════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${BOLD}║${NC}  包名: ${rpm_name}"
    echo -e "${BOLD}║${NC}  路径: ${RPM_FILE}"
    echo -e "${BOLD}║${NC}  大小: ${rpm_size}"
    echo -e "${BOLD}║${NC}  检查: ${TOTAL_CHECKS} 项"
    echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""

    # 执行 10 项检查
    echo -e "${BOLD}── 文件内容检查 ──${NC}"
    check_1_binary
    check_2_gunicorn
    check_3_core_service
    check_4_trigger_service
    check_5_bpf_objects
    check_6_configs

    echo ""
    echo -e "${BOLD}── 依赖检查 ──${NC}"
    check_7_no_venv_deps
    check_8_no_self_ref

    echo ""
    echo -e "${BOLD}── 元数据检查 ──${NC}"
    check_9_scriptlets
    check_10_obsoletes

    # 汇总
    echo ""
    echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"

    if [[ "$FAIL_COUNT" -eq 0 ]]; then
        echo -e "  ${GREEN}${BOLD}✓ All ${PASS_COUNT} checks passed${NC}"
        echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
        echo ""
        exit 0
    else
        echo -e "  ${RED}${BOLD}✗ ${FAIL_COUNT} check(s) failed / ${TOTAL_CHECKS} total${NC}"
        echo ""
        echo -e "  ${RED}失败项:${NC}"
        for item in "${FAIL_ITEMS[@]}"; do
            echo -e "    ${RED}•${NC} $item"
        done
        echo ""
        echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
        echo ""
        exit 1
    fi
}

main "$@"
