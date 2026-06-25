#!/bin/bash
# 构建 joblens-trigger RPM 包
#
# 采用 Python virtualenv 方案：所有 Python 依赖安装到
# /usr/lib/joblens-trigger/venv/，与系统 Python 完全隔离。
# spec 仅需 python3 + python3-pip 作为构建依赖。
#
# 用法:
#   ./scripts/build-trigger-rpm.sh              # 仅构建
#   ./scripts/build-trigger-rpm.sh --install-deps # 先安装构建依赖，再构建
#   ./scripts/build-trigger-rpm.sh --clean         # 清理构建产物
set -e

# Docker 容器内以 root 运行无需提权 (PAM 不可用)
if [ "$(id -u)" -eq 0 ]; then
    sudo() { "$@"; }
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TRIGGER_DIR="$PROJECT_ROOT/trigger"
BUILD_DIR="$PROJECT_ROOT/build-trigger-rpm"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ---------- 版本提取 ----------
extract_version() {
    local ver_file="$TRIGGER_DIR/version.py"
    if [[ ! -f "$ver_file" ]]; then
        log_error "版本文件不存在: $ver_file"
        exit 1
    fi
    TRIGGER_VERSION=$(python3 -c "
import runpy, os, sys
os.chdir('$TRIGGER_DIR')
ns = runpy.run_path('version.py')
print(ns['__version__'])
" 2>/dev/null)
    if [[ -z "$TRIGGER_VERSION" ]]; then
        log_error "无法从 version.py 提取版本号"
        exit 1
    fi
    log_info "Trigger 版本: $TRIGGER_VERSION"
}

# ---------- 依赖安装 ----------
install_deps() {
    log_info "安装构建依赖..."

    if ! command -v dnf &>/dev/null && ! command -v yum &>/dev/null; then
        if command -v apt &>/dev/null; then
            log_warn "apt 环境不支持 RPM 构建，请在 Fedora/RHEL 容器中运行"
            exit 1
        else
            log_warn "未检测到已知包管理器，跳过依赖安装"
        fi
        return
    fi

    if grep -qi 'Red Hat\|CentOS\|AlmaLinux\|Rocky' /etc/redhat-release 2>/dev/null; then
        log_info "检测到 RHEL 系列，启用 EPEL 和 CRB..."
        sudo dnf install -y epel-release 2>/dev/null || true
        sudo dnf config-manager --set-enabled crb 2>/dev/null || \
        sudo dnf config-manager --set-enabled powertools 2>/dev/null || true
    fi

    log_info "安装 RPM 构建工具..."
    sudo dnf install -y rpm-build rpmdevtools python3 python3-devel systemd-rpm-macros \
        python3-setuptools python3-wheel python3-pip

    log_info "安装 spec 构建依赖（python3, python3-pip）..."
    if ! sudo dnf builddep -y "$PROJECT_ROOT/trigger/joblens-trigger.spec" 2>&1; then
        log_warn "=============================================="
        log_warn "dnf builddep 失败！将尝试以 --nodeps 继续构建"
        log_warn "=============================================="
    fi
}

# ---------- 构建环境准备 ----------
setup_rpmbuild() {
    log_info "准备 RPM 构建目录..."
    if [[ ! -d ~/rpmbuild ]]; then
        rpmdev-setuptree 2>/dev/null || mkdir -p ~/rpmbuild/{SOURCES,SPECS,RPMS,SRPMS,BUILD,BUILDROOT}
    fi
}

# ---------- 源码包 ----------
create_tarball() {
    log_info "创建源码包..."
    local tarball="/tmp/JobLens-Trigger-${TRIGGER_VERSION}-Source.tar.gz"
    rm -f "$tarball"

    tar -czf "$tarball" \
        --exclude='venv' \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        --exclude='*.pyo' \
        --exclude='*.egg-info' \
        --exclude='*.swp' \
        -C "$PROJECT_ROOT" trigger

    log_info "源码包: $tarball ($(du -h "$tarball" | cut -f1))"
}

# ---------- 复制到 rpmbuild ----------
prepare_sources() {
    log_info "复制文件到 rpmbuild..."
    cp -f "/tmp/JobLens-Trigger-${TRIGGER_VERSION}-Source.tar.gz" ~/rpmbuild/SOURCES/
    cp -f "$TRIGGER_DIR/joblens-trigger.service" ~/rpmbuild/SOURCES/
    cp -f "$TRIGGER_DIR/joblens-trigger.spec" ~/rpmbuild/SPECS/
}

# ---------- 构建 ----------
build_rpm() {
    log_info "开始 RPM 构建..."
    if rpmbuild -ba \
        --define "trigger_version ${TRIGGER_VERSION}" \
        ~/rpmbuild/SPECS/joblens-trigger.spec; then
        return 0
    fi

    log_warn "标准构建失败，尝试跳过 RPM 依赖检查（--nodeps）..."
    rpmbuild -ba --nodeps \
        --define "trigger_version ${TRIGGER_VERSION}" \
        ~/rpmbuild/SPECS/joblens-trigger.spec
}

# ---------- 验证 ----------
verify_rpm() {
    local rpm_file
    rpm_file=$(ls ~/rpmbuild/RPMS/x86_64/joblens-trigger-${TRIGGER_VERSION}*.rpm 2>/dev/null | head -1)
    if [[ -z "$rpm_file" ]]; then
        log_error "未找到 RPM 产物（搜索路径: ~/rpmbuild/RPMS/x86_64/）"
        log_error "可用包列表:"
        find ~/rpmbuild/RPMS -name '*.rpm' 2>/dev/null || true
        exit 1
    fi

    echo ""
    echo "=== RPM 文件列表（前 30 项） ==="
    rpm -qpl "$rpm_file" | head -30

    echo ""
    echo "=== 关键产物检查 ==="
    rpm -qpl "$rpm_file" | grep -q 'joblens-trigger.service' && echo -e "${GREEN}✓${NC} systemd unit" || { echo -e "${RED}✗${NC} 缺少 systemd unit"; exit 1; }
    rpm -qpl "$rpm_file" | grep -q '/etc/JobLens/trigger/gunicorn.conf.py' && echo -e "${GREEN}✓${NC} gunicorn 配置" || { echo -e "${RED}✗${NC} 缺少 gunicorn.conf.py"; exit 1; }
    rpm -qpl "$rpm_file" | grep -q '/venv/bin/gunicorn' && echo -e "${GREEN}✓${NC} venv gunicorn" || { echo -e "${RED}✗${NC} 缺少 venv/bin/gunicorn"; exit 1; }
    rpm -qpl "$rpm_file" | grep -q '/site-packages/flask/__init__' && echo -e "${GREEN}✓${NC} Flask (venv)" || { echo -e "${RED}✗${NC} 缺少 Flask"; exit 1; }
    rpm -qpl "$rpm_file" | grep -q '/site-packages/trigger/app' && echo -e "${GREEN}✓${NC} trigger 包 (venv)" || { echo -e "${RED}✗${NC} 缺少 trigger 包"; exit 1; }

    echo ""
    echo "=== RPM 信息 ==="
    echo "文件: $rpm_file"
    echo "大小: $(ls -lh "$rpm_file" | awk '{print $5}')"
    echo "文件数: $(rpm -qpl "$rpm_file" | wc -l)"

    echo ""
    echo "=== RPM 依赖 ==="
    rpm -qp --requires "$rpm_file" | sort

    echo ""
    echo "=== venv 内 Python 包 ==="
    rpm -qpl "$rpm_file" | grep 'site-packages/[^/]*/__init__\.py$' | sed 's|.*site-packages/||;s|/__init__.py||' | sort | head -15
    echo "    ... ($(rpm -qpl "$rpm_file" | grep 'site-packages/[^/]*/__init__\.py$' | wc -l) 个包总计)"

    log_info "构建成功！安装命令:"
    echo "  sudo rpm -ivh $rpm_file"
}

# ---------- 清理 ----------
clean() {
    log_info "清理构建产物..."
    rm -f /tmp/JobLens-Trigger-*-Source.tar.gz
    rm -rf "$BUILD_DIR"
    log_info "清理完成"
}

# ---------- 帮助 ----------
usage() {
    echo "用法: $0 [选项]"
    echo "选项:"
    echo "  --install-deps   安装构建依赖（需要 sudo）"
    echo "  --clean          清理构建产物"
    echo "  -h, --help       显示此帮助"
    echo ""
    echo "示例:"
    echo "  $0                          # 仅构建"
    echo "  $0 --install-deps           # 安装依赖 + 构建"
    echo "  $0 --clean                  # 清理"
}

# ---------- 主流程 ----------
main() {
    local do_install_deps=false
    local do_clean=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            --install-deps) do_install_deps=true; shift ;;
            --clean)        do_clean=true; shift ;;
            -h|--help)      usage; exit 0 ;;
            *)              log_error "未知参数: $1"; usage; exit 1 ;;
        esac
    done

    if [[ "$do_clean" == true ]]; then
        clean
        exit 0
    fi

    echo ""
    log_info "===== joblens-trigger RPM 构建 ====="

    extract_version
    [[ "$do_install_deps" == true ]] && install_deps
    setup_rpmbuild
    create_tarball
    prepare_sources
    build_rpm
    verify_rpm
}

main "$@"
