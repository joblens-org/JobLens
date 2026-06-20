#!/bin/bash
# build-unified-rpm.sh — JobLens 统一 RPM 一站构建脚本
#
# 构建 joblens 统一 RPM 包（Core C++17 Agent + Python Trigger Gateway）。
# 参考 scripts/install-deps.sh 的依赖安装逻辑和统一打包规范。
#
# 用法:
#   ./scripts/build-unified-rpm.sh                 # 仅构建（默认）
#   ./scripts/build-unified-rpm.sh --install-deps   # 安装全部构建依赖，再构建
#   ./scripts/build-unified-rpm.sh --clean           # 清理构建产物
#   ./scripts/build-unified-rpm.sh --help            # 显示帮助
#
# 版本来源: CMakeLists.txt 的 project(JobLens VERSION X.Y.Z ...) 声明
# spec 路径: scripts/rpm/joblens-unified.spec
#
# 注意: 请在 Fedora/RHEL 环境（或兼容容器）中运行，不支持 apt/deb 系发行版。
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build-unified-rpm"

# —— 颜色定义 ——
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ============================================================================
# 阶段 1: 版本提取
#   从 CMakeLists.txt 提取统一版本号。
#   与 scripts/check-version-consistency.sh 使用相同的提取逻辑：
#   tr '\n' ' ' 合并跨行声明，grep -oP 提取 VERSION 后的数字。
# ============================================================================
extract_version() {
    local cmake_file="$PROJECT_ROOT/CMakeLists.txt"
    if [[ ! -f "$cmake_file" ]]; then
        log_error "CMakeLists.txt 不存在: $cmake_file"
        exit 1
    fi

    VERSION=$(tr '\n' ' ' < "$cmake_file" \
        | grep -oP 'project\(\s*JobLens\s+VERSION\s+\K[\d.]+' \
        | head -1)

    if [[ -z "$VERSION" ]]; then
        log_error "无法从 CMakeLists.txt 提取版本号"
        log_error "请确认 project() 声明格式: project(JobLens VERSION X.Y.Z ...)"
        exit 1
    fi

    log_info "JobLens 统一版本: $VERSION"
}

# ============================================================================
# 阶段 2: 依赖安装 (--install-deps)
#   1) 调用 scripts/install-deps.sh 安装 C++ 构建依赖（cmake, clang, bpftool 等）
#   2) 安装 RPM 构建工具和 Python 依赖（rpm-build, python3-pip 等）
# ============================================================================
install_deps() {
    log_info "===== 安装 C++ 构建依赖 ====="
    if [[ -f "$SCRIPT_DIR/install-deps.sh" ]]; then
        bash "$SCRIPT_DIR/install-deps.sh"
    else
        log_warn "scripts/install-deps.sh 不存在，跳过 C++ 依赖安装"
    fi

    echo ""
    log_info "===== 安装 RPM 构建工具和 Python 依赖 ====="

    if ! command -v dnf &>/dev/null && ! command -v yum &>/dev/null; then
        if command -v apt &>/dev/null; then
            log_warn "apt 环境不支持 RPM 构建，请在 Fedora/RHEL/AlmaLinux/Rocky 环境中运行"
            exit 1
        else
            log_warn "未检测到已知包管理器，跳过 RPM 工具安装"
        fi
        return
    fi

    # RHEL 系列启用 EPEL + CRB/PowerTools（参考 install-deps.sh 逻辑）
    if [[ -f /etc/redhat-release ]]; then
        if grep -qi 'Red Hat\|CentOS\|AlmaLinux\|Rocky' /etc/redhat-release 2>/dev/null; then
            log_info "检测到 RHEL 系列发行版，启用 EPEL 和 CRB/PowerTools..."
            sudo dnf install -y epel-release 2>/dev/null || true
            sudo dnf config-manager --set-enabled crb 2>/dev/null || \
                sudo dnf config-manager --set-enabled powertools 2>/dev/null || true
        fi
    fi

    log_info "安装: rpm-build rpmdevtools python3 python3-devel python3-pip python3-setuptools python3-wheel systemd-rpm-macros"
    sudo dnf install -y \
        rpm-build \
        rpmdevtools \
        python3 \
        python3-devel \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        systemd-rpm-macros

    log_info "构建依赖安装完成"
}

# ============================================================================
# 阶段 3: 准备 rpmbuild 目录树
#   使用 rpmdev-setuptree 或手动创建 ~/rpmbuild/{SOURCES,SPECS,RPMS,SRPMS,BUILD,BUILDROOT}
# ============================================================================
setup_rpmbuild() {
    log_info "准备 RPM 构建目录..."

    if [[ -d "$HOME/rpmbuild" ]]; then
        log_info "~/rpmbuild 目录已存在"
    else
        if command -v rpmdev-setuptree &>/dev/null; then
            rpmdev-setuptree
            log_info "通过 rpmdev-setuptree 创建 ~/rpmbuild 目录树"
        else
            mkdir -p ~/rpmbuild/{SOURCES,SPECS,RPMS,SRPMS,BUILD,BUILDROOT}
            log_info "手动创建 ~/rpmbuild 目录树"
        fi
    fi
}

# ============================================================================
# 阶段 4: 创建源码 tarball
#   打包整个项目，排除: build/ .git/ venv/ __pycache__ *.pyc *.egg-info
# ============================================================================
create_tarball() {
    log_info "创建源码包（排除 build/、.git/、venv/、__pycache__、*.pyc、*.egg-info）..."

    local tarball="/tmp/JobLens-Unified-${VERSION}-Source.tar.gz"
    rm -f "$tarball"

    # 使用临时目录创建带顶层目录的 tarball
    # 避免 tar --transform 的跨平台兼容性问题
    local tmpdir
    tmpdir=$(mktemp -d)
    local tarball_name="JobLens-${VERSION}"
    cp -r "$PROJECT_ROOT" "$tmpdir/$tarball_name"

    tar -czf "$tarball" \
        --exclude='build' \
        --exclude='build-unified-rpm' \
        --exclude='.git' \
        --exclude='venv' \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        --exclude='*.pyo' \
        --exclude='*.egg-info' \
        --exclude='*.swp' \
        -C "$tmpdir" "$tarball_name"

    rm -rf "$tmpdir"

    local tarball_size
    tarball_size=$(du -h "$tarball" | cut -f1)
    log_info "源码包: $tarball ($tarball_size)"
}

# ============================================================================
# 阶段 5: 准备 rpmbuild SOURCES
#   将源码 tarball 和 systemd service 文件复制到 ~/rpmbuild/SOURCES/
#
#   spec 中的 Source0 为 JobLens-%{version}.tar.gz（%{version} 由 spec 内 Version 标签决定）
#   因此 SOURCES 中的文件名必须精确匹配 spec 声明。
#
#   systemd unit 文件:
#     - joblens.service: 从模板 scripts/init/joblens.service.in 生成（替换 CMake 变量）
#     - joblens-trigger.service: 直接使用 trigger/ 中的预置文件
# ============================================================================
prepare_sources() {
    log_info "复制源码和 systemd unit 到 rpmbuild SOURCES..."

    # Source0: 源码 tarball — 必须匹配 spec 的 Source0 命名
    cp -f "/tmp/JobLens-Unified-${VERSION}-Source.tar.gz" \
       "$HOME/rpmbuild/SOURCES/JobLens-${VERSION}.tar.gz"
    log_info "  SOURCES/JobLens-${VERSION}.tar.gz"

    # Source1: joblens.service — 从 template 生成（参考 CMake configure_file 逻辑）
    local service_in="$PROJECT_ROOT/scripts/init/joblens.service.in"
    local service_out="$HOME/rpmbuild/SOURCES/joblens.service"

    if [[ -f "$service_in" ]]; then
        log_info "  从模板 scripts/init/joblens.service.in 生成 joblens.service..."
        sed -e 's|@CMAKE_INSTALL_FULL_BINDIR@|/usr/bin|g' \
            -e 's|@JOBLENS_SERVICE_ENV@|# Environment overrides (set via drop-in or override)|g' \
            "$service_in" > "$service_out"
        log_info "  SOURCES/joblens.service"
    else
        log_warn "  scripts/init/joblens.service.in 不存在，跳过 joblens.service"
    fi

    # Source2: joblens-trigger.service — 预置文件
    local trigger_service="$PROJECT_ROOT/trigger/joblens-trigger.service"
    if [[ -f "$trigger_service" ]]; then
        cp -f "$trigger_service" "$HOME/rpmbuild/SOURCES/joblens-trigger.service"
        log_info "  SOURCES/joblens-trigger.service"
    else
        log_warn "  trigger/joblens-trigger.service 不存在，跳过"
    fi
}

# ============================================================================
# 阶段 6: 构建 RPM
#   运行 rpmbuild -ba，失败时自动回退 --nodeps 重试。
#   传递 unified_version 宏给 spec（供未来 spec 中使用 %{unified_version}）。
# ============================================================================
build_rpm() {
    local spec_file="$PROJECT_ROOT/scripts/rpm/joblens-unified.spec"

    if [[ ! -f "$spec_file" ]]; then
        log_error "spec 文件不存在: $spec_file"
        exit 1
    fi

    log_info "开始 RPM 构建（rpmbuild -ba）..."
    echo ""

    if rpmbuild -ba \
        --define "unified_version ${VERSION}" \
        "$spec_file"; then
        log_info "rpmbuild 构建成功"
        return 0
    fi

    log_warn "=============================================="
    log_warn "标准构建失败，尝试跳过 RPM 依赖检查（--nodeps）"
    log_warn "=============================================="
    echo ""

    rpmbuild -ba --nodeps \
        --define "unified_version ${VERSION}" \
        "$spec_file"
}

# ============================================================================
# 阶段 7: 验证产物
#   确认 ~/rpmbuild/RPMS/x86_64/joblens-*.rpm 存在。
#   显示文件大小和安装命令。
# ============================================================================
verify_rpm() {
    log_info "验证 RPM 产物..."

    local rpm_file
    rpm_file=$(ls ~/rpmbuild/RPMS/x86_64/joblens-${VERSION}*.rpm 2>/dev/null | head -1)

    if [[ -z "$rpm_file" ]]; then
        log_error "未找到 RPM 产物（搜索路径: ~/rpmbuild/RPMS/x86_64/）"
        log_error "可用包列表:"
        find ~/rpmbuild/RPMS -name '*.rpm' 2>/dev/null || true

        echo ""
        log_error "可能的原因:"
        echo "  1. spec 文件中的 %%prep/%%build/%%install 段为 TODO 状态（未实现）"
        echo "  2. 源码 tarball 内容与 spec 期望不匹配"
        echo "  3. 构建依赖缺失，尝试: $0 --install-deps"
        exit 1
    fi

    local rpm_size
    rpm_size=$(ls -lh "$rpm_file" | awk '{print $5}')

    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                    RPM 构建成功！                            ║${NC}"
    echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║${NC}  产物: $rpm_file"
    echo -e "${GREEN}║${NC}  大小: $rpm_size"
    echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║${NC}  安装: sudo rpm -ivh $rpm_file"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# ============================================================================
# 清理 (--clean)
#   清除统一 RPM 临时产物:
#     - /tmp/JobLens-Unified-*-Source.tar.gz   (temp 源码包)
#     - build-unified-rpm/                      (本地构建目录)
#     - ~/rpmbuild/SOURCES/JobLens-*.tar.gz    (rpmbuild SOURCES 中的源码包)
# ============================================================================
clean() {
    log_info "清理统一 RPM 构建产物..."

    # 临时目录中的源码 tarball
    local cleaned=0
    for f in /tmp/JobLens-Unified-*-Source.tar.gz; do
        if [[ -f "$f" ]]; then
            rm -f "$f"
            log_info "  删除: $f"
            cleaned=1
        fi
    done

    # 本地构建目录
    if [[ -d "$BUILD_DIR" ]]; then
        rm -rf "$BUILD_DIR"
        log_info "  删除: $BUILD_DIR"
        cleaned=1
    fi

    # rpmbuild SOURCES 中的统一包源码
    for f in "$HOME/rpmbuild/SOURCES/JobLens-"*.tar.gz; do
        if [[ -f "$f" ]]; then
            rm -f "$f"
            log_info "  删除: $f"
            cleaned=1
        fi
    done

    if [[ "$cleaned" -eq 0 ]]; then
        log_info "没有需要清理的文件"
    else
        log_info "清理完成"
    fi
}

# ============================================================================
# 帮助 (--help / -h)
#   输出所有选项说明和用法示例。
# ============================================================================
usage() {
    echo "用法: $0 [选项]"
    echo ""
    echo "JobLens 统一 RPM 一站构建脚本"
    echo "构建包含 Core C++ Agent + Python Trigger Gateway 的统一 RPM 包。"
    echo ""
    echo "选项:"
    echo "  --install-deps   安装全部构建依赖（需要 sudo 权限）"
    echo "                   包括:"
    echo "                     - C++ 编译依赖（通过 scripts/install-deps.sh）"
    echo "                     - RPM 构建工具（rpm-build, rpmdevtools）"
    echo "                     - Python 工具链（python3-devel, pip, setuptools, wheel）"
    echo "                     - systemd RPM 宏（systemd-rpm-macros）"
    echo ""
    echo "  --clean          清理构建产物"
    echo "                   清除:"
    echo "                     - /tmp/JobLens-Unified-*-Source.tar.gz"
    echo "                     - build-unified-rpm/ 目录"
    echo "                     - ~/rpmbuild/SOURCES/JobLens-*.tar.gz"
    echo ""
    echo "  -h, --help       显示此帮助信息"
    echo ""
    echo "构建流程（默认行为）:"
    echo "  1. 从 CMakeLists.txt 提取统一版本号"
    echo "  2. 准备 ~/rpmbuild/ 目录树"
    echo "  3. 创建源码 tarball（排除 build/、.git/、venv/ 等）"
    echo "  4. 复制源码和 systemd unit 到 rpmbuild SOURCES"
    echo "  5. 运行 rpmbuild -ba scripts/rpm/joblens-unified.spec"
    echo "  6. 验证 RPM 产物（~/rpmbuild/RPMS/x86_64/joblens-*.rpm）"
    echo ""
    echo "版本来源:"
    echo "  CMakeLists.txt 中的 project(JobLens VERSION X.Y.Z ...) 声明"
    echo "  （跨行声明通过 tr '\\n' ' ' 合并后解析）"
    echo ""
    echo "示例:"
    echo "  $0                          # 仅构建（依赖已安装）"
    echo "  $0 --install-deps           # 首次运行：安装依赖 + 构建"
    echo "  $0 --clean                  # 清理构建产物"
    echo ""
    echo "环境要求:"
    echo "  - Fedora / RHEL / AlmaLinux / Rocky Linux（或兼容容器）"
    echo "  - sudo 权限（--install-deps 时需要）"
}

# ============================================================================
# 主流程
# ============================================================================
main() {
    local do_install_deps=false
    local do_clean=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            --install-deps)
                do_install_deps=true
                shift
                ;;
            --clean)
                do_clean=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "未知参数: $1"
                echo ""
                usage
                exit 1
                ;;
        esac
    done

    # --clean 独立执行（不与构建混合）
    if [[ "$do_clean" == true ]]; then
        clean
        exit 0
    fi

    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║     JobLens 统一 RPM 构建                        ║${NC}"
    echo -e "${GREEN}║     Core (C++17) + Trigger (Python)              ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════╝${NC}"
    echo ""

    extract_version
    [[ "$do_install_deps" == true ]] && install_deps
    setup_rpmbuild
    create_tarball
    prepare_sources
    build_rpm
    verify_rpm

    echo ""
    log_info "===== JobLens 统一 RPM 构建完成 ====="
}

main "$@"
