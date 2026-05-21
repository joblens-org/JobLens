#!/bin/bash
#   Copyright 2026 - 2026 wzycc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#!/bin/bash
set -e

# JobLens RPM包构建脚本
# 用法：./scripts/build-rpm.sh [clean]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build-rpm"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在
check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "命令 '$1' 未找到，请安装后重试"
        return 1
    fi
}

# 检查sudo访问权限
check_sudo_access() {
    if [ "$(id -u)" -eq 0 ]; then
        echo ""
        return 0
    fi
    
    # 检查是否有免密码sudo权限
    if sudo -n true 2>/dev/null; then
        echo "sudo"
        return 0
    else
        echo "NEED_PASSWORD"
        return 1
    fi
}

# 检查构建依赖
check_dependencies() {
    # 检查sudo访问权限
    local SUDO_CMD=$(check_sudo_access)
    if [ "$SUDO_CMD" = "NEED_PASSWORD" ]; then
        log_warn "没有免密码sudo权限，跳过依赖安装"
        log_warn "请手动运行: sudo dnf install -y rpm-build"
        SUDO_CMD=""  # 设置为空，跳过依赖安装
    elif [ -z "$SUDO_CMD" ]; then
        log_info "当前为root用户，跳过sudo"
    else
        log_info "检测到免密码sudo权限"
    fi
    
    log_info "检查构建依赖..."
    
    # 基本构建工具
    check_command cmake || return 1
    check_command make || return 1
    check_command g++ || return 1
    check_command clang || return 1
    check_command rsync || return 1
    
    # RPM构建工具
    check_command rpmbuild || {
        log_warn "rpmbuild 未找到，尝试安装..."
        
        # 检查是否有sudo权限
        if [ "$SUDO_CMD" = "NEED_PASSWORD" ] || [ -z "$SUDO_CMD" ]; then
            log_error "没有安装rpmbuild且无sudo权限，请手动安装:"
            if command -v dnf &> /dev/null; then
                log_error "  sudo dnf install -y rpm-build"
            elif command -v yum &> /dev/null; then
                log_error "  sudo yum install -y rpm-build"
            elif command -v apt &> /dev/null; then
                log_error "  sudo apt install -y rpm"
            fi
            return 1
        fi
        
        if command -v dnf &> /dev/null; then
            $SUDO_CMD dnf install -y rpm-build || return 1
        elif command -v yum &> /dev/null; then
            $SUDO_CMD yum install -y rpm-build || return 1
        elif command -v apt &> /dev/null; then
            $SUDO_CMD apt install -y rpm || return 1
        else
            log_error "无法安装 rpmbuild，请手动安装"
            return 1
        fi
    }
    
    # 检查RPM构建目录结构
    if [[ ! -d ~/rpmbuild ]]; then
        log_info "创建RPM构建目录结构..."
        rpmdev-setuptree 2>/dev/null || {
            mkdir -p ~/rpmbuild/{SOURCES,SPECS,RPMS,SRPMS,BUILD,BUILDROOT}
        }
    fi
    
    log_info "所有依赖检查通过"
    return 0
}

# 清理构建目录
clean_build() {
    log_info "清理构建目录..."
    rm -rf "$BUILD_DIR"
    rm -rf "$PROJECT_ROOT/build"
    rm -f /tmp/JobLens-*-Source.tar.gz
    rm -f /tmp/JobLens-Trigger-*-Source.tar.gz
}

# 获取版本信息
get_version() {
    local version_file="$PROJECT_ROOT/include/common/version.h"
    if [[ -f "$version_file" ]]; then
        VERSION=$(grep 'PROJ_VERSION' "$version_file" | awk -F '"' '{print $2}')
        BUILD_ID=$(grep 'PROJ_BUILD_ID' "$version_file" | awk -F '"' '{print $2}')
    else
        log_warn "版本文件未找到，使用默认版本"
        VERSION="0.0.12"
        BUILD_ID="unknown"
    fi
    
    # 从CMakeLists.txt获取项目版本
    if [[ -f "$PROJECT_ROOT/CMakeLists.txt" ]]; then
        CMAKE_VERSION=$(grep -m1 'project.*VERSION' "$PROJECT_ROOT/CMakeLists.txt" | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)
        if [[ -n "$CMAKE_VERSION" ]]; then
            VERSION="$CMAKE_VERSION"
        fi
    fi
    
    # 从trigger/version.py获取trigger版本
    local trigger_version_file="$PROJECT_ROOT/trigger/version.py"
    if [[ -f "$trigger_version_file" ]]; then
        TRIGGER_VERSION=$(grep '__version__' "$trigger_version_file" | awk -F '"' '{print $2}')
        log_info "Trigger版本: $TRIGGER_VERSION"
    else
        log_warn "Trigger版本文件未找到，使用默认版本"
        TRIGGER_VERSION="0.0.8"
    fi
    
    log_info "项目版本: $VERSION (构建ID: $BUILD_ID)"
}

# 构建JobLens
build_joblens() {
    log_info "构建JobLens..."
    
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    
    cmake "$PROJECT_ROOT" \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DJOBLENS_INSTALL_SYSTEM_DEPS=OFF
    
    make -j$(nproc)
    
    log_info "JobLens构建完成"
}

# 创建源码包
# 参数: $1 = "core" (只创建JobLens), $2 = "trigger" (只创建Trigger), 或空 (创建两者)
create_source_tarballs() {
    local build_core=true
    local build_trigger=true
    
    # 解析参数
    if [[ "$1" == "core" ]]; then
        build_trigger=false
    elif [[ "$1" == "trigger" ]]; then
        build_core=false
    fi
    
    log_info "创建源码包..."
    
    # 创建临时目录用于打包
    local temp_dir="/tmp/joblens-${VERSION}"
    local trigger_temp_dir="/tmp/trigger"
    
    # 清理旧的临时目录
    rm -rf "$temp_dir" "$trigger_temp_dir"
    
    # 创建JobLens源码包
    if [[ "$build_core" == true ]]; then
        log_info "准备JobLens源码目录..."
        mkdir -p "$temp_dir"
        
        # 复制文件到临时目录，排除不需要的文件
        rsync -q -av --exclude='build*' \
              --exclude='build-rpm' \
              --exclude='cmake-build-*' \
              --exclude='CMakeCache.txt' \
              --exclude='CMakeFiles' \
              --exclude='cmake_install.cmake' \
              --exclude='CPackConfig.cmake' \
              --exclude='CPackSourceConfig.cmake' \
              --exclude='CPackProperties.cmake' \
              --exclude='Makefile' \
              --exclude='compile_commands.json' \
              --exclude='.git' \
              --exclude='test' \
              --exclude='doc' \
              --exclude='packaging' \
              --exclude='scripts/__pycache__' \
              --exclude='*.swp' \
              --exclude='.*.swp' \
              "$PROJECT_ROOT/" "$temp_dir/"
        
        # 创建JobLens源码包
        log_info "打包JobLens源码..."
        tar -czf "/tmp/JobLens-${VERSION}-Source.tar.gz" \
            -C "/tmp" "joblens-${VERSION}"
    fi
    
    # 创建Trigger源码包
    if [[ "$build_trigger" == true ]]; then
        log_info "准备Trigger源码目录..."
        mkdir -p "$trigger_temp_dir"
        
        # 复制Trigger文件
        rsync -q -av --exclude='venv' \
              --exclude='__pycache__' \
              --exclude='*.pyc' \
              --exclude='*.pyo' \
              --exclude='*.swp' \
              --exclude='.*.swp' \
              "$PROJECT_ROOT/trigger/" "$trigger_temp_dir/"
        
        # 创建Trigger源码包
        log_info "打包Trigger源码..."
        tar -czf "/tmp/JobLens-Trigger-${TRIGGER_VERSION}-Source.tar.gz" \
            -C "/tmp" "trigger"
    fi
    
    # 清理临时目录
    rm -rf "$temp_dir" "$trigger_temp_dir"
    
    log_info "源码包创建完成:"
    ls -lh /tmp/JobLens-*-Source.tar.gz 2>/dev/null || echo "  无JobLens源码包"
    ls -lh /tmp/JobLens-Trigger-*-Source.tar.gz 2>/dev/null || echo "  无Trigger源码包"
}

# 准备RPM构建环境
# 参数: $1 = "core" (只准备JobLens), $2 = "trigger" (只准备Trigger), 或空 (准备两者)
prepare_rpm_build() {
    local build_core=true
    local build_trigger=true
    
    # 解析参数
    if [[ "$1" == "core" ]]; then
        build_trigger=false
    elif [[ "$1" == "trigger" ]]; then
        build_core=false
    fi
    
    log_info "准备RPM构建环境..."
    
    # 根据构建环境确定spec文件后缀
    local SPEC_SUFFIX=""
    if [[ "$BUILD_ENV" == "dev" ]]; then
        SPEC_SUFFIX="-dev"
        log_info "使用开发环境spec文件: joblens${SPEC_SUFFIX}.spec, joblens-trigger${SPEC_SUFFIX}.spec"
    else
        log_info "使用生产环境spec文件: joblens.spec, joblens-trigger.spec"
    fi
    
    # 复制JobLens源码包和spec文件
    if [[ "$build_core" == true ]]; then
        if [[ -f "/tmp/JobLens-${VERSION}-Source.tar.gz" ]]; then
            cp -f "/tmp/JobLens-${VERSION}-Source.tar.gz" ~/rpmbuild/SOURCES/
            log_info "已复制JobLens源码包"
        else
            log_warn "JobLens源码包未找到: /tmp/JobLens-${VERSION}-Source.tar.gz"
        fi
        cp -f "$PROJECT_ROOT/packaging/joblens${SPEC_SUFFIX}.spec" ~/rpmbuild/SPECS/
        sed -i "s|%{?_version}|$VERSION|g" ~/rpmbuild/SPECS/joblens${SPEC_SUFFIX}.spec 2>/dev/null || true
    fi
    
    # 复制Trigger源码包和spec文件
    if [[ "$build_trigger" == true ]]; then
        if [[ -f "/tmp/JobLens-Trigger-${TRIGGER_VERSION}-Source.tar.gz" ]]; then
            cp -f "/tmp/JobLens-Trigger-${TRIGGER_VERSION}-Source.tar.gz" ~/rpmbuild/SOURCES/
            log_info "已复制Trigger源码包"
        else
            log_warn "Trigger源码包未找到: /tmp/JobLens-Trigger-${TRIGGER_VERSION}-Source.tar.gz"
        fi
        cp -f "$PROJECT_ROOT/packaging/joblens-trigger${SPEC_SUFFIX}.spec" ~/rpmbuild/SPECS/
        sed -i "s|^Version:[[:space:]]*[0-9.]*|Version: $TRIGGER_VERSION|" ~/rpmbuild/SPECS/joblens-trigger${SPEC_SUFFIX}.spec 2>/dev/null || true
    fi
}

# 构建RPM包
# 参数: $1 = "core" (只构建JobLens), $2 = "trigger" (只构建Trigger), 或空 (构建两者)
build_rpm_packages() {
    local build_core=true
    local build_trigger=true
    
    # 解析参数
    if [[ "$1" == "core" ]]; then
        build_trigger=false
    elif [[ "$1" == "trigger" ]]; then
        build_core=false
    fi
    
    # 根据构建环境确定spec文件后缀
    local SPEC_SUFFIX=""
    if [[ "$BUILD_ENV" == "dev" ]]; then
        SPEC_SUFFIX="-dev"
        log_info "使用开发环境spec文件进行构建"
    fi
    
    # 检查sudo访问权限
    local SUDO_CMD=$(check_sudo_access)
    if [ "$SUDO_CMD" = "NEED_PASSWORD" ]; then
        log_warn "没有免密码sudo权限，跳过依赖安装"
        log_warn "请确保已安装以下构建依赖:"
        log_warn "  sudo dnf install -y librdkafka-devel python3-devel python3-virtualenv"
        SUDO_CMD=""  # 设置为空，跳过依赖安装
    elif [ -z "$SUDO_CMD" ]; then
        log_info "当前为root用户，跳过sudo"
    else
        log_info "检测到免密码sudo权限"
    fi
    
    # 构建JobLens RPM包
    if [[ "$build_core" == true ]]; then
        log_info "构建JobLens RPM包..."
        
        # 安装构建依赖
        log_info "安装JobLens RPM构建依赖..."
        if [ -n "$SUDO_CMD" ] && [ "$SUDO_CMD" != "NEED_PASSWORD" ]; then
            if command -v dnf &> /dev/null; then
                $SUDO_CMD dnf builddep -y ~/rpmbuild/SPECS/joblens${SPEC_SUFFIX}.spec
            elif command -v yum &> /dev/null; then
                $SUDO_CMD yum-builddep -y ~/rpmbuild/SPECS/joblens.spec
            else
                log_warn "未找到dnf或yum，无法自动安装构建依赖"
            fi
        else
            log_warn "跳过依赖安装（无sudo权限或已跳过）"
            log_warn "请确保已安装以下依赖: librdkafka-devel"
        fi
        
        # 构建JobLens主程序包
        rpmbuild -ba ~/rpmbuild/SPECS/joblens${SPEC_SUFFIX}.spec
    fi
    
    # 构建Trigger RPM包
    if [[ "$build_trigger" == true ]]; then
        log_info "构建Trigger RPM包..."
        
        # 安装Trigger构建依赖
        log_info "安装Trigger RPM构建依赖..."
        if [ -n "$SUDO_CMD" ] && [ "$SUDO_CMD" != "NEED_PASSWORD" ]; then
            if command -v dnf &> /dev/null; then
                $SUDO_CMD dnf builddep -y ~/rpmbuild/SPECS/joblens-trigger${SPEC_SUFFIX}.spec
            elif command -v yum &> /dev/null; then
                $SUDO_CMD yum-builddep -y ~/rpmbuild/SPECS/joblens-trigger.spec
            else
                log_warn "未找到dnf或yum，无法自动安装构建依赖"
            fi
        else
            log_warn "跳过依赖安装（无sudo权限或已跳过）"
            log_warn "请确保已安装以下依赖: python3-devel python3-virtualenv python3-gunicorn python3-Flask"
        fi
        
        # 构建Trigger服务包
        rpmbuild -ba ~/rpmbuild/SPECS/joblens-trigger${SPEC_SUFFIX}.spec
    fi
    
    log_info "RPM包构建完成"
}

# 显示构建结果
show_results() {
    log_info "构建完成的RPM包:"
    
    echo ""
    echo "=== JobLens主程序包 ==="
    find ~/rpmbuild/RPMS -name "joblens-*.rpm" -type f | while read rpm; do
        echo "  $rpm"
        rpm -qip "$rpm" 2>/dev/null | grep -E "(Name|Version|Release|Architecture|Size)" || true
        echo ""
    done
    
    echo ""
    echo "=== Trigger服务包 ==="
    find ~/rpmbuild/RPMS -name "joblens-trigger-*.rpm" -type f | while read rpm; do
        echo "  $rpm"
        rpm -qip "$rpm" 2>/dev/null | grep -E "(Name|Version|Release|Architecture|Size)" || true
        echo ""
    done
    
    echo ""
    echo "=== 源代码包 ==="
    find ~/rpmbuild/SRPMS -name "*.rpm" -type f | while read srpm; do
        echo "  $srpm"
    done
    
    echo ""
    log_info "RPM包位置:"
    echo "  二进制RPM包: ~/rpmbuild/RPMS/"
    echo "  源代码RPM包: ~/rpmbuild/SRPMS/"
}

# 显示用法函数
usage() {
    echo "用法: $0 [选项]"
    echo "选项:"
    echo "  -e, --env <环境>      构建环境，可选值为 dev 或 prod"
    echo "  --trigger-only        只构建 Trigger 服务包"
    echo "  --core-only           只构建 JobLens 主程序包"
    echo "  clean                 清理构建目录"
    echo "  -h, --help            显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 --env dev              构建开发环境RPM包（包含主程序和Trigger）"
    echo "  $0 --env dev --trigger-only  只构建开发环境Trigger包"
    echo "  $0 --env dev --core-only     只构建开发环境主程序包"
    echo "  $0 --env prod             构建生产环境RPM包"
    echo "  $0 clean                 清理构建目录"
}

# 主函数
main() {
    local BUILD_ENV=""
    local CLEAN_ONLY=false
    local BUILD_TRIGGER_ONLY=false
    local BUILD_CORE_ONLY=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--env)
                BUILD_ENV="$2"
                shift 2
                ;;
            --trigger-only)
                BUILD_TRIGGER_ONLY=true
                shift
                ;;
            --core-only)
                BUILD_CORE_ONLY=true
                shift
                ;;
            clean)
                CLEAN_ONLY=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "未知参数: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    # 检查参数互斥
    if [[ "$BUILD_TRIGGER_ONLY" == true && "$BUILD_CORE_ONLY" == true ]]; then
        log_error "不能同时指定 --trigger-only 和 --core-only"
        usage
        exit 1
    fi
    
    # 处理清理参数
    if [[ "$CLEAN_ONLY" == true ]]; then
        clean_build
        exit 0
    fi

    # 验证环境参数
    if [[ "$BUILD_ENV" != "dev" && "$BUILD_ENV" != "prod" ]]; then
        log_error "环境参数必须是 'dev' 或 'prod'，当前为: $BUILD_ENV"
        usage
        exit 1
    fi
    
    # 确定构建模式
    local build_mode="both"
    if [[ "$BUILD_TRIGGER_ONLY" == true ]]; then
        build_mode="trigger"
        log_info "开始构建Trigger RPM包（环境: $BUILD_ENV）"
    elif [[ "$BUILD_CORE_ONLY" == true ]]; then
        build_mode="core"
        log_info "开始构建JobLens主程序RPM包（环境: $BUILD_ENV）"
    else
        log_info "开始构建JobLens RPM包（环境: $BUILD_ENV，包含主程序和Trigger）"
    fi
    
    # 检查依赖
    check_dependencies || exit 1
    
    # 构建JobLens（只在需要时）
    if [[ "$build_mode" == "core" || "$build_mode" == "both" ]]; then
        build_joblens
    fi

    # 获取版本信息
    get_version
    
    # 创建源码包
    if [[ "$build_mode" == "trigger" ]]; then
        create_source_tarballs "trigger"
    elif [[ "$build_mode" == "core" ]]; then
        create_source_tarballs "core"
    else
        create_source_tarballs
    fi
    
    # 准备RPM构建环境
    if [[ "$build_mode" == "trigger" ]]; then
        prepare_rpm_build "trigger"
    elif [[ "$build_mode" == "core" ]]; then
        prepare_rpm_build "core"
    else
        prepare_rpm_build
    fi
    
    # 构建RPM包
    if [[ "$build_mode" == "trigger" ]]; then
        build_rpm_packages "trigger"
    elif [[ "$build_mode" == "core" ]]; then
        build_rpm_packages "core"
    else
        build_rpm_packages
    fi
    
    # 显示结果
    show_results
    
    log_info "RPM包构建完成！"
    log_info "安装测试命令示例:"
    if [[ "$build_mode" == "trigger" ]]; then
        echo "  sudo rpm -ivh ~/rpmbuild/RPMS/noarch/joblens-trigger-${TRIGGER_VERSION}-1.*.rpm"
    elif [[ "$build_mode" == "core" ]]; then
        echo "  sudo rpm -ivh ~/rpmbuild/RPMS/x86_64/joblens-${VERSION}-1.*.rpm"
    else
        echo "  sudo rpm -ivh ~/rpmbuild/RPMS/x86_64/joblens-${VERSION}-1.*.rpm"
        echo "  sudo rpm -ivh ~/rpmbuild/RPMS/noarch/joblens-trigger-${TRIGGER_VERSION}-1.*.rpm"
    fi
    echo ""
    log_info "安装后启动服务:"
    if [[ "$build_mode" == "trigger" ]]; then
        echo "  sudo systemctl daemon-reload"
        echo "  sudo systemctl start joblens-trigger"
    elif [[ "$build_mode" == "core" ]]; then
        echo "  sudo systemctl daemon-reload"
        echo "  sudo systemctl start joblens"
    else
        echo "  sudo systemctl daemon-reload"
        echo "  sudo systemctl start joblens"
        echo "  sudo systemctl start joblens-trigger"
    fi
}

# 执行主函数
main "$@"