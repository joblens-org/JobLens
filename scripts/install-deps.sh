#!/usr/bin/env bash
# 自动检测发行版并安装 JobLens 编译依赖
# 用法: ./scripts/install-deps.sh

set -e

if [ ! -f /etc/os-release ]; then
    echo "Cannot detect OS. Please install dependencies manually:"
    echo "  - Debian/Ubuntu: apt-get install clang libbpf-dev libelf-dev cmake ninja-build pkg-config libssl-dev zlib1g-dev libcurl4-openssl-dev libsasl2-dev libzstd-dev liblz4-dev librdkafka-dev libleveldb-dev libnl-3-dev libnl-genl-3-dev lua5.4-dev libspdlog-dev libyaml-cpp-dev libfmt-dev libboost-all-dev libxxhash-dev libhowardhinnant-date-dev"
    exit 1
fi

source /etc/os-release

install_sol2() {
    local SOL2_VERSION="${SOL2_VERSION:-v3.5.0}"
    local INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"

    # 检查是否已安装（系统路径或自定义路径）
    for prefix in "${INSTALL_PREFIX}" "/usr"; do
        if [ -f "${prefix}/include/sol/sol.hpp" ] && \
           [ -f "${prefix}/include/sol/config.hpp" ]; then
            echo "    sol2 already installed at ${prefix}/include/sol/"
            return 0
        fi
    done

    echo "==> Installing sol2 (${SOL2_VERSION})..."

    # 优先尝试系统包管理器（非交互，失败静默 fallback）
    case "$ID" in
        fedora)
            if sudo dnf install -y sol2-devel 2>/dev/null; then
                echo "✅ sol2 installed via dnf (sol2-devel)"
                return 0
            fi
            echo "    dnf sol2-devel not available, falling back to tarball"
            ;;
        ubuntu|debian)
            if sudo apt-get install -y sol2-dev 2>/dev/null; then
                echo "✅ sol2 installed via apt (sol2-dev)"
                return 0
            fi
            echo "    apt sol2-dev not available, falling back to tarball"
            ;;
    esac

    # Fallback: 从 GitHub release tarball 安装完整 include/sol
    local SOL2_URL="https://github.com/ThePhD/sol2/archive/refs/tags/${SOL2_VERSION}.tar.gz"
    local SOL2_TMP_DIR
    SOL2_TMP_DIR=$(mktemp -d)
    trap "rm -rf ${SOL2_TMP_DIR}" RETURN

    curl -fsSL --retry 3 --retry-delay 5 --retry-max-time 60 \
        "${SOL2_URL}" -o "${SOL2_TMP_DIR}/sol2.tar.gz" || {
        echo "ERROR: Failed to download sol2 (HTTP 500 is transient GitHub tarball issue, retry later)"
        return 1
    }

    tar -xzf "${SOL2_TMP_DIR}/sol2.tar.gz" -C "${SOL2_TMP_DIR}"
    sudo mkdir -p "${INSTALL_PREFIX}/include"
    sudo cp -r "${SOL2_TMP_DIR}/sol2-${SOL2_VERSION#v}/include/sol" "${INSTALL_PREFIX}/include/"

    if [ -f "${INSTALL_PREFIX}/include/sol/sol.hpp" ] && \
       [ -f "${INSTALL_PREFIX}/include/sol/config.hpp" ]; then
        echo "✅ sol2 installed successfully"
    else
        echo "❌ Installation failed"
        return 1
    fi
}

install_bpftool_deb() {
    local BPFTOOL_VERSION="${BPFTOOL_VERSION:-v7.7.0}"
    local BPFTOOL_ARCH
    case "$(uname -m)" in
        x86_64|amd64)
            BPFTOOL_ARCH="amd64"
            ;;
        aarch64|arm64)
            BPFTOOL_ARCH="arm64"
            ;;
        *)
            echo "Unsupported bpftool binary architecture: $(uname -m)"
            return 1
            ;;
    esac

    sudo apt-get install -y --no-install-recommends bpftool 2>/dev/null || \
        sudo apt-get install -y --no-install-recommends linux-tools-$(uname -r) 2>/dev/null || \
        sudo apt-get install -y --no-install-recommends linux-tools-azure linux-cloud-tools-azure 2>/dev/null || \
        sudo apt-get install -y --no-install-recommends linux-tools-generic 2>/dev/null || \
        echo "⚠ bpftool 未通过 apt 安装，尝试下载预编译 bpftool"

    if command -v bpftool >/dev/null 2>&1 && bpftool version >/dev/null 2>&1; then
        echo "✅ bpftool available: $(command -v bpftool)"
        return 0
    fi

    local BPFTOOL_TMP_DIR
    local BPFTOOL_URL="https://github.com/libbpf/bpftool/releases/download/${BPFTOOL_VERSION}/bpftool-${BPFTOOL_VERSION}-${BPFTOOL_ARCH}.tar.gz"
    BPFTOOL_TMP_DIR=$(mktemp -d)
    if ! curl -fsSL --retry 3 --retry-delay 5 --retry-max-time 60 \
        "${BPFTOOL_URL}" -o "${BPFTOOL_TMP_DIR}/bpftool.tar.gz"; then
        rm -rf "${BPFTOOL_TMP_DIR}"
        return 1
    fi
    if ! tar -xzf "${BPFTOOL_TMP_DIR}/bpftool.tar.gz" -C "${BPFTOOL_TMP_DIR}"; then
        rm -rf "${BPFTOOL_TMP_DIR}"
        return 1
    fi
    if ! sudo install -m 0755 "${BPFTOOL_TMP_DIR}/bpftool" /usr/local/bin/bpftool; then
        rm -rf "${BPFTOOL_TMP_DIR}"
        return 1
    fi
    rm -rf "${BPFTOOL_TMP_DIR}"
    /usr/local/bin/bpftool version >/dev/null
    echo "✅ bpftool installed: /usr/local/bin/bpftool"
}

install_deb() {
    echo "Detected Debian/Ubuntu. Installing dependencies..."
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends \
        clang \
        cmake \
        ninja-build \
        pkg-config \
        libbpf-dev \
        libelf-dev \
        libleveldb-dev \
        libnl-3-dev \
        libnl-genl-3-dev \
        libssl-dev \
        zlib1g-dev \
        lua5.4-dev \
        libcurl4-openssl-dev \
        librdkafka-dev \
        libboost-all-dev \
        libxxhash-dev \
        libspdlog-dev \
        libyaml-cpp-dev \
        libfmt-dev \
        libhowardhinnant-date-dev \
        libsasl2-dev \
        libzstd-dev \
        liblz4-dev \
        nlohmann-json3-dev \
        linux-tools-common
    install_bpftool_deb
    install_sol2
    echo "Done."
}

install_rpm() {
    echo "Detected RHEL/Fedora/AlmaLinux. Installing dependencies..."
    sudo dnf install -y \
        clang \
        cmake \
        ninja-build \
        pkgconfig \
        libbpf-devel \
        elfutils-libelf-devel \
        leveldb-devel \
        libnl3-devel \
        openssl-devel \
        zlib-devel \
        lua-devel \
        libcurl-devel \
        cyrus-sasl-devel \
        libzstd-devel \
        lz4-devel \
        librdkafka-devel \
        boost-devel \
        xxhash-devel \
        spdlog-devel \
        yaml-cpp-devel \
        fmt-devel \
        date-devel \
        nlohmann-json-devel \
        bpftool
    install_sol2
    echo "Done."
}

install_arch() {
    echo "Detected Arch Linux. Installing dependencies..."
    sudo pacman -S --needed \
        clang \
        cmake \
        ninja \
        pkgconf \
        libbpf \
        libelf \
        leveldb \
        libnl \
        openssl \
        zlib \
        lua \
        curl \
        libsasl \
        zstd \
        lz4 \
        librdkafka \
        boost \
        xxhash \
        spdlog \
        yaml-cpp \
        fmt \
        chrono-date \
        nlohmann-json \
        bpftool
    install_sol2
    echo "Done."
}

case "$ID" in
    ubuntu|debian)
        install_deb
        ;;
    centos|rhel|fedora|almalinux|rocky)
        install_rpm
        ;;
    arch)
        install_arch
        ;;
    *)
        echo "Unsupported OS: $ID"
        echo "Please install dependencies manually and submit a PR to add support."
        exit 1
        ;;
esac
