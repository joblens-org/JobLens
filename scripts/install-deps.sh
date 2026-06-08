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
    local SOL2_VERSION="v3.3.0"
    local SOL2_INSTALL_DIR="/usr/local/include/sol"
    local SOL2_URL="https://github.com/ThePhD/sol2/releases/download/${SOL2_VERSION}/sol.hpp"

    if [ -f "${SOL2_INSTALL_DIR}/sol.hpp" ]; then
        echo "sol2 already installed at ${SOL2_INSTALL_DIR}"
        return 0
    fi

    echo "Installing sol2 ${SOL2_VERSION} single header..."
    sudo mkdir -p "${SOL2_INSTALL_DIR}"
    curl -fsSL "${SOL2_URL}" -o "${SOL2_INSTALL_DIR}/sol.hpp" || {
        echo "ERROR: Failed to download sol2"
        return 1
    }
    echo "sol2 installed to ${SOL2_INSTALL_DIR}"
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
        nlohmann-json3-dev
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
        nlohmann-json-devel
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
        nlohmann-json
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
