#!/usr/bin/env bash
# 自动检测发行版并安装 JobLens 编译依赖
# 用法: ./scripts/install-deps.sh

set -e

# Docker 容器内以 root 运行无需提权 (PAM 不可用)
if [ "$(id -u)" -eq 0 ]; then
    sudo() { "$@"; }
fi

if [ ! -f /etc/os-release ]; then
    echo "Cannot detect OS. Please install dependencies manually:"
    echo "  - Debian/Ubuntu: apt-get install clang libbpf-dev libelf-dev cmake ninja-build pkg-config libssl-dev zlib1g-dev libcurl4-openssl-dev libsasl2-dev libzstd-dev liblz4-dev librdkafka-dev libleveldb-dev libnl-3-dev libnl-genl-3-dev lua5.4-dev libspdlog-dev libyaml-cpp-dev libfmt-dev libboost-all-dev libxxhash-dev libhowardhinnant-date-dev"
    exit 1
fi

source /etc/os-release

# ---------------------------------------------------------------------------
# 下载物完整性校验
#
# 三个源码/二进制依赖都从 GitHub 拉取；这里按 "类型:版本[:架构]" 固定 sha256。
# 未登记的版本默认失败（fail closed），确需临时跳过时显式设置
# JOBLENS_SKIP_DOWNLOAD_VERIFY=1。
# ---------------------------------------------------------------------------
pinned_sha256() {
    local key="$1"
    case "${key}" in
        date:v3.0.1)
            echo "7a390f200f0ccd207e8cff6757e04817c1a0aec3e327b006b7eb451c57ee3538" ;;
        sol2:v3.5.0)
            echo "86c0f6d2836b184a250fc2907091c076bf53c9603dd291eaebade36cc342e13c" ;;
        bpftool:v7.7.0:amd64)
            echo "09150596f09356b0ff632dd7f9856e0ea86bf96b269e9bc94278d9a9432a268c" ;;
        bpftool:v7.7.0:arm64)
            echo "d68443d945c146080151eb215bebaa400294ab7ada22af4f5d0a940f5992b9d9" ;;
        *)
            return 1 ;;
    esac
}

verify_download() {
    local file="$1" key="$2" label="$3"
    if [ "${JOBLENS_SKIP_DOWNLOAD_VERIFY:-0}" = "1" ]; then
        echo "    WARNING: skipping sha256 verification for ${label} (JOBLENS_SKIP_DOWNLOAD_VERIFY=1)"
        return 0
    fi
    local expected
    expected="$(pinned_sha256 "${key}" || true)"
    if [ -z "${expected}" ]; then
        echo "ERROR: no pinned sha256 for ${label} (key '${key}')"
        echo "       add it to pinned_sha256() in scripts/install-deps.sh, or set"
        echo "       JOBLENS_SKIP_DOWNLOAD_VERIFY=1 to bypass (not recommended)"
        return 1
    fi
    local actual
    actual="$(sha256sum "${file}" | awk '{print $1}')"
    if [ "${actual}" != "${expected}" ]; then
        echo "ERROR: sha256 mismatch for ${label}"
        echo "       expected: ${expected}"
        echo "       actual:   ${actual}"
        return 1
    fi
    echo "    verified sha256 for ${label}"
}

install_date() {
    local DATE_VERSION="${DATE_VERSION:-v3.0.1}"
    local INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"

    # Check if already installed
    if [ -f "${INSTALL_PREFIX}/include/date/date.h" ]; then
        echo "    date already installed at ${INSTALL_PREFIX}/include/date/"
        return 0
    fi

    echo "==> Installing Howard Hinnant date (${DATE_VERSION}) from source..."
    local DATE_TMP_DIR
    DATE_TMP_DIR=$(mktemp -d)
    trap "rm -rf ${DATE_TMP_DIR}" RETURN

    curl -fsSL --retry 3 --retry-delay 5 --retry-max-time 60 \
        "https://github.com/HowardHinnant/date/archive/refs/tags/${DATE_VERSION}.tar.gz" \
        -o "${DATE_TMP_DIR}/date.tar.gz" || {
        echo "ERROR: Failed to download date"
        return 1
    }
    verify_download "${DATE_TMP_DIR}/date.tar.gz" "date:${DATE_VERSION}" "date ${DATE_VERSION}" || return 1

    tar -xzf "${DATE_TMP_DIR}/date.tar.gz" -C "${DATE_TMP_DIR}"
    cd "${DATE_TMP_DIR}/date-${DATE_VERSION#v}"

    # Build and install (provides date::date CMake target + tz library)
    cmake -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
        -DBUILD_TZ_LIB=ON \
        -DUSE_SYSTEM_TZ_DB=ON
    cmake --build build
    sudo cmake --install build

    if [ -f "${INSTALL_PREFIX}/include/date/date.h" ]; then
        echo "✅ date installed successfully"
    else
        echo "❌ date installation failed"
        return 1
    fi
}

install_sol2() {
    local SOL2_VERSION="${SOL2_VERSION:-v3.5.0}"
    local INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local}"

    # Idempotency: installed copy already carries the Lua 5.5 compat patch
    if [ -f "${INSTALL_PREFIX}/include/sol/sol.hpp" ] && \
       grep -q 'LUA_VERSION_NUM > 505' "${INSTALL_PREFIX}/include/sol/compatibility/compat-5.3.h" 2>/dev/null; then
        echo "    sol2 already installed (Lua 5.5 compatible) at ${INSTALL_PREFIX}/include/sol/"
        return 0
    fi

    echo "==> Installing sol2 (${SOL2_VERSION}, patched for Lua 5.5)..."

    # Always install from the upstream tarball into INSTALL_PREFIX:
    #   - distro packages (dnf sol2-devel / apt sol2-dev) cannot carry the Lua
    #     5.5 compat patch and may be older (e.g. Debian ships 3.3.0);
    #   - ${INSTALL_PREFIX}/include precedes /usr/include, shadowing any
    #     system copy (e.g. Fedora sol2-devel).
    local SOL2_URL="https://github.com/ThePhD/sol2/archive/refs/tags/${SOL2_VERSION}.tar.gz"
    local SOL2_TMP_DIR
    SOL2_TMP_DIR=$(mktemp -d)
    trap "rm -rf ${SOL2_TMP_DIR}" RETURN

    curl -fsSL --retry 3 --retry-delay 5 --retry-max-time 60 \
        "${SOL2_URL}" -o "${SOL2_TMP_DIR}/sol2.tar.gz" || {
        echo "ERROR: Failed to download sol2 (HTTP 500 is transient GitHub tarball issue, retry later)"
        return 1
    }
    verify_download "${SOL2_TMP_DIR}/sol2.tar.gz" "sol2:${SOL2_VERSION}" "sol2 ${SOL2_VERSION}" || return 1

    tar -xzf "${SOL2_TMP_DIR}/sol2.tar.gz" -C "${SOL2_TMP_DIR}"

    # Lua 5.5 compat patch (upstream sol2 support is still an unmerged PR;
    # version-gated so Lua <= 5.4 behavior is unchanged):
    #   1) compat-5.3.h: version gate accepts LUA_VERSION_NUM 5.5
    #   2) compat-5.4.h: LUA_ERRGCMM fallback define applies to >= 5.4
    #   3) state.hpp:    lua_newstate gained a seed parameter in 5.5
    local SOL2_SRC="${SOL2_TMP_DIR}/sol2-${SOL2_VERSION#v}/include/sol"
    sed -i 's/LUA_VERSION_NUM < 501 || LUA_VERSION_NUM > 504/LUA_VERSION_NUM < 501 || LUA_VERSION_NUM > 505/' \
        "${SOL2_SRC}/compatibility/compat-5.3.h"
    sed -i 's/LUA_VERSION_NUM == 504/LUA_VERSION_NUM >= 504/' \
        "${SOL2_SRC}/compatibility/compat-5.4.h"
    sed -i '/^namespace sol {/i\#if LUA_VERSION_NUM >= 505\n#define SOL_LUA_NEWSTATE(f, ud) lua_newstate(f, ud, 0)\n#else\n#define SOL_LUA_NEWSTATE(f, ud) lua_newstate(f, ud)\n#endif\n' \
        "${SOL2_SRC}/state.hpp"
    sed -i 's/unique_base(lua_newstate(alfunc, alpointer))/unique_base(SOL_LUA_NEWSTATE(alfunc, alpointer))/' \
        "${SOL2_SRC}/state.hpp"

    if ! grep -q 'LUA_VERSION_NUM > 505' "${SOL2_SRC}/compatibility/compat-5.3.h" || \
       ! grep -q 'SOL_LUA_NEWSTATE' "${SOL2_SRC}/state.hpp"; then
        echo "ERROR: sol2 Lua 5.5 compat patch did not apply (unexpected sol2 layout?)"
        return 1
    fi

    sudo mkdir -p "${INSTALL_PREFIX}/include"
    sudo rm -rf "${INSTALL_PREFIX}/include/sol"
    sudo cp -r "${SOL2_SRC}" "${INSTALL_PREFIX}/include/sol"

    if [ -f "${INSTALL_PREFIX}/include/sol/sol.hpp" ] && \
       grep -q 'LUA_VERSION_NUM > 505' "${INSTALL_PREFIX}/include/sol/compatibility/compat-5.3.h"; then
        echo "✅ sol2 installed successfully (Lua 5.4/5.5 compatible)"
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
    if ! verify_download "${BPFTOOL_TMP_DIR}/bpftool.tar.gz" \
        "bpftool:${BPFTOOL_VERSION}:${BPFTOOL_ARCH}" "bpftool ${BPFTOOL_VERSION} ${BPFTOOL_ARCH}"; then
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

    # RHEL-based distros (AlmaLinux, Rocky, CentOS, RHEL) need EPEL + CRB
    # Fedora already has these packages in its main repositories
    if [ "$ID" != "fedora" ]; then
        dnf install -y dnf-plugins-core epel-release
        dnf config-manager --set-enabled crb 2>/dev/null || \
        dnf config-manager --set-enabled powertools 2>/dev/null || true
    fi

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
        nlohmann-json-devel \
        bpftool

    # date-devel (Howard Hinnant) system package has broken CMake config
    # on some Fedora versions (references /usr/include/date/date.h as source).
    # Always build from source for consistent behavior across platforms.
    echo "==> Building Howard Hinnant date from source..."
    install_date
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
