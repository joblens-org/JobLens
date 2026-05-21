# 自动检测发行版并安装编译期依赖（含 clang）
if(NOT EXISTS "/etc/os-release")
    message(STATUS "Cannot detect OS, skip system-deps installation")
    return()
endif()

file(READ "/etc/os-release" OS_RELEASE)

string(REGEX MATCH "ID=[\"']?([^\"' \n]+)[\"']?" _ ${OS_RELEASE})
set(OS_ID almalinux)

# 通用包列表 —— 已包含 clang
set(PKGS_DEB
    "clang libbpf-dev libelf-dev cmake pkg-config"
    "libssl-dev zlib1g-dev libcurl4-openssl-dev"
    "libsasl2-dev libzstd-dev liblz4-dev librdkafka-dev")

set(PKGS_RPM
    "clang libbpf-devel elfutils-libelf-devel cmake pkgconfig openssl-devel zlib-devel libcurl-devel cyrus-sasl-devel libzstd-devel lz4-devel librdkafka-devel libnl3-devel nlohmann-json-devel fmt-devel spdlog-devel yaml-cpp-devel
")

set(PKGS_ALPINE
    "clang libbpf-dev elfutils-dev cmake pkgconf"
    "openssl-dev zlib-dev curl-dev cyrus-sasl-dev"
    "zstd-dev lz4-dev librdkafka-dev")

# 根据 OS 生成安装命令
if(OS_ID STREQUAL "ubuntu" OR OS_ID STREQUAL "debian")
    set(PKG_CMD "sudo apt-get update && sudo apt-get install -y ${PKGS_DEB}")
elseif(OS_ID STREQUAL "centos" OR OS_ID STREQUAL "rhel" OR OS_ID STREQUAL "fedora" OR OS_ID STREQUAL "almalinux")
    set(PKG_CMD "sudo dnf config-manager --set-enabled crb && sudo dnf install -y ${PKGS_RPM}")
                # sudo dnf makecache && 
                
elseif(OS_ID STREQUAL "arch")
    set(PKG_CMD "sudo pacman -S --needed clang libbpf libelf cmake pkgconf "
                "openssl zlib curl cyrus-sasl zstd lz4 librdkafka")
elseif(OS_ID STREQUAL "alpine")
    set(PKG_CMD "sudo apk add ${PKGS_ALPINE}")
else()
    message(STATUS "Unsupported OS ${OS_ID}, please install deps manually")
    return()
endif()

# 执行安装
execute_process(
    COMMAND bash -c "${PKG_CMD}"
    RESULT_VARIABLE _ret
)
if(NOT _ret EQUAL 0)
    message(WARNING
        "System dependencies installation failed (no sudo?);\n"
        "Please run manually:\n  ${PKG_CMD}")
endif()