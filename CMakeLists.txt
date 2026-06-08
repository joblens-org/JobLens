# ============================================================
# JobLens — 高性能分布式作业监控 Agent (build-only)
# 打包配置见 scripts/cmake/Packaging.cmake
# 环境依赖安装见 scripts/install-deps.sh
# ============================================================

cmake_minimum_required(VERSION 3.25)
project(JobLens
        VERSION 0.0.21
        DESCRIPTION "High-performance job profiler"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type: Debug, Release, RelWithDebInfo, MinSizeRel")
include(GNUInstallDirs)

# ---- 版本信息 ----
find_package(Git QUIET)
if(Git_FOUND)
  execute_process(
    COMMAND git rev-parse --short=7 HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE BUILD_ID
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
endif()
if(NOT BUILD_ID)
  set(BUILD_ID "unknown")
endif()
string(TIMESTAMP BUILD_TIME UTC)

# 生成 version.h 到构建目录，避免污染源码树
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/include/common)
configure_file(
  ${CMAKE_SOURCE_DIR}/include/common/version.h.in
  ${CMAKE_BINARY_DIR}/include/common/version.h
  @ONLY)

set(PROFILER_INC_DIR "${CMAKE_SOURCE_DIR}/include")
set(PROFILER_SRC_DIR "${CMAKE_SOURCE_DIR}/src")

# ---- 构建工具 ----
find_program(CLANG clang REQUIRED)
find_package(PkgConfig REQUIRED)

# ---- 系统库 ----
pkg_check_modules(LIBBPF REQUIRED libbpf)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

find_path(ELF_INCLUDE_DIR elf.h REQUIRED)
find_library(ELF_LIBRARY NAMES elf REQUIRED)
add_library(ELF::ELF INTERFACE IMPORTED)
set_target_properties(ELF::ELF PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${ELF_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${ELF_LIBRARY}")

find_package(Lua 5.4 REQUIRED)
pkg_check_modules(XXHASH REQUIRED IMPORTED_TARGET libxxhash)

find_library(SASL2_LIB NAMES sasl2 libsasl2)
find_library(ZSTD_LIB NAMES zstd libzstd)
find_library(LZ4_LIB  NAMES lz4  liblz4)

# ---- 第三方 C++ 库 ----
find_package(fmt QUIET)
if(fmt_FOUND AND NOT TARGET fmt::fmt)
    add_library(fmt::fmt ALIAS fmt)
endif()

pkg_check_modules(NETLINK REQUIRED libnl-3.0 libnl-genl-3.0)
find_package(spdlog REQUIRED)
find_package(yaml-cpp REQUIRED)
if(NOT TARGET yaml-cpp::yaml-cpp AND TARGET yaml-cpp)
    add_library(yaml-cpp::yaml-cpp ALIAS yaml-cpp)
endif()

find_package(date REQUIRED)

find_package(CURL REQUIRED)

find_package(nlohmann_json REQUIRED)

find_path(RDKAFKA_INCLUDE_DIR
          NAMES rdkafkacpp.h
          PATHS /usr/local/include /usr/include
          PATH_SUFFIXES librdkafka
          NO_DEFAULT_PATH)
find_library(RDKAFKA_LIB NAMES rdkafka)
find_library(RDKAFKAPP_LIB NAMES rdkafka++)
add_library(RdKafka::rdkafka++ STATIC IMPORTED GLOBAL)
set_target_properties(RdKafka::rdkafka++ PROPERTIES
                      IMPORTED_LOCATION "${RDKAFKAPP_LIB}"
                      INTERFACE_INCLUDE_DIRECTORIES "${RDKAFKA_INCLUDE_DIR}"
                      INTERFACE_LINK_LIBRARIES "${RDKAFKA_LIB}")

find_package(Boost REQUIRED)

# LevelDB — Debian/Ubuntu 的 libleveldb-dev 不提供 leveldb.pc，用原生 CMake 查找
find_path(LEVELDB_INCLUDE_DIR NAMES leveldb/db.h)
find_library(LEVELDB_LIBRARY NAMES leveldb)
if(NOT LEVELDB_INCLUDE_DIR OR NOT LEVELDB_LIBRARY)
    message(FATAL_ERROR "LevelDB not found.")
endif()
add_library(LevelDB::LevelDB UNKNOWN IMPORTED)
set_target_properties(LevelDB::LevelDB PROPERTIES
    IMPORTED_LOCATION "${LEVELDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LEVELDB_INCLUDE_DIR}")

# ---- eBPF 编译 ----
file(GLOB BPF_C_LIST CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/ebpf/*.bpf.c")
set(BPF_OBJECT_DIR "${CMAKE_BINARY_DIR}/bpf_obj")
file(MAKE_DIRECTORY ${BPF_OBJECT_DIR})

foreach(bpf_c ${BPF_C_LIST})
    get_filename_component(name ${bpf_c} NAME_WE)
    set(bpf_o "${BPF_OBJECT_DIR}/${name}.bpf.o")
    add_custom_command(OUTPUT ${bpf_o}
        COMMAND ${CLANG} -g -O2 -target bpf
                -I${CMAKE_SOURCE_DIR}/include
                -c ${bpf_c} -o ${bpf_o}
        DEPENDS ${bpf_c}
        COMMENT "[eBPF] Building ${bpf_o}")
    list(APPEND BPF_O_LIST ${bpf_o})
endforeach()
add_custom_target(ebpf_objects ALL DEPENDS ${BPF_O_LIST})

# ---- 主可执行 ----
file(GLOB_RECURSE PROFILER_SOURCES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.cpp")
list(FILTER PROFILER_SOURCES EXCLUDE REGEX ".*/ebpf/.*")

add_executable(JobLens ${PROFILER_SOURCES})
add_dependencies(JobLens ebpf_objects)

target_include_directories(JobLens
    PRIVATE
        ${CMAKE_BINARY_DIR}/include      # 生成的 version.h 优先于源码目录
        ${PROFILER_INC_DIR}
        ${NETLINK_INCLUDE_DIRS}
        /usr/include/libnl3
        fmt::fmt   spdlog::spdlog
        yaml-cpp::yaml-cpp
        PkgConfig::CURL
        RdKafka::rdkafka++
        OpenSSL::SSL OpenSSL::Crypto
        Boost::boost
        PkgConfig::XXHASH)

target_link_libraries(JobLens
    PRIVATE
        ELF::ELF    ZLIB::ZLIB
        OpenSSL::SSL OpenSSL::Crypto
        Threads::Threads
        ${LIBBPF_LIBRARIES}
        fmt::fmt    spdlog::spdlog
        yaml-cpp::yaml-cpp
        CURL::libcurl
        RdKafka::rdkafka++
        Boost::boost
        date::date
        nlohmann_json::nlohmann_json
        ${SASL2_LIB} ${ZSTD_LIB} ${LZ4_LIB}
        ${NETLINK_LIBRARIES}
        LevelDB::LevelDB
        pthread dl
        lua5.4
        PkgConfig::XXHASH)

# ---- 打包 (install / systemd / CPack) ----
include(${CMAKE_CURRENT_SOURCE_DIR}/scripts/cmake/Packaging.cmake)
