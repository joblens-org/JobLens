# ============================================================
# JobLens — High-Performance Distributed Job Monitoring Agent (build-only)
# Packaging config: see scripts/cmake/Packaging.cmake
# Environment dependency install: see scripts/install-deps.sh
# ============================================================

cmake_minimum_required(VERSION 3.25)
project(JobLens
        VERSION 0.2.1
        DESCRIPTION "High-performance job profiler"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type: Debug, Release, RelWithDebInfo, MinSizeRel")
include(GNUInstallDirs)

# ---- Packaging Strategy Options ----
# JOBLENS_BUNDLE_LIBS=ON : 传统模式，打包所有非系统 .so (collect_libs.sh + RPATH)
# JOBLENS_BUNDLE_LIBS=OFF: 依赖系统 RPM 包管理器，不打包 .so，不设 RPATH
option(JOBLENS_BUNDLE_LIBS "Bundle runtime .so into RPM" ON)

# JOBLENS_STATIC_EPEL=ON : 静态链接 leveldb / xxhash，减少 EPEL 运行时依赖
option(JOBLENS_STATIC_EPEL "Statically link EPEL-only libraries (leveldb, xxhash)" OFF)

# ---- Version Info ----
# BUILD_ID can be set externally (e.g. -DBUILD_ID=abc1234 from CI)
if(NOT BUILD_ID)
  find_package(Git QUIET)
  if(Git_FOUND)
    execute_process(
      COMMAND git rev-parse --short=7 HEAD
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      OUTPUT_VARIABLE BUILD_ID
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET)
  endif()
endif()
if(NOT BUILD_ID)
  set(BUILD_ID "unknown")
endif()
string(TIMESTAMP BUILD_TIME UTC)

# Generate version.h in the build directory to avoid polluting source tree
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/include/common)
configure_file(
  ${CMAKE_SOURCE_DIR}/include/common/version.h.in
  ${CMAKE_BINARY_DIR}/include/common/version.h
  @ONLY)

set(PROFILER_INC_DIR "${CMAKE_SOURCE_DIR}/include")
set(PROFILER_SRC_DIR "${CMAKE_SOURCE_DIR}/src")

# ---- Build Tools ----
find_program(CLANG clang REQUIRED)
find_package(PkgConfig REQUIRED)

# ---- System Libraries ----
pkg_check_modules(LIBBPF REQUIRED libbpf)
find_program(BPFTOOL bpftool REQUIRED)
if(NOT BPFTOOL)
    message(FATAL_ERROR "bpftool not found. Install it via: bash scripts/install-deps.sh")
endif()
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

# In static link mode, additionally search for .a to replace dynamic linking
if(JOBLENS_STATIC_EPEL)
    find_library(XXHASH_STATIC_LIB NAMES libxxhash.a)
    if(XXHASH_STATIC_LIB)
        message(STATUS "Using static xxhash: ${XXHASH_STATIC_LIB}")
        set(XXHASH_USE_STATIC ON)
    else()
        message(WARNING "JOBLENS_STATIC_EPEL=ON but libxxhash.a not found, falling back to shared")
    endif()
endif()

find_library(SASL2_LIB NAMES sasl2 libsasl2)
find_library(ZSTD_LIB NAMES zstd libzstd)
find_library(LZ4_LIB  NAMES lz4  liblz4)

# ---- Third-Party C++ Libraries ----
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


find_path(SOL2_INCLUDE_DIR NAMES sol/sol.hpp)
if(NOT SOL2_INCLUDE_DIR)
    message(FATAL_ERROR "sol2 not found")
endif()
add_library(sol2::sol2 INTERFACE IMPORTED)
set_target_properties(sol2::sol2 PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SOL2_INCLUDE_DIR}")

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

# LevelDB — Prefer static linking (when JOBLENS_STATIC_EPEL=ON)
find_path(LEVELDB_INCLUDE_DIR NAMES leveldb/db.h)
if(JOBLENS_STATIC_EPEL)
    find_library(LEVELDB_LIBRARY NAMES libleveldb.a leveldb)
    if(LEVELDB_LIBRARY MATCHES "\\.a$")
        message(STATUS "Using static LevelDB: ${LEVELDB_LIBRARY}")
    else()
        message(STATUS "Static LevelDB not found, using shared")
    endif()
else()
    find_library(LEVELDB_LIBRARY NAMES leveldb)
endif()
if(NOT LEVELDB_INCLUDE_DIR OR NOT LEVELDB_LIBRARY)
    message(FATAL_ERROR "LevelDB not found.")
endif()
add_library(LevelDB::LevelDB UNKNOWN IMPORTED)
set_target_properties(LevelDB::LevelDB PROPERTIES
    IMPORTED_LOCATION "${LEVELDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LEVELDB_INCLUDE_DIR}")

# ---- eBPF Compilation ----
file(GLOB BPF_C_LIST CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/ebpf/*.bpf.c")
set(BPF_OBJECT_DIR "${CMAKE_BINARY_DIR}/bpf_obj")
file(MAKE_DIRECTORY ${BPF_OBJECT_DIR})

# 构建时自动生成 vmlinux.h (CO-RE header)
set(VMLINUX_H_OUT "${CMAKE_BINARY_DIR}/include/ebpf/vmlinux.h")
add_custom_command(OUTPUT ${VMLINUX_H_OUT}
    COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_BINARY_DIR}/include/ebpf"
    COMMAND ${BPFTOOL} btf dump file /sys/kernel/btf/vmlinux format c > ${VMLINUX_H_OUT}
    COMMENT "[eBPF] Generating vmlinux.h via bpftool"
    VERBATIM)

foreach(bpf_c ${BPF_C_LIST})
    get_filename_component(name ${bpf_c} NAME_WE)
    set(bpf_o "${BPF_OBJECT_DIR}/${name}.bpf.o")
    add_custom_command(OUTPUT ${bpf_o}
        COMMAND ${CLANG} -g -O2 -target bpf
                -I${CMAKE_BINARY_DIR}/include
                -I${CMAKE_SOURCE_DIR}/include
                -c ${bpf_c} -o ${bpf_o}
        DEPENDS ${bpf_c} ${VMLINUX_H_OUT}
        COMMENT "[eBPF] Building ${bpf_o}")
    list(APPEND BPF_O_LIST ${bpf_o})
endforeach()
add_custom_target(ebpf_objects ALL DEPENDS ${BPF_O_LIST})

# ---- Main Executable ----
file(GLOB_RECURSE PROFILER_SOURCES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.cpp")
list(FILTER PROFILER_SOURCES EXCLUDE REGEX ".*/ebpf/.*")

add_executable(JobLens ${PROFILER_SOURCES})
add_dependencies(JobLens ebpf_objects)

target_include_directories(JobLens
    PRIVATE
        ${CMAKE_BINARY_DIR}/include      # Generated version.h takes priority over source directory
        ${PROFILER_INC_DIR}
        ${LUA_INCLUDE_DIR}
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
        sol2::sol2
        ${SASL2_LIB} ${ZSTD_LIB} ${LZ4_LIB}
        ${NETLINK_LIBRARIES}
        LevelDB::LevelDB
        pthread dl
        ${LUA_LIBRARIES}
        $<$<BOOL:${XXHASH_USE_STATIC}>:${XXHASH_STATIC_LIB}>
        $<$<NOT:$<BOOL:${XXHASH_USE_STATIC}>>:PkgConfig::XXHASH>)

# 将安装时的 lib 目录作为编译期宏传入，用于运行时拼接 bpf 对象路径。
# 注意：RPM spec 必须使用相同的 %{_libdir} 安装 bpf_obj 文件以保持路径一致。
target_compile_definitions(JobLens PRIVATE JOBLENS_INSTALL_LIBDIR="${CMAKE_INSTALL_LIBDIR}")

# ---- Packaging (install / systemd / CPack) ----
include(${CMAKE_CURRENT_SOURCE_DIR}/scripts/cmake/Packaging.cmake)
