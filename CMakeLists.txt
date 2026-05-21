# ===============  1. 基础要求 ===============
cmake_minimum_required(VERSION 3.16)
project(JobLens
        VERSION 0.0.21
        DESCRIPTION "High-performance job profiler"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_BUILD_TYPE RelWithDebInfo)



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
  set(BUILD_ID "BuildByRPM")
endif()

string(TIMESTAMP BUILD_TIME UTC)

configure_file(
  ${CMAKE_SOURCE_DIR}/include/common/version.h.in
  ${CMAKE_SOURCE_DIR}/include/common/version.h
  @ONLY)

set(PROFILER_INC_DIR "${CMAKE_SOURCE_DIR}/include")
set(PROFILER_SRC_DIR "${CMAKE_SOURCE_DIR}/src")


# ===============  2. 自动装系统依赖 ===============
# 该模块若不需要，可传 -DJOBLENS_INSTALL_SYSTEM_DEPS=OFF 关闭
option(JOBLENS_INSTALL_SYSTEM_DEPS
       "Auto-install system packages (libbpf-dev, clang ...)" OFF)
if(JOBLENS_INSTALL_SYSTEM_DEPS)
    # 失败时不中断，仅提示
    include(${CMAKE_CURRENT_SOURCE_DIR}/scripts/install-deps.cmake OPTIONAL
            RESULT_VARIABLE _install_script)
    if(NOT _install_script)
        message(STATUS "install-deps.cmake not found, skip system-deps installation")
    endif()
endif()

# ===============  3. 查找必备工具 ===============
find_program(CLANG clang)
find_package(PkgConfig REQUIRED)

# ===============  4. 系统库 ===============

pkg_check_modules(LIBBPF REQUIRED libbpf)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

# libelf
find_path(ELF_INCLUDE_DIR elf.h REQUIRED)
find_library(ELF_LIBRARY NAMES elf REQUIRED)
add_library(ELF::ELF INTERFACE IMPORTED)
set_target_properties(ELF::ELF PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${ELF_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${ELF_LIBRARY}")

find_package(Lua 5.4 REQUIRED)


pkg_check_modules(XXHASH REQUIRED IMPORTED_TARGET libxxhash)


# 其他可选系统库
find_library(SASL2_LIB NAMES sasl2 libsasl2)
find_library(ZSTD_LIB NAMES zstd libzstd)
find_library(LZ4_LIB  NAMES lz4  liblz4)
find_library(RDKAFKA_LIB NAMES rdkafka)
find_library(RDKAFKAPP_LIB NAMES rdkafka++)

# ===============  5. 第三方 C++ 库 ===============
option(JOBLENS_AUTO_DEPS "Auto-download missing 3rd-party libs" ON)
include(FetchContent)
macro(fetch_if_missing pkg url)
    if(NOT ${pkg}_FOUND AND JOBLENS_AUTO_DEPS)
        FetchContent_Declare(${pkg} GIT_REPOSITORY ${url})
        set(BUILD_DATE_INSTALL OFF CACHE INTERNAL "")
        FetchContent_MakeAvailable(${pkg})
    endif()
endmacro()

macro(fetch_interface_if_missing pkg url)
    if(NOT ${pkg}_FOUND AND JOBLENS_AUTO_DEPS)
            FetchContent_Declare(${pkg} GIT_REPOSITORY ${url})
            FetchContent_GetProperties(date)
            if(NOT ${pkg}_POPULATED)
                FetchContent_Populate(${pkg})
            endif()
    endif()
endmacro()

find_package(fmt QUIET)
if(fmt_FOUND AND NOT TARGET fmt::fmt)
    add_library(fmt::fmt ALIAS fmt)
endif()
# fetch_if_missing(fmt https://code.ihep.ac.cn/wangzhenyuan/fmt.git)

pkg_check_modules(NETLINK  REQUIRED libnl-3.0 libnl-genl-3.0)

find_package(spdlog REQUIRED)  
# fetch_if_missing(spdlog https://code.ihep.ac.cn/wangzhenyuan/spdlog.git)

find_package(yaml-cpp REQUIRED) 
if(NOT TARGET yaml-cpp::yaml-cpp AND TARGET yaml-cpp)
    add_library(yaml-cpp::yaml-cpp ALIAS yaml-cpp)
endif()
# fetch_if_missing(yaml-cpp https://code.ihep.ac.cn/wangzhenyuan/yaml-cpp.git)

find_package(date QUIET)     
fetch_interface_if_missing(date https://code.ihep.ac.cn/wangzhenyuan/date.git)
add_library(date_interface INTERFACE)
target_include_directories(date_interface INTERFACE
        ${date_SOURCE_DIR}/include)


find_package(CURL REQUIRED) 

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

# 排除SQLiteCpp的脚本文件，避免CMake错误
set(SQLITECPP_INCLUDE_SCRIPT OFF CACHE BOOL "Include SQLiteCpp config & script files" FORCE)

# 禁用SQLiteCpp安装，避免在make install时将SQLiteCpp文件安装到系统目录
set(SQLITECPP_INSTALL OFF CACHE BOOL "Disable SQLiteCpp installation" FORCE)

# 禁用SQLite3安装，避免在make install时将sqlite3.h和libsqlite3.a文件安装到系统目录
set(SQLITE3_INSTALL OFF CACHE BOOL "Disable SQLite3 installation" FORCE)

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/thirdparty/SQLiteCpp)


# ===============  6. eBPF 编译 ===============
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

# ===============  7. 主可执行 ===============
file(GLOB_RECURSE PROFILER_SOURCES CONFIGURE_DEPENDS
     "${CMAKE_SOURCE_DIR}/src/*.cpp")
list(FILTER PROFILER_SOURCES EXCLUDE REGEX ".*/ebpf/.*")

add_executable(JobLens ${PROFILER_SOURCES})
add_dependencies(JobLens ebpf_objects)

# -------------- 包含目录 & 链接库 -----------------
target_include_directories(JobLens
    PUBLIC
        ${PROFILER_INC_DIR}
        ${NETLINK_INCLUDE_DIRS}
        /usr/include/libnl3
        ELF::ELF
        ZLIB::ZLIB
        fmt::fmt 
        spdlog::spdlog
        date::date
        yaml-cpp::yaml-cpp
        PkgConfig::CURL
        RdKafka::rdkafka++
        OpenSSL::SSL
        OpenSSL::Crypto
        ZLIB::ZLIB
        Boost::boost
        ${CMAKE_CURRENT_LIST_DIR}/thirdparty/include
        lua5.4
        PkgConfig::XXHASH
)

target_link_libraries(JobLens
    PRIVATE
        ELF::ELF
        ZLIB::ZLIB
        OpenSSL::SSL OpenSSL::Crypto
        Threads::Threads
        ${LIBBPF_LIBRARIES}
        ELF::ELF
        ZLIB::ZLIB
        fmt::fmt
        spdlog::spdlog
        yaml-cpp::yaml-cpp
        CURL::libcurl
        RdKafka::rdkafka++
        Boost::boost
        date_interface
        # $<$<TARGET_EXISTS:RdKafka::rdkafka++>:RdKafka::rdkafka++>
        ${SASL2_LIB} ${ZSTD_LIB} ${LZ4_LIB}
        ${NETLINK_LIBRARIES}
        SQLiteCpp
        sqlite3
        pthread
        dl
        lua5.4
        PkgConfig::XXHASH
)

install(TARGETS JobLens
        RUNTIME DESTINATION bin)

# ===============  8. 安装 & 运行时依赖收集 ===============
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "High-performance job monitor")
set(CPACK_PACKAGE_VENDOR "nowzycc")
set(CPACK_PACKAGE_CONTACT "joblens@example.com")
# set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
# set(CPACK_RESOURCE_FILE_README  "${CMAKE_SOURCE_DIR}/README.md")

# 生成 tar.gz + deb + rpm
set(CPACK_GENERATOR "TGZ;DEB;RPM")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "")   # 自带 .so，留空
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_CONTACT}")

# 11-1 准备 staging 目录
set(DEPS_STAGING_DIR "${CMAKE_BINARY_DIR}/runtime_deps")  # 临时收集目录
file(MAKE_DIRECTORY "${DEPS_STAGING_DIR}")

# 11-2 获取目标所有运行时依赖（排除系统白名单）
add_custom_target(collect_deps ALL
    COMMAND ${CMAKE_COMMAND} -E env
        LD_LIBRARY_PATH=${DEPS_STAGING_DIR}:$ENV{LD_LIBRARY_PATH}
        bash "${CMAKE_SOURCE_DIR}/scripts/collect_libs.sh"
            $<TARGET_FILE:JobLens>          # 主可执行文件
            ${DEPS_STAGING_DIR}             # 输出目录
    COMMENT "Collecting runtime .so ..."
    VERBATIM
)

# 11-3 把收集到的 .so 安装到 lib/joblens 目录，并自动加入包
install(DIRECTORY ${DEPS_STAGING_DIR}/
        DESTINATION lib/joblens
        FILES_MATCHING PATTERN "*.so*")

install(CODE "
    message(STATUS 'Removing redundant date headers/cmake files ...')
    file(REMOVE_RECURSE
        \${CMAKE_INSTALL_PREFIX}/include/date
        \${CMAKE_INSTALL_PREFIX}/lib64/cmake/date
        \${CMAKE_INSTALL_PREFIX}/lib/cmake/date)
")

install(DIRECTORY ${CMAKE_SOURCE_DIR}/build/bpf_obj/
        DESTINATION bin/bpf_obj/
        FILES_MATCHING PATTERN "*.bpf.o")

# 11-4 让可执行文件优先找同目录 lib/joblens 下的 .so
#     注意：必须在 install() 之后，否则 install(TARGETS) 会覆盖 rpath
set_target_properties(JobLens PROPERTIES
    INSTALL_RPATH "$ORIGIN/../lib/joblens"   # 相对可执行文件 bin/JobLens -> ../lib/joblens
)

# ----------------------------------------------
# RPM包配置
# ----------------------------------------------
set(CPACK_RPM_PACKAGE_NAME "joblens")
set(CPACK_RPM_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_RPM_PACKAGE_RELEASE "1")
set(CPACK_RPM_PACKAGE_SUMMARY "High-performance job monitoring and profiling system")
set(CPACK_RPM_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})
set(CPACK_RPM_PACKAGE_LICENSE "Apache-2.0")
set(CPACK_RPM_PACKAGE_GROUP "System Environment/Daemons")
set(CPACK_RPM_PACKAGE_URL "https://github.com/nowzycc/JobLens")
set(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
set(CPACK_RPM_PACKAGE_MAINTAINER ${CPACK_PACKAGE_CONTACT})

# RPM依赖
set(CPACK_RPM_PACKAGE_REQUIRES "libbpf >= 1.0, libnl3 >= 3.7, openssl >= 1.1, lua >= 5.4, libcurl, librdkafka, boost")

# RPM构建依赖
set(CPACK_RPM_BUILDREQUIRES "cmake >= 3.16, gcc-c++, libbpf-devel, libnl3-devel, openssl-devel, zlib-devel, lua-devel, clang, libcurl-devel, librdkafka-devel, boost-devel, sqlite-devel, elfutils-libelf-devel, xxhash-devel, spdlog-devel, yaml-cpp-devel, fmt-devel")



# ----------------------------------------------
# 12. 启用 CPack
# ----------------------------------------------
include(CPack)
