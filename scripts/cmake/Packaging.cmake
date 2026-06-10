# ============================================================
# Packaging.cmake — 安装规则、systemd 集成、CPack 打包配置
# ============================================================
# 本文件在 CMakeLists.txt 末尾 include，所有 target 已定义完毕

# ---- systemd 检测 ----
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_get_variable(SYSTEMD_UNIT_DIR systemd systemdsystemunitdir)
endif()
if(NOT SYSTEMD_UNIT_DIR)
    if(EXISTS "/lib/systemd/system")
        set(SYSTEMD_UNIT_DIR "/lib/systemd/system")
    else()
        set(SYSTEMD_UNIT_DIR "/usr/lib/systemd/system")
    endif()
endif()
message(STATUS "systemd unit dir: ${SYSTEMD_UNIT_DIR}")

# ---- systemd 服务文件 ----
# 自包含模式：通过 LD_LIBRARY_PATH 优先加载打包的 .so
# 系统依赖模式：不设置，由系统 ldconfig 处理
if(JOBLENS_BUNDLE_LIBS)
    set(JOBLENS_SERVICE_ENV "Environment=LD_LIBRARY_PATH=${CMAKE_INSTALL_FULL_LIBDIR}/joblens")
else()
    set(JOBLENS_SERVICE_ENV "# LD_LIBRARY_PATH not set — using system ld.so.cache")
endif()

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/init/joblens.service.in
    ${CMAKE_CURRENT_BINARY_DIR}/joblens.service
    @ONLY)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/joblens.service
        DESTINATION ${SYSTEMD_UNIT_DIR})

get_filename_component(SYSTEMD_PRESET_DIR
    "${SYSTEMD_UNIT_DIR}/../system-preset" ABSOLUTE)
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/90-joblens.preset
    "enable joblens.service\n")
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/90-joblens.preset
        DESTINATION ${SYSTEMD_PRESET_DIR})

# ---- 安装规则 ----
install(TARGETS JobLens
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# 运行时 .so 收集与 RPATH（仅传统模式）
if(JOBLENS_BUNDLE_LIBS)
    set(DEPS_STAGING_DIR "${CMAKE_BINARY_DIR}/runtime_deps")
    file(MAKE_DIRECTORY ${DEPS_STAGING_DIR})

    add_custom_target(collect_deps ALL
        COMMAND ${CMAKE_COMMAND} -E env
            LD_LIBRARY_PATH=${DEPS_STAGING_DIR}:$ENV{LD_LIBRARY_PATH}
            bash "${CMAKE_SOURCE_DIR}/scripts/collect_libs.sh"
                $<TARGET_FILE:JobLens>
                ${DEPS_STAGING_DIR}
        COMMENT "Collecting runtime .so ..."
        VERBATIM)

    install(DIRECTORY ${DEPS_STAGING_DIR}/
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/joblens
            FILES_MATCHING PATTERN "*.so*")

    set_target_properties(JobLens PROPERTIES
        INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}/joblens")
endif()

# eBPF 对象文件（非标准 ELF，放到 /usr/lib/joblens/）
install(DIRECTORY ${BPF_OBJECT_DIR}/
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/joblens/bpf_obj/
        FILES_MATCHING PATTERN "*.bpf.o")

# 配置文件
install(FILES ${CMAKE_SOURCE_DIR}/config/config.example.yaml
        DESTINATION /etc/JobLens
        RENAME config.yaml)

# 运行时目录
install(DIRECTORY DESTINATION /var/JobLens
        DIRECTORY_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
install(DIRECTORY DESTINATION /var/JobLens/node_pids
        DIRECTORY_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)

# 清理 date 库的冗余安装产物
install(CODE "
    message(STATUS 'Removing redundant date headers/cmake files ...')
    file(REMOVE_RECURSE
        \"\${CMAKE_INSTALL_PREFIX}/include/date\"
        \"\${CMAKE_INSTALL_PREFIX}/\${CMAKE_INSTALL_LIBDIR}/cmake/date\")
")

# ---- CPack 通用配置 ----
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "High-performance job monitor")
set(CPACK_PACKAGE_VENDOR "nowzycc")
set(CPACK_PACKAGE_CONTACT "joblens@example.com")
set(CPACK_SET_DESTDIR ON)

set(CPACK_GENERATOR "TGZ;DEB;RPM")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_CONTACT}")

# ---- CPack RPM 配置 ----
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

# ---- RPM 运行时依赖 ----
# 自包含模式 vs 系统依赖模式
if(JOBLENS_BUNDLE_LIBS)
    # 自包含模式：.so 已打包进 RPM，RPATH 优先加载自带版本
    # RPM Requires 最小化，适合无网络离线部署
    set(CPACK_RPM_PACKAGE_REQUIRES "systemd")
else()
    # 系统依赖模式：所有动态库由 RPM 包管理器提供（含 EPEL）
    set(CPACK_RPM_PACKAGE_REQUIRES
        "systemd, libbpf >= 1.0, libnl3 >= 3.7, openssl-libs >= 1.1, lua-libs >= 5.4, libcurl, librdkafka, zlib, cyrus-sasl-lib, libzstd, lz4-libs, elfutils-libelf, leveldb, xxhash-libs")
endif()
message(STATUS "RPM requires: ${CPACK_RPM_PACKAGE_REQUIRES}")
set(CPACK_RPM_BUILDREQUIRES "cmake >= 3.25, gcc-c++, systemd, libbpf-devel, libnl3-devel, openssl-devel, zlib-devel, lua-devel, clang, libcurl-devel, librdkafka-devel, boost-devel, leveldb-devel, elfutils-libelf-devel, xxhash-devel, spdlog-devel, yaml-cpp-devel, fmt-devel")

set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/rpm/postinstall.sh)
set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/rpm/preuninstall.sh)
set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/scripts/rpm/postuninstall.sh)

# RPM brp-strip 无法处理 eBPF .bpf.o（非标准 ELF 字节码），禁用 strip
set(CPACK_RPM_SPEC_MORE_DEFINE "%define __strip /bin/true")

include(CPack)
