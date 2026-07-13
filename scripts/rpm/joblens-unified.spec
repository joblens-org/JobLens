# spec 文件：joblens（统一包）
# JobLens Core Agent + Trigger Gateway 统一 RPM 打包
#
# 策略：
#   - Core (C++17)：cmake --install 安装 JobLens 二进制、eBPF .bpf.o、配置文件
#   - Trigger (Python)：pip + venv 安装到 /usr/lib/joblens/trigger-venv/，与系统 Python 隔离
#   - 统一 RPM 取代旧的 joblens (CPack core) + joblens-trigger (spec trigger) 双包
#
# 注意事项：
#   - eBPF .bpf.o 文件非标准 ELF，brp-strip 会破坏它 → 需要 %%define __strip /bin/true
#   - Python venv 的 pip install 会在 shebang 中写入 buildroot 路径 → 需要 sed 修复
#   - 旧 joblens-trigger 包通过 Obsoletes/Provides 实现平滑升级
#   - 旧 trigger 有 Requires: joblens，统一后移除自引用
#
# 参考：
#   - scripts/cmake/Packaging.cmake（core install 规则）
#   - .omo/notepads/unified-rpm-packaging/ownership-map.md（路径清单）
#   - Fedora Packaging Guidelines: Bundling
#   - rpm-macros-virtualenv (kushaldas/rpm-macros-virtualenv)

%global debug_package %{nil}

# ---- venv auto-dependency suppression ----
# 抑制 RPM 自动依赖检测：Python venv 内的所有依赖均为捆绑（bundled），
# 不应由 RPM 的自动依赖扫描器（find-requires/find-provides）处理。
#
# __requires_exclude       — 抑制 python3dist(...) 格式的 Requires
# __requires_exclude_from  — 抑制对 venv 目录内 .so 文件的自动 Requires 扫描
# __provides_exclude_from  — 抑制 venv 目录内的自动 Provides
#
# 路径已从旧的 /usr/lib/joblens-trigger/venv/ 更新为统一路径。
%global __requires_exclude ^python3(\\.\\d+)?dist\\(
%global __requires_exclude_from ^/usr/lib/joblens/trigger-venv/.*$
%global __provides_exclude_from ^/usr/lib/joblens/trigger-venv/.*$

# ---- eBPF strip workaround ----
# .bpf.o 文件是 BPF 字节码（非标准 ELF），rpmbuild 的 brp-strip 会破坏它们。
# 用 /bin/true 替换 strip，配合 %%global debug_package %%{nil} 跳过 debuginfo 生成。
%define __strip /bin/true

# ---- Python byte-compile 禁用 ----
# brp-python-bytecompile 会在 check-buildroot 之后运行，
# 生成的 .pyc 文件嵌入 buildroot 路径，且触发 noarch 架构检查。
%undefine __brp_python_bytecompile

# ============================================================
# 包元数据
# ============================================================
Name:           joblens

# 版本号通过 rpmbuild --define "_version X.Y.Z" 动态传入，以 CMakeLists.txt 为准。
# 未传参时回退到占位值 0.0.0（直接 rpmbuild 本地调试用）。
%{!?_version: %define _version 0.0.0 }
Version:        %{_version}

Release:        1%{?dist}
Summary:        JobLens — high-performance job monitoring agent with RESTful trigger gateway

License:        Apache-2.0
URL:            https://github.com/joblens-org/JobLens
Vendor:         nowzycc

# ---- 源码 ----
# Source0: 主源码 tarball（包含 C++ core + Python trigger）
Source0:        JobLens-%{version}.tar.gz
# Source1/2: systemd unit 文件（由构建脚本从 templates 生成）
Source1:        joblens.service
Source2:        joblens-trigger.service

# ---- 旧包替换 ----
# 确保已安装的 joblens-trigger RPM 能被本统一包平滑升级替换。
# Obsoletes: 声明本包替换旧包（版本比较用 <）
# Provides:  声明本包提供旧的包名（版本匹配用 =），满足其他包对 joblens-trigger 的依赖
Obsoletes:      joblens-trigger < %{version}-%{release}
Provides:       joblens-trigger = %{version}-%{release}

# ============================================================
# 构建依赖
# ============================================================
# --- C++ core 构建依赖 ---
# 来源：scripts/cmake/Packaging.cmake CPACK_RPM_BUILDREQUIRES
#       + CMakeLists.txt 中的额外构建工具
BuildRequires:  cmake >= 3.25
BuildRequires:  gcc-c++
BuildRequires:  clang
BuildRequires:  bpftool
BuildRequires:  systemd-rpm-macros
BuildRequires:  libbpf-devel
BuildRequires:  libnl3-devel
BuildRequires:  openssl-devel
BuildRequires:  zlib-devel
BuildRequires:  lua-devel
BuildRequires:  libcurl-devel
BuildRequires:  librdkafka-devel
BuildRequires:  boost-devel
BuildRequires:  leveldb-devel
BuildRequires:  elfutils-libelf-devel
BuildRequires:  xxhash-devel
BuildRequires:  spdlog-devel
BuildRequires:  yaml-cpp-devel
BuildRequires:  fmt-devel
# date-devel: Howard Hinnant date library — 在 EPEL 9 等发行版中不可用，
# 需通过 scripts/install-deps.sh 从源码安装。不在 RPM BuildRequires 中声明，
# 避免 rpmbuild 在依赖检查时失败。
# BuildRequires:  date-devel
BuildRequires:  nlohmann_json-devel

# --- Python trigger 构建依赖 ---
BuildRequires:  python3 >= 3.8
BuildRequires:  python3-pip

# ============================================================
# 运行时依赖
# ============================================================
# 注意：不要包含 Requires: joblens（统一后无自引用）。
# C++ 动态库的 .so 依赖由 rpmbuild 自动检测（find-requires 扫描 ELF NEEDED）。
# Python 依赖全部打包在 venv 中（通过 auto-dep suppression 抑制）。
#
# systemd 用于服务管理；python3 提供 venv 运行时解释器。
Requires:       systemd
Requires:       python3 >= 3.8

# ============================================================
# 描述
# ============================================================
%description
JobLens is a high-performance distributed job monitoring and performance
analysis agent for HTC/HPC environments.

This unified RPM package contains:
  - JobLens Core Agent (C++17) — eBPF-based job monitoring, system profiling,
    and performance data collection daemon.
  - JobLens Trigger Gateway (Python/Flask) — RESTful API service providing
    HTTP interface for job management, configuration, metrics export, and
    system administration.

The Trigger service runs as a Python virtual environment at
/usr/lib/joblens/trigger-venv/ for complete isolation from system Python
packages. All eBPF object files (*.bpf.o) are installed to
/usr/lib/joblens/bpf_obj/.

# ============================================================
# %%prep — 源码准备
# ============================================================
%prep
# 解压主源码 tarball (Source0)
%setup -q -n JobLens-%{version}

# 复制预生成的 systemd service 文件到构建目录
# Source1 (joblens.service) 由 CMake 在 %%build 中从 .in 模板生成，
# 此处提供的 Source1 作为 fallback（如果 cmake 未生成则使用此文件）
cp %{SOURCE1} . 2>/dev/null || :
cp %{SOURCE2} . 2>/dev/null || :

# ============================================================
# %%build — 构建
# ============================================================
%build
# ===== C++ core 构建（全部配置来自 CMakePresets.json）=====
# %{preset} 由 rpmbuild --define "preset <name>" 传入，默认 rpm-system-deps
cmake --preset %{?preset}%{!?preset:rpm-system-deps}
cmake --build --preset %{?preset}%{!?preset:rpm-system-deps}

# ===== Python trigger：纯 Python 项目 =====
# pip install 在 %%install 中直接处理（与旧 trigger spec 策略一致）

# ============================================================
# %%install — 安装
# ============================================================
%install
# ===== Trigger 安装 =====

# 8. 创建 Python 虚拟环境（新路径：/usr/lib/joblens/trigger-venv/）
#    不使用 --copies：复制 Python 解释器会导致 ELF build-id 与系统 python3 包冲突
#    （RPM find-debuginfo.sh 会为复制的 python 二进制生成 .build-id 条目）
#    使用默认 symlink 模式：解释器符号链接到系统 /usr/bin/python3
#    Requires: python3 >= 3.8 确保运行时解释器一定存在
%{__python3} -m venv %{buildroot}/usr/lib/joblens/trigger-venv

# 9. 安装运行时 Python 依赖 + trigger 包本身
#    --no-compile: 不生成 .pyc，避免嵌入 buildroot 路径被 check-buildroot 拦截
%{buildroot}/usr/lib/joblens/trigger-venv/bin/pip install \
    --no-input --no-cache-dir --no-compile \
    -r trigger/requirements.txt trigger/

# 10. 修复 venv 中硬编码的 buildroot 路径
#     pip 安装的入口脚本（gunicorn、flask 等）的 shebang 包含 buildroot 路径
#     activate 脚本的 VIRTUAL_ENV 变量也包含 buildroot 路径
#     RPM 的 check-buildroot 不允许已安装文件中出现 buildroot 路径
find %{buildroot}/usr/lib/joblens/trigger-venv/bin -type f | while read f; do
    if head -1 "$f" 2>/dev/null | grep -q "^#!.*%{buildroot}"; then
        sed -i "1s|%{buildroot}||" "$f"
    fi
done
for activate in %{buildroot}/usr/lib/joblens/trigger-venv/bin/activate*; do
    [ -f "$activate" ] && sed -i "s|%{buildroot}||g" "$activate"
done

# 10b. 修复 pyvenv.cfg 中的 buildroot 路径
#      check-buildroot 不允许已安装文件中出现 buildroot 路径
sed -i "s|%{buildroot}||g" %{buildroot}/usr/lib/joblens/trigger-venv/pyvenv.cfg

# 11. 清理 pip 可能遗留的 .pyc 和 __pycache__（仍可能包含 buildroot 路径）
find %{buildroot}/usr/lib/joblens/trigger-venv -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
find %{buildroot}/usr/lib/joblens/trigger-venv -name '*.pyc' -delete 2>/dev/null || true

# 12. 安装 Trigger 配置文件
install -d -m 755 %{buildroot}/etc/JobLens/trigger
install -m 644 trigger/config.example.yaml %{buildroot}/etc/JobLens/trigger/
install -m 644 trigger/gunicorn.conf.py %{buildroot}/etc/JobLens/trigger/
install -m 644 trigger/config.example.yaml %{buildroot}/etc/JobLens/trigger/config.yaml

# 13. 安装 Trigger systemd unit
# 先创建目录（core 的 cmake --install 尚未运行）
install -d -m 755 %{buildroot}%{_unitdir}
install -m 644 joblens-trigger.service %{buildroot}%{_unitdir}/joblens-trigger.service

# 14. Trigger 运行时状态目录
install -d -m 755 %{buildroot}%{_sharedstatedir}/joblens

# ===== Core 安装（对应原 Packaging.cmake 逻辑）=====

# 15. cmake --install：安装 JobLens 二进制 + eBPF 对象 + 配置文件 + systemd service
#     DESTDIR=%%{buildroot} 确保所有产物写入 RPM buildroot
#     使用 CMakePresets 中预设定义的构建目录（build/）
DESTDIR=%{buildroot} cmake --install build --prefix %{_prefix}

# 16. 验证 eBPF 对象文件已由 cmake --install 安装到位
#     cmake --install 通过 Packaging.cmake 的 install(DIRECTORY ...) 安装 .bpf.o
#     到 ${CMAKE_INSTALL_LIBDIR}/joblens/bpf_obj/（64-bit 系统为 lib64）。
#     路径必须与 CMakeLists.txt 的 JOBLENS_INSTALL_LIBDIR 编译宏一致，
#     否则运行时 JobLens 无法找到 eBPF 程序。
if ! ls %{buildroot}%{_libdir}/joblens/bpf_obj/*.bpf.o >/dev/null 2>&1; then
    echo "FATAL: eBPF 对象文件未在预期路径找到"
    echo "  预期: %{buildroot}%{_libdir}/joblens/bpf_obj/*.bpf.o"
    echo "  提示: 检查 Packaging.cmake 的 install(DIRECTORY ... DESTINATION ...) 配置"
    exit 1
fi
echo "✓ eBPF 对象文件已安装到 %{_libdir}/joblens/bpf_obj/"
# 清理 cmake --install 可能遗留到 /lib/systemd/ 的文件（Fedora usrmerge 兼容）
rm -rf %{buildroot}/lib/systemd 2>/dev/null || true

# 17. 安装 core 配置文件（确保权限正确）
#     cmake --install 已安装 config.example.yaml → /etc/JobLens/config.yaml，
#     此处显式安装以明确意图并确保正确的文件权限
install -d -m 755 %{buildroot}%{_sysconfdir}/JobLens
install -m 644 config/config.example.yaml %{buildroot}%{_sysconfdir}/JobLens/config.yaml

# 18. 安装 core systemd unit（cmake --install 已安装到 %%{_unitdir}，此处显式确认）
install -d -m 755 %{buildroot}%{_unitdir}
install -m 644 build/joblens.service %{buildroot}%{_unitdir}/joblens.service

# 19. 安装 systemd preset（cmake --install 已生成并安装 90-joblens.preset）
install -d -m 755 %{buildroot}/usr/lib/systemd/system-preset
install -m 644 build/90-joblens.preset %{buildroot}/usr/lib/systemd/system-preset/90-joblens.preset

# 20. 创建 core 运行时目录
#     /var/JobLens/ — core 守护进程工作目录（WorkingDirectory）
#     /var/JobLens/node_pids/ — PID 跟踪数据
install -d -m 755 %{buildroot}/var/JobLens
install -d -m 755 %{buildroot}/var/JobLens/node_pids

# 21. 清理冗余文件（原 Packaging.cmake install(CODE) 逻辑：
#     防止 cmake 将开发头文件和 cmake 模块安装到系统目录）
rm -rf %{buildroot}%{_includedir}/date 2>/dev/null || :
rm -rf %{buildroot}%{_libdir}/cmake/date 2>/dev/null || :

# ============================================================
# %%files — 文件清单
# ============================================================
%files
%defattr(-,root,root,-)

# ===== 二进制 & 库 =====
/usr/bin/JobLens
%dir %{_libdir}/joblens/
%if 0%{?with_bundled_libs}
%{_libdir}/joblens/*.so*
%endif
%dir %{_libdir}/joblens/bpf_obj/
%{_libdir}/joblens/bpf_obj/*.bpf.o

# ===== Python venv（整个目录树，架构无关，固定 /usr/lib/）=====
/usr/lib/joblens/trigger-venv/

# ===== 配置文件 =====
%config(noreplace) %{_sysconfdir}/JobLens/config.yaml
%config(noreplace) %{_sysconfdir}/JobLens/trigger/config.yaml
%{_sysconfdir}/JobLens/trigger/config.example.yaml
%config(noreplace) %{_sysconfdir}/JobLens/trigger/gunicorn.conf.py

# ===== systemd unit 文件 =====
%{_unitdir}/joblens.service
%{_unitdir}/joblens-trigger.service

# ===== systemd preset =====
/usr/lib/systemd/system-preset/90-joblens.preset

# ===== 运行时目录（%%dir 声明，不含内容）=====
%dir /var/JobLens/
%dir /var/JobLens/node_pids/
%dir %{_sharedstatedir}/joblens

# ===== 文档 & 许可证 =====
%doc README.md
%license LICENSE

# ============================================================
# %%pre — 安装前脚本
# ============================================================
%pre
# 来源：scripts/rpm/preinstall.sh（core）+ trigger 配置备份扩展
# $1 = 1 (首次安装) / 2 (升级)
#
# 升级时将现有配置文件移出 RPM 视线，
# 防止 RPM 因数据库记录"未修改"而覆盖用户配置。
# %%post 脚本会将备份恢复（如内容不同则保留旧版）。

if [ -x /usr/bin/systemctl ]; then
    if /usr/bin/systemctl list-unit-files joblens-trigger.service >/dev/null 2>&1 || \
       /usr/bin/systemctl status joblens-trigger.service >/dev/null 2>&1; then
        /usr/bin/systemctl stop joblens-trigger.service >/dev/null 2>&1 || :
        /usr/bin/systemctl disable joblens-trigger.service >/dev/null 2>&1 || :
        /usr/bin/systemctl reset-failed joblens-trigger.service >/dev/null 2>&1 || :
        /usr/bin/systemctl daemon-reload >/dev/null 2>&1 || :
    fi
fi

if [ "$1" -eq 2 ]; then
    # core 配置备份
    if [ -f %{_sysconfdir}/JobLens/config.yaml ]; then
        mv %{_sysconfdir}/JobLens/config.yaml %{_sysconfdir}/JobLens/config.yaml.rpmorig
    fi
    # trigger 配置备份
    if [ -f %{_sysconfdir}/JobLens/trigger/config.yaml ]; then
        mv %{_sysconfdir}/JobLens/trigger/config.yaml %{_sysconfdir}/JobLens/trigger/config.yaml.rpmorig
    fi
    if [ -f %{_sysconfdir}/JobLens/trigger/gunicorn.conf.py ]; then
        mv %{_sysconfdir}/JobLens/trigger/gunicorn.conf.py %{_sysconfdir}/JobLens/trigger/gunicorn.conf.py.rpmorig
    fi
fi

exit 0

# ============================================================
# %%post — 安装后脚本
# ============================================================
%post
# 来源：scripts/rpm/postinstall.sh（core）+ trigger systemd_post
# $1 = 1 (首次安装) / 2 (升级)

# 清理可能残留的锁文件
rm -f /var/JobLens/JobLens.lock

# ---- 升级时恢复配置备份 ----
# %%pre 脚本将旧配置重命名为 *.rpmorig，此处将备份恢复
# 注意：保存 RPM 脚本参数 $1 到变量，避免函数内参数遮蔽
rpm_args="$1"
restore_config() {
    local cfg_path="$1"
    local bak_path="${cfg_path}.rpmorig"
    local new_path="${cfg_path}.rpmnew"

    if [ "$rpm_args" -eq 2 ] && [ -f "$bak_path" ]; then
        if cmp -s "$bak_path" "$cfg_path" 2>/dev/null; then
            # 内容相同，删除备份即可
            rm -f "$bak_path"
        else
            # 内容不同（用户修改过），保留旧配置，新版本存为 .rpmnew
            mv "$cfg_path" "$new_path"
            mv "$bak_path" "$cfg_path"
        fi
    fi
}

restore_config %{_sysconfdir}/JobLens/config.yaml
restore_config %{_sysconfdir}/JobLens/trigger/config.yaml
restore_config %{_sysconfdir}/JobLens/trigger/gunicorn.conf.py

# systemd 操作
if [ -x /usr/bin/systemctl ]; then
    /usr/bin/systemctl daemon-reload >/dev/null 2>&1 || :
fi

if [ "$1" -eq 1 ] && [ -x /usr/bin/systemctl ]; then
    # 首次安装：preset 启用 core 服务（由 preset 文件决定是否 auto-start）
    /usr/bin/systemctl preset joblens.service >/dev/null 2>&1 || :
elif [ "$1" -eq 2 ] && [ -x /usr/bin/systemctl ]; then
    # 升级：尝试重启 core 服务
    /usr/bin/systemctl try-restart joblens.service >/dev/null 2>&1 || :
fi

# trigger 服务的 systemd post 逻辑（首次安装 enable，升级不操作）
%systemd_post joblens-trigger.service

exit 0

# ============================================================
# %%preun — 卸载前脚本
# ============================================================
%preun
# 来源：scripts/rpm/preuninstall.sh（core）+ trigger systemd_preun
# $1 = 0 (完全卸载) / 1 (升级)

if [ -x /usr/bin/systemctl ]; then
    if [ "$1" -eq 0 ]; then
        # 完全卸载：先 disable（下次开机不再启动），再停止当前实例
        /usr/bin/systemctl --no-reload disable joblens.service >/dev/null 2>&1 || :
        /usr/bin/systemctl stop joblens.service >/dev/null 2>&1 || :
    elif [ "$1" -eq 1 ]; then
        # 升级：停止旧版本，%%post 中 try-restart 将启动新版本
        /usr/bin/systemctl stop joblens.service >/dev/null 2>&1 || :
    fi
fi

# trigger 服务的 preun 逻辑
%systemd_preun joblens-trigger.service

exit 0

# ============================================================
# %%postun — 卸载后脚本
# ============================================================
%postun
# 来源：scripts/rpm/postuninstall.sh（core）+ trigger systemd_postun_with_restart
# $1 = 0 (完全卸载) / 1 (升级)

if [ "$1" -eq 0 ] && [ -x /usr/bin/systemctl ]; then
    # 完全卸载后：重新加载 systemd 配置
    /usr/bin/systemctl daemon-reload >/dev/null 2>&1 || :
fi

# core 服务的 postun 逻辑：升级时 restart
%systemd_postun_with_restart joblens.service

# trigger 服务的 postun 逻辑
%systemd_postun_with_restart joblens-trigger.service

exit 0

# ============================================================
# %%changelog
# ============================================================
%changelog
* Sat Jun 20 2026 JobLens Team <joblens@example.com> - 0.1.0-1
- 创建统一 RPM spec 骨架，合并 core (CPack) + trigger (spec) 打包
- 重命名 venv 路径：/usr/lib/joblens-trigger/venv/ → /usr/lib/joblens/trigger-venv/
- 添加 Obsoletes/Provides 实现旧 joblens-trigger 包平滑升级
- 统一路径为硬编码 /usr/lib/joblens/，消除 CMAKE_INSTALL_LIBDIR 歧义
- 实现 %%prep/%%build 段：cmake + Ninja 构建 C++ core
- 实现 %%install core 段：cmake --install + eBPF + config + systemd + preset + runtime dirs
- 更新 %%files 段：声明全部 core 产物路径（binary、eBPF、config、unit、preset、dirs）
- 实现 %%pre/%%post/%%preun/%%postun 脚本段：core + trigger 双服务的完整生命周期管理
