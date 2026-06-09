# spec 文件：joblens-trigger
# JobLens Trigger 服务 RPM 打包
#
# 策略：采用 Python 虚拟环境 (virtualenv) 方案，所有 Python 依赖安装到
#       /usr/lib/joblens-trigger/venv/，与系统 Python 完全隔离。
#       这是 Fedora 打包指南推荐的「捆绑 (bundling)」模式，
#       适用于依赖包在 EL9 EPEL 中不可用的场景。
#
# 注意：未使用 BuildArch: noarch，因为 grpcio/psutil 等包含架构相关的
#       预编译 C 扩展（.so 文件），必须构建为架构相关包。
#
# 参考：
#   - Fedora Packaging Guidelines: Bundling
#   - AWX RPM 打包实践 (MrMEEE/awx-build)
#   - rpm-macros-virtualenv (kushaldas/rpm-macros-virtualenv)
%global debug_package %{nil}

# 全局抑制 RPM 自动依赖检测：
#   __requires_exclude      — 抑制 python3dist(...) 格式的 Requires
#   __requires_exclude_from — 抑制对 venv 目录内 .so 文件的自动扫描
#   __provides_exclude_from — 抑制 venv 目录内的自动 Provides
%global __requires_exclude ^python3(\\.\\d+)?dist\\(
%global __requires_exclude_from ^/usr/lib/joblens-trigger/venv/.*$
%global __provides_exclude_from ^/usr/lib/joblens-trigger/venv/.*$

# 禁用 RPM 自动 Python byte-compile
# 原因：brp-python-bytecompile 会在 check-buildroot 之后运行，
#       生成的 .pyc 文件嵌入 buildroot 路径，且会触发 noarch 架构检查
%undefine __brp_python_bytecompile

%{!?trigger_version: %global trigger_version 0.0.0}

Name:           joblens-trigger
Version:        %{trigger_version}
Release:        1%{?dist}
Summary:        JobLens Trigger Service - RESTful API gateway
License:        MIT
URL:            https://github.com/joblens-org/JobLens
Source0:        JobLens-Trigger-%{version}-Source.tar.gz
Source1:        joblens-trigger.service

# 构建依赖
# python3-pip 提供 pip wheel，ensurepip 用其初始化 venv 中的 pip
BuildRequires:  python3 >= 3.8
BuildRequires:  python3-pip

# ---- 运行时依赖 ----
# Python 包依赖全部打包在 venv 中，此处仅声明系统级依赖
Requires:       python3 >= 3.8
Requires:       joblens >= 0.0.12

%description
JobLens Trigger provides RESTful API endpoints for managing and monitoring
JobLens instances. It includes service registration, job management,
performance metrics, configuration management, and system upgrade capabilities.

All Python dependencies are bundled in a virtual environment at
/usr/lib/joblens-trigger/venv/ for isolation from system Python packages.

%prep
%setup -q -n trigger
cp %{SOURCE1} .

%build
# 纯 Python 项目，pip install 在 %%install 中直接处理

%install
# 1. 创建 Python 虚拟环境
%{__python3} -m venv %{buildroot}/usr/lib/joblens-trigger/venv

# 2. 安装运行时 Python 依赖 + trigger 包本身
#    --no-compile: 不生成 .pyc，避免嵌入 buildroot 路径被 check-buildroot 拦截
%{buildroot}/usr/lib/joblens-trigger/venv/bin/pip install \
    --no-input --no-cache-dir --no-compile \
    -r requirements.txt .

# 3. 修复 venv 中硬编码的 buildroot 路径
#    pip 安装的入口脚本（gunicorn、flask 等）的 shebang 包含 buildroot 路径
#    activate 脚本的 VIRTUAL_ENV 变量也包含 buildroot 路径
#    RPM 的 check-buildroot 不允许已安装文件中出现 buildroot 路径
find %{buildroot}/usr/lib/joblens-trigger/venv/bin -type f | while read f; do
    if head -1 "$f" 2>/dev/null | grep -q "^#!.*%{buildroot}"; then
        sed -i "1s|%{buildroot}||" "$f"
    fi
done
for activate in %{buildroot}/usr/lib/joblens-trigger/venv/bin/activate*; do
    [ -f "$activate" ] && sed -i "s|%{buildroot}||g" "$activate"
done

# 4. 清理 pip 可能遗留的 .pyc 和 __pycache__（仍可能包含 buildroot 路径）
find %{buildroot}/usr/lib/joblens-trigger/venv -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
find %{buildroot}/usr/lib/joblens-trigger/venv -name '*.pyc' -delete 2>/dev/null || true

# 5. 安装配置文件到 /etc/JobLens/trigger/
install -d -m 755 %{buildroot}%{_sysconfdir}/JobLens/trigger
install -m 644 config.example.yaml %{buildroot}%{_sysconfdir}/JobLens/trigger/
install -m 644 gunicorn.conf.py %{buildroot}%{_sysconfdir}/JobLens/trigger/

# 6. 安装 systemd unit
install -d -m 755 %{buildroot}%{_unitdir}
install -m 644 %{SOURCE1} %{buildroot}%{_unitdir}/

# 7. 运行时状态目录
install -d -m 755 %{buildroot}%{_sharedstatedir}/joblens

%files
%defattr(-,root,root,-)
%doc README.md
/usr/lib/joblens-trigger/venv/
%config(noreplace) %{_sysconfdir}/JobLens/trigger/gunicorn.conf.py
%{_sysconfdir}/JobLens/trigger/config.example.yaml
%config(noreplace) %{_sysconfdir}/JobLens/trigger/config.yaml
%{_unitdir}/joblens-trigger.service
%dir %{_sharedstatedir}/joblens

%post
%systemd_post joblens-trigger.service

%preun
%systemd_preun joblens-trigger.service

%postun
%systemd_postun_with_restart joblens-trigger.service

%changelog
* Tue Jun 09 2026 JobLens Team <joblens@example.com> - 0.0.13-1
- 采用 virtualenv 方案替代 pip --root --prefix 方案（Fedora 捆绑最佳实践）
- Python 依赖全部安装到 /usr/lib/joblens-trigger/venv/，与系统 Python 完全隔离
- 移除 %%generate_buildrequires / %%pyproject_* 宏，简化构建链路
- 添加 __requires_exclude_from 防止 RPM 扫描 venv 内 .so 文件
- systemd 服务使用 venv 中的 gunicorn 启动
