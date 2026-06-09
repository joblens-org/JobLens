# spec 文件：joblens-trigger
# JobLens Trigger 服务 RPM 打包 — 使用 pyproject 宏标准流程
%global debug_package %{nil}

# 版本通过 rpmbuild --define "trigger_version X.Y.Z" 注入
# 未注入时使用默认值（方便开发时 rpmbuild 不报错）
%{!?trigger_version: %global trigger_version 0.0.0}

Name:           joblens-trigger
Version:        %{trigger_version}
Release:        1%{?dist}
Summary:        JobLens Trigger Service - RESTful API gateway
License:        MIT
URL:            https://github.com/joblens-org/JobLens
Source0:        JobLens-Trigger-%{version}-Source.tar.gz
Source1:        joblens-trigger.service

BuildArch:      noarch

BuildRequires:  python3-devel
BuildRequires:  pyproject-rpm-macros

# 运行时依赖通过 %%pyproject_save_files 自动生成；
# 此处仅列出非 Python 的系统依赖
Requires:       joblens >= 0.0.12

%description
JobLens Trigger provides RESTful API endpoints for managing and monitoring
JobLens instances. It includes service registration, job management,
performance metrics, configuration management, and system upgrade capabilities.

%prep
%setup -q -n trigger

# 将 systemd unit 复制到构建目录（来源：Source1）
cp %{SOURCE1} .

%generate_buildrequires
%pyproject_buildrequires -r

%build
%pyproject_wheel

%install
%pyproject_install
%pyproject_save_files trigger

# 安装 gunicorn 配置文件（%config(noreplace) 保护用户修改）
install -d -m 755 %{buildroot}%{_sysconfdir}/JobLens/trigger
install -m 644 gunicorn.conf.py %{buildroot}%{_sysconfdir}/JobLens/trigger/gunicorn.conf.py

# 安装 systemd unit
install -d -m 755 %{buildroot}%{_unitdir}
install -m 644 joblens-trigger.service %{buildroot}%{_unitdir}/

# 运行时状态目录
install -d -m 755 %{buildroot}%{_sharedstatedir}/joblens

%files -f %{pyproject_files}
%defattr(-,root,root,-)
%doc README.md
%config(noreplace) %{_sysconfdir}/JobLens/trigger/gunicorn.conf.py
%{_unitdir}/joblens-trigger.service
%dir %{_sharedstatedir}/joblens

%post
%systemd_post joblens-trigger.service

%preun
%systemd_preun joblens-trigger.service

%postun
%systemd_postun_with_restart joblens-trigger.service

%changelog
* Sun Jun 08 2026 JobLens Team <joblens@example.com> - 0.0.13-1
- 迁移到 pyproject RPM 宏：消除运行时 venv 创建，使用系统 site-packages 安装
- systemd unit 独立文件，gunicorn 配置标记为 %config(noreplace)
