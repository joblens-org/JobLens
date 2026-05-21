Name:           joblens-trigger
Version:        0.0.9
Release:        1.dev%{?dist}
%global debug_package %{nil}
Summary:        JobLens Trigger Service - RESTful API for JobLens
Group:          System Environment/Daemons
License:        MIT
URL:            https://code.ihep.ac.cn/wangzhenyuan/JobLens
Source0:        JobLens-Trigger-%{version}-Source.tar.gz

BuildRequires:  python3.13

Requires:       joblens >= 0.0.12
Requires:       python3.13

%description
JobLens Trigger provides RESTful API endpoints for managing and monitoring 
JobLens instances. It includes service registration, job management, 
performance metrics, configuration management, and system upgrade capabilities.

%prep
%setup -q -n trigger

%build
# 创建 Python 3.13 虚拟环境，确保打包的 wheel 与运行环境一致
python3.13 -m venv venv

# 使用 venv 的 pip 安装 shiv
venv/bin/pip install shiv

# 使用 venv 的 shiv 创建独立的可执行文件
# 打包整个当前目录，入口点为 entrypoint:main
venv/bin/shiv -e entrypoint:main -o joblens-trigger-py3.pyz .

%install
rm -rf %{buildroot}

# 安装应用文件（排除 venv 构建产物，运行时由 %post 创建）
install -d -m 755 %{buildroot}/opt/JobLens/trigger
cp -r $(ls -A | grep -v ^venv$) %{buildroot}/opt/JobLens/trigger/

# 移除开发文件
rm -rf %{buildroot}/opt/JobLens/trigger/__pycache__
rm -rf %{buildroot}/opt/JobLens/trigger/build
rm -rf %{buildroot}/opt/JobLens/trigger/joblens_trigger.egg-info
rm -rf %{buildroot}/opt/JobLens/trigger/venv  # 确保 venv 不被打包

# 将 shiv 生成的可执行文件移动到目标位置，并添加执行权限
mv joblens-trigger-py3.pyz %{buildroot}/opt/JobLens/trigger/
chmod +x %{buildroot}/opt/JobLens/trigger/joblens-trigger-py3.pyz

# 创建日志目录并设置权限
install -d -m 755 %{buildroot}/var/log/joblens

# 安装systemd服务文件（直接使用 venv/bin/gunicorn，不经过 .pyz 以避免 bootstrap 导入问题）
install -d -m 755 %{buildroot}%{_unitdir}
cat > %{buildroot}%{_unitdir}/joblens-trigger.service << EOF
[Unit]
Description=JobLens Trigger Service
After=network.target joblens.service
Wants=joblens.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/JobLens/trigger
Environment=JOBLENS_CONFIG_PATH=/etc/JobLens/config.yaml
Environment=PATH=/opt/JobLens/trigger/venv/bin:/usr/local/bin:/usr/bin:/bin
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/JobLens/trigger/venv/bin/gunicorn \\
          --config /opt/JobLens/trigger/gunicorn.conf.py \\
          app:app
Restart=no
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

%files
%defattr(-,root,root,-)
%doc README.md
/opt/JobLens/trigger
/var/log/joblens/
%{_unitdir}/joblens-trigger.service

%post

# 创建 Python 3.13 虚拟环境（用于运行 PYZ）
if [ ! -d /opt/JobLens/trigger/venv ]; then
    python3.13 -m venv /opt/JobLens/trigger/venv
fi

# 重新加载systemd并启动服务
systemctl daemon-reload
systemctl enable joblens-trigger.service
systemctl restart joblens-trigger.service

%preun
if [ $1 -eq 0 ]; then
    systemctl stop joblens-trigger.service
    systemctl disable joblens-trigger.service
fi

%postun
if [ $1 -eq 0 ]; then
    rm -rf /opt/JobLens/trigger/venv
    systemctl daemon-reload
fi