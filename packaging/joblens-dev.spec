Name:           joblens
Version:        %{?_version}
Release:        1.dev%{?dist}
Summary:        High-performance job monitoring and profiling system
Group:          System Environment/Daemons
License:        MIT
URL:            https://code.ihep.ac.cn/wangzhenyuan/JobLens
Source0:        JobLens-%{version}-Source.tar.gz

BuildRequires:  cmake >= 3.16
BuildRequires:  gcc-c++
BuildRequires:  libbpf-devel
BuildRequires:  libnl3-devel
BuildRequires:  openssl-devel
BuildRequires:  zlib-devel
BuildRequires:  lua-devel
BuildRequires:  clang
BuildRequires:  libcurl-devel
BuildRequires:  librdkafka-devel
BuildRequires:  boost-devel
BuildRequires:  sqlite-devel
BuildRequires:  elfutils-libelf-devel
BuildRequires:  xxhash-devel
BuildRequires:  spdlog-devel
BuildRequires:  yaml-cpp-devel
BuildRequires:  fmt-devel

Requires:       libbpf >= 1.0
Requires:       libnl3 >= 3.7
Requires:       openssl >= 1.1
Requires:       lua >= 5.4
Requires:       libcurl
Requires:       librdkafka
Requires:       boost

%description
JobLens is a distributed job monitoring and performance analysis system 
designed for High Performance Computing (HPC) environments. It provides 
real-time job resource usage tracking, detailed performance metrics, 
and supports multiple data export formats.

%prep
%setup -q

%build
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DJOBLENS_INSTALL_SYSTEM_DEPS=OFF \
      ..
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
cd build
make install DESTDIR=%{buildroot}

# 安装配置文件
install -d -m 755 %{buildroot}/etc/JobLens
install -p -m 644 ../config/config.yaml %{buildroot}/etc/JobLens/

# 创建运行时目录
install -d -m 755 %{buildroot}/var/JobLens
install -d -m 755 %{buildroot}/var/JobLens/node_pids

# 安装systemd服务文件
install -d -m 755 %{buildroot}%{_unitdir}
cat > %{buildroot}%{_unitdir}/joblens.service << EOF
[Unit]
Description=JobLens - high-performance job monitoring daemon
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/var/JobLens
Environment=LD_LIBRARY_PATH=/usr/local/lib/joblens
Environment=PATH=/usr/local/bin:/usr/bin:/bin
ExecStart=/usr/local/bin/JobLens \
          -m service \
          -c /etc/JobLens/config.yaml
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

%files
%defattr(-,root,root,-)
%doc README.md CHANGELOG.md
/usr/local/bin/JobLens
/usr/local/lib/joblens/
/usr/local/bin/bpf_obj/
%dir /etc/JobLens
/etc/JobLens/config.yaml
/var/JobLens/
%{_unitdir}/joblens.service
%exclude /usr/local/include/sqlite3.h
%exclude /usr/local/lib64/libsqlite3.a

%post
rm -f /var/JobLens/JobLens.lock
systemctl daemon-reload
systemctl enable joblens.service
systemctl restart joblens.service

%preun
if [ $1 -eq 0 ]; then
    systemctl stop joblens.service
    systemctl disable joblens.service
fi

%postun
if [ $1 -eq 0 ]; then
    systemctl daemon-reload
fi