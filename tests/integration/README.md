# JobLens 集成测试

双节点 Vagrant/libvirt 集成测试框架, 覆盖 HTCondor/Slurm 作业自动发现、Trigger REST API、FileWriter JSONL 输出和性能基线检查。共 6 个测试文件, 35 个测试用例。

## 架构概览

```
┌──────────────────────┐          ┌──────────────────────────────┐
│   VM1: controller    │          │       VM2: worker             │
│   192.168.56.10      │          │       192.168.56.20           │
│                      │          │                              │
│  HTCondor:           │          │  HTCondor:                   │
│    master, collector │          │    master, startd             │
│    negotiator, schedd│          │                              │
│                      │          │  Slurm:                      │
│  Slurm:              │          │    slurmd, munge              │
│    slurmctld, munge  │          │                              │
│                      │          │  JobLens:                    │
│  (提交 HTCondor/     │          │    JobLens (C++ Agent)       │
│   Slurm 作业)        │          │    joblens-trigger (Flask)   │
└──────────────────────┘          └──────────────────────────────┘
          │                                  │
          └──── private_network ─────────────┘
                 192.168.56.0/24
```

- **controller**: 运行 HTCondor/Slurm 调度守护进程, 测试用例通过 `fabric.Connection` 在此节点提交作业
- **worker**: 运行 HTCondor/Slurm 工作守护进程 + JobLens Agent + Trigger, JobLens 通过 eBPF 钩子自动发现作业
- **测试主机**: 运行 pytest, 通过 SSH (fabric/Vagrant) 与 VM 通信, 通过 HTTP 调用 Worker 的 Trigger REST API

## 前置条件

### 硬件
- 支持 KVM 的 x86_64 主机 (Intel VT-x 或 AMD-V)
- 内存 >= 8 GB (VM1: 2GB, VM2: 3GB, 剩余给宿主机)
- 磁盘 >= 50 GB 可用空间 (每个 VM 磁盘 20GB)

### 软件
| 组件 | 最低版本 | 用途 |
|------|---------|------|
| KVM/Libvirt | — | VM 虚拟化 |
| Vagrant | >= 2.2 | VM 生命周期管理 |
| vagrant-libvirt | >= 0.12 | Vagrant libvirt provider |
| bpftool | >= 5.14 | 部署脚本中 eBPF 特性探测 (`common.sh`) |
| Python | >= 3.9 | 运行 pytest |
| Vagrant Box | `almalinux/9` | VM 基础镜像 (自动下载) |

### 安装依赖 (Fedora/RHEL/AlmaLinux 宿主机示例)

```bash
# KVM + libvirt
sudo dnf install -y @virtualization
sudo systemctl enable --now libvirtd

# Vagrant + vagrant-libvirt
sudo dnf install -y vagrant
vagrant plugin install vagrant-libvirt

# Python 测试依赖 (在 tests/integration/ 目录下)
cd tests/integration
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 手动使用

### 1. 准备 RPM 包

JobLens 通过 RPM 部署到 worker VM。将以下 RPM 放到 `tests/integration/rpms/` 目录:

```bash
# 构建 Core RPM
bash scripts/install-deps.sh
cmake --preset rpm-system-deps
cmake --build --preset rpm-system-deps
cpack --preset rpm-system-deps
cp build/joblens-[0-9]*.rpm tests/integration/rpms/

# 构建 Trigger RPM
bash scripts/build-trigger-rpm.sh --install-deps
cp ~/rpmbuild/RPMS/x86_64/joblens-trigger-*.rpm tests/integration/rpms/
```

或用环境变量指定预构建 RPM 路径 (见下方 [环境变量](#环境变量))。

### 2. 启动 VM 并部署

```bash
cd tests/integration

# 启动双 VM
vagrant up --provider=libvirt

# 部署 controller (HTCondor/Slurm 控制服务)
vagrant ssh controller -c "sudo bash /vagrant/provisioning/common.sh controller"
vagrant ssh controller -c "sudo bash /vagrant/provisioning/condor/controller.sh"
vagrant ssh controller -c "sudo bash /vagrant/provisioning/slurm/controller.sh"

# 部署 worker (HTCondor/Slurm worker + JobLens)
vagrant ssh worker -c "sudo bash /vagrant/provisioning/common.sh worker"
vagrant ssh worker -c "sudo bash /vagrant/provisioning/condor/worker.sh"
vagrant ssh worker -c "sudo bash /vagrant/provisioning/slurm/worker.sh"
vagrant ssh worker -c "sudo bash /vagrant/provisioning/joblens/deploy.sh"
```

> **部署顺序很重要**: controller 的 Slurm 部署会生成 munge 密钥, 写入 `/vagrant/.runtime/slurm/`, worker 的 Slurm 部署必须读取此密钥。

### 3. 运行测试

```bash
source .venv/bin/activate
pytest -v --tb=short
```

### 4. 清理

```bash
# 销毁 VM (默认行为)
vagrant destroy -f

# 或挂起 VM (下次可快速恢复)
vagrant suspend
```

## 测试文件说明

| 文件 | 用例数 | 验证内容 |
|------|--------|---------|
| `test_01_health.py` | 6 | systemd 服务状态、Trigger 健康端点、RPC 健康、初始作业计数为零 |
| `test_02_htcondor_discovery.py` | 4 | HTCondor 作业自动发现、PID 捕获、采集器附加、sub_attr 验证 |
| `test_03_slurm_discovery.py` | 4 | Slurm 作业自动发现、PID 捕获、采集器附加、sub_attr 验证 |
| `test_04_filewriter_schema.py` | 8 | JSONL 文件存在性、可解析性、字段完整性、时间戳格式、CPU/内存数据 |
| `test_05_trigger_api.py` | 8 | REST API 端点: `/`, `/metrics`, `/joblens/jobs`, 详情/404、config/status、RPC functions |
| `test_06_performance_baseline.py` | 5 | 收集延迟、RPC 延迟 (<100ms)、内存 (<256MB)、CPU (<5%)、eBPF 程序加载 |

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `JOBLENS_RPM_PATH` | 预构建 RPM 目录路径 (CI 使用, 包含 `joblens-*.rpm` 和 `joblens-trigger-*.rpm`) | 无 (从源码构建) |
| `KEEP_VMS` | 设为 `1` 时测试结束后保留 VM (调试用, 不销毁) | 无 (自动销毁) |

### pytest CLI 选项

| 选项 | 说明 |
|------|------|
| `--keep-vms` | 测试结束后保留 VM (不销毁不挂起) |
| `--skip-vagrant-up` | 跳过 `vagrant up` (假设 VM 已运行) |
| `--skip-vagrant-destroy` | 不用 `vagrant destroy`, 改用 `vagrant suspend` |
| `--trigger-base-url` | 覆盖 Trigger API 基础 URL |

## 调试

### SSH 进入 VM

```bash
vagrant ssh controller   # controller 节点
vagrant ssh worker       # worker 节点
```

### 查看服务日志

```bash
# controller VM 内
sudo journalctl -u condor -f          # HTCondor 日志
sudo journalctl -u slurmctld -f       # Slurm 控制器日志

# worker VM 内
sudo journalctl -u joblens -f         # JobLens Agent 日志
sudo journalctl -u joblens-trigger -f # Trigger (Flask) 日志
sudo journalctl -u condor -f          # HTCondor worker 日志
sudo journalctl -u slurmd -f          # Slurm worker 日志
```

### 检查 JobLens 状态

```bash
# 从宿主机
curl http://192.168.56.20:7592/joblens/healthy
curl http://192.168.56.20:7592/joblens/jobs
curl http://192.168.56.20:7592/joblens/jobs/count

# 从 worker VM 内
sudo systemctl status joblens joblens-trigger
sudo bpftool prog list | grep joblens   # eBPF 程序是否加载
cat /var/log/joblens/output.log          # FileWriter JSONL 输出
```

### 保留 VM 调试

```bash
# 运行测试但保留 VM
KEEP_VMS=1 pytest -v --tb=long

# 或使用 pytest 选项
pytest -v --tb=long --keep-vms --skip-vagrant-destroy
```

### 手动提交测试作业

```bash
# controller VM 内 — HTCondor
echo 'executable=/usr/bin/sleep
arguments=60
output=/tmp/test.out
queue' | condor_submit

# controller VM 内 — Slurm
sbatch --wrap='sleep 60'
```

## GitHub Actions Self-Hosted Runner 设置

### 1. 安装前置软件

在运行 AlmaLinux 9 的物理机上:

```bash
# 安装 GH Actions runner
# 参见: https://github.com/actions/runner/releases
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -o actions-runner.tar.gz -L <runner-download-url>
tar xzf actions-runner.tar.gz
./config.sh --url https://github.com/<org>/<repo> --token <token>

# 安装 KVM/libvirt/Vagrant 依赖
sudo dnf install -y @virtualization libvirt-devel
sudo systemctl enable --now libvirtd
sudo dnf install -y vagrant ruby-devel gcc make
vagrant plugin install vagrant-libvirt

# 安装项目构建依赖
cd /path/to/JobLens
sudo bash scripts/install-deps.sh
```

### 2. 添加 Runner 标签

在 GitHub 仓库 Settings → Actions → Runners 中, 确认 runner 带有以下标签:

```
self-hosted, kvm, almalinux9
```

如果缺少标签, 在 runner 配置时添加:

```bash
./config.sh --labels self-hosted,kvm,almalinux9
```

### 3. 确保 Vagrant Box 预下载

```bash
vagrant box add almalinux/9 --provider=libvirt
```

首次 `vagrant up` 会自动下载, 但 CI 环境下建议预下载以减少启动时间。

## 故障排查

### `vagrant up` 失败

| 症状 | 原因 | 解决方法 |
|------|------|---------|
| `The provider 'libvirt' could not be found` | vagrant-libvirt 插件未安装 | `vagrant plugin install vagrant-libvirt` |
| `Call to virConnectOpen failed` | libvirtd 未运行或用户无权限 | `sudo systemctl start libvirtd; sudo usermod -aG libvirt $USER` |
| `Box 'almalinux/9' could not be found` | Vagrant box 未下载 | `vagrant box add almalinux/9 --provider=libvirt` |
| `SSH authentication failed` | VM 启动超时或 SSH key 未生成 | 增加 `config.vm.boot_timeout` (Vagrantfile 已设为 600) |

### Provision 脚本失败

| 症状 | 原因 | 解决方法 |
|------|------|---------|
| `FATAL: BTF 不可用` | 内核无 BTF 支持 | 确保使用 AlmaLinux 9 默认内核 (5.14+, CONFIG_DEBUG_INFO_BTF=y) |
| `FATAL: 未找到 munge.key` | controller 的 Slurm 部署未完成 | 确保先执行 `slurm/controller.sh`, 再执行 `slurm/worker.sh` |
| `FATAL: RPM 目录不存在` | RPM 未放入 `/vagrant/rpms/` | 在宿主机 `tests/integration/rpms/` 放入 RPM 包 |
| `munge -n \| ssh controller munge -n \| unmunge` 失败 | munge key 不一致或 SSH 不通 | 检查 `/etc/munge/munge.key` 内容一致, `ssh controller` 可达 |

### 测试失败

| 症状 | 原因 | 解决方法 |
|------|------|---------|
| `test_condor_auto_discovery` 超时 | condor_schedd 未运行或作业未调度 | `ssh controller` → `systemctl status condor` → `condor_q` |
| `test_slurm_auto_discovery` 超时 | slurmctld/slurmd 未运行或节点未注册 | `ssh controller` → `sinfo`; `ssh worker` → `systemctl status slurmd` |
| `test_jsonl_file_exists_and_nonempty` 失败 | cpumem_collector 未启用或 FileWriter 未输出 | 检查 `/etc/JobLens/config.yaml` 中 `enable_collector_perf` |
| `test_ebpf_programs_loaded` 失败 | eBPF 程序加载失败 | `ssh worker` → `sudo bpftool prog list` → 检查 `dmesg` |
| `ConnectionRefusedError` 或 `HTTP 503` | joblens-trigger 未启动或端口冲突 | `ssh worker` → `sudo systemctl status joblens-trigger` → `sudo ss -tlnp \| grep 7592` |

### CI 环境特有

| 症状 | 原因 | 解决方法 |
|------|------|---------|
| `vagrant up` 在 CI 中超时 | Box 下载慢或 libvirt 资源不足 | CI runner 预下载 box, 确保足够的 CPU/内存/磁盘 |
| `pip install` 网络错误 | CI runner 无外网 | 离线安装或配置 proxy |
| `pytest` 全部 skip | VM 连接不上 | 检查 `~/.ssh/config` 中 Vagrant 条目, `vagrant ssh-config` 输出 |
