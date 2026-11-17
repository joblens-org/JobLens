# JobLens 集成测试

双节点 Vagrant/libvirt 集成测试框架, 覆盖 HTCondor/Slurm 作业自动发现、Trigger REST API、FileWriter 纯文本输出和性能基线检查。共 6 个测试文件, 35 个测试用例。

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

### NetUsage repeated-work and writer compatibility regression

```bash
bash tests/integration/run_net_usage_test.sh build
```

Requires the configured Ninja build, `strace`, Python 3, permission to trace a
child process, and local loopback TCP/UDP plus socket-diagnostic netlink access.
The test links the production collector and creates two live processes sharing
local sockets. It checks connection ownership, smallest-FD selection, shared
TCP sampling/rates, closed sockets, netlink disable/cleanup, and literal
ES/FileWriter/Prometheus output contracts (with and without summary).
Actual syscall counts verify one FD scan per PID, four table reads per known
network namespace and one request per shared TCP socket. A narrowly injected
namespace-stat permission failure verifies that unknown namespaces do not
share snapshots. No production endpoints or BPF maps are accessed.
When `unshare` and user/network namespaces are available, a separate regression
moves the first reader into a new namespace during collection and verifies
that connections belonging to a stable process are still collected. Otherwise,
the script prints an explicit skip for this case.

Snapshot reuse is limited to one `collect(Job)` invocation; it does not cache
across Jobs/cycles or change the scheduler. Existing per-process output and
process-sum summary semantics are retained, including shared-socket totals.
Run same-load measurements on a deployment node to assess end-to-end gains;
the syscall regression is not a timing benchmark.

### 作业清单 / 采集解耦回归

在已配置 Ninja 构建目录、具备项目 C++ 编译依赖的本机运行（无需特权、无需 eBPF）：

```bash
bash tests/integration/run_registry_tests.sh build            # 可加 --suite identity|store|intent|maintenance
bash tests/integration/run_collection_rpc_test.sh build
bash tests/integration/run_watcher_scan_test.sh build
```

- `job_identity_test.cpp`：Condor/Slurm 规范身份、uint64 内部 ID、Slurm step 归一化
  （`batch` = `SLURM_BATCH_SCRIPT` 0xfffffffe、`extern` = `SLURM_EXTERN_CONT` 0xfffffffd）、
  PID 复用、实例句柄与序列化。
- `job_store_test.cpp`：零绑定 upsert 幂等、旧 schema 迁移与恢复、冲突隔离、
  持久写失败与 ID 不复用。
- `job_collection_intent_test.cpp`：手工选择优先级、持久停止与 `reset_auto`、重复/过期修订、
  重启后的意图恢复；自动策略在手工覆盖下不推进客户端可见修订。
- `job_maintenance_test.cpp`：启动竞争、事件回放、两次完整缺席才退休、EPERM 不算死亡、
  队列溢出与重扫、tracker 不可用、旧代际迟到清理；另含**无发现源时手工登记记录的刷新与退休**
  以及**事件触发/按需（`ensure_registered`）的全量对账**。
- `collection_rpc_test.cpp`：全部清单/采集 RPC 方法通过真实 AF_UNIX RPC 驱动，覆盖选择器形式、
  分页游标失效、409/503/404 分类、`ensure_registered` 实际触发一次权威发现、
  以及“无采集运行时不虚报 applied_collectors”的契约。
- `job_watcher_scan_test.cpp`：fixture procfs/cgroup/job-ad 下的扫描、健康状态与
  start/stop 幂等；exec 事件在 poll 线程同步登记，未就绪或执行证据不完整时
  请求一次合并 rescan，由下一轮权威扫描收敛（无登记批次队列）。
  `run_watcher_scan_test.sh` 默认优先使用 `build-watcher/`（若存在），
  因此改动头文件后请勿设置 `SKIP_BUILD=1` 复用过期的私有构建目录。

采集意图真正落到采集器的端到端验证在
`run_execution_tests.sh --suite lifecycle`（`collector_execution_lifecycle_test.cpp`）：
无绑定的清单实例不产生采集、提交绑定时挂载 lane 并报告 applied、持久停止后摘除且保留清单、
无论停止后退休还是绑定中自然结束都能被确认且不残留 lane。

`run_registry_tests.sh --suite lifecycle`（`job_registry_lifecycle_test.cpp`）验证
JobRegistry 自己的生命周期与自动选择：构造即启动清单栈（providers 发布）、
`stopDiscovery` 幂等且排空期间状态可见、`deinit` 归档并关库且幂等；
`auto_add` 折算后新实例自动获得配置的采集器、手工覆盖不被覆盖、
defaults/rules/reject 三种选择语义直接断言。exec hint 的定向观察（不触发全量扫描）
在 maintenance 套件的 `EXEC` 用例中验证；`EXEC2` 的契约是"一次 hint 一次同步
尝试，由发现源自己的 poll 循环重读收敛"，注册表不持有 exec ingest 线程。

### 统一采集执行套件 / Unified collector execution suites

```bash
bash tests/integration/run_execution_tests.sh build --suite all
# 单套件: --suite executor|matrix|poller|lifecycle|sources
```

无需特权。覆盖共享执行器(worker 上限/串行 lane/合并/续跑/异常恢复)、四契约矩阵、共享就绪轮询器(staged 注册、one-shot rearm、挂起、失败隔离、线程归属)、生命周期分发(事件分发、System 信封、closing 确认),以及采集器自持真实源:

- `collector_event_source_test.cpp`:`collector_event_fixture.h` 协议下的 AF_UNIX 数据报(Job/System 两作用域、有界续跑、超限/畸形/结束标记拒绝、路径所有权)与流式连接夹具(非阻塞 accept、动态客户端注册、半帧缓冲、断连尾数据、连接上限、慢客户端隔离)。
- `collector_event_bpf_test.cpp` + `run_event_bpf_test.sh`:私有 eBPF 探针(ring + 精确丢失统计),consume_n 与旧版回调预算两条有界消费路径、两种作用域信封、同 lane 混合 ring+socket 就绪。**需要 root/BPF 特权,无特权时显式跳过(exit 77)**:

```bash
sudo bash tests/integration/run_event_bpf_test.sh build
```

### 原生输出契约 / 路由 / 调度集成回归

在已配置 Ninja 构建目录、具备项目 C++ 编译依赖的本机运行：

```bash
bash tests/integration/run_output_tests.sh
```

可传入构建目录作为第一个参数，`SKIP_BUILD=1` 可跳过重新构建。三个测试分别覆盖：

- `output_contract_test.cpp`：有界准入（`QueueFull`/`TooLarge`/`Closed`/`Unsupported`）、不可变记录扇出、并发投递、记账不变量与未知通道行为。
- `output_routing_test.cpp`：`outputs` 显式路由、旧版 `use_writers` 回退、显式禁用、无效绑定诊断、重复目标去重、`emit_to` 限制、记录标识。
- `collector_output_test.cpp`：真实调度器 + Unix socket RPC + 测试 writer；覆盖 System/Job 作用域分发、零输出、Skipped、Failed、异常、emit 后返回 Skipped 的契约错误、部分样本保留、`operation_semantics_version: 2` 与嵌套 write 计数。

测试只使用本地临时 socket、配置与 LevelDB 目录，不连接生产后端，也不修改内核 map。

### ESWriter 退避重试与性能计数回归

在已配置 Ninja 构建目录、具备项目 C++ 编译依赖的本机运行：

```bash
bash tests/integration/run_writer_retry_test.sh
```

可传入构建目录作为第一个参数。脚本复用 JobLens 的实际目标文件和链接参数，运行本地随机端口 HTTP 模拟服务与独立 Unix socket RPC，不连接生产 ES，不需要 eBPF 权限。覆盖 Bulk 部分失败、成功项不重发、永久错误、异常响应、超时、指数退避、次数耗尽、重试字节分包、非法配置、真实后台失败钩子，以及 writer/collector 的 call_cnt 单次计数。测试程序和 socket 在结束时清理。

### Lua 规则沙箱回归

在已配置 Ninja 构建目录的本机运行：

```bash
bash tests/integration/run_rule_engine_sandbox_test.sh
```

脚本链接实际 `RuleEngine` / `RulesManager`，覆盖：`io`/`package`/`require`/`load`
不可达、加载期与执行期死循环被指令/内存预算中断、超大规则被拒、规则目录
之外的路径与 `../` 穿越被拒。不加载 BPF，不需要 root。

### NewIOUsage 按作业缓存索引回归

在有项目 Ninja 构建目录及 BPF 权限的 Linux 主机运行：

```bash
bash tests/integration/run_new_io_cache_test.sh
```

脚本先验证索引的作业隔离、刷新和失效，再链接实际采集器代码，以独立、未 pin、无探针的 BPF map 调用真实 `collect()`。覆盖明细开关、文件明细、速率基线、同 PID 跨 Job 隔离、短命进程清理及空缓存刷新。不启动 JobLens 服务，不访问现有内核 map。测试文件和 map 随进程结束清理。

无 BPF 权限时可单独编译运行 `new_io_fd_index_test.cpp`；其中 146 作业规模用例核对条目访问数量，不是端到端性能基准。索引优化不改变现有 200 ms 缓存 TTL，缓存过期或短命进程清理后的刷新仍需要读取全表。

### 独立 HASH map 批量读取回归

在具备项目构建依赖、BTF 与 BPF 权限的 Linux 主机上运行：

```bash
sudo bash tests/integration/run_batch_lookup_test.sh
```

该测试不启动 JobLens、不访问现有 map、不修改配置。它创建未 pin 的独立 map，直接调用生产 `EbpfCommon::lookup_hashmap_batch()`，分别使用 NewIOUsage 与 FSMetadata 的真实 key/value 类型验证 0、1、1023、1024、1025、2051 条数据，检查数量、唯一性及全部字段，覆盖 ENOENT 有效尾批和跨批次游标。临时 map/文件随测试关闭清理。

### NewIOUsage / FSMetadata 运行态回归

在测试执行节点使用已安装的 JobLens、Trigger、jq 和 wzycc 用户。两个采集器必须注册且挂载 ESWriter，诊断日志级别需为 debug。测试通过 HTTP 手动注册真实 PID，不代表调度器自动发现或前端页面验收。

```bash
cc -O2 -Wall -Wextra -Werror tests/integration/io_metadata_workload.c -o /tmp/jl-io-meta-workload
sudo bash tests/integration/verify_io_metadata.sh 99129999 /tmp/jl-io-meta-workload true
```

负载持续读写/fsync，并执行成功及 ENOENT 的 open。脚本验证两种采集器的汇总增长、速率、目标 PID 明细、文件明细、元数据成功/错误计数及 last_errno，并检查 ESWriter HTTP 200。它检查实际序列化文档日志，而非直接查询 ES；不会暴露 ES 凭据。将两个配置节的 `include_process_details` 设置为 false 并重启后，用新 JobID 和参数 `false` 可验证仅关闭明细、汇总仍增长。操作者必须在结束后恢复原配置。

脚本自动移除本轮采集和负载文件，需使用未占用的 JobID；工作负载可执行文件由操作者删除。脚本不修改配置、不重启服务。

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

JobLens 通过统一 RPM 部署到 worker VM。将 RPM 放到 `tests/integration/rpms/` 目录:

```bash
# 构建统一 RPM (Core C++ Agent + Python Trigger 合一)
bash scripts/install-deps.sh
bash scripts/build-unified-rpm.sh --preset rpm-system-deps
cp ~/rpmbuild/RPMS/x86_64/joblens-*.rpm tests/integration/rpms/
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
| `test_04_filewriter_schema.py` | 8 | 纯文本文件存在性、非 JSONL wrapper、CPU/内存字段文本输出 |
| `test_05_trigger_api.py` | 8 | REST API 端点: `/`, `/metrics`, `/joblens/jobs`, 详情/404、config/status、RPC functions |
| `test_06_performance_baseline.py` | 5 | 收集延迟、RPC 延迟 (<100ms)、内存 (<256MB)、CPU (<5%)、eBPF 程序加载 |

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `JOBLENS_RPM_PATH` | 预构建统一 RPM 目录路径 (CI 使用, 包含 `joblens-*.rpm`) | 无 (从源码构建) |
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
cat /var/log/joblens/output.log          # FileWriter 纯文本输出
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
| `test_text_file_exists_and_nonempty` 失败 | cpumem_collector 未启用或 FileWriter 未输出 | 检查 `/etc/JobLens/config.yaml` 中 `enable_collector_perf` |
| `test_ebpf_programs_loaded` 失败 | eBPF 程序加载失败 | `ssh worker` → `sudo bpftool prog list` → 检查 `dmesg` |
| `ConnectionRefusedError` 或 `HTTP 503` | joblens-trigger 未启动或端口冲突 | `ssh worker` → `sudo systemctl status joblens-trigger` → `sudo ss -tlnp \| grep 7592` |

### CI 环境特有

| 症状 | 原因 | 解决方法 |
|------|------|---------|
| `vagrant up` 在 CI 中超时 | Box 下载慢或 libvirt 资源不足 | CI runner 预下载 box, 确保足够的 CPU/内存/磁盘 |
| `pip install` 网络错误 | CI runner 无外网 | 离线安装或配置 proxy |
| `pytest` 全部 skip | VM 连接不上 | 检查 `~/.ssh/config` 中 Vagrant 条目, `vagrant ssh-config` 输出 |
