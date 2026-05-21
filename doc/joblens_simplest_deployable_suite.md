# JobLens 最简可部署套件 (Agent + Agent Trigger)

> **Document Version**: v1.0.0  
> **Last Updated**: 2026-04-25  
> **Agent RPM**: joblens-0.0.16-1.dev.el9.x86_64.rpm  
> **Trigger RPM**: joblens-trigger-0.0.9-1.dev.el9.x86_64.rpm  
> **Status**: Production-tested at IHEP, open for cross-site evaluation

## 1. 项目简介
- JobLens是一个Job-Native的可观测性系统，其中Agent为部署在作业节点上的采集Daemon
- 本套件目标：在受控节点上快速部署 Agent 与本地控制组件，实现作业级数据采集与基本查询/转发
- 适用对象：站点管理员、实验计算运维、合作机构评估人员
- 当前仅提供rpm包，开源代码正在准备中

## 2. 术语表
- JobLens Agent：采集守护进程
- JobLens Trigger：Agent 的本地控制接口，负责 Agent 生命周期管理、规则注入、作业元数据注册。（内部代号“Trigger”，因历史原因保留。）
- JobLens Manager（后续组件，本套件不含）：集中式控制平面，统一管理配置文件，规则文件，以及提供相关监控

## 3. 架构速览

### 部署拓扑图(IHEP)
目前在ihep的部署模式如下
![deploy arch](img/arch.png)
### 数据流图
![data flow](img/data_flow.png)
## 4. 资源要求与兼容性
Validated Environment: AlmaLinux 9 (Kernel 5.14.0-427.el9). Other RHEL 9-compatible distributions (Rocky Linux, CentOS Stream 9) are expected to work but not yet fully tested. If you run a different OS, please contact us before deployment.

Resource Footprint: Designed for minimal overhead. On a 256-core production node: <0.15% total CPU (~37% of a single core), ~145 MB RAM. Scales linearly with core count due to per-CPU eBPF maps.

## 5. 快速入门
### 5.0 基础准备

- 操作系统：AlmaLinux 9 (已验证)，其他 RHEL 9 兼容发行版（未充分测试）
- systemd 作为服务管理器
- HTCondor 作业节点（测试阶段建议搭建 HTCondor 测试集群）

**数据存储选择（二选一）：**
- **Option A: 完整评估（含可视化）**：需自建 Elasticsearch (>= 7.x) 作为数据后端，Python 可视化脚本将从中查询数据。
- **Option B: 仅 Agent 采集（无 ES）**：Agent 可独立运行，输出至本地文件。当loglevel使用debug时，也可以通过 `journalctl -u joblens -f` 实时观测采集指标。需要注意的是，当大量Job的metrics被写入本地文件时，可能会导致io卡顿，影响作业本身，此方法仅用以测试。

### 5.1 安装 RPM
- 下载rpm包
- `dnf install ./joblens-*.el9.x86_64.rpm ./joblens-trigger-*.el9.x86_64.rpm`
- 验证安装路径与二进制

### 5.2 基础配置
安装后rpm包后会带有默认的配置，请根据需要进行修改
- Agent 配置文件概览（`/etc/JobLens/config.yaml`）
  - 监听地址与 Trigger 通信方式
  - 日志级别与输出
  - es相关配置
- Trigger 配置文件概览（`/opt/JobLens/trigger/config.yaml`）
  - 监听端口

### 5.3 启动与自检
- 安装rpm包会自动启动agent和trigger
- 自检命令（`service joblens status`，`curl localhost:7592/joblens/healthy`）

### 5.4 可视化
由于当前在ihep部署的可视化和ihep内部业务耦合严重，所以尚且不能提供部署的网页版

作为替代，我们提供了一个python脚本用以进行快速的可视化

该python脚本的原理是使用plotly.graph_objects生成交互式的网页，该网页会定时刷新

使用方法为首先在环境变量中设置ES存储后端相关参数
```bash

export ES_HOST=your_es_host
export ES_PORT=your_es_port
export ES_SCHEME=http/https
export ES_USERNAME=es_access_username
export ES_PASSWORD=es_access_passwd

```
之后运行该指令
```bash
python3 joblens-simple-viz.py jobid
```

该指令也有一些配置参数
```bash
usage: joblens-simple-viz.py [-h] [--cluster {ihep_condor,ihep_slurm}] [--refresh SEC] [--port PORT] [--hours N] [--no-browser] jobid
positional arguments:
  jobid                 Job ID (如 "12345.0" 或 "123456")

options:
  -h, --help            show this help message and exit
  --refresh SEC         页面自动刷新间隔秒数 (默认: 10)
  --port PORT           本地 HTTP 端口 (默认: 8765)
  --hours N             查询最近 N 小时的数据 (默认: 2, 设为 0 表示不限)
  --no-browser          不自动打开浏览器

example:
  export ES_USERNAME=readonly
  export ES_PASSWORD=your_password
  python tools/joblens_viz.py 12345.0
  python tools/joblens_viz.py 12345.0 --cluster ihep_slurm --refresh 15
  python tools/joblens_viz.py 12345.0 --hours 12
```

### 5.5 卸载与回滚

```bash
# 停止服务
systemctl stop joblens joblens-trigger

# 卸载RPM（保留配置备份）
dnf remove joblens joblens-trigger

# 验证：确认无残留进程
ps aux | grep joblens
```

## 6. 部署模式详解
### 6.1 单节点评估模式
- 面向试用或小规模验证

### 6.2 与上游 Manager 对接（预留模式）
- Trigger 配置上游 Manager 地址，实现服务注册，规则和配置集中分发
- 本套件仅预留接口，Manager 部署后续会提供

### 6.3 作业关联方式
- 手动关联：Trigger 提供 API 注入 PID → Job 映射
- 自动关联：Condor启动的作业可以自动进行添加并且关联

## 7. 配置详解
见文件configuration.md

## 8. 数据写入器配置
- 使用ElasticSearch
Writers: This release focuses on Elasticsearch validation. The file writers are functional for local testing. Additional backends (Prometheus, Kafka, InfluxDB) are interface-ready and will be validated in upcoming releases based on demand.

## 9. 验证指南
- 通过condor_submit提交作业
- 观察 Agent 日志确认采集命中
- 解读返回的示例 Job 视图（JSON 结构）

## 10. 共建邀请以及反馈联系方式

JobLens is actively evolving toward v1.0 API stability. We welcome:

- **Trial Feedback**: 我们正在收集各站点的实际部署环境，以构建 v1.0 的兼容性矩阵。
- **Scheduler Integration**: If you have expertise in SLURM, PBS, or UGE 
  job discovery mechanisms, we would love to collaborate on auto-attachment.
- **Backend Validation**: Help us verify your existing storage stack 
  (InfluxDB, Prometheus, or others) as a JobLens sink.

The core agent is **production-hardened at IHEP** (~1,000 nodes). 
Peripheral components (Manager, Web UI) are iterating rapidly based on 
community input. Your participation will help JobLens become a 
portable, job-native observability foundation for HPC/HTC sites.

Contact: wangzhenyuan@ihep.ac.cn (cc: shijy@ihep.ac.cn)  

