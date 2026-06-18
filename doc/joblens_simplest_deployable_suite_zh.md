# JobLens 最简可部署套件 (Agent + Agent Trigger)

> **文档版本**: v1.1.0  
> **最后更新**: 2026-06-17  
> **Agent RPM**: joblens-0.1.1-1.el9.x86_64.rpm  
> **Trigger RPM**: joblens-trigger-0.1.0-1.el9.x86_64.rpm  
> **状态**: 已在 IHEP 生产环境验证，欢迎跨站点评估

## 1. 项目简介
- JobLens是一个Job-Native的可观测性系统，其中Agent为部署在作业节点上的采集Daemon
- 本套件目标：在受控节点上快速部署 Agent 与本地控制组件，实现作业级数据采集与基本查询/转发
- 适用对象：站点管理员、实验计算运维、合作机构评估人员
- 源代码已在 GitHub 开放：[joblens-org/JobLens](https://github.com/joblens-org/JobLens)，支持从源码编译或通过 RPM 包快速部署

## 2. 术语表
- JobLens Agent：采集守护进程
- JobLens Trigger：Agent 的本地控制接口，负责 Agent 生命周期管理、规则注入、作业元数据注册。（内部代号"Trigger"，因历史原因保留。）
- JobLens Manager（控制平面，可以不部署）：集中式控制平面，统一管理配置文件，规则文件，以及提供相关监控
- JobLens TAP(Trigger&Access Point): 统一数据获取中台，提供统一的数据拉取服务

## 3. 架构速览

### 部署拓扑图(IHEP)
目前在ihep的部署模式如下
![deploy arch](img/arch.png)
### 数据流图
![data flow](img/data_flow.png)
## 4. 资源要求与兼容性

**已验证环境**

- 操作系统：AlmaLinux 9 (Kernel `5.14.0-427.el9.x86_64`)
- 其他 RHEL 9 兼容发行版（Rocky Linux 9、CentOS Stream 9）预计可正常运行，但尚未充分测试。  

**资源占用**

- 设计目标为极小开销。在 256 核生产节点上监控251个HTCondor Job时：
  - **CPU**：**< 0.15%** 节点总容量（约 37% 单个逻辑核）
  - **内存**：**~145 MB** RSS
- 由于 per-CPU eBPF maps，资源消耗随核数大致线性增长。

## 5. 快速入门
### 5.0 基础准备

- 操作系统：AlmaLinux 9 (已验证)，其他 RHEL 9 兼容发行版（未充分测试）
- systemd 作为服务管理器
- HTCondor 作业节点（测试阶段建议搭建 HTCondor 测试集群）

**数据存储选择（二选一）：**
- **方案 A：完整评估（含可视化）**：需自建 Elasticsearch (>= 7.x) 作为数据后端，通过 [JobLens-TAP](https://github.com/joblens-org/JobLens-TAP) 查询和可视化数据。
- **方案 B：仅 Agent 采集（无 ES）**：Agent 可独立运行，输出至本地文件。当 `log_level: debug` 时，可通过 `journalctl -u joblens -f` 实时观测采集指标。需要注意的是，当大量 Job 的 metrics 被写入本地文件时，可能导致 I/O 压力影响作业本身，此方法仅用以测试。

### 5.1 安装 RPM
- 下载 RPM 包后执行安装：

```bash
dnf install ./joblens-*.el9.x86_64.rpm ./joblens-trigger-*.el9.x86_64.rpm
```

- 验证安装路径与二进制：

```bash
rpm -ql joblens joblens-trigger
ls /usr/bin/JobLens
systemctl status joblens-trigger
```

### 5.2 基础配置
安装后rpm包后会带有默认的配置，请根据需要进行修改
- Agent 配置文件概览（`/etc/JobLens/config.yaml`）
  - 监听地址与 Trigger 通信方式
  - 日志级别与输出
  - es相关配置
- Trigger 配置文件概览（`/etc/JobLens/trigger/config.yaml`）
  - 监听端口

### 5.3 启动与自检
安装 RPM 包后默认服务不启动，需要手动启动服务。

```bash
# 启动服务
systemctl start joblens
systemctl start joblens-trigger

# 检查服务状态
systemctl status joblens joblens-trigger

# 健康检查
curl http://localhost:7592/joblens/healthy
```

### 5.4 可视化

JobLens 提供独立的可视化数据中台 [JobLens-TAP](https://github.com/joblens-org/JobLens-TAP)（Telemetry Access Point），
一个基于 Go 的高性能、无状态查询网关，统一屏蔽底层多个 Elasticsearch 集群，提供标准化 RESTful API：

- **原始数据查询** (`/data/raw`)：获取采集器级别的原始采样数据
- **时序聚合查询** (`/data/timeseries`)：多指标时序聚合，支持指定聚合粒度和分组维度
- **任务摘要查询** (`/data/summary`)：作业级聚合摘要
- **Schema 发现** (`/schema`)：查询字段与集群元数据
- **采集触发** (`/collect`)：远程触发指定节点的 JobLens Agent 采集

TAP 独立部署，通过环境变量配置，支持多集群并行查询、字段别名映射、游标分页和 SIGHUP 热重载。
详细部署与配置请参考 [JobLens-TAP 仓库](https://github.com/joblens-org/JobLens-TAP) 中的 README。

### 5.5 卸载与回滚

```bash
# 停止服务
systemctl stop joblens joblens-trigger

# 卸载RPM（配置文件保留为 .rpmsave）
dnf remove joblens joblens-trigger

# 验证：确认无残留进程
ps aux | grep -i joblens
```

> **零残留状态**：卸载将移除所有二进制和 eBPF 程序。已存储至 Elasticsearch 的数据根据您的 ES 保留策略保留。

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
见文件 configuration_zh.md

## 8. 数据写入器配置
- 当前版本重点验证 Elasticsearch sink。
- 当前代码中已实现的写入器类型为 Elasticsearch、FileWriter、KafkaWriter 和 PrometheusExporterWriter。不同环境的部署验证状态可能不同，具体配置键以 `configuration_zh.md` 为准。

## 9. 验证指南
1. 通过 `condor_submit` 提交测试作业。
2. 观察 Agent 日志确认采集命中：
   ```bash
   journalctl -u joblens -f | grep "job_id"
   ```
3. 通过 Trigger API 或 ES 查询返回的 Job 视图（JSON 结构）。

## 10. 共建邀请以及反馈联系方式

JobLens 正积极迈向 **v1.0 API 稳定性**。我们欢迎您的参与：

- **试用反馈**：我们正在收集各站点的实际部署环境（操作系统、内核、调度器），以构建 v1.0 的兼容性矩阵。
- **调度器集成**：如果您在 SLURM、PBS、UGE 等批处理系统的作业发现机制方面有经验，我们非常乐意在自动关联方面展开合作。
- **后端验证**：帮助我们在您的环境中验证已实现的存储/导出路径，如 Kafka 和 Prometheus。

核心 Agent 已在 **IHEP 生产环境经受考验**（~1,200 节点）。外围组件（Manager、Web UI）正根据反馈快速迭代。您的参与将帮助 JobLens 成为面向 HPC/HTC 站点的可移植、Job-Native 可观测性基础组件。

联系方式：wangzhenyuan@ihep.ac.cn（抄送：shijy@ihep.ac.cn）
