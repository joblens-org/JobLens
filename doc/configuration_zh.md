# 配置手册

JobLens 使用 YAML 格式配置文件，通过 `--config` / `-c` 命令行参数指定路径。

---

## 配置加载机制

配置文件由 `Config` 类（单例）加载。详见 `src/common/config.cpp`。

**加载流程**：
1. 解析命令行参数，获取配置文件路径。
2. `YAML::LoadFile()` 加载 YAML 文件。
3. 递归扫描所有节点，解析环境变量占位符 `{{ENV_NAME}}` 或 `{{ENV_NAME:default_value}}`。
4. 使用 `parentKey` + `key` 两级索引访问配置项。

**环境变量占位符机制**（`resolveEnvVars`，见 `src/common/config.cpp`）：
- 格式：`{{ENV_VAR_NAME}}` 或 `{{ENV_VAR_NAME:default_value}}`
- 读取环境变量值替换占位符。
- 如果环境变量不存在且未提供默认值，抛出运行时异常。
- 支持 YAML 中的任意字符串字段。

```yaml
# 示例：环境变量替换
lens_config:
  rpc_socket_path: "{{JOBLENS_RPC_PATH:/var/JobLens/rpc.sock}}"
  log_level: "{{JOBLENS_LOG_LEVEL:info}}"
```

---

## 配置节概览

| 顶级键 | 类型 | 描述 | 必填 |
|--------|------|------|------|
| `lens_config` | Map | 核心系统配置 | 是 |
| `job_registry_config` | Map | 作业注册中心配置 | 否 |
| `collectors_config` | Map | 收集器全局配置及收集器列表 | 否 |
| `writers_config` | Map | 写入器全局配置及写入器列表 | 否 |
| `condor_job_watcher` | Map | Condor 作业监听器配置 | 否 |
| `slurm_job_watcher` | Map | Slurm 作业监听器配置 | 否 |
| `{collector/writer}_config` | Map | 各收集器/写入器的具体配置节（名称由 `config` 字段指定） | 按需 |

---

## 1. `lens_config` — 核心系统配置

```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock   # 本地 RPC 通信的 UNIX Domain Socket 路径
  lock_path: /var/JobLens/JobLens.lock      # 进程锁文件路径，防止多实例运行
  max_collector_threads: 8                   # 收集器调度器最大线程数
  log_level: info                            # 日志级别：trace/debug/info/warn/error/critical/off
```

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `rpc_socket_path` | string | 无 | 本地 RPC 通信的 UNIX Domain Socket 路径。若为空或缺失，服务抛出运行时异常。 |
| `lock_path` | string | 无 | 进程锁文件，`already_running()` 用于检测是否已有实例运行。父目录会自动创建。 |
| `max_collector_threads` | int | 无 | 收集器定时调度器（`TimerScheduler`）的线程池大小。 |
| `log_level` | string | `"info"`（硬编码回退） | 日志级别：`trace` / `debug` / `info` / `warn` / `error` / `critical` / `off`。若配置值不在映射表中，回退到 `info`。 |

---

## 2. `job_registry_config` — 作业注册中心配置

```yaml
job_registry_config:
  job_db_path: /var/JobLens/job.db      # 持久化作业状态的 LevelDB 数据库目录路径
  auto_add_condorjob: true               # 是否自动监控 Condor 作业
  auto_add_slurmjob: true                # 是否自动监控 Slurm 作业
```

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `job_db_path` | string | 无 | LevelDB 数据库目录路径。无法打开数据库时仅降级运行，不抛异常 |
| `auto_add_condorjob` | bool | false | 自动启动 CondorJobWatcher，通过 eBPF 追踪 `condor_starter` 进程并自动添加 Condor 作业 |
| `auto_add_slurmjob` | bool | false | 自动启动 SlurmJobWatcher，通过 eBPF 追踪 `slurm_stepd` 进程并自动添加 Slurm 作业 |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/condor_jobs`（Condor）或 `{JobLensRootDir}/../config/rules/slurm_jobs`（Slurm） | Lua 规则文件目录，CondorJobWatcher 和 SlurmJobWatcher 在 `use_rules` 启用时使用。注意：默认值因 watcher 而异。 |

**作业数据存储**（LevelDB 键值存储）：
- 键使用前缀 `job_`、`job_history_`、`counter_`，值以 JSON 序列化
- 由 `JobRegistry` 内部管理；基于 LevelDB 持久化

---

## 3. `collectors_config` — 收集器全局配置

```yaml
collectors_config:
  enable_collector_perf: true       # 是否启用收集器性能计数器
  perf_window_size: 1000            # 性能计数器滑动窗口大小（样本数）
  default_freq: 1                   # 默认采样频率（Hz），收集器未配置 `freq` 时使用
  default_use_writers:              # 默认写入器列表，收集器未配置 `use_writers` 时使用
    - es_writer
  collectors:                       # 收集器实例列表
    - name: cpumem_collector
      type: CPUMemCollector
      config: cpumem_collector_config
    # ... 更多收集器
```

### 通用参数

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `enable_collector_perf` | bool | 无 | 启用后每次 `collect` 调用记录耗时，可通过 RPC 查询性能统计 |
| `perf_window_size` | int | 无 | 性能窗口大小，影响均值/方差计算的样本数 |
| `default_freq` | int | 无 | 收集器默认采样频率（Hz） |
| `default_use_writers` | string[] | 无 | 收集器默认写入器列表 |

### `collectors` 数组元素

每个收集器实例是一个包含以下字段的对象：

```yaml
collectors:
  - name: <实例名称>       # string，唯一标识此收集器实例
    type: <收集器类型>     # string，对应注册的收集器类型名（如 CPUMemCollector）
    config: <配置节名称>   # string，指向同级配置节，包含此收集器的具体参数
```

| 字段 | 类型 | 描述 |
|------|------|------|
| `name` | string | 收集器实例名称，在作业的 `CollectorNames` 中引用 |
| `type` | string | 收集器类型，必须与注册名称一致（区分大小写） |
| `config` | string | 引用同级配置节名称；该节下的键值对将作为 JSON 配置传递给收集器的 `init()` |

### 收集器配置节通用参数

每个收集器的配置节（由 `config` 字段引用）可包含以下通用字段：

```yaml
cpumem_collector_config:       # 配置节名称，由 collectors 数组中的 config 字段指定
  freq: 1                      # (double) 采样频率（Hz），优先级：freq > period > default_freq
  period: 1                    # (double) 采样周期（秒），与 freq 互斥：period = 1/freq
  use_writers:                 # (string[]) 输出写入器列表；未设置时使用 default_use_writers
    - es_writer
    - prmxs_writer
```

**注意：** freq 和 period 只需设置其一，下同。

| 键 | 类型 | 优先级 | 描述 |
|----|------|--------|------|
| `freq` | double | 最高 | 采样频率（Hz），如 `0.2` 表示每 5 秒采集一次 |
| `period` | double | 次高 | 采样周期（秒），与 `freq` 互斥 |
| `use_writers` | string[] | - | 输出写入器列表。未配置时使用 `collectors_config.default_use_writers` |

系统级收集器额外支持：
```yaml
net_sys_collector_config:
  auto_start: true     # 系统级收集器在启动时自动开始运行
```

### 各收集器 `init()` JSON 配置参数

收集器的 `init(json_cfg)` 方法接收从 YAML 配置节点转换的 JSON 对象。以下是各收集器识别的键：

> **注意**：`freq`、`period` 和 `use_writers` 在**调度器层**（`collector_scheduler.cpp`）解析，所有收集器共享——它们**不**在各收集器的 `init()` 方法内解析。唯一例外是 `GPUUsageCollector`，它在 `init()` 中额外读取 `freq` 用于计算缓存刷新间隔。

| 收集器类型 | 配置参数 | 值类型 | 描述 |
|-----------|---------|--------|------|
| **CPUMemCollector** | `summary` | string | 为 `"true"` 时，将所有进程的 CPU/内存数据聚合为一条摘要记录（pid=0） |
| **IOUsageCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的 I/O 数据 |
| | `use_ebpf` | string | 为 `"true"` 时，使用 eBPF 采集文件级 I/O 数据（需要 root 权限和内核支持） |
| **NetUsageCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的网络数据 |
| | `use_netlink` | string | 为 `"true"` 时，使用 netlink 查询 TCP_INFO（RTT、重传次数等） |
| **BasicInfoCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的 taskstats 数据（TGID 级摘要） |
| **GPUUsageCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的 GPU 使用数据 |
| | `freq` | double | 在 `init()` 中额外读取：用于计算 GPU 缓存刷新间隔（`1.0 / (freq * 1.5)`）。这是调度器层 `freq` 之外的额外使用。 |
| **ProcCollector** | *(无)* | — | 无 init 配置参数。从 `/proc/[pid]/stat`、`/proc/[pid]/status`、`/proc/[pid]/io` 等读取进程信息。**注意**：源码中已标记为将来弃用（`//TODO: 这个模块将会逐步弃用`）。 |
| **TaskstatsCollector** | *(无)* | — | 无 init 配置参数。使用 Linux taskstats netlink 接口。**注意**：部分实现——`collect()` 方法仅记录 PID，不产生数据输出；`get_writer_parser()` 返回 `nullptr`。 |

---

## 4. `writers_config` — 写入器全局配置

```yaml
writers_config:
  enable_writer_perf: true       # 是否启用写入器性能计数器
  perf_window_size: 1000         # 性能计数器滑动窗口大小
  buffer_capacity: 4096          # BaseWriter 内部缓冲区容量（记录数）
  writers:                       # 写入器实例列表
    - name: es_writer
      type: ESWriter
      config: ES_writer_config
    # ... 更多写入器
```

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `enable_writer_perf` | bool | 无 | 启用后记录每次 `flush` 耗时，支持 RPC 查询 |
| `perf_window_size` | int | 无 | 性能窗口大小 |
| `buffer_capacity` | int | 无 | BaseWriter 内部环形缓冲区最大消息数 |

### `writers` 数组元素

结构与收集器类似：

```yaml
writers:
  - name: <实例名称>       # string，写入器实例名称
    type: <写入器类型>     # string，对应注册的类型（ESWriter, KafkaWriter, FileWriter, PrometheusExporterWriter）
    config: <配置节名称>   # string，指向同级具体配置节
```

---

## 5. ESWriter 配置

```yaml
ES_writer_config:
  host: 192.168.1.100           # Elasticsearch 服务器地址
  port: 9200                     # Elasticsearch 端口
  user: ""                       # ES 用户名（可选）
  passwd: ""                     # ES 密码（可选）
  index_prefix: collector        # 索引名前缀
  batch_size: 100                # 批量写入大小
  write_timeout: 30              # 写入超时（秒）
  indexs:                        # 可选的索引名映射列表
    - collector_name: cpumem_collector
      index_name: cpu_mem_metrics
    - collector_name: io_usage_collector
      index_name: io_metrics
```

| 键 | 类型 | 必填 | 默认值 | 描述 |
|----|------|------|--------|------|
| `host` | string | 是 | - | ES 服务器地址；支持 `http://` 前缀 |
| `port` | int | 是 | - | ES 服务器端口 |
| `user` | string | 否 | `""` | Basic 认证用户名 |
| `passwd` | string | 否 | `""` | Basic 认证密码 |
| `index_prefix` | string | 否 | `"collector"` | 索引名前缀，最终索引名为 `{prefix}_{collector_name}_{YYYY.MM.DD}` |
| `batch_size` | int | 是 | - | 批量大小 |
| `write_timeout` | int | 是 | - | 写入超时（秒） |
| `indexs` | array | 否 | - | 收集器到索引名的显式映射，每个元素包含 `collector_name` 和 `index_name` |

**索引渲染支持变量模板**：索引名支持 `[[job_info.field]]` 语法，运行时通过 `Utils::render_bracket()` 和 `Utils::flatten_json()` 渲染。

**时间后缀**：自动追加 `_{YYYY.MM.DD}` 格式的日期后缀。

**单条文档结构**（ESWriter 自动包装）：
```json
{
  "@timestamp": "2026-04-24T10:30:00+0800",
  "hostname": "node-1",
  "job_info": { "JobID": 1001, ... },
  "data": { ... }  // 由收集器的 parser 函数生成
}
```

---

## 6. KafkaWriter 配置

**注意：** 当前部署模式未涉及此写入器，尚未经过测试。

```yaml
kafka_writer_config:
  brokers:                            # Kafka broker 列表
    - 192.168.1.100:9092
    - 192.168.1.101:9092
  topic_prefix: collector-data        # Topic 前缀，最终 topic 名为 {prefix}{collector_name}
  topic_dlq: collector-data-dlq       # 死信队列 topic
  client_id: joblens-producer         # 客户端 ID
  transactional_id: joblens-txn       # 事务 ID
  batch_rows: 100                     # 批量发送大小
  linger_ms: 10                       # 发送前最大等待毫秒数
  enable_transaction: false           # 是否启用 Kafka 事务
  security_protocol: plaintext        # 安全协议：plaintext / sasl_plaintext / ssl / sasl_ssl
  # 以下仅在 security_protocol != "plaintext" 时使用
  sasl_mechanism: PLAIN               # SASL 机制：PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
  username: joblens                   # SASL 用户名
  password: joblens_secret            # SASL 密码
```

| 键 | 类型 | 必填 | 前置条件 | 描述 |
|----|------|------|----------|------|
| `brokers` | string[] | 是 | - | Kafka broker 地址列表 |
| `topic_prefix` | string | 是 | - | Topic 前缀，最终 `topic = topic_prefix + collector_name` |
| `topic_dlq` | string | 是 | - | 死信队列 topic 名 |
| `client_id` | string | 是 | - | Kafka 客户端标识 |
| `transactional_id` | string | 是 | - | 事务 ID |
| `batch_rows` | int | 是 | - | 每批次消息数 |
| `linger_ms` | int | 是 | - | 发送前最大等待时间 |
| `enable_dlq` | bool | 不适用 | `true`（内部默认） | 此键在写入器参数列表中**声明**但**不从配置文件读取**。实际值自动管理：内部默认为 `true`；当 `security_protocol` 为 `"plaintext"` 时自动设为 `false`。 |
| `enable_transaction` | bool | 是 | - | 是否启用事务 |
| `security_protocol` | string | 是 | - | 安全协议 |
| `sasl_mechanism` | string | 是 | security_protocol != "plaintext" | SASL 机制 |
| `username` | string | 是 | security_protocol != "plaintext" | 认证用户名 |
| `password` | string | 是 | security_protocol != "plaintext" | 认证密码 |

**消息序列化结构**：
```json
{
  "collector_name": "cpumem_collector",
  "hostname": "node-1",
  "@timestamp": 1745476200000,
  "job_info": { "JobID": 1001, ... },
  "data": { ... }
}
```

---

## 7. FileWriter 配置

代码位置：`src/writer/file_writer.cpp`

将收集器格式化后的纯文本写入本地文件。`FileWriter` 对应的收集器 parser 返回完整文本块（包括需要的换行符），FileWriter 按原样写入。支持追加和原子覆盖两种写入模式、基于大小的文件轮转以及按收集器的输出路由。

### 基本配置

```yaml
file_writer_config:
  path: /var/log/joblens/output.log    # 默认输出文件路径（必填）
```

### 完整配置（含所有选项）

```yaml
file_writer_config:
  path: /var/log/joblens/output.log          # 默认输出文件路径（必填）
  write_mode: append                         # 写入模式：'append' 或 'overwrite'
  flush_on_shutdown: true                    # 优雅关闭时刷新剩余记录
  enable_rotation: false                     # 启用基于大小的文件轮转（仅追加模式）
  max_file_size_bytes: 104857600             # 轮转阈值（字节，100 MB）
  max_files: 5                               # 最多保留的轮转文件数
  outputs:                                   # 按收集器路由文件（可选）
    - collector_name: cpumem_collector
      file_path: /var/log/joblens/cpumem.log
    - collector_name: io_usage_collector
      file_path: /var/log/joblens/io.log
```

### 选项参考

| 键 | 类型 | 必填 | 默认值 | 描述 |
|----|------|------|--------|------|
| `path` | string | **是** | - | 默认输出文件路径。未在 `outputs` 中列出的收集器记录写入此文件。 |
| `write_mode` | string | 否 | `"append"` | 写入模式。`"append"` 以追加模式打开文件，保留所有历史记录。`"overwrite"` 使用同目录临时文件加重命名的方式原子替换目标文件。 |
| `flush_on_shutdown` | bool | 否 | `true` | 为 `true` 时，优雅关闭期间将所有缓冲记录刷新到各目标文件。为 `false` 时，关闭期间跳过 FileWriter 层面的显式 `flush()` 调用，但 BaseWriter 的最终缓冲区刷新仍会在流关闭前执行。 |
| `enable_rotation` | bool | 否 | `false` | 为 `true` 时对目标文件启用基于大小的轮转。仅追加模式支持。 |
| `max_file_size_bytes` | int | 否 | `104857600`（100 MB） | 轮转阈值。当写入下一批次后活动文件将超过此大小时，写入前触发轮转。 |
| `max_files` | int | 否 | `5` | 最多保留的轮转文件数。轮转文件命名规则为 `path.1`（最新）、`path.2`、...、`path.N`（最旧）。超出 `max_files` 时删除最旧文件。 |
| `outputs` | array | 否 | `[]` | 收集器到文件的路由映射列表。每项包含 `collector_name` 和 `file_path`。为空或省略时，所有收集器均写入 `path`。 |

### 收集器到文件的路由

`outputs` 列表将特定收集器实例映射到专用输出文件。每项包含：

| 字段 | 类型 | 描述 |
|------|------|------|
| `collector_name` | string | 收集器实例名称（须与 `collectors_config.collectors` 中的 `name` 字段一致）。 |
| `file_path` | string | 此收集器输出对应的目标文件路径。 |

**回退行为**：当收集器未在 `outputs` 中列出时，其记录写入默认 `path`。每个未映射的收集器在首次写入数据时仅发出一次警告，便于发现配置遗漏。

**示例**：将 `cpumem_collector` 路由到专用文件，其他收集器回退到默认路径：

```yaml
file_writer_config:
  path: /var/log/joblens/output.log
  outputs:
    - collector_name: cpumem_collector
      file_path: /var/log/joblens/cpumem.log
```

### 冲突规则

以下配置会在启动时被拒绝，并给出明确的错误信息：

| 冲突 | 适用模式 | 错误信息 |
|------|---------|---------|
| `write_mode: overwrite` + `enable_rotation: true` | 所有模式 | "write_mode 'overwrite' is incompatible with enable_rotation" |
| `outputs` 中存在重复的 `collector_name` | 所有模式 | "duplicate collector_name 'X' in 'outputs'" |
| `outputs` 中存在重复的 `file_path` | 仅覆盖模式 | "duplicate file_path 'X' in 'outputs' (forbidden in overwrite mode)" |
| `outputs` 条目中的 `file_path` 等于默认 `path` | 仅覆盖模式 | "'outputs' entry for collector 'X' resolves to default path 'Y', which is forbidden in overwrite mode" |

**追加模式下允许**：
- 多个收集器可以共享同一输出文件路径。两个收集器的文本块按刷新顺序追加，生成合并的纯文本文件。
- `outputs` 条目可以指向与默认 `path` 相同的路径。共享收集器和回退收集器均追加到同一文件。

### 写入模式详解

**追加模式**（`write_mode: append`）：
- 每个目标文件以追加模式打开。
- 文本块按刷新顺序追加到文件中。文件单调增长。
- 当 `enable_rotation: true` 时支持基于大小的轮转。
- 多个收集器可以共享同一目标路径。

**覆盖模式**（`write_mode: overwrite`）：
- 每次刷新批次写入目标目录下的临时文件，然后原子重命名覆盖目标文件。
- 目标文件仅包含最新刷新批次的内容。不保留历史记录。
- 轮转被禁用；同时配置 `overwrite` 和 `enable_rotation: true` 会导致快速失败。
- `outputs` 条目中不允许重复的输出路径。
- 失败时，尽可能保留原目标文件，清理临时文件，并记录错误。

> **说明**：原子覆盖使用同目录临时文件（`target.tmp.<pid>.<timestamp>`）以确保重命名的原子性。成功重命名后临时文件自动删除。如果进程在写入过程中崩溃，可能残留孤儿临时文件，这些文件无害，可手动清理。

### 轮转命名与保留策略

轮转仅基于大小，使用以下命名规则：

| 文件 | 描述 |
|------|------|
| `path` | 活动文件（始终为最新数据） |
| `path.1` | 最近的轮转文件 |
| `path.2` | 第二近的轮转文件 |
| ... | ... |
| `path.N` | 最旧的轮转文件（N = max_files） |

轮转触发时：
1. 若 `path.N` 存在，则删除。
2. `path.(N-1)` 重命名为 `path.N`，逐级向下，直至 `path.1` 重命名为 `path.2`。
3. 活动文件 `path` 重命名为 `path.1`。
4. 打开新的活动文件 `path` 用于写入。

整个轮转级联在触发批次的写入之前完成，因此批次始终写入新的活动文件。不会跨轮转边界拆分批次。

> **说明**：轮转默认关闭。此版本仅提供基于大小的轮转。

---

## 8. PrometheusExporterWriter 配置

代码位置：`src/writer/prometheus_exporter_writer.cpp`

通过 RPC 端点暴露采集的指标数据供 Prometheus 抓取。此写入器维护一个按 JobID → PID 组织的内存指标表，涵盖 CPU、内存、I/O 和网络指标。

**RPC 端点**（自动注册）：

| 端点 | 描述 |
|------|------|
| `/<writer_name>/metrics` | 以 JSON 返回所有作业指标 |
| `/<writer_name>/info` | 返回写入器元数据 |

```yaml
prmxs_writer_config: {}    # 当前无配置参数；此节必须存在以供 `config` 字段引用
```

> **说明**：该写入器完全通过 RPC 暴露指标（不使用 FIFO 或文件输出）。配置节当前为空——所有行为由收集器通过调度器管道推送的数据驱动。

---

## 9. 作业监听器配置

### `condor_job_watcher`

代码位置：`include/job_watcher/condor_job_watcher.hpp`

```yaml
condor_job_watcher:
  auto_add_collectors:          # 当 use_rules 为 false 时，发现新 Condor 作业后自动追加的默认收集器列表
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false              # 是否启用规则引擎过滤作业
  rules_prefix: condor_job_     # Lua 规则文件前缀（默认：condor_job_）
```

> **说明**：Lua 规则的 `rules_dir` 从 `job_registry_config.rules_dir` 读取，不在此节中。当 Condor 启用 `use_rules` 时，默认值为 `{JobLensRootDir}/../config/rules/condor_jobs`。

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `auto_add_collectors` | string[] | 无 | 自动添加到 Condor 作业的收集器名称列表 |
| `use_rules` | bool | `false` | 是否使用规则引擎过滤作业（不匹配规则的作业不添加） |
| `rules_prefix` | string | `"condor_job_"` | 规则引擎加载的规则文件前缀 |

### `slurm_job_watcher`

代码位置：`include/job_watcher/slurm_job_watcher.hpp`

```yaml
slurm_job_watcher:
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false
  rules_prefix: slurm_job_
```

> **说明**：Lua 规则的 `rules_dir` 从 `job_registry_config.rules_dir` 读取，不在此节中。当 Slurm 启用 `use_rules` 时，默认值为 `{JobLensRootDir}/../config/rules/slurm_jobs`。

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `auto_add_collectors` | string[] | 无 | 自动添加到 Slurm 作业的收集器名称列表 |
| `use_rules` | bool | `false` | 是否使用规则引擎过滤作业 |
| `rules_prefix` | string | `"slurm_job_"` | 规则引擎加载的规则文件前缀 |

---

## 10. Writer Parser V2 Context API

每个收集器为每种写入器类型提供 parser 函数，将原始采集数据转换为写入器特定格式。V2 parser API 在原有 V1 `std::any(std::any)` 签名基础上增加了只读的 `WriterParseContext` 参数，使 parser 可以访问运行时元数据，无需修改现有代码。

### V1 兼容性

旧版 `get_writer_parser()` 接口（V1）**仍然完全支持**。现有收集器和写入器无需任何修改。V1 parser 由 `ICollector::get_writer_parser_v2()` 中的默认适配器自动包装。

### V2 是 Opt-in 的

收集器通过覆写虚方法选择加入 V2：

```cpp
// include/collector/icollector.h
virtual CollectDataParseFuncV2 get_writer_parser_v2(const std::string& writer_type);
```

默认实现透明地包装 V1 parser，丢弃 `WriterParseContext` 并委托给旧签名。未覆写此方法的收集器将按原有方式继续工作。

### WriterParseContext

上下文结构体是**只读**的，在每次调用时按调用点构造。包含以下字段：

| 字段 | 类型 | 描述 |
|------|------|------|
| `writer_name` | `std::string` | 写入器实例名称（如 `"file_writer"`） |
| `writer_type` | `std::string` | 写入器类型标识（如 `"FileWriter"`） |
| `writer_config_name` | `std::string` | 写入器配置节名称（如 `"file_writer_config"`） |
| `collector_name` | `std::string` | 触发此写入的收集器实例名称 |
| `job` | `Job` | 正在采集的 `Job` 对象（包含 JobID、PIDs、sub_attr 等） |
| `timestamp` | `std::chrono::system_clock::time_point` | parser 被调用的时间戳 |

写入器在调用 parser 之前，从自身元数据和批次记录构造上下文：

```cpp
// src/writer/file_writer.cpp（简化）
WriterParseContext ctx{name_, type_, config_name_, collect_name, job, timestamp};
auto parser = CollectorRegistry::instance().resolveBestParserV2(collect_name, type_);
```

### Parser 解析顺序（回退链）

写入器推荐使用 `CollectorRegistry::resolveBestParserV2()` 入口，按以下顺序解析最佳 parser：

1. **V2 parser** — `get_writer_parser_v2()` 返回原生 V2 parser（收集器已覆写此方法）
2. **V1 适配器** — 默认的 `ICollector::get_writer_parser_v2()` 将 V1 parser 包装为丢弃 `WriterParseContext` 的 lambda
3. **V1 回退** — 显式调用 `get_writer_parser()` 作为防御性回退
4. **写入器特定回退** — 返回 `nullptr`；写入器自行处理原始数据（如 FileWriter 接受纯 `std::string`）

### 首个 PoC 示例：CPUMemCollector → FileWriter

`CPUMemCollector` 是首个提供原生 V2 parser 的收集器，专门针对 `FileWriter`。当 `writer_type == "FileWriter"` 时，V2 parser：

- 将 CPU/内存数据序列化为 JSON，包含每个进程的详细信息和可选的汇总数据
- 在输出中注入上下文元数据（`_writer_name`、`_collector_name`、`_job_id`、`_timestamp`），便于追踪

对于其他 writer 类型，`CPUMemCollector::get_writer_parser_v2()` 回退到默认的 V1 适配器。

### 为新收集器添加 V2 支持

```cpp
// 在收集器头文件中：
CollectDataParseFuncV2 get_writer_parser_v2(const std::string& writer_type) override;

// 在收集器实现中：
CollectDataParseFuncV2 MyCollector::get_writer_parser_v2(const std::string& writer_type) {
    if (writer_type == "FileWriter") {
        return [this](const WriterParseContext& ctx, std::any data) -> std::any {
            // 使用 ctx.writer_name、ctx.job.JobID、ctx.timestamp 等
            // 返回写入器特定格式的输出
            return formatted_output;
        };
    }
    // 其他 writer 类型回退到 V1 适配器
    return ICollector::get_writer_parser_v2(writer_type);
}
```

> **注意**：目前仅有 `CPUMemCollector` 实现了原生 V2 parser。其他收集器继续使用 V1 parser 接口，由适配器自动转换。完整 API 定义见 `include/collector/icollector.h` 和 `include/core/collector_type.h`。

---

## 完整配置文件示例

```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock
  lock_path: /var/JobLens/JobLens.lock
  max_collector_threads: 8
  log_level: info

job_registry_config:
  job_db_path: /var/JobLens/job.db   # LevelDB 数据库目录路径
  auto_add_condorjob: true
  auto_add_slurmjob: false
  rules_dir: /etc/JobLens/rules/

collectors_config:
  enable_collector_perf: true
  perf_window_size: 1000
  default_freq: 1
  default_use_writers:
    - es_writer
  collectors:
    - name: cpumem_collector
      type: CPUMemCollector
      config: cpumem_collector_config
    - name: net_collector
      type: NetUsageCollector
      config: net_collector_config
    - name: io_collector
      type: IOUsageCollector
      config: io_collector_config
    - name: basic_info_collector
      type: BasicInfoCollector
      config: basic_info_collector_config
    - name: gpu_collector
      type: GPUUsageCollector
      config: gpu_collector_config
    - name: proc_collector
      type: ProcCollector
      config: proc_collector_config
    - name: taskstats_collector
      type: TaskstatsCollector
      config: taskstats_collector_config

cpumem_collector_config:
  freq: 1
  summary: false
  use_writers:
    - es_writer
    - prmxs_writer

net_collector_config:
  freq: 1
  use_netlink: true
  summary: false
  use_writers:
    - es_writer

io_collector_config:
  freq: 1
  summary: false
  use_ebpf: false
  use_writers:
    - es_writer

basic_info_collector_config:
  freq: 0.2
  summary: false

gpu_collector_config:
  freq: 1
  summary: true

proc_collector_config:
  freq: 1

taskstats_collector_config:
  freq: 1

writers_config:
  enable_writer_perf: true
  perf_window_size: 1000
  buffer_capacity: 4096
  writers:
    - name: es_writer
      type: ESWriter
      config: ES_writer_config
    - name: prmxs_writer
      type: PrometheusExporterWriter
      config: prmxs_writer_config
    - name: kafka_writer
      type: KafkaWriter
      config: kafka_writer_config
    - name: file_writer
      type: FileWriter
      config: file_writer_config

ES_writer_config:
  host: 192.168.1.100
  port: 9200
  index_prefix: collector
  batch_size: 100
  write_timeout: 30
  indexs:
    - collector_name: cpumem_collector
      index_name: cpu_mem_metrics

prmxs_writer_config: {}

kafka_writer_config:
  brokers:
    - 192.168.1.100:9092
  topic_prefix: collector-data
  topic_dlq: collector-data-dlq
  client_id: joblens-producer
  transactional_id: joblens-txn
  batch_rows: 100
  linger_ms: 10
  enable_transaction: false
  security_protocol: plaintext

file_writer_config:
  path: /var/log/joblens/output.log
  # write_mode: append
  # flush_on_shutdown: true
  # enable_rotation: false
  # max_file_size_bytes: 104857600
  # max_files: 5
  # outputs:
  #   - collector_name: cpumem_collector
  #     file_path: /var/log/joblens/cpumem.log

condor_job_watcher:
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false
```

---

## 命令行启动参数

```bash
JobLens - 作业监控系统

用法：
  JobLens [选项] [子命令]

选项：
  -h, --help                    显示帮助信息
  -c, --config <path>           配置文件路径（默认：config.yaml）
  -m, --mode <mode>             运行模式（默认：service）
  -v, --version                 显示版本信息

运行模式：
  service    纯服务模式：启动采集调度器，持续运行直到收到关闭信号

子命令：
  collector  管理收集器（列出、获取帮助）
  writer     管理写入器（列出、获取帮助）

信号处理：
  SIGINT / SIGTERM              优雅关闭服务

示例：
  # 服务模式启动
  ./JobLens -m service -c config.yaml

  # 显示版本
  ./JobLens -v

  # 列出所有可用收集器
  ./JobLens collector --list

  # 显示收集器详细帮助
  ./JobLens collector --doc CPUMemCollector

  # 列出所有可用写入器
  ./JobLens writer --list

  # 显示写入器详细帮助
  ./JobLens writer --doc ESWriter
```

---

## 运行时动态配置管理

配置加载后，可通过 **RPC 接口** 在运行时查询和管理，所有通信通过本地 UNIX Domain Socket：

| RPC 方法名 | 描述 |
|-----------|------|
| `Config/get_config` | 通过点分隔路径获取配置字段，参数 `{"path": "lens_config.log_level"}` |
| `Config/dump_config` | 将完整配置导出为 JSON |
| `Config/list_sections` | 列出所有顶级配置节 |
| `Config/validate_config` | 验证配置完整性（检查必填字段） |

---

## 查看收集器和写入器注册信息

使用子命令可在运行时查看已注册的收集器和写入器及其参数说明：

```bash
# 列出所有可用收集器
./JobLens collector -l

# 显示收集器详细帮助（含配置参数）
./JobLens collector -d CPUMemCollector

# 列出所有可用写入器
./JobLens writer -l

# 显示写入器详细帮助
./JobLens writer -d ESWriter
```

示例输出（`./JobLens collector -d CPUMemCollector`）：
```
TYPE:  CPUMemCollector
HELP:  从 /proc/[pid]/ 收集 CPU 和内存使用统计

PARAMS:
  freq       采样频率（Hz），如 0.2 表示每 5 秒采集一次
  summary    是否汇总所有进程数据（true/false），默认 false
```
