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
  pid_dir: /var/JobLens/node_pids            # PID 文件目录，分布式节点存储 node_<pid> 文件
  max_collector_threads: 8                   # 收集器调度器最大线程数
  log_level: info                            # 日志级别：trace/debug/info/warn/error/critical/off
```

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `rpc_socket_path` | string | 无 | 本地 RPC 通信的 UNIX Domain Socket 路径 |
| `lock_path` | string | 无 | 进程锁文件，`already_running()` 用于检测是否已有实例运行 |
| `max_collector_threads` | int | 无 | 收集器定时调度器的线程池大小 |
| `log_level` | string | 回退到 info | 日志级别：trace/debug/info/warn/error/critical/off |

---

## 2. `job_registry_config` — 作业注册中心配置

```yaml
job_registry_config:
  job_db_path: /var/JobLens/job.db      # 持久化作业状态的 SQLite 数据库路径
  auto_add_condorjob: true               # 是否自动监控 Condor 作业
  auto_add_slurmjob: true                # 是否自动监控 Slurm 作业
```

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `job_db_path` | string | 无 | SQLite 数据库路径，存储 `jobs` 和 `jobs_history` 表。无法打开数据库时仅降级运行，不抛异常 |
| `auto_add_condorjob` | bool | false | 自动启动 CondorJobWatcher，通过 eBPF 追踪 `condor_starter` 进程并自动添加 Condor 作业 |
| `auto_add_slurmjob` | bool | false | 自动启动 SlurmJobWatcher，通过 eBPF 追踪 `slurm_stepd` 进程并自动添加 Slurm 作业 |

**数据库表结构**（自动创建）：
- `jobs` — 当前活跃作业表，包含列 `jobid_high`, `jobid_low`, `jobtype`, `subtype`, `pids`, `collectors`, `status`
- `jobs_history` — 历史作业表，与 `jobs` 结构相同，额外包含 `end_time` 时间戳
- `job_id_counter` — 自增 ID 计数器表

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
| `job_adder_fifo` | string | 仅在头文件中定义 | 外部进程向此 FIFO 写入 JSON 以动态添加作业 |

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

收集器的 `init(json_cfg)` 方法接收从 YAML 节点转换的 JSON。以下是各收集器识别的键：

| 收集器类型 | 配置参数 | 值类型 | 描述 |
|-----------|---------|--------|------|
| **CPUMemCollector** | `summary` | string | 为 `"true"` 时，将所有进程的 CPU/内存数据聚合为一条摘要记录（pid=0） |
| **IOUsageCollector** | `summary` | string | 为 `"true"` 时，聚合 I/O 数据 |
| | `use_ebpf` | string | 为 `"true"` 时，使用 eBPF 采集文件级 I/O 数据 |
| **NetUsageCollector** | `summary` | string | 为 `"true"` 时，聚合网络数据 |
| | `use_netlink` | string | 为 `"true"` 时，使用 netlink 查询 TCP_INFO（默认 true） |

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
  enable_dlq: false                   # 是否启用死信队列
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
| `enable_dlq` | bool | 是 | - | 是否启用死信队列 |
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

## 7. PrometheusExporterWriter 配置

**注意：** 当前部署模式未涉及此写入器，尚未经过测试。

---

## 8. 作业监听器配置

### `condor_job_watcher`

```yaml
condor_job_watcher:
  auto_add_collectors:          # 当 use_rules 为 false 时，发现新 Condor 作业后自动追加的默认收集器列表
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false              # 是否启用规则引擎过滤作业
  rules_dir: /path/to/rules/condor_jobs     # Lua 规则文件目录
  rules_prefix: condor_job_     # Lua 规则文件前缀（默认：condor_job_）
```

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `auto_add_collectors` | string[] | 无 | 自动添加到 Condor 作业的收集器名称列表 |
| `use_rules` | bool | `false` | 是否使用规则引擎过滤作业（不匹配规则的作业不添加） |
| `rules_dir` | string | `{InstallDir}/../config/rules/condor_jobs` | Lua 规则文件目录 |
| `rules_prefix` | string | `condor_job_` | 规则文件前缀 |

### `slurm_job_watcher`

```yaml
slurm_job_watcher:
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false
  rules_dir: /path/to/rules/slurm_jobs
  rules_prefix: slurm_job_
```

参数同上，默认 `rules_dir` 为 `{InstallDir}/../config/rules/slurm_jobs`。

---

## 完整配置文件示例

```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock
  lock_path: /var/JobLens/JobLens.lock
  pid_dir: /var/JobLens/node_pids
  max_collector_threads: 8
  log_level: info

job_registry_config:
  job_db_path: /var/JobLens/job.db
  auto_add_condorjob: true
  auto_add_slurmjob: false

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

prmxs_writer_config:
  fifo_path: /tmp/prom_metrics.fifo

kafka_writer_config:
  brokers:
    - 192.168.1.100:9092
  topic_prefix: collector-data
  topic_dlq: collector-data-dlq
  client_id: joblens-producer
  transactional_id: joblens-txn
  batch_rows: 100
  linger_ms: 10
  enable_dlq: false
  enable_transaction: false
  security_protocol: plaintext

file_writer_config:
  path: /var/log/joblens/output.log

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
  -m, --mode <mode>             运行模式（默认：starter）
  -e, --exec <executable>       要运行的可执行文件路径（starter 模式使用）
  -a, --args <args>             可执行文件的参数列表
  -v, --version                 显示版本信息

运行模式：
（注意：这是历史遗留问题。在 JobLens 早期设计阶段，设计目标是针对单个指定作业提供细粒度追踪，因此保留了此模式。
经过讨论，此模式已废弃，但启动选项尚未移除）

  starter    启动+监控模式：启动子进程并监控其性能（默认）
  service    纯服务模式：启动采集调度器，持续运行直到收到关闭信号

子命令：
  collector  管理收集器（列出、获取帮助）
  writer     管理写入器（列出、获取帮助）

信号处理：
  SIGINT / SIGTERM              优雅关闭服务

示例：
  # 基本启动（starter 模式）
  ./JobLens -c config.yaml

  # 服务模式
  ./JobLens -m service -c config.yaml

  # 启动并监控特定进程
  ./JobLens -c config.yaml -e /path/to/program -a "arg1 arg2"

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
