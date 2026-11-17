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
  auto_add_condorjob: true               # 已弃用：Condor 自动监控（改用 maintenance）
  auto_add_slurmjob: true                # 已弃用：Slurm 自动监控（改用 maintenance）
  maintenance:
    types: [condor, slurm]               # 需要完整本地作业记录的调度器类型（opt-in，缺省为 []）
    capture_all: true                    # 无条件登记每个捕获的作业（默认）；false = 登记与否交给 auto_add
    reconcile_interval_sec: 30           # 对账周期，最小 1 秒
```

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `job_db_path` | string | 无 | LevelDB 数据库目录路径。无法打开数据库时仅降级运行，不抛异常 |
| `auto_add_condorjob` | bool | false | **已弃用**。启动旧版 CondorJobWatcher 并自动登记作业。该类型出现在 `maintenance.types` 时忽略（由发现源负责） |
| `auto_add_slurmjob` | bool | false | **已弃用**。同上，作用于 Slurm |
| `maintenance.types` | list | `[]` | 需要完整本地作业记录的调度器类型（包含未被采集的作业）。取值 `condor` / `slurm`，未知值启动即拒绝 |
| `maintenance.capture_all` | bool | `true` | 全量登记开关。`true`：捕获的作业无条件登记；`false`：观测是否登记由该类型的 `<type>_job_watcher.auto_add`判定 |
| `maintenance.reconcile_interval_sec` | int | 30 | 周期性全量对账间隔，单位秒，最小 1 |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/condor_jobs`（Condor）或 `{JobLensRootDir}/../config/rules/slurm_jobs`（Slurm） | Lua 规则文件目录，在该类型的 `<type>_job_watcher.auto_add` 为 `rule` 时使用。注意：默认值因 watcher 而异。 |

### 三开关模型

自动登记与自动采集由且仅由三个开关决定，各自回答不同的问题，互不越权：

| 开关 | 键 | 回答的问题 | 默认值 |
|---|---|---|---|
| 全量登记 | `job_registry_config.maintenance.capture_all` | 捕获的作业是否可以无条件进入作业注册表？ | `true` |
| 按类型启停 Watcher | `job_registry_config.maintenance.types` | 该调度器类型是否启动发现 Watcher？ | `[]`（关） |
| 自动路径动作 | `<type>_job_watcher.auto_add`（`off` / `default` / `rule`） | 自动路径对该类型的观测做什么？ | `off` |

`auto_add` 的三态：

| `auto_add` | 含义 |
|---|---|
| `off` | 自动路径对该类型零动作 |
| `default` | 登记作业，并把 `<type>_job_watcher.auto_add_collectors` 逐字武装上去 |
| `rule` | 是否登记由 Lua 规则引擎逐作业判定：accept = 登记并加规则的采集器列表（规则未指定时回退 `auto_add_collectors`）；reject = 不登记（全量登记开启时：登记空选择留在库中） |

`auto_add` 的非法取值（非 `off`/`default`/`rule`）启动即拒绝。

#### Lua 规则契约

规则文件返回一个带 `condition(job)` 函数的表。condition 必须返回
`(bool[, string])`：

- **bool** 是该规则自己的判定——`true` 接受，`false` 不接受；
- 可选的 **string** 是逗号分隔的采集器列表（如
  `"cpumem_collector, net_collector"`）；空串或缺省即"接受但未指定列表"。

旧的 `{passed = ..., collectors = {...}}` 表返回不属于契约：返回表、缺首个
返回值或第二个返回值非字符串都按评估错误处理——该观测不登记（也不计
入 reject），下一条观测重新评估。

规则按优先级降序行走，**首个接受者胜出**：它的列表即选择，后续规则
不再评估；返回 `false` 的规则只是不接受，不否决后面的接受。所有规则
都不接受时该作业被拒绝。

接受了但未指定采集器的规则打 info 日志并回退到
`<type>_job_watcher.auto_add_collectors`；未配置缺省列表时该判定被拒
绝：打 error 日志、不登记该观测，计入 `admission_errors`（而非
reject）。

### 八状态组合矩阵

Watcher 关闭时 `auto_add` 失去意义，只有全量登记区分两行；Watcher 开启时
全量登记 × `auto_add` 共六种组合：

| # | Watcher | `capture_all` | `auto_add` | 该类型观测的行为 |
|---|---|---|---|---|
| 1 | 关 | ——（开） | —— | 该类型无发现；手工 CRUD 仍可用 |
| 2 | 关 | ——（关） | —— | 同上：无发现，自然无登记 |
| 3 | 开 | 开 | off | 全量登记；无自动采集器（仅显式请求） |
| 4 | 开 | 开 | default | 全量登记 + 默认采集器列表自动武装 |
| 5 | 开 | 开 | rule | 全量登记；规则引擎选采集器（reject 或无缺省的裸接受 = 登记空选择） |
| 6 | 开 | 关 | off | Watcher 照常扫描，但不登记任何作业（丢弃计入 `dropped_observations`） |
| 7 | 开 | 关 | default | 登记并把默认采集器列表作为初始选择 |
| 8 | 开 | 关 | rule | accept = 登记并加规则列表（裸接受回退 `auto_add_collectors`）；reject = 不登记（计入 `rejected_observations`）；引擎错误或无缺省的裸接受 = 本次不登记（计入 `admission_errors`），下一条观测重试 |

全量登记关闭时，准入计数在作业列表状态中可见：`capture_all`、
`dropped_observations`、`rejected_observations`、`admission_errors`。
事件路径返回类型化的 `not_admitted` 结果，且绝不会要求 watcher 重扫。

### 旧键映射

当 `auto_add` 缺省时，`auto_collect` / `use_rules` 组合与旧
`auto_add_<type>job` 旗标仍作为映射来源被接受，解析结果永远落在三态上：

| 旧配置 | 折算后的 `auto_add` |
|---|---|
| `auto_collect: false`（或缺省） | `off` |
| `auto_collect: true` + `use_rules: false` | `default` |
| `auto_collect: true` + `use_rules: true` | `rule` |
| `auto_add_<type>job: true`（旧旗标） | 先折算为 `auto_collect: true`，再按上表映射 |

显式给出的 `<type>_job_watcher.auto_add` 永远优先于旧键组合。

`<type>_job_watcher.cluster_name`（可选）在环境变量
（`COLLECTOR_HOST` / `SLURM_CLUSTER_NAME`）无法推导命名空间时显式指定；
完全没有来源时观测保持 incomplete，并在作业列表状态中可见。

发现集中在 `JobRegistry`：构造函数为每个已配置类型建一个 watcher 并启动
维护控制器（含首次全量扫描），整个栈通过 `stopDiscovery()` 与
`deinit()` 关闭。每个调度器类型只有一个 eBPF 源。

采集意图（期望选择）持久化在 LevelDB 中，由运行中的调度器应用到采集通道。
`jobs/collection/get` 区分 `desired_collectors` 与 `applied_collectors`：请求可以被
受理而运行时应用失败（见 `errors`），接口绝不会把未生效的绑报成"正在采集"。

### 手工 CRUD 面

手工路径与三开关正交——它不经由准入门，自动策略也永远不覆盖手工选择
（显式空的手工选择即 stop，会恒压自动策略）。

| 操作 | 入口 | 语义 |
|---|---|---|
| 增 / 改 | `jobs/upsert`（单次 upsert） | 身份命中活跃实例（类型化 `sub_attr` + 调度器命名空间）路由到 UPDATE：请求采集器**并入**现有手工选择（union 去重，绝不整替），事实只补空合并。类型化 sub_attr 命中的行，只有在其身份未解析、或 subtype 携带的 scheduler（schedd）归属与请求一致时，才按 internal id 路由 UPDATE：同一 native id 在另一个 schedd 下属于不同作业（各自 CREATE），归属未知的旧记录保持 legacy 容忍，多条命中报 `ambiguous_selector`，绝不猜测。其余走 CREATE：旧版手工登记语义，其查重按同样的 scheduler（schedd）归属判别，空采集器列表仍被拒绝（UPDATE 允许空列表 = 纯事实补录） |
| 查 / 改选择 | `jobs/collection/get`、`jobs/collection/set`、`jobs/collection/set (reset_auto)` | 不变；手工选择（含空 stop）永远压过自动选择 |
| 删 —— stop 档（默认） | `jobs/stop` | 停止采集（持久化空的手工选择）；作业记录保留 |
| 删 —— erase 档 | `jobs/erase` | 真删：先持久化 closing，采集过的实例归档为 `h:` 历史行，从未采集过的直接持久化删除。响应以 `mode: stop\|erase` 区分两档 |

归档还是删除由采集**事实**决定——退休被接受时捕获的运行时实际挂载快照。
一个已文档化的例外：进程在 closing 持久化与最终归档之间重启（内存快照随
进程消失）时，恢复的 closing 行回退使用其持久化的生效选择。因此「被选中
但从未成功挂载」的实例在这种重启后可能留下历史行。失败方向是宁可多留一
行也不静默丢弃；不经重启完成退休的实例始终按严格事实分类。

两档删除都支持按 `JobID` 或 `matcher` 寻址（与 stop 档一直使用的目标解析
一致）。trigger 的 `/joblens/job` 端点原样透传 `erase` 字段；缺省即 stop 档。

**作业数据存储**（LevelDB 键值存储）：
- 键使用前缀 `j:`（活跃作业）、`h:`（历史）、`c:`（ID 分配器）、`m:`（schema/DB UUID/迁移）、`i:`（规范身份索引）、`b:`（采集意图），值以 JSON 序列化
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
| | `pss` | bool | 默认 `true`。采集 PSS（比例集大小，共享页按进程数均摊）；`false` 时完全不读 `smaps_rollup`，输出中省略 `mem_pss_kb` |
| | `pss_fallback_smaps` | bool | 默认 `true`。内核无 `smaps_rollup` 时降级为逐 VMA 解析 `smaps`（成本更高）；设为 `false` 则直接关闭 PSS |
| | `pss_max_processes` | int | 默认 `64`。作业存活进程数超过该值时本样本整体跳过 PSS（全有或全无，不做部分求和）；`0` 表示不限 |
| **IOUsageCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的 I/O 数据 |
| | `use_ebpf` | string | 为 `"true"` 时，使用 eBPF 采集文件级 I/O 数据（需要 root 权限和内核支持） |
| | `collect_file_paths` | string | 为 `"true"` 时输出每个 fd 的 `path` 与 `mount_point`；默认 `"false"`。文件路径会暴露用户读写哪些文件，共享登录节点上应仅在排障时开启。 |
| **NetUsageCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的网络数据 |
| | `use_netlink` | string | 为 `"true"` 时，使用 netlink 查询 TCP_INFO（RTT、重传次数等） |
| **BasicInfoCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的 taskstats 数据（TGID 级摘要） |
| **GPUUsageCollector** | `summary` | string | 为 `"true"` 时，聚合所有进程的 GPU 使用数据 |
| | `freq` | double | 在 `init()` 中额外读取：用于计算 GPU 缓存刷新间隔（`1.0 / (freq * 1.5)`）。这是调度器层 `freq` 之外的额外使用。 |
| **TaskstatsCollector** | *(无)* | — | 无 init 配置参数。使用 Linux taskstats netlink 接口。**注意**：部分实现——在 taskstats 采样实现前，该采集器返回 `Skipped` 且不产生输出。 |
| **FSMetadataCollector** | `include_process_details` | string | 为 `"true"`（默认）时，输出 Job 聚合数据和按进程明细；为 `"false"` 时，只输出 Job 聚合数据 |
| | `freq` | double | 调度器层采样频率（Hz），由 `CollectorScheduler` 解析，不在 FSMetadataCollector 的 `init()` 中解析。建议元数据负载场景使用 1 Hz |
| **NewIOUsageCollector** | `include_process_details` | string | 为 `"true"`（默认）时，输出 Job 聚合数据和进程级/文件级明细；为 `"false"` 时，只输出 Job 聚合数据（`processes`/`files` 为空），可显著降低高文件句柄作业的输出量 |
| | `freq` | double | 调度器层采样频率（Hz），由 `CollectorScheduler` 解析，不在 NewIOUsageCollector 的 `init()` 中解析 |

**CPUMemCollector 说明**：
- 摘要记录（`summary: "true"`，`pid=0`）中的 `mem_peak_rss_kb` 是**每次采样聚合值**的时序最大值，
  不再是把各进程 `VmHWM` 相加（各进程峰值出现时刻不同，相加会系统性偏高）。
  进程级记录仍保留 `VmHWM` 语义；`sample_count` 表示峰值基于多少次采样（1 表示只采到一次）。
- `mem_pss_kb` 为 PSS（比例集大小）：共享页按映射它的进程数均摊，因此跨进程求和不会像 RSS
  那样重复计数（共享库、`/dev/shm` 暂存、fork 出的 worker 页）。单进程成本为一次
  `smaps_rollup` 读取（实测 64MB 约 0.5ms、256MB 约 1.3ms —— 内核按页表统计，成本随 RSS 增长），
  由 `pss` / `pss_fallback_smaps` 控制；作业存活进程数超过 `pss_max_processes`（默认 64）时
  本样本整体跳过 PSS，不做部分求和。PSS 不可用时字段直接省略，而不是上报 0。
- 作业 CPU 总量为 `utime + stime + cutime + cstime`：`cutime`/`cstime` 保留已被回收子进程的 CPU，
  否则两次采样之间起停的短命令 CPU 会丢失；`cpuPercent` 同样计入这部分时间。

**FSMetadataCollector 说明**：
- 通用文件系统元数据压力采集器，捕获 open/close/stat/readdir/rename/unlink/xattr/fsync 等操作
- 始终基于 eBPF：收集器内部初始化 eBPF，没有 `use_ebpf` 开关
- 当前输出维度为 JobID、`include_process_details=true` 时的 PID、元数据操作，以及 Job/操作延迟桶
- `include_process_details=true` 输出 Job 聚合指标和按进程明细；`include_process_details=false` 只输出 Job 聚合指标
- 输出不包含 `mount_point` 或 `fs_type`，此收集器不提供文件系统特定归因

**IO 文件元数据**：`IOUsageCollector` 与 `NewIOUsageCollector` 通过 `/proc/<pid>/fd` 解析文件路径，通过 `/proc/<pid>/mountinfo` 解析挂载信息，二者不会对目标文件调用 `stat()`，避免远端文件系统认证等待（例如 AFS）阻塞整个采集周期。旧 `pos` 字段此前上报的其实是文件大小而非当前文件偏移，现改为 JSON 输出 `null`、文本输出 `unknown`。文件级明细不再限定常规文件与块设备：任何绝对路径的 fd 目标都可能出现，包括目录或已删除但仍打开的文件。在不访问目标文件元数据的情况下，既有 IO 计数器仍照常可用。

**NewIOUsageCollector 说明**：
- 通用 I/O 计量采集器（VFS syscall 层语义，rchar/wchar），始终基于 eBPF，没有 `use_ebpf` 开关
- 输出维度：Job 级总量（`job_total`）、读/写时延直方图、进程级明细（含短命进程）、文件级明细（含按进程细分）——后两者仅在 `include_process_details=true` 时输出
- `include_process_details=false` 时同时跳过 fd→path 反查（mountinfo/readlink/stat），即明细路径用户态开销的大头
- 短命进程状态机与 `job_fd_stat` eBPF 条目清理不受此开关影响，持续运行，关闭明细不会导致 eBPF map 无限增长

---

## 4. `writers_config` — 写入器全局配置

```yaml
writers_config:
  enable_writer_perf: true       # 是否启用写入器性能计数器
  perf_window_size: 1000         # 性能计数器滑动窗口大小
  buffer_capacity: 1024          # 每个刷新缓冲区的预留容量（不是硬限制）
  max_pending_records: 4096      # 前后缓冲区中待处理记录的硬上限
  max_pending_bytes: 67108864    # 已计入字节的硬上限（64 MiB）
  max_record_bytes: 8388608      # 单条记录上限，超过即拒绝（8 MiB）
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
| `buffer_capacity` | int | `1024` | 每个刷新缓冲区的预留容量，**不是**队列上限 |
| `max_pending_records` | int | `4096` | writer 仍持有的已接受记录硬上限（前缓冲、活动缓冲与保留的重试） |
| `max_pending_bytes` | int | `67108864` | 已计入的 payload/元数据字节硬上限；准入不等待、不淘汰已接受记录 |
| `max_record_bytes` | int | `8388608` | 单条记录超过该值时返回 `TooLarge`；不得超过 `max_pending_bytes` |

所有队列限制都可以在各 writer 自己的配置节中用同名键覆盖。零或负值是配置错误，不是"无限制"模式。准入是异步的：`Accepted` 只表示 writer 取得了记录所有权，不代表已到达后端。

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

通过 RPC 端点暴露采集的指标数据供 Prometheus 抓取。此写入器维护一个以“指标名 + 标签”为键的通用 metric series 表；指标名、类型、标签和值由采集器决定。采集器未提供 `jobid` 时由写入器注入，写入器不再包含按采集器类型分支的 switch，也不再依赖中央指标结构体。

**RPC 端点**（自动注册）：

| 端点 | 描述 |
|------|------|
| `/<writer_name>/metrics` | 以通用 JSON 返回指标样本 |
| `/<writer_name>/metrics_text` | 返回已格式化的 Prometheus exposition 文本 |
| `/<writer_name>/info` | 返回写入器元数据及 series/job 计数 |

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
  auto_add: rule                # off | default | rule
  auto_add_collectors:          # auto_add: default 时逐字武装；rule 裸接受时作为回退
    - cpumem_collector
    - net_collector
    - io_collector
```

> **说明**：规则文件的匹配前缀就是 adapter 名（Condor 为 `condor`），只有文件名以它开头的 `.lua` 才会被该类型加载；`rules_prefix` 键已废弃并被忽略。规则在**启动时**加载（`auto_add: rule` 才会武装规则引擎）。`rules_dir` 优先读本节，缺省时回退到已废弃的 `job_registry_config.rules_dir`，再缺省为 `{JobLensRootDir}/../config/rules/condor_jobs`。目录为空或没有匹配前缀的文件时，启动日志会给出 warning，该类型不会选任何采集器。

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `auto_add` | string（`off`/`default`/`rule`） | `off` | 自动路径动作三态；显式给出时优先于已弃用的 `auto_collect`/`use_rules` 组合 |
| `auto_add_collectors` | string[] | 无 | 默认收集器列表：`auto_add: default` 时逐字武装；`auto_add: rule` 的裸接受以它回退 |
| `auto_collect` | bool | `false` | 旧键映射来源：`auto_add` 缺省时折算入 `auto_add` |
| `use_rules` | bool | `false` | 旧键映射来源：`auto_collect: true` + `use_rules: true` 折算为 `auto_add: rule` |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/condor_jobs` | 规则目录；支持多个类型共享，前缀保证互不串味 |
| `rules_prefix` | — | 已废弃 | 匹配前缀固定为 adapter 名，配置值被忽略并告警 |

### `slurm_job_watcher`

代码位置：`include/job_watcher/slurm_job_watcher.hpp`

```yaml
slurm_job_watcher:
  auto_add: off                 # off | default | rule
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
```

> **说明**：规则文件的匹配前缀就是 adapter 名（Slurm 为 `slurm`），`rules_prefix` 键已废弃并被忽略；规则在启动时随 `auto_add: rule` 一起加载。`rules_dir` 优先读本节，缺省时回退到已废弃的 `job_registry_config.rules_dir`，再缺省为 `{JobLensRootDir}/../config/rules/slurm_jobs`。

| 键 | 类型 | 默认值 | 描述 |
|----|------|--------|------|
| `auto_add` | string（`off`/`default`/`rule`） | `off` | 自动路径动作三态；显式给出时优先于已弃用的 `auto_collect`/`use_rules` 组合 |
| `auto_add_collectors` | string[] | 无 | 默认收集器列表：`auto_add: default` 时逐字武装；`auto_add: rule` 的裸接受以它回退 |
| `auto_collect` | bool | `false` | 旧键映射来源：`auto_add` 缺省时折算入 `auto_add` |
| `use_rules` | bool | `false` | 旧键映射来源：`auto_collect: true` + `use_rules: true` 折算为 `auto_add: rule` |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/slurm_jobs` | 规则目录 |
| `rules_prefix` | — | 已废弃 | 匹配前缀固定为 adapter 名，配置值被忽略并告警 |

---

## 10. 采集器输出通道

采集器在 `collect()` 内产出带类型的输出记录，通过 `OutputContext` 提交；writer 在 flush 时不再反查采集器实例或 parser。

### 声明通道

采集器声明自己能产出的通道（`document`、`text`、`metrics` 或额外名称），并实现：

```cpp
CollectOutcome collect(const Job& job, OutputContext& output) override;
std::vector<OutputChannel> output_channels() const override;
```

`CollectOutcome` 区分三种状态：

| 状态 | 含义 |
|------|------|
| `Success` | 采样成功；允许零输出（例如数值未变化） |
| `Skipped` | 没有可用样本；不得输出任何记录 |
| `Failed` | 采样失败；`error` 携带诊断。抛异常同样视为采集失败 |

输出后再返回 `Skipped` 属于契约错误，会出现在调度器状态中。输出被拒绝不会把一次成功的传感器读取变成失败的读取，但调度器会把该次回调标记为失败。

### 路由配置

```yaml
cpumem_collector_config:
  freq: 1
  summary: false
  outputs:
    document: [es_writer, kafka_writer]
    text: [file_writer]
    metrics: [prometheus_writer]
```

规则：

1. `outputs` 是显式配置：未列出的已声明通道被禁用；`outputs: {}` 禁用全部输出。
2. `outputs` 与旧版 `use_writers` 互斥，同时出现是配置错误。
3. 没有 `outputs` 时回退到采集器级 `use_writers`，再回退到 `collectors_config.default_use_writers`。显式空列表禁用输出，不回退默认值。
4. 旧版 writer 条目映射到该采集器唯一接受其 payload 类型的通道。无匹配或多匹配都是绑定错误，需要显式配置 `outputs`。
5. 同一通道内重复的目标会去重；同一 writer 被两个通道引用时会产生两条记录（有意为之）。
6. 未知 writer/通道、重复 writer 实例名、错误的值类型、不兼容的 payload 类型会让该采集器路由无效：该采集器不启动，其它采集器继续运行，错误出现在调度器状态中。

payload 契约：`FileWriter` 接受 `Text`；`ESWriter`/`KafkaWriter` 接受 `Document`（JSON 对象）；`PrometheusExporterWriter` 接受 `Metrics`（`MetricBatch`）。

### 标识与时间戳

WriterManager 在初始化时生成 128 位进程运行标识，并维护单调递增的采样计数器。`sample_id` 由两者组成；每次 emit 消耗一个上下文内序号，`record_id = sample_id + ":" + sequence`。ES 使用 `record_id` 作为 `_id`，相同时间戳的多条记录不再互相覆盖。`sampled_at` 在采集入口捕获一次，由该次采样的所有记录共享。

### 队列观测

`WriterManager/output_status` 始终可用（不依赖性能计数器开关），按 writer 返回：`accepted`、`rejected_full`、`rejected_too_large`、`rejected_closed`、`rejected_invalid`、`rejected_failed`、`pending_records`、`pending_bytes`、高水位、`released_records`、`flush_errors`。一致快照下满足 `accepted = pending_records + released_records`。

### 调度器状态语义

`CollectorScheduler/status` 返回 `operation_semantics_version: 2`：

- 一次 `emit()`/`emit_to()` 计为一次 `write` 操作（含扇出/准入）。零输出即零 write 操作；已声明但被禁用的通道是一次成功的空操作写入。
- `collect` 包含 payload 构造与嵌套写入；`callback` 覆盖整个调用。两者时间重叠，不能相加。
- 输出执行期间 `current_job.stage` 为 `write`；返回后恢复为 `collect`。

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
  # 文件路径属于敏感信息；定位 I/O 卡顿时再设为 true
  collect_file_paths: false
  use_writers:
    - es_writer

basic_info_collector_config:
  freq: 0.2
  summary: false

gpu_collector_config:
  freq: 1
  summary: true

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
  auto_add: off
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
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
