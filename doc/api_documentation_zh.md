# JobLens Trigger API 文档

基础 URL：`http://<host>:<port>`

除非另有说明，所有 API 路由均返回 JSON 响应。

---

## 1. Metrics

### `GET /metrics`

获取 JobLens writer 的 Prometheus 格式指标数据。

**响应格式：** `text/plain`（Prometheus exposition 格式）

**状态码：**
- `200` - 成功
- `503` - RPC 调用失败
- `500` - 未知错误

### `GET /`

获取 Trigger 服务基本信息，包括版本、状态和可用端点。

**响应体：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `service` | string | 服务名称（如 `"JobLens Trigger"`） |
| `version` | string | Trigger 组件版本 |
| `status` | string | 服务状态（如 `"running"`） |
| `endpoints` | object | 关键 API 端点参考 |

**状态码：**
- `200` - 成功

### `GET /trigger/health`

获取 Trigger 服务各组件的详细健康状态，不依赖 JobLens RPC。

**响应体：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `service` | string | 服务名称 |
| `version` | string | Trigger 版本 |
| `status` | string | 服务状态 |
| `components` | object | 内部组件健康状态（`rpc_client`、`service_registrar`、`config_manager`、`rule_manager`） |
| `joblens` | object | JobLens 核心版本信息（`version`、`build_id`、`build_time`） |

**状态码：**
- `200` - 成功

---

## 2. 健康检查

### `GET /joblens/healthy`

检查 JobLens systemd 服务的健康状态。

**响应体（HealthResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `active` | bool | 服务是否处于 `active` 状态 |
| `state` | string | Systemd active state（如 `"active"`） |
| `sub` | string | Systemd sub state（如 `"running"`） |
| `healthy` | bool | 整体健康状态：当 state=`"active"` 且 sub=`"running"` 时为 `true` |

**状态码：**
- `200` - 成功
- `400` - 运行时错误（如 systemctl 不可用）
- `500` - 内部错误

### `GET /joblens/rpc/health`

通过 RPC 调用检查 JobLens 服务器健康状态，包含延迟测量。

**响应体（RPCHealthResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `rpc_latency_ms` | float | RPC 往返延迟（毫秒） |
| `*` | mixed | `health` RPC 调用返回的其他动态字段 |

**状态码：**
- `200` - 成功
- `503` - RPC 调用失败
- `500` - 未知错误

---

## 3. 作业管理

### `POST /joblens/job`

通用作业操作（添加或移除）。

**请求体（JobRequest）：**

| 字段 | 类型 | 必填 | 描述 |
|---|---|---|---|
| `opt` | string | **是** | 操作类型：`"add"` 或 `"remove"` |
| `type` | string | **是** | 作业类型：`"job.condor"`、`"job.slurm"` 或 `"job.common"` |
| `JobID` | int | 否 | 作业 ID |
| `JobPIDs` | array[int] | 否 | 进程 PID 列表 |
| `Lens` | array[string] | 否 | 收集器列表（默认：`["cpumem_collector", "io_collector", "net_collector"]`） |
| `sub_attr` | object | 否 | 子属性：`condor` 类型需 `cluster_id`/`proc_id`，`slurm` 类型需 `job_id`/`step_id` |

**示例请求：**
```json
{
  "opt": "add",
  "type": "job.condor",
  "JobID": 1,
  "JobPIDs": [1],
  "Lens": [""]
}
```

**响应体（JobResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态（如 `"ok"`） |
| `received` | object | 处理后的请求数据 |

**状态码：**
- `200` - 成功
- `400` - 无效的请求体或字段
- `500` - 作业操作失败

---

### `POST /joblens/condor_job`

Condor 作业专用操作，目前仅支持 `"add"`。

**请求体（CondorJobRequest）：**

| 字段 | 类型 | 必填 | 描述 |
|---|---|---|---|
| `opt` | string | **是** | 操作类型，仅支持 `"add"` |
| `JobID` | int | 否 | 作业 ID |
| `slot` | string | **是** | 槽位名称，必须以 `"slot"` 开头 |
| `Lens` | array[string] | 否 | 收集器列表（默认：`["cpumem_collector", "io_collector", "net_collector"]`） |
| `sub_attr` | object | 否 | 子属性（如 `{"cluster_id": 123456, "proc_id": 0}`） |

**示例请求：**
```json
{
  "opt": "add",
  "JobID": 1,
  "slot": "slotx",
  "Lens": [""]
}
```

**响应体（JobResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态（如 `"ok"`） |
| `received` | object | 处理后的请求数据 |

**状态码：**
- `200` - 成功
- `400` - 无效的请求体、缺失或无效的 `slot`、不支持的 `opt`
- `500` - 内部错误

---

### `POST /joblens/slurm_job`

Slurm 作业专用操作，目前仅支持 `"add"`。

**请求体（SlurmJobRequest）：**

| 字段 | 类型 | 必填 | 描述 |
|---|---|---|---|
| `opt` | string | **是** | 操作类型，仅支持 `"add"` |
| `JobID` | int | **是** | Slurm 作业 ID |
| `Lens` | array[string] | 否 | 收集器列表（默认：`["cpumem_collector", "io_collector", "net_collector"]`） |
| `sub_attr` | object | 否 | 子属性（如 `{"job_id": 12345, "step_id": 0}`） |

**示例请求：**
```json
{
  "opt": "add",
  "JobID": 12345,
  "Lens": [""]
}
```

**响应体（JobResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态（如 `"ok"`） |
| `received` | object | 处理后的请求数据 |

**状态码：**
- `200` - 成功
- `400` - 无效的请求体或不支持的 `opt`
- `500` - 内部错误

---

### `GET /joblens/jobs/count`

获取当前已注册作业数量（排除 JobID=0 的彩蛋作业）。

**响应体（JobCountResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态 |
| `job_count` | int | 已注册作业总数 |

**状态码：**
- `200` - 成功
- `503` - RPC 调用失败
- `500` - 未知错误

---

### `GET /joblens/jobs`

列出所有已注册作业（排除 JobID=0 的彩蛋作业）。

**响应体（JobsListResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态 |
| `jobs` | array[object] | 作业对象列表（动态字段） |

**状态码：**
- `200` - 成功
- `503` - RPC 调用失败
- `500` - 未知错误

---

### `GET /joblens/jobs/{job_id}`

按 JobID 获取特定作业的详细信息。

**路径参数：**

| 参数 | 类型 | 描述 |
|---|---|---|
| `job_id` | int | 要查询的作业 ID |

**响应体（JobDetailResponse）：** RPC 结果中的动态字段。

**状态码：**
- `200` - 成功
- `404` - 作业未找到
- `503` - RPC 调用失败
- `500` - 未知错误

---

## 4. RPC 函数

### `GET /joblens/rpc/functions`

列出服务器上注册的所有可用 RPC 方法。

**响应体（RPCFunctionsResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 响应状态（如 `"ok"`） |
| `functions` | array[string] | 可用 RPC 函数名列表 |
| `count` | int | 可用函数数量 |

**状态码：**
- `200` - 成功
- `503` - RPC 调用失败
- `500` - 未知错误

---

## 5. 收集器

### `GET /joblens/collectors/perf`

获取所有收集器的性能统计。

**响应体（CollectorsPerfResponse）：** RPC 结果中的动态字段。

**状态码：**
- `200` - 成功
- `500` - 收集器性能数据错误
- `503` - RPC 调用失败
- `500` - 未知错误

---

## 6. 写入器

### `GET /joblens/writers/{writer_name}/info`

获取特定写入器的基本信息。

**路径参数：**

| 参数 | 类型 | 描述 |
|---|---|---|
| `writer_name` | string | 写入器名称（如 `"es_writer"`、`"kafka_writer"`、`"file_writer"`） |

**响应体（WriterInfoResponse）：** RPC 结果中的动态字段。

**状态码：**
- `200` - 成功
- `404` - 写入器未找到
- `503` - RPC 调用失败
- `500` - 未知错误

---

### `GET /joblens/writers/perf`

获取所有写入器的性能统计。

**响应体（WritersPerfResponse）：** RPC 结果中的动态字段。

**状态码：**
- `200` - 成功
- `500` - 写入器性能数据错误
- `503` - RPC 调用失败
- `500` - 未知错误

---

## 7. 配置管理

### `GET /joblens/config/status`

获取配置管理器状态。

**响应体（ConfigStatusResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `enabled` | bool | 配置管理器是否启用 |
| `running` | bool | 配置管理器是否正在运行 |
| `mode` | string | 当前配置模式 |
| `use_etcd` | bool | 是否使用 etcd |
| `sync_status` | object | 同步状态详情 |
| `message` | string | 状态消息（未启用时出现） |

**状态码：**
- `200` - 成功

### `POST /joblens/config/update`

手动触发配置重新加载（调用配置管理器的回调）。

**响应体（ConfigUpdateResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态（如 `"ok"`） |
| `message` | string | 状态消息 |

**状态码：**
- `200` - 成功
- `503` - ConfigManager 未初始化
- `500` - 配置更新失败

---

## 8. 服务注册中心

### `GET /joblens/registry/status`

获取服务注册状态。

**响应体（RegistryStatusResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `enabled` | bool | 服务注册器是否启用 |
| `registered` | bool | 服务是否已注册 |
| `service_id` | string | 服务 ID |
| `service_host` | string | 服务主机名/IP |
| `service_port` | int | 服务端口 |
| `registry_url` | string | 注册中心 URL |
| `heartbeat_interval` | int | 心跳间隔（秒） |
| `version` | string | 版本信息 |
| `etcd_path` | string | etcd 键路径 |
| `etcd_workdir` | string | etcd 工作目录 |
| `etcd_addr` | string | etcd 地址 |
| `etcd_port` | int | etcd 端口 |
| `message` | string | 状态消息（未启用时出现） |

**状态码：**
- `200` - 成功

---

### `POST /joblens/registry/register`

手动触发服务注册。

**响应体（RegistryRegisterResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态（`"ok"` 或 `"warning"`） |
| `message` | string | 状态消息 |

**状态码：**
- `200` - 注册成功
- `202` - 注册失败但服务继续运行（status=`"warning"`）
- `503` - ServiceRegistrar 未初始化
- `500` - 注册错误

---

## 9. 规则管理

### `GET /joblens/rules`

列出所有本地规则文件。

**响应体（RulesListResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `enabled` | bool | 规则管理器是否启用 |
| `count` | int | 规则数量 |
| `rules` | array[string] | 规则文件名列表 |
| `message` | string | 状态消息（未启用时出现） |

**状态码：**
- `200` - 成功
- `500` - 获取规则列表失败

---

### `GET /joblens/rules/{filename}`

获取特定规则文件的内容。

**路径参数：**

| 参数 | 类型 | 描述 |
|---|---|---|
| `filename` | string | 规则文件名（必须以 `.lua` 结尾，不含路径分隔符） |

**响应格式：** `text/plain`（原始 Lua 文件内容）

**状态码：**
- `200` - 成功
- `400` - 无效的文件名
- `404` - 规则文件未找到
- `503` - RuleManager 未初始化
- `500` - 读取错误

---

### `POST /joblens/rules/sync`

手动触发从 etcd 到本地文件的完整规则同步。

**响应体（RulesSyncResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `status` | string | 操作状态（如 `"ok"`） |
| `message` | string | 状态消息 |

**状态码：**
- `200` - 成功
- `503` - RuleManager 未初始化
- `500` - 同步失败

---

### `GET /joblens/rules/status`

获取规则管理器状态。

**响应体（RuleStatusResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `enabled` | bool | 规则管理器是否启用 |
| `running` | bool | 规则管理器是否正在运行 |
| `etcd_priority` | bool | etcd 是否具有优先级 |
| `etcd_role_path` | string | etcd 角色路径 |
| `etcd_workdir_prefix` | string | etcd 工作目录前缀 |
| `local_rules_dir` | string | 本地规则目录路径 |
| `role_id` | string | 角色 ID |
| `local_rules_count` | int | 本地规则数量 |
| `remote_rules_count` | int | 远程规则数量 |
| `callbacks_count` | int | 已注册回调数量 |
| `etcd_role_watch_id` | string | etcd 角色监听 ID |
| `etcd_role_info_watch_id` | string | etcd 角色信息监听 ID |
| `file_watcher_running` | bool | 文件监听器是否正在运行 |
| `syncing` | bool | 是否正在同步 |
| `sync_lock_locked` | bool | 同步锁是否被持有 |

**状态码：**
- `200` - 成功

---

## 10. 版本与升级

### `GET /joblens/version`

获取 Trigger 和底层 JobLens 服务的版本信息。

**响应体（VersionResponse）：**

| 字段 | 类型 | 描述 |
|---|---|---|
| `trigger_version` | string | Trigger 组件版本 |
| `joblens_version` | string | JobLens 服务版本 |
| `build_id` | string | 构建标识符 |
| `build_time` | string | 构建时间戳 |

**状态码：**
- `200` - 成功（始终返回 200，出错时回退为 `"UNKNOWN"` 值）

---

## 11. 其他

### `GET /utils/hardware_info`

获取硬件信息，包括 CPU 型号、内存大小、磁盘信息等。

**响应体（HardwareInfoResponse）：** 硬件信息工具返回的动态字段。

**状态码：**
- `200` - 成功
- `500` - 获取硬件信息失败

---

## 12. 作业清单与采集控制

以下接口透传 Core 的清单/采集契约（`doc/job_inventory_collection_decoupling_plan.md` §9）。
Trigger 只做校验与转发：不分配、不推导内部 ID，Core 的错误绝不会被包装成 HTTP 200。

### `GET /joblens/inventory`

有界、只读的清单快照。

| 查询参数 | 类型 | 说明 |
|---|---|---|
| `limit` | int | 每页条数，1–1000，默认 100 |
| `scheduler_type` | string | `condor` 或 `slurm` |
| `native_job_id` | string | 调度器原生 ID（精确匹配） |
| `state` | string | `active`、`suspect` 或 `closing` |
| `cursor` | string | 上一页返回的不透明游标 |

**响应（200）：**
```json
{
  "status": "ok",
  "snapshot_revision": 1234567890,
  "records": [{
    "instance_id": "ji1-...",
    "internal_job_id": "42",
    "schema_version": 2,
    "facts_revision": 3,
    "state": "active",
    "source": "condor",
    "identity_complete": true,
    "scheduler_type": "condor",
    "cluster_name": "htcondor01",
    "cluster_tag": "schedd-a",
    "native_job_id": "172.0",
    "step_id": "",
    "generation": {"boot_id": "...", "manager_pid": 1234, "manager_start_ticks": 999},
    "processes": [{"pid": 1235, "start_ticks": 1000, "boot_id": "..."}],
    "attributes": {"owner": "alice"}
  }],
  "next_cursor": null
}
```

游标仅在快照未变化时有效，否则返回 `409 cursor_invalidated`。新增接口中的内部 ID
一律为十进制字符串，避免 JavaScript 的 uint64 精度丢失。

### `GET /joblens/inventory/status`

来源、对账、队列与持久化健康状态（active/suspect/closing 计数、扫描时间、
丢弃事件、存储降级、不完整/隔离身份、已生效与待排空状态）。原样透传 Core，
即使状态为 `degraded` 也返回 `200`。

### `POST /joblens/collection`

对一个精确解析出的目标集合设置或重置采集意图。

**请求体：**
```json
{
  "selector": {"scheduler_type": "condor", "cluster_name": "htcondor01",
               "cluster_tag": "schedd-a", "native_job_id": "172.0"},
  "scope": "instance",
  "ensure_registered": true,
  "operation": "set",
  "collectors": ["cpumem_collector"],
  "expected_revision": 0
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `selector` | 是 | 二选一：`instance_id`，或 `scheduler_type` + `cluster_name` + `native_job_id`（可选 `cluster_tag`、`step_id`） |
| `scope` | 否 | `instance`（默认）或 `job`。`instance_id` 只支持 `instance`；Slurm `job` 覆盖当前已观测的所有本地 step |
| `ensure_registered` | 否 | 匹配前先执行一次权威发现 |
| `operation` | 是 | `set` 或 `reset_auto` |
| `collectors` | `set` 必填 | 显式列表；`[]` 表示持久停止。`reset_auto` 必须省略 |
| `expected_revision` | 否 | 绑定修订号校验，仅单实例请求可携带（`0` 表示尚无意图） |

**响应（200）：**
```json
{
  "status": "accepted",
  "operation": "set",
  "scope": "instance",
  "targets": [{
    "instance_id": "ji1-...",
    "internal_job_id": "42",
    "native_job_id": "172.0",
    "binding_revision": 1,
    "mode": "manual",
    "desired_collectors": ["cpumem_collector"],
    "applied_collectors": ["cpumem_collector"],
    "applied_revision": 1,
    "errors": [],
    "changed": true
  }]
}
```

`applied_collectors` 是运行时状态：Agent 没有采集运行时、或采集器拒绝绑定时，
它是空的并在 `errors` 中给出原因。`status: accepted` 只表示意图已持久化。

### `GET /joblens/collection?instance_id=...`

查询单个实例句柄的期望/已生效选择、模式、修订号、策略修订号与逐采集器错误。
句柄不存在时 `404`，指向正在关闭的代际时 `409`。

### `GET /joblens/capabilities`

声明 `inventory_version`、`collection_control_version`、`native_identity_selectors`、
`job_scope` 与 DB schema 版本。Trigger 绝不代替 Core 声明能力。

**状态码：**
- `200` - 成功
- `400` - 请求非法（未知采集器、缺少 `collectors`、选择器不合法）
- `404` - 没有已验证的当前实例
- `409` - 选择器歧义、`expected_revision` 过期、代际正在关闭、游标失效
- `502` - Core 返回的响应不符合契约
- `503` - Core RPC 不可用、存储不可用或清单仍在迁移

---

## 响应状态码汇总

| 状态码 | 描述 |
|---|---|
| `200` | 成功 |
| `202` | 已接受（异步操作已启动） |
| `400` | 请求错误（无效的参数或请求体） |
| `401` | 未授权（缺失或无效的认证头） |
| `403` | 禁止访问（无效 token） |
| `404` | 资源未找到 |
| `409` | 冲突（选择器歧义、修订号过期、代际正在关闭、游标失效） |
| `500` | 内部服务器错误 |
| `502` | Core 返回的响应不符合契约 |
| `503` | 服务不可用（RPC 故障、存储不可用或清单初始化中） |
