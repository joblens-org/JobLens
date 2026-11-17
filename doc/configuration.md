# Configuration Manual

JobLens uses YAML format configuration files, with the path specified via the `--config` / `-c` command-line argument.

---

## Configuration Loading Mechanism

The configuration file is loaded by the `Config` class (singleton). See `src/common/config.cpp` for details.

**Loading process**:
1. Parse command-line arguments to obtain the configuration file path.
2. `YAML::LoadFile()` loads the YAML file.
3. Recursively scan all nodes, resolving environment variable placeholders `{{ENV_NAME}}` or `{{ENV_NAME:default_value}}`.
4. Access configuration items using a two-level index of `parentKey` + `key`.

**Environment variable placeholder mechanism** (`resolveEnvVars` in `src/common/config.cpp`):
- Format: `{{ENV_VAR_NAME}}` or `{{ENV_VAR_NAME:default_value}}`
- Reads the environment variable value to replace the placeholder.
- If the environment variable does not exist and no default value is provided, throws a runtime exception.
- Supported in any string field in YAML.

```yaml
# Example: Environment variable substitution
lens_config:
  rpc_socket_path: "{{JOBLENS_RPC_PATH:/var/JobLens/rpc.sock}}"
  log_level: "{{JOBLENS_LOG_LEVEL:info}}"
```

---

## Configuration Sections Overview

| Top-level Key | Type | Description | Required |
|---------------|------|-------------|----------|
| `lens_config` | Map | Core system configuration | Yes |
| `job_registry_config` | Map | Job registry configuration | Yes (service mode) |
| `collectors_config` | Map | Collector global configuration and collector list | Yes (service mode) |
| `writers_config` | Map | Writer global configuration and writer list | Yes (service mode) |
| `condor_job_watcher` | Map | Condor job watcher configuration | No |
| `slurm_job_watcher` | Map | Slurm job watcher configuration | No |
| `{collector/writer}_config` | Map | Specific configuration sections for each collector/writer (name specified by the `config` field) | As needed |

---

## 1. `lens_config` — Core System Configuration


```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock   # UNIX Domain Socket path for local RPC communication
  lock_path: /var/JobLens/JobLens.lock      # Process lock file path to prevent multiple instances (main_utils::already_running)
  max_collector_threads: 8                   # Maximum threads for collector scheduler (TimerScheduler thread pool size)
  log_level: info                            # Log level: trace/debug/info/warn/error/critical/off
```

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `rpc_socket_path` | string | none | UNIX Domain Socket path for local RPC communication. If empty or missing, the service throws a runtime exception. | `main_utils.cpp:92`; `RPCServer::instance(RPC_Socket)` |
| `lock_path` | string | none | Process lock file, used by `already_running()` to detect if another instance is running. The parent directory is created automatically if needed. | `main_utils.cpp:104` |
| `max_collector_threads` | int | none | Thread pool size for the collector timer scheduler (`TimerScheduler`). | `collector_scheduler.cpp:25` |
| `log_level` | string | `"info"` (hard-coded fallback) | Log level: `trace` / `debug` / `info` / `warn` / `error` / `critical` / `off`. If the configured value is not in the map, falls back to `info`. | `main_utils.cpp:76-82` |

---

## 2. `job_registry_config` — Job Registry Configuration

```yaml
job_registry_config:
  job_db_path: /var/JobLens/job.db      # LevelDB database directory path for persisting job state
  auto_add_condorjob: true               # Deprecated: automatic Condor monitoring (see maintenance below)
  auto_add_slurmjob: true                # Deprecated: automatic Slurm monitoring (see maintenance below)
  maintenance:
    types: [condor, slurm]               # Types with a full local record set (opt-in, absent = [])
    capture_all: true                    # Register every captured job (default); false = admission by auto_add
    reconcile_interval_sec: 30           # Reconciliation interval, must be >= 1
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `job_db_path` | string | none | LevelDB database directory path. If the database cannot be opened, no exception is thrown; it only degrades operation |
| `auto_add_condorjob` | bool | false | **Deprecated.** Automatically start the legacy CondorJobWatcher and auto-add jobs. Ignored for a type listed in `maintenance.types` (the discovery source owns it) |
| `auto_add_slurmjob` | bool | false | **Deprecated.** Same as above for Slurm |
| `maintenance.types` | list | `[]` | Scheduler types that get job records, including unselected jobs. `condor` / `slurm`; unknown values are rejected at startup |
| `maintenance.capture_all` | bool | `true` | The blanket-registration switch. `true`: every captured job registers unconditionally. `false`: whether an observation registers is decided by the type's `<type>_job_watcher.auto_add` |
| `maintenance.reconcile_interval_sec` | int | 30 | Periodic full reconciliation interval in seconds (minimum 1) |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/condor_jobs` (Condor) or `{JobLensRootDir}/../config/rules/slurm_jobs` (Slurm) | Directory for Lua rule files, used when `<type>_job_watcher.auto_add` is `rule`. Note: the default varies by watcher context. |

### The three switches

Automatic registration and collection are governed by exactly three
switches. They answer different questions and never override each other:

| Switch | Key | Question it answers | Default |
|---|---|---|---|
| blanket registration | `job_registry_config.maintenance.capture_all` | May every captured job enter the registry unconditionally? | `true` |
| per-type watcher | `job_registry_config.maintenance.types` | Does this scheduler type get a discovery watcher at all? | `[]` (off) |
| automatic action | `<type>_job_watcher.auto_add` (`off` / `default` / `rule`) | What does the automatic path do with an observation of this type? | `off` |

The `auto_add` tri-state:

| `auto_add` | Meaning |
|---|---|
| `off` | The automatic path takes no action for this type |
| `default` | Register the job and arm `<type>_job_watcher.auto_add_collectors` verbatim |
| `rule` | Register is decided per job by the Lua rule engine: accept = register with the rule's collector list (or the `auto_add_collectors` defaults when the rule names none), reject = do not register (with blanket registration on: register with an empty selection) |

An invalid `auto_add` value (anything but `off`/`default`/`rule`) rejects
startup.

#### The Lua rule contract

A rule file returns a table with a `condition(job)` function. The condition
must return `(bool[, string])`:

- the **bool** is the rule's own verdict — `true` accepts the job, `false`
  declines it;
- the optional **string** is a comma-separated collector list (for example
  `"cpumem_collector, net_collector"`); an empty or absent string means
  "accepted without a specified list".

The old `{passed = ..., collectors = {...}}` table return is not part of the
contract: a table, a missing first value or a non-string second value is an
evaluation error — the observation is not registered (and not counted as a
reject) and the next one re-evaluates.

Rules walk in priority order (descending) and the **first accepting rule
wins**: its list is the selection and later rules are not consulted. A
declining (`false`) rule never vetoes a later accept. When every rule
declines the job is rejected.

An accepting rule that names no collectors logs at info and falls back to
`<type>_job_watcher.auto_add_collectors`. When no defaults are configured
the decision is refused: an error is logged, the observation is not
registered, and it is counted in `admission_errors` (not as a reject).

### The eight-state matrix

With the per-type watcher off, `auto_add` is irrelevant and only blanket
registration distinguishes the two rows. With the watcher on there are six
combinations of blanket registration and `auto_add`:

| # | watcher | `capture_all` | `auto_add` | Behavior for an observation of this type |
|---|---|---|---|---|
| 1 | off | — (on) | — | No discovery for the type; manual CRUD still works |
| 2 | off | — (off) | — | Same: nothing is discovered, so nothing is admitted |
| 3 | on | on | off | Blanket registration; no automatic collectors (explicit requests only) |
| 4 | on | on | default | Blanket registration + the default collector list armed automatically |
| 5 | on | on | rule | Blanket registration; the rule engine picks the collectors (reject or a refused bare accept = registered with an empty selection) |
| 6 | on | off | off | The watcher keeps scanning, but nothing registers (drops counted in `dropped_observations`) |
| 7 | on | off | default | Registers with the default collector list as the initial selection |
| 8 | on | off | rule | accept = registers with the rule's list (a bare accept falls back to `auto_add_collectors`); reject = never registers (`rejected_observations`); an engine error or a bare accept without defaults registers nothing, is counted in `admission_errors` and is retried on the next observation |

With blanket registration off, the admission counters are observable in the
joblens status: `capture_all`, `dropped_observations`,
`rejected_observations` and `admission_errors`. The event path reports a
typed `not_admitted` result that never asks the watcher to rescan.

### Legacy key mapping

When `auto_add` is absent, the `auto_collect` / `use_rules` pair and the
legacy `auto_add_<type>job` flags remain accepted as the mapping source. The
resolved state is always the tri-state:

| Old configuration | Resolved `auto_add` |
|---|---|
| `auto_collect: false` (or absent) | `off` |
| `auto_collect: true` + `use_rules: false` | `default` |
| `auto_collect: true` + `use_rules: true` | `rule` |
| `auto_add_<type>job: true` (legacy flag) | folded into `auto_collect: true` first, then mapped as above |

An explicit `<type>_job_watcher.auto_add` always wins over the legacy
combination.

`<type>_job_watcher.cluster_name` (optional) pins the scheduler namespace used
for identity when it cannot be derived from the environment
(`COLLECTOR_HOST` / `SLURM_CLUSTER_NAME`); without any source the observation
stays incomplete and is visible in the joblens status.

Discovery is centralized in `JobRegistry`: its constructor builds exactly one
watcher per configured type and starts the registry heartbeat (initial scan
included), and the whole stack stops through `stopDiscovery()`
and `deinit()`. There is no second eBPF source per scheduler type.

Collection intent (desired selection) is stored in LevelDB and applied to the
collector lanes by the running scheduler. `jobs/collection/get` distinguishes
`desired_collectors` from `applied_collectors`: a request can be accepted while
runtime application fails (`errors`), and the response never claims collection
is active when it is not.

### The manual CRUD surface

The manual path is orthogonal to the three switches — it never runs through
the admission gate, and the automatic policy never overwrites a manual
selection (an explicit empty manual selection, a stop, pins the automatic
policy down forever).

| Operation | Entry | Semantics |
|---|---|---|
| Create / Update | `jobs/upsert` (one upsert) | Identity hit on a live instance (typed `sub_attr` + scheduler namespace) routes to UPDATE: the requested collectors UNION into the existing manual selection (never a wholesale replace) and facts merge fill-empty-only. A typed-sub_attr duplicate routes to UPDATE by internal id only when the row has no resolved identity or its scheduler (schedd) attribution agrees with the request: the same native id under another schedd is a different job and creates its own record, a row with an unknown schedd keeps the legacy tolerance, and several surviving duplicates are an `ambiguous_selector` error, never a guess. Otherwise CREATE: the legacy manual registration, whose duplicate check discriminates by the same scheduler (schedd) attribution, and which still rejects an empty collector list (an UPDATE with no collectors is a pure facts update and stays valid) |
| Read / modify selection | `jobs/collection/get`, `jobs/collection/set`, `jobs/collection/reset` | Unchanged; a manual selection (including the empty stop) always wins over the automatic one |
| Remove — stop tier (default) | `jobs/stop` | Stops collection (a persistent empty manual selection); the job record is retained |
| Remove — erase tier | `jobs/erase` | Retires the instance for real: the closing fact is durable first, then a collected instance is archived as an `h:` history row and a never-collected one is durably deleted. The response distinguishes the tiers through `mode: stop\|erase` |

Archive-versus-delete is decided by the collection *fact* — the runtime's
actual attachment snapshot captured when the retirement is accepted. One
documented exception: when the process restarts between the durable
Closing write and the finalization (the in-memory capture dies with the
process), the restored closing row falls back to its durable effective
selection instead. A selected-but-never-attached instance may therefore
leave a history row after such a restart. The failure direction is
keep-a-row over silently dropping one; instances that retired without a
restart are classified strictly.

Both remove tiers address targets by `JobID` or by `matcher`, the same
resolution the stop tier always used. The trigger's `/joblens/job` endpoint
forwards the `erase` field verbatim; absent means the stop tier.

**Job data storage** (LevelDB key-value store):
- Keys use prefixes `j:` (active jobs), `h:` (history), `c:` (ID allocator), `m:` (schema/DB UUID/migration), `i:` (canonical identity index) and `b:` (collection intent) with values serialized as JSON
- Internally managed by `JobRegistry`; backed by LevelDB for persistence

---

## 3. `collectors_config` — Collector Global Configuration

```yaml
collectors_config:
  enable_collector_perf: true       # Whether to enable collector performance counters
  perf_window_size: 1000            # Sliding window size for performance counters (number of samples)
  default_freq: 1                   # Default sampling frequency (Hz), used when a collector does not configure `freq`
  default_use_writers:              # Default list of writers, used when a collector does not configure `use_writers`
    - es_writer
  collectors:                       # List of collector instances (see detailed description below)
    - name: cpumem_collector
      type: CPUMemCollector
      config: cpumem_collector_config
    # ... more collectors
```

### Common Parameters

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `enable_collector_perf` | bool | none | When enabled, each `collect` call records its duration; performance statistics can be queried via RPC | `collector_registry.cpp:30` |
| `perf_window_size` | int | none | Performance window size, affects the number of samples for mean/variance calculation | `collector_registry.cpp:32` |
| `default_freq` | int | none | Default sampling frequency (Hz) for collectors | `collector_scheduler.cpp:27` |
| `default_use_writers` | string[] | none | Default list of writers for collectors | `collector_scheduler.cpp:28` |

### `collectors` Array Elements

Each collector instance is an object containing:

```yaml
collectors:
  - name: <instance_name>       # string, uniquely identifies this collector instance
    type: <collector_type>     # string, corresponds to a registered collector type name (e.g., CPUMemCollector)
    config: <config_section_name>   # string, points to a configuration section at the same level containing this collector's specific parameters
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Collector instance name, referenced in a Job's `CollectorNames` |
| `type` | string | Collector type, must match the registered name (case‑sensitive) |
| `config` | string | References the name of a configuration section at the same level; the key‑value pairs under that section are passed as JSON configuration to `init()` for this collector |

### Common Collector Configuration Section Parameters

The configuration section for each collector (referenced by the `config` field) can include the following common fields:

```yaml
cpumem_collector_config:       # configuration section name, specified by the `config` field in the collectors array
  freq: 1                      # (double) Sampling frequency (Hz), priority: freq > period > default_freq
  period: 1                    # (double) Sampling period (seconds), mutually exclusive with freq: period = 1/freq
  use_writers:                 # (string[]) List of writers to output to; if not set, default_use_writers is used
    - es_writer
    - prmxs_writer
```

**Note:** Only one of freq and period needs to be set, the same applies hereinafter

| Key | Type | Priority | Description |
|-----|------|----------|-------------|
| `freq` | double | Highest | Sampling frequency (Hz), e.g., `0.2` means collect once every 5 seconds |
| `period` | double | Second highest | Sampling period (seconds), mutually exclusive with `freq` |
| `use_writers` | string[] | - | List of output writers. If not configured, `collectors_config.default_use_writers` is used |

System‑wide collectors additionally support:
```yaml
net_sys_collector_config:
  auto_start: true     # System‑level collector automatically starts running at startup
```

### Collector‑Specific `init()` JSON Configuration Parameters

The collector's `init(json_cfg)` method receives a JSON object converted from the YAML config node. Below are the keys recognized by each collector:

> **Note**: `freq`, `period`, and `use_writers` are parsed at the **scheduler level** (`collector_scheduler.cpp`) and shared by all collectors — they are **not** parsed inside each collector's `init()` method. The only exception is `GPUUsageCollector`, which additionally reads `freq` inside `init()` for cache refresh interval calculation.

| Collector Type | Configuration Parameter | Value Type | Description | Source Reference |
|----------------|------------------------|------------|-------------|------------------|
| **CPUMemCollector** | `summary` | string | When `"true"`, aggregates CPU/memory data from all processes into a single summary record (pid=0) |
| | `pss` | bool | Default `true`. Collects PSS (proportional set size, shared pages split across processes). `false` disables `smaps_rollup` reads entirely and omits `mem_pss_kb` from the output |
| | `pss_fallback_smaps` | bool | Default `true`. On kernels without `smaps_rollup`, degrades to per-VMA `smaps` parsing (higher cost); set `false` to disable PSS instead |
| | `pss_max_processes` | int | Default `64`. When a job has more live pids than this, PSS is skipped for that sample (all-or-nothing, never a partial sum). `0` means unlimited | `cpumem_collector.cpp:42` |
| **IOUsageCollector** | `summary` | string | When `"true"`, aggregates I/O data across all processes | `io_usage_collector.cpp:186` |
| | `use_ebpf` | string | When `"true"`, uses eBPF to collect file‑level I/O data (requires root + kernel support) | `io_usage_collector.cpp:192` |
| | `collect_file_paths` | string | When `"true"`, emits per-fd `path` and `mount_point`. Default `"false"`: file paths reveal what each user reads/writes and should not be collected on shared login nodes unless needed for diagnosis. | `io_usage_collector.cpp` |
| **NetUsageCollector** | `summary` | string | When `"true"`, aggregates network data across all processes | `net_usage_collector.cpp:477` |
| | `use_netlink` | string | When `"true"`, uses netlink to query TCP_INFO (RTT, retransmissions, etc.) | `net_usage_collector.cpp:482` |
| **BasicInfoCollector** | `summary` | string | When `"true"`, aggregates taskstats data across all processes (TGID-level summary) | `basic_info_collector.cpp:45` |
| **GPUUsageCollector** | `summary` | string | When `"true"`, aggregates GPU usage data across all processes | `gpu_usage_collector.cpp:162` |
| | `freq` | double | Additionally read in `init()`: used to calculate GPU cache refresh interval (`1.0 / (freq * 1.5)`). This is in addition to the scheduler-level `freq`. | `gpu_usage_collector.cpp:156-197` |
| **TaskstatsCollector** | *(none)* | — | No init config parameters. Uses Linux taskstats netlink interface. **Note**: partially implemented — the collector returns `Skipped` and produces no output until taskstats sampling is implemented. | `taskstats_collector.cpp:44-47` |
| **FSMetadataCollector** | `include_process_details` | string | When `"true"` (default), emits a Job aggregate plus per-process detail. When `"false"`, emits the Job aggregate only. | `fs_metadata_collector.cpp:64` |
| | `freq` | double | Scheduler-level sampling frequency (Hz), parsed by `CollectorScheduler`, not by FSMetadataCollector `init()`. Recommended: 1 Hz for metadata workloads | `collector_scheduler.cpp` |
| **NewIOUsageCollector** | `include_process_details` | string | When `"true"` (default), emits Job aggregate plus per-process and per-file detail. When `"false"`, emits the Job aggregate only (empty `processes`/`files`), significantly reducing output volume for jobs with many open file handles. | `new_io_usage_collector.cpp:116` |
| | `freq` | double | Scheduler-level sampling frequency (Hz), parsed by `CollectorScheduler`, not by NewIOUsageCollector `init()` | `collector_scheduler.cpp` |

**CPUMemCollector Notes**:
- Job summary records (`summary: "true"`, `pid=0`) report `mem_peak_rss_kb` as the
  maximum of the **per-sample aggregated** RSS, not the sum of each process's
  `VmHWM`. Per-process records keep `VmHWM` semantics. `sample_count` states how
  many samples the peak is based on (1 means the job was only seen once).
- `mem_pss_kb` is PSS (proportional set size): shared pages are split across the
  processes that map them, so summing PSS over a job does not over-count shared
  libraries, `/dev/shm` staging or forked worker pages the way summing RSS does.
  Per-process cost is one `smaps_rollup` read (~0.1 ms for a 60 MB / 50-VMA
  process on WSL2, but 0.5-1.3 ms for 64-256 MB on an AlmaLinux 9 node: the cost
  tracks RSS because the kernel walks page tables), controlled by `pss` /
  `pss_fallback_smaps`. Jobs with more than `pss_max_processes` (default 64) live
  pids skip PSS for that sample instead of producing a partial sum. When PSS is
  unavailable the field is omitted instead of reported as `0`.
- Job total CPU time is `utime + stime + cutime + cstime`: `cutime`/`cstime`
  carry the CPU of children that were already reaped, which is otherwise lost
  when a short-lived child exits between two samples. `cpuPercent` includes it.

**FSMetadataCollector Notes**:
- Generic filesystem metadata pressure collector capturing open/close/stat/readdir/rename/unlink/xattr/fsync operations
- Always eBPF-backed: the collector initializes eBPF internally and has no `use_ebpf` switch
- Current output dimensions are JobID, PID when `include_process_details=true`, metadata operation, and Job/operation latency buckets
- `include_process_details=true` emits both Job aggregate metrics and per-process detail. `include_process_details=false` emits Job aggregate metrics only
- Output does not include `mount_point` or `fs_type`; the collector does not provide filesystem-specific attribution

**IO file metadata**: `IOUsageCollector` and `NewIOUsageCollector` resolve file
paths through `/proc/<pid>/fd` and mount information through `/proc/<pid>/mountinfo`.
They do not call `stat()` on target files, avoiding remote filesystem authentication
waits (for example, AFS). The legacy `pos` field previously reported file size,
not the current file offset; it is now `null` in JSON and `unknown` in text output.
File details are no longer restricted to regular files and block devices: any
absolute fd target may appear, including directories or deleted open files.
Existing IO counters remain available without target metadata access.
Per-fd `path`/`mount_point` are only emitted when `collect_file_paths=true`;
the default is `false` because paths reveal user file access on shared nodes.

**NewIOUsageCollector Notes**:
- Generic I/O accounting collector at the VFS syscall layer (rchar/wchar semantics), always eBPF-backed with no `use_ebpf` switch
- Output dimensions: Job-level totals (`job_total`), read/write latency histograms, per-process detail (including short-lived processes), and per-file detail with per-process breakdown — the latter two only when `include_process_details=true`
- `include_process_details=false` also skips fd→path resolution (mountinfo/readlink/stat), the dominant user-space cost of the detail path
- Short-lived process lifecycle tracking and `job_fd_stat` eBPF entry cleanup keep running regardless of this switch; disabling details does not cause eBPF map growth

---

## 4. `writers_config` — Writer Global Configuration

Code location: `src/core/writer_manager.cpp`, `src/writer/base_writer.cpp`

```yaml
writers_config:
  enable_writer_perf: true       # Whether to enable writer performance counters
  perf_window_size: 1000         # Sliding window size for performance counters
  buffer_capacity: 1024          # Reserve capacity per flush buffer (not a hard limit)
  max_pending_records: 4096      # Hard bound on records pending in front/active buffers
  max_pending_bytes: 67108864    # Hard bound on accounted bytes (64 MiB)
  max_record_bytes: 8388608      # Reject a single record above this size (8 MiB)
  writers:                       # List of writer instances
    - name: es_writer
      type: ESWriter
      config: ES_writer_config
    # ... more writers
```

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `enable_writer_perf` | bool | `true` | When enabled, records duration of each `flush`, supports RPC queries | `writer_manager.cpp:28` |
| `perf_window_size` | int | `1000` | Performance window size | `writer_manager.cpp:30` |
| `buffer_capacity` | int | `1024` | Reserve capacity per flush buffer; it is **not** a queue limit | `base_writer.cpp` |
| `max_pending_records` | int | `4096` | Hard bound on accepted records still owned by the writer (front + active buffers, including retained retries) | `base_writer.cpp` |
| `max_pending_bytes` | int | `67108864` | Hard bound on accounted payload/metadata bytes; admission never waits or evicts accepted records | `base_writer.cpp` |
| `max_record_bytes` | int | `8388608` | Reject a single record larger than this with `TooLarge`; must not exceed `max_pending_bytes` | `base_writer.cpp` |

All queue limits may be overridden per writer instance inside that writer's config section (same key names). Zero or negative limits are configuration errors, not an unlimited mode. Admission is asynchronous: `Accepted` means the writer owns the record, not that it reached the backend.

### `writers` Array Elements

The structure is similar to collectors:

```yaml
writers:
  - name: <instance_name>       # string, writer instance name
    type: <writer_type>     # string, corresponds to a registered type (ESWriter, KafkaWriter, FileWriter, PrometheusExporterWriter)
    config: <config_section_name>   # string, points to a specific configuration section at the same level
```

---

## 5. ESWriter Configuration

Code location: `src/writer/es_writer.cpp`

```yaml
ES_writer_config:
  host: 192.168.1.100           # Elasticsearch server address
  port: 9200                     # Elasticsearch port
  user: ""                       # ES username (optional)
  passwd: ""                     # ES password (optional)
  index_prefix: collector        # Index name prefix
  batch_size: 100                # 兼容旧配置，当前不控制 HTTP 请求分包
  max_bulk_bytes: 1048576        # 请求体字节上限，包含元数据和换行
  max_retries: 3                # 首次发送失败后的额外重试轮数，0 表示禁用
  retry_initial_backoff_ms: 200
  retry_max_backoff_ms: 5000
  write_timeout: 30              # Write timeout (seconds)
  indexs:                        # Optional index name mapping list
    - collector_name: cpumem_collector
      index_name: cpu_mem_metrics
    - collector_name: io_usage_collector
      index_name: io_metrics
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `host` | string | Yes | - | ES server address; supports `http://` prefix |
| `port` | int | Yes | - | ES server port |
| `user` | string | No | `""` | Basic authentication username |
| `passwd` | string | No | `""` | Basic authentication password |
| `index_prefix` | string | No | `"collector"` | Index name prefix, final index name is `{prefix}_{collector_name}_{YYYY.MM.DD}` |
| `batch_size` | int | No | `1` | Legacy buffer reservation setting; does not cap HTTP request size |
| `max_bulk_bytes` | int | No | `1048576` | Positive maximum bulk request body size in bytes, including action metadata and both newline delimiters |
| `max_retries` | int | No | `3` | Additional retry rounds, 0–10; each document is sent at most 1 + max_retries times |
| `retry_initial_backoff_ms` | int | No | `200` | Initial backoff ceiling in milliseconds; positive and no greater than retry_max_backoff_ms |
| `retry_max_backoff_ms` | int | No | `5000` | Backoff ceiling, at most 60000 ms |
| `write_timeout` | int | No | `5` | Positive timeout in seconds per HTTP request, including retries |
| `indexs` | array | No | - | Explicit mapping from collector to index name. Each element contains `collector_name` and `index_name` |

**Index rendering supports variable templates**: Index names support `[[job_info.field]]` syntax, rendered at runtime using `Utils::render_bracket()` and `Utils::flatten_json()`.

**Time suffix**: Automatically appends a date suffix of the form `_{YYYY.MM.DD}`.

**Byte-based batching**: ESWriter measures the actual serialized UTF-8 NDJSON bytes and splits only between complete action/source pairs. A request exactly equal to `max_bulk_bytes` is allowed. Set this value no higher than the smallest request body limit across your proxies and Elasticsearch nodes. Zero and negative values are rejected when initializing the writer.

If a single action/source pair exceeds the limit, it is not sent or truncated: the writer logs its collector, job ID, byte size and limit, continues sending other valid documents, and reports the flush as failed. Parser failures are logged and dropped rather than indexing an error placeholder. Increasing the proxy limit or changing the document model is still necessary for oversized individual documents.

**Failure handling**: HTTP success is checked against the Bulk `errors` and individual `items`. The ESWriter `on_flush_error()` hook retries only transient failures and records with unknown delivery results. Retryable HTTP/item statuses are 408, 429, 500, 502, 503 and 504; temporary resolution, connection, timeout, send/receive and incomplete-response errors are also retried. Other statuses (including 400, 401, 403, 404, 409 and 413), TLS/configuration errors and oversized documents are logged and dropped without retry. Missing or malformed Bulk responses are treated as unknown delivery, not success. When the item count matches, valid successful items are excluded from retry even if another item is malformed.

Retries reuse the original serialized action/source, index and ID, and obey the same byte limit. Each round waits a random delay between half and all of `min(initial_backoff * 2^(round-1), max_backoff)`. After the configured rounds, remaining failures are logged and discarded; nothing is persisted or replayed after restart. Unknown-delivery retries may repeat an already applied `index` operation: stable IDs prevent allocating new IDs, but do not provide exactly-once delivery or protect against concurrent updates to the same ID.

Retries run synchronously on the flush path and can delay subsequent batches and shutdown. The retry count limits attempts, not total flush wall time across multiple HTTP chunks. Pending retry payloads are retained in memory for the current flush only. Writer `call_cnt` counts each nonempty logical flush once, not HTTP attempts; `err_cnt` counts initially failed flushes even when the hook later recovers, and timing includes backoff and retries. Collector timing retains its existing success-only sampling semantics; exceptions increment `err_cnt` without appending a sample.

**Per‑document structure** (automatically wrapped by ESWriter):
```json
{
  "@timestamp": "2026-04-24T10:30:00+0800",
  "hostname": "node-1",
  "job_info": { "JobID": 1001, ... },
  "data": { ... }  // generated by the collector's parser function
}
```

---

## 6. KafkaWriter Configuration

**Note:** Since the current deployment model does not involve this writer, it has not been tested yet

```yaml
kafka_writer_config:
  brokers:                            # Kafka broker list
    - 192.168.1.100:9092
    - 192.168.1.101:9092
  topic_prefix: collector-data        # Topic prefix, final topic name is {prefix}{collector_name}
  topic_dlq: collector-data-dlq       # Dead letter queue topic
  client_id: joblens-producer         # Client ID
  transactional_id: joblens-txn       # Transactional ID
  batch_rows: 100                     # Batch send size
  linger_ms: 10                       # Maximum milliseconds to wait before sending
  enable_transaction: false           # Whether to enable Kafka transactions
  security_protocol: plaintext        # Security protocol: plaintext / sasl_plaintext / ssl / sasl_ssl
  # The following are used only when security_protocol != "plaintext"
  sasl_mechanism: PLAIN               # SASL mechanism: PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
  username: joblens                   # SASL username
  password: joblens_secret            # SASL password
```

| Key | Type | Required | Condition | Description |
|-----|------|----------|-----------|-------------|
| `brokers` | string[] | Yes | - | List of Kafka broker addresses |
| `topic_prefix` | string | Yes | - | Topic prefix, final `topic = topic_prefix + collector_name` |
| `topic_dlq` | string | Yes | - | Dead letter queue topic name |
| `client_id` | string | Yes | - | Kafka client identifier |
| `transactional_id` | string | Yes | - | Transactional ID |
| `batch_rows` | int | Yes | - | Number of messages per batch; `queue.buffering.max.messages` is set to `batch_rows * 10` |
| `linger_ms` | int | Yes | - | Maximum time to wait before sending messages |
| `enable_dlq` | bool | N/A | `true` (internal default) | This key is **declared** in the writer's parameter list but **not read from config file**. The actual value is auto-managed: defaults to `true` internally; set to `false` automatically when `security_protocol` is `"plaintext"`. |
| `enable_transaction` | bool | Yes | - | Whether to enable transactions |
| `security_protocol` | string | Yes | - | Security protocol |
| `sasl_mechanism` | string | Yes | security_protocol != "plaintext" | SASL mechanism |
| `username` | string | Yes | security_protocol != "plaintext" | Authentication username |
| `password` | string | Yes | security_protocol != "plaintext" | Authentication password |

**Message serialization structure**:
```json
{
  "collector_name": "cpumem_collector",
  "hostname": "node-1",
  "@timestamp": 1745476200000,
  "job_info": { "JobID": 1001, ... },
  "data": { ... }
}
```

## 7. FileWriter Configuration

Code location: `src/writer/file_writer.cpp`

Writes collector-formatted plain text to local files. The collector parser for `FileWriter` returns the complete text chunk, including any required newline characters, and FileWriter writes that text verbatim. Supports append and atomic overwrite modes, size-based file rotation, and per-collector output routing.

### Basic Configuration

```yaml
file_writer_config:
  path: /var/log/joblens/output.log    # Default output file path (required)
```

### Full Configuration with All Options

```yaml
file_writer_config:
  path: /var/log/joblens/output.log          # Default output file path (required)
  write_mode: append                         # Write mode: 'append' or 'overwrite'
  flush_on_shutdown: true                    # Flush remaining records on graceful shutdown
  enable_rotation: false                     # Enable size-based rotation (append only)
  max_file_size_bytes: 104857600             # Rotation threshold in bytes (100 MB)
  max_files: 5                               # Maximum rotated files to retain
  outputs:                                   # Per-collector file routing (optional)
    - collector_name: cpumem_collector
      file_path: /var/log/joblens/cpumem.log
    - collector_name: io_usage_collector
      file_path: /var/log/joblens/io.log
```

### Option Reference

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `path` | string | **Yes** | - | Default output file path. Records from collectors not listed in `outputs` are written to this file. |
| `write_mode` | string | No | `"append"` | Write mode. `"append"` opens the file in append mode and preserves all history. `"overwrite"` atomically replaces the target file with each flush batch using a same-directory temp file and rename. |
| `flush_on_shutdown` | bool | No | `true` | When `true`, any remaining buffered records are flushed to all destination files during graceful shutdown. When `false`, FileWriter-level explicit `flush()` is skipped during shutdown, but BaseWriter final buffer flush still occurs before streams are closed. |
| `enable_rotation` | bool | No | `false` | When `true`, size-based rotation is applied to destination files. Only supported in append mode. |
| `max_file_size_bytes` | int | No | `104857600` (100 MB) | Rotation threshold. When the active file would exceed this size after writing the next batch, rotation is triggered before the write. |
| `max_files` | int | No | `5` | Maximum number of rotated files to retain. Rotated files use the naming pattern `path.1` (newest), `path.2`, ..., `path.N` (oldest). The oldest file is deleted when the count exceeds `max_files`. |
| `outputs` | array | No | `[]` | List of collector-to-file routing entries. Each entry has `collector_name` and `file_path`. When empty or omitted, all collectors write to `path`. |

### Collector-to-File Routing

The `outputs` list maps specific collector instances to dedicated output files. Each entry contains:

| Field | Type | Description |
|-------|------|-------------|
| `collector_name` | string | Name of the collector instance (must match the `name` field in `collectors_config.collectors`). |
| `file_path` | string | Target file path for this collector's output. |

**Fallback behavior**: When a collector is not listed in `outputs`, its records are written to the default `path`. A warning is emitted once per unmapped collector when that collector first writes data at runtime, to help catch configuration mistakes.

**Example**: Route `cpumem_collector` to a dedicated file while other collectors fall back to the default path:

```yaml
file_writer_config:
  path: /var/log/joblens/output.log
  outputs:
    - collector_name: cpumem_collector
      file_path: /var/log/joblens/cpumem.log
```

### Conflict Rules

The following configurations are rejected at startup with a clear error message:

| Conflict | Applies To | Error Message |
|----------|------------|---------------|
| `write_mode: overwrite` + `enable_rotation: true` | All modes | "write_mode 'overwrite' is incompatible with enable_rotation" |
| Duplicate `collector_name` in `outputs` | All modes | "duplicate collector_name 'X' in 'outputs'" |
| Duplicate `file_path` in `outputs` | Overwrite mode only | "duplicate file_path 'X' in 'outputs' (forbidden in overwrite mode)" |
| `outputs` entry `file_path` equals default `path` | Overwrite mode only | "'outputs' entry for collector 'X' resolves to default path 'Y', which is forbidden in overwrite mode" |

**Allowed in append mode**:
- Multiple collectors can share the same output file path. Text chunks from both collectors are appended in flush order, producing a combined plain-text file.
- An `outputs` entry can point to the same path as the default `path`. Shared collectors and fallback collectors both append to the same file.

### Write Mode Details

**Append mode** (`write_mode: append`):
- Each destination file is opened in append mode.
- Text chunks are appended to the file in flush order. The file grows monotonically.
- Supports size-based rotation when `enable_rotation: true`.
- Multiple collectors can share the same destination path.

**Overwrite mode** (`write_mode: overwrite`):
- Each flush batch is written to a temporary file in the same directory as the target, then atomically renamed over the target.
- The target file contains only the latest flush batch. No history is preserved.
- Rotation is disabled; configuring both `overwrite` and `enable_rotation: true` causes a fast-fail.
- Duplicate output paths across `outputs` entries are forbidden.
- On failure, the previous target file is preserved where possible, the temp file is cleaned up, and the error is logged.

> **Note**: Atomic overwrite uses same-directory temp files (`target.tmp.<pid>.<timestamp>`) to ensure rename atomicity. Temp files are automatically removed after a successful rename. If the process crashes mid-write, orphan temp files may remain; these are harmless and can be cleaned up manually.

### Rotation Naming and Retention

Rotation is size-only and uses the following naming scheme:

| File | Description |
|------|-------------|
| `path` | Active file (always the newest data) |
| `path.1` | Most recent rotated file |
| `path.2` | Second most recent |
| ... | ... |
| `path.N` | Oldest rotated file (N = max_files) |

When rotation is triggered:
1. If `path.N` exists, it is deleted.
2. `path.(N-1)` is renamed to `path.N`, cascading down to `path.1` → `path.2`.
3. The active `path` is renamed to `path.1`.
4. A new active `path` is opened for writing.

The entire rotation cascade happens before the triggering batch is written, so the batch is always written to the new active file. No batch is split across rotation boundaries.

> **Note**: Rotation is disabled by default. Only size-based rotation is available in this version.

---

## 8. PrometheusExporterWriter Configuration

Code location: `src/writer/prometheus_exporter_writer.cpp`

Exposes collected metrics via RPC endpoints for Prometheus scraping. This writer maintains a generic metric series table keyed by metric name plus labels; collectors choose the metric names, types, labels and values. The writer injects `jobid` when the collector omits it and does not contain a per-collector switch or a central metric schema.

**RPC Endpoints** (automatically registered):

| Endpoint | Description |
|----------|-------------|
| `/<writer_name>/metrics` | Returns generic metric samples as JSON |
| `/<writer_name>/metrics_text` | Returns preformatted Prometheus exposition text |
| `/<writer_name>/info` | Returns writer metadata and series/job counts |

```yaml
prmxs_writer_config: {}    # Currently no config parameters; the section must exist for the `config` field to reference
```

> **Note**: The writer exposes metrics purely via RPC (no FIFO or file output). The configuration section is currently empty — all behavior is driven by the collector data pushed through the scheduler pipeline.

---

## 9. Job Watcher Configuration

### `condor_job_watcher`

Code location: `include/job_watcher/condor_job_watcher.hpp`

```yaml
condor_job_watcher:
  auto_add: rule                # off | default | rule
  auto_add_collectors:          # armed for auto_add: default; fallback for a bare rule accept
    - cpumem_collector
    - net_collector
    - io_collector
```

> **Note**: The rule file prefix is the adapter name (`condor`): only `.lua` files whose basename starts with it are loaded, and `rules_prefix` is deprecated and ignored. Rules are loaded at **startup** (`auto_add: rule` arms the engine). `rules_dir` resolves from this section first, then the deprecated `job_registry_config.rules_dir`, then the default `{JobLensRootDir}/../config/rules/condor_jobs`. An empty directory or no prefix match logs a startup warning and selects no collectors for the type.

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `auto_add` | string (`off`/`default`/`rule`) | `off` | The automatic-action tri-state; an explicit value wins over the deprecated `auto_collect`/`use_rules` pair | `maintenance_config.cpp` |
| `auto_add_collectors` | string[] | none | The default collector list: armed verbatim for `auto_add: default`, and the fallback for an `auto_add: rule` accept that names no collectors | `condor_job_watcher.hpp:28` |
| `auto_collect` | bool | `false` | Legacy mapping source: folds into `auto_add` when `auto_add` is absent | `maintenance_config.cpp` |
| `use_rules` | bool | `false` | Legacy mapping source: `auto_collect: true` + `use_rules: true` maps to `auto_add: rule` | `condor_job_watcher.hpp:29` |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/condor_jobs` | Rule directory; several types may share it safely thanks to the prefix | `maintenance_config.cpp` |
| `rules_prefix` | — | deprecated | The matching prefix is always the adapter name; a configured value is ignored with a warning | `maintenance_config.cpp` |

### `slurm_job_watcher`

Code location: `include/job_watcher/slurm_job_watcher.hpp`

```yaml
slurm_job_watcher:
  auto_add: off                 # off | default | rule
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
```

> **Note**: The rule file prefix is the adapter name (`slurm`); `rules_prefix` is deprecated and ignored, and rules load at startup with `auto_add: rule`. `rules_dir` resolves from this section first, then the deprecated `job_registry_config.rules_dir`, then the default `{JobLensRootDir}/../config/rules/slurm_jobs`.

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `auto_add` | string (`off`/`default`/`rule`) | `off` | The automatic-action tri-state; an explicit value wins over the deprecated `auto_collect`/`use_rules` pair | `maintenance_config.cpp` |
| `auto_add_collectors` | string[] | none | The default collector list: armed verbatim for `auto_add: default`, and the fallback for an `auto_add: rule` accept that names no collectors | `slurm_job_watcher.hpp:29` |
| `auto_collect` | bool | `false` | Legacy mapping source: folds into `auto_add` when `auto_add` is absent | `maintenance_config.cpp` |
| `use_rules` | bool | `false` | Legacy mapping source: `auto_collect: true` + `use_rules: true` maps to `auto_add: rule` | `slurm_job_watcher.hpp:30` |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/slurm_jobs` | Rule directory | `maintenance_config.cpp` |
| `rules_prefix` | — | deprecated | The matching prefix is always the adapter name; a configured value is ignored with a warning | `maintenance_config.cpp` |

---

## 10. Collector Output Channels

Collectors produce typed output records during `collect()` and submit them through an `OutputContext`. Writers never look up a collector instance or parser while flushing.

### Declaring channels

A collector declares the channels it can produce (`document`, `text`, `metrics`, or additional names) and implements:

```cpp
CollectOutcome collect(const Job& job, OutputContext& output) override;
std::vector<OutputChannel> output_channels() const override;
```

`CollectOutcome` distinguishes three states:

| State | Meaning |
|-------|---------|
| `Success` | Sampling succeeded; zero emissions are allowed (for example unchanged values) |
| `Skipped` | No sample was available; the collector must not emit anything |
| `Failed` | Sampling failed; `error` carries a diagnostic. Exceptions are also collection failures. |

Returning `Skipped` after emitting is a contract error reported in scheduler status. An output rejection does not turn a successful sensor read into a failed read, but the scheduler reports it as a failed callback.

### Routing configuration

```yaml
cpumem_collector_config:
  freq: 1
  summary: false
  outputs:
    document: [es_writer, kafka_writer]
    text: [file_writer]
    metrics: [prometheus_writer]
```

Rules:

1. `outputs` is explicit: omitted declared channels are disabled; `outputs: {}` disables all output.
2. `outputs` and the legacy `use_writers` key are mutually exclusive.
3. Without `outputs`, `use_writers` (collector level) or `collectors_config.default_use_writers` is used. An explicit empty list disables output and does not fall back.
4. A legacy writer entry maps to the collector's unique channel that accepts its payload kind. No match or multiple matches is a binding error; configure `outputs` explicitly.
5. Duplicate destinations within one channel are deduplicated. One writer referenced by two channels intentionally receives two records.
6. Unknown writers/channels, duplicate writer instance names, wrong value types, and incompatible payload kinds make the collector's route invalid. That collector does not start; other collectors continue and the error appears in scheduler status.

Payload contracts: `FileWriter` accepts `Text`, `ESWriter` and `KafkaWriter` accept `Document` (JSON object), `PrometheusExporterWriter` accepts `Metrics` (`MetricBatch`).

### Identity and timestamps

WriterManager creates a 128-bit process-run identifier at initialization and a monotonic per-sample counter. `sample_id` combines both values; every emit consumes a per-context sequence and `record_id = sample_id + ":" + sequence`. ES uses `record_id` as `_id`, so records sharing a timestamp no longer overwrite each other. `sampled_at` is captured once at collection entry and shared by all records of that sample.

### Queue observability

`WriterManager/output_status` is always available (independent of performance counters) and returns per writer: `accepted`, `rejected_full`, `rejected_too_large`, `rejected_closed`, `rejected_invalid`, `rejected_failed`, `pending_records`, `pending_bytes`, high-water marks, `released_records`, and `flush_errors`. At a coherent snapshot `accepted = pending_records + released_records`.

### Scheduler status semantics

`CollectorScheduler/status` reports `operation_semantics_version: 2`:

- One `emit()`/`emit_to()` call counts as one `write` operation, including fan-out/admission. Zero emissions produce zero write operations; a disabled valid channel is a successful no-op write.
- `collect` includes payload preparation and nested writes; `callback` wraps the whole invocation. Those durations overlap and must not be added together.
- While an emission runs, `current_job.stage` is `write`; after it returns the stage is `collect` again.

---

## Complete Configuration File Example

```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock
  lock_path: /var/JobLens/JobLens.lock
  max_collector_threads: 8
  log_level: info

job_registry_config:
  job_db_path: /var/JobLens/job.db   # LevelDB database directory path
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

## Command‑Line Startup Arguments

```bash
JobLens - A job monitor system

Usage:
  JobLens [options] [subcommand]

Options:
  -h, --help                    Show help information
  -c, --config <path>           Configuration file path (default: config.yaml)
  -m, --mode <mode>             Running mode (default: service)
  -v, --version                 Show version information

Running modes:
  service    Pure service mode: start the collection scheduler and run continuously until a shutdown signal is received

Subcommands:
  collector  Manage collectors (list, get help)
  writer     Manage writers (list, get help)

Signal handling:
  SIGINT / SIGTERM              Gracefully shut down the service

Examples:
  # Start in service mode
  ./JobLens -m service -c config.yaml

  # Show version
  ./JobLens -v

  # List all available collectors
  ./JobLens collector --list

  # Show detailed help for a collector
  ./JobLens collector --doc CPUMemCollector

  # List all available writers
  ./JobLens writer --list

  # Show detailed help for a writer
  ./JobLens writer --doc ESWriter
```

---

## Runtime Dynamic Configuration Management

After the configuration is loaded, it can be queried and managed at runtime via **RPC interfaces**, all communicating over a local UNIX Domain Socket:

| RPC Method Name | Description |
|----------------|-------------|
| `Config/get_config` | Get a configuration field by dot‑separated path, parameter `{"path": "lens_config.log_level"}` |
| `Config/dump_config` | Export the full configuration as JSON |
| `Config/list_sections` | List all top‑level configuration sections |
| `Config/validate_config` | Validate configuration completeness (check required fields) |

---

## Viewing Collector and Writer Registration Information

Using subcommands, you can view registered collectors and writers and their parameter descriptions at runtime:

```bash
# List all available collectors
./JobLens collector -l

# Show detailed help for a collector (including configuration parameters)
./JobLens collector -d CPUMemCollector

# List all available writers
./JobLens writer -l

# Show detailed help for a writer
./JobLens writer -d ESWriter
```

Example output (`./JobLens collector -d CPUMemCollector`):
```
TYPE:  CPUMemCollector
HELP:  Collect CPU and memory usage statistics from /proc/[pid]/

PARAMS:
  freq       Sampling frequency in Hz, e.g., 0.2 for once every 5 seconds
  summary    Whether to summarize data across all processes (true/false), default false
```
