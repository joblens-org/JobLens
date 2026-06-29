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
| `job_registry_config` | Map | Job registry configuration | No |
| `collectors_config` | Map | Collector global configuration and collector list | No |
| `writers_config` | Map | Writer global configuration and writer list | No |
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
  auto_add_condorjob: true               # Whether to automatically monitor Condor jobs
  auto_add_slurmjob: true                # Whether to automatically monitor Slurm jobs
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `job_db_path` | string | none | LevelDB database directory path. If the database cannot be opened, no exception is thrown; it only degrades operation |
| `auto_add_condorjob` | bool | false | Automatically start CondorJobWatcher and auto-add Condor jobs discovered from cgroup v2 creation events |
| `auto_add_slurmjob` | bool | false | Automatically start SlurmJobWatcher and auto-add Slurm jobs discovered from cgroup v2 creation events |
| `rules_dir` | string | `{JobLensRootDir}/../config/rules/condor_jobs` (Condor) or `{JobLensRootDir}/../config/rules/slurm_jobs` (Slurm) | Directory for Lua rule files, used by CondorJobWatcher and SlurmJobWatcher when `use_rules` is enabled. Note: the default varies by watcher context. |

### cgroup v2 process discovery

For HTCondor and Slurm jobs, JobLens uses a single eBPF `raw_tracepoint/cgroup_mkdir` hook as the automatic discovery trigger and uses cgroup v2 `cgroup.procs` as the primary source for job process membership. The cgroup v2 mount point is discovered from `/proc/self/mountinfo`; cgroup event paths are normalized through that mount point, and scheduler metadata is still resolved from representative PIDs through existing scheduler metadata paths. Existing Trigger and RPC request payloads remain compatible and do not require cgroup fields.

First-round support is limited to the cgroup v2 unified hierarchy for HTCondor and Slurm. cgroup v1/hybrid setups, container-runtime-specific cgroup namespaces, systemd D-Bus integration, and CommonJob cgroup discovery are not supported in this mode.

**Job data storage** (LevelDB key-value store):
- Keys use prefixes `job_`, `job_history_`, `counter_` with values serialized as JSON
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
| **CPUMemCollector** | `summary` | string | When `"true"`, aggregates CPU/memory data from all processes into a single summary record (pid=0) | `cpumem_collector.cpp:42` |
| **IOUsageCollector** | `summary` | string | When `"true"`, aggregates I/O data across all processes | `io_usage_collector.cpp:186` |
| | `use_ebpf` | string | When `"true"`, uses eBPF to collect file‑level I/O data (requires root + kernel support) | `io_usage_collector.cpp:192` |
| **NetUsageCollector** | `summary` | string | When `"true"`, aggregates network data across all processes | `net_usage_collector.cpp:477` |
| | `use_netlink` | string | When `"true"`, uses netlink to query TCP_INFO (RTT, retransmissions, etc.) | `net_usage_collector.cpp:482` |
| **BasicInfoCollector** | `summary` | string | When `"true"`, aggregates taskstats data across all processes (TGID-level summary) | `basic_info_collector.cpp:45` |
| **GPUUsageCollector** | `summary` | string | When `"true"`, aggregates GPU usage data across all processes | `gpu_usage_collector.cpp:162` |
| | `freq` | double | Additionally read in `init()`: used to calculate GPU cache refresh interval (`1.0 / (freq * 1.5)`). This is in addition to the scheduler-level `freq`. | `gpu_usage_collector.cpp:156-197` |
| **ProcCollector** | *(none)* | — | No init config parameters. Reads process information from `/proc/[pid]/stat`, `/proc/[pid]/status`, `/proc/[pid]/io`, etc. **Note**: source code marks this collector for future deprecation (`//TODO: 这个模块将会逐步弃用`). | `proc_collector_func.cpp:276-285` |
| **TaskstatsCollector** | *(none)* | — | No init config parameters. Uses Linux taskstats netlink interface. **Note**: partially implemented — `collect()` method logs PIDs but does not produce data output; `get_writer_parser()` returns `nullptr`. | `taskstats_collector.cpp:44-47` |

---

## 4. `writers_config` — Writer Global Configuration

Code location: `src/core/writer_manager.cpp`, `src/writer/base_writer.cpp`

```yaml
writers_config:
  enable_writer_perf: true       # Whether to enable writer performance counters
  perf_window_size: 1000         # Sliding window size for performance counters
  buffer_capacity: 4096          # Internal buffer capacity for BaseWriter (number of records)
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
| `buffer_capacity` | int | none | Maximum number of messages in the BaseWriter's internal ring buffer | `base_writer.cpp:32` |

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
  batch_size: 100                # Batch write size
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
| `batch_size` | int | Yes | - | Batch size |
| `write_timeout` | int | Yes | - | Write timeout in seconds |
| `indexs` | array | No | - | Explicit mapping from collector to index name. Each element contains `collector_name` and `index_name` |

**Index rendering supports variable templates**: Index names support `[[job_info.field]]` syntax, rendered at runtime using `Utils::render_bracket()` and `Utils::flatten_json()`.

**Time suffix**: Automatically appends a date suffix of the form `_{YYYY.MM.DD}`.

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

Writes collected data to a local file in JSONL (line-delimited JSON) format.

```yaml
file_writer_config:
  path: /var/log/joblens/output.log    # Output file path (appended)
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `path` | string | Yes | - | Output file path. The file is opened in append mode; each `flush` appends one or more JSON lines. |

---

## 8. PrometheusExporterWriter Configuration

Code location: `src/writer/prometheus_exporter_writer.cpp`

Exposes collected metrics via RPC endpoints for Prometheus scraping. This writer maintains an in-memory metrics table organized by JobID → PID, covering CPU, memory, I/O, and network metrics.

**RPC Endpoints** (automatically registered):

| Endpoint | Description |
|----------|-------------|
| `/<writer_name>/metrics` | Returns all job metrics as JSON |
| `/<writer_name>/info` | Returns writer metadata |

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
  auto_add_collectors:          # When use_rules is false, the default collector list is automatically appended upon the discovery of a new Condor job.
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false              # Whether to enable the rule engine for filtering jobs
  rules_prefix: condor_job_     # Prefix for Lua rule files (default: condor_job_)
```

> **Note**: The `rules_dir` for Lua rules is read from `job_registry_config.rules_dir`, not from this section. Its default is `{JobLensRootDir}/../config/rules/condor_jobs` when `use_rules` is enabled for Condor.

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `auto_add_collectors` | string[] | none | List of collector names to automatically add to Condor jobs | `condor_job_watcher.hpp:28` |
| `use_rules` | bool | `false` | Whether to filter jobs using the rule engine (jobs not matching the rules are not added) | `condor_job_watcher.hpp:29` |
| `rules_prefix` | string | `"condor_job_"` | Prefix for rule files loaded by the rule engine | `condor_job_watcher.hpp:34` |

### `slurm_job_watcher`

Code location: `include/job_watcher/slurm_job_watcher.hpp`

```yaml
slurm_job_watcher:
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false
  rules_prefix: slurm_job_
```

> **Note**: The `rules_dir` for Lua rules is read from `job_registry_config.rules_dir`, not from this section. Its default is `{JobLensRootDir}/../config/rules/slurm_jobs` when `use_rules` is enabled for Slurm.

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `auto_add_collectors` | string[] | none | List of collector names to automatically add to Slurm jobs | `slurm_job_watcher.hpp:29` |
| `use_rules` | bool | `false` | Whether to filter jobs using the rule engine | `slurm_job_watcher.hpp:30` |
| `rules_prefix` | string | `"slurm_job_"` | Prefix for rule files loaded by the rule engine | `slurm_job_watcher.hpp:35` |

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

condor_job_watcher:
  auto_add_collectors:
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false
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
