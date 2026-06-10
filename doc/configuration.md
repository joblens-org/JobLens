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
  pid_dir: /var/JobLens/node_pids            # PID file directory, distributed nodes store node_<pid> files
  max_collector_threads: 8                   # Maximum threads for collector scheduler (TimerScheduler thread pool size)
  log_level: info                            # Log level: trace/debug/info/warn/error/critical/off
```

| Key | Type | Default | Description | Source Reference |
|-----|------|---------|-------------|------------------|
| `rpc_socket_path` | string | none | UNIX Domain Socket path for local RPC communication | `main_utils.cpp:83` → `RPCServer::instance(RPC_Socket)` |
| `rpc_timeout` | float | 5 | RPC call timeout in seconds | trigger `app_factory.py:183` (used by Trigger) |
| `lock_path` | string | none | Process lock file, used by `already_running()` to detect if another instance is running | `main_utils.cpp:95`, `distributed_node.cpp:61` |
| `pid_dir` | string | none | PID file directory, stores per-node process information in distributed mode | `distributed_node.cpp` |
| `max_collector_threads` | int | none | Thread pool size for the collector timer scheduler | `collector_scheduler.cpp:12` |
| `log_level` | string | fallback to info | Log level: trace/debug/info/warn/error/critical/off | `main_utils.cpp:67` |

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
| `auto_add_condorjob` | bool | false | Automatically start CondorJobWatcher, trace `condor_starter` processes via eBPF, and auto-add Condor jobs |
| `auto_add_slurmjob` | bool | false | Automatically start SlurmJobWatcher, trace `slurm_stepd` processes via eBPF, and auto-add Slurm jobs |
| `rules_dir` | string | none | Directory for Lua rule files, used by the rule engine |

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
| `enable_collector_perf` | bool | none | When enabled, each `collect` call records its duration; performance statistics can be queried via RPC | `collector_registry.cpp:17` |
| `perf_window_size` | int | none | Performance window size, affects the number of samples for mean/variance calculation | `collector_registry.cpp:19` |
| `default_freq` | int | none | Default sampling frequency (Hz) for collectors | `collector_scheduler.cpp:14` |
| `default_use_writers` | string[] | none | Default list of writers for collectors | `collector_scheduler.cpp:15` |

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

The collector's `init(json_cfg)` method receives a JSON converted from the YAML node. Below are the keys recognized by each collector:

| Collector Type | Configuration Parameter | Value Type | Description |
|----------------|------------------------|------------|-------------|
| **CPUMemCollector** | `summary` | string | When `"true"`, aggregates CPU/memory data from all processes into a single summary record (pid=0) |
| **IOUsageCollector** | `summary` | string | When `"true"`, aggregates I/O data |
| | `use_ebpf` | string | When `"true"`, uses eBPF to collect file‑level I/O data |
| **NetUsageCollector** | `summary` | string | When `"true"`, aggregates network data |
| | `use_netlink` | string | When `"true"`, uses netlink to query TCP_INFO (default true) |

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
| `enable_writer_perf` | bool | none | When enabled, records duration of each `flush`, supports RPC queries | `writer_manager.cpp:15` |
| `perf_window_size` | int | none | Performance window size | `writer_manager.cpp:17` |
| `buffer_capacity` | int | none | Maximum number of messages in the BaseWriter's internal ring buffer | `base_writer.cpp:19` |

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
  enable_dlq: false                   # Whether to enable the dead letter queue
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
| `enable_dlq` | bool | Yes | - | Whether to enable dead letter queue; forced to false when `plaintext` is used |
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

## 7. PrometheusExporterWriter Configuration

**Note:** Since the current deployment model does not involve this writer, it has not been tested yet

## 8. Job Watcher Configuration

### `condor_job_watcher`

```yaml
condor_job_watcher:
  auto_add_collectors:          # When use_rules is configured as false, the default collector list is automatically appended upon the discovery of a new Condor job.
    - cpumem_collector
    - net_collector
    - io_collector
  use_rules: false              # Whether to enable the rule engine for filtering jobs
  rules_dir: /path/to/rules/condor_jobs     # Directory for Lua rule files (default: {JobLensRootDir}/../config/rules/condor_jobs)
  rules_prefix: condor_job_     # Prefix for Lua rule files (default: condor_job_)
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `auto_add_collectors` | string[] | none | List of collector names to automatically add to Condor jobs |
| `use_rules` | bool | `false` | Whether to filter jobs using the rule engine (jobs not matching the rules are not added) |
| `rules_dir` | string | `{InstallDir}/../config/rules/condor_jobs` | Directory for Lua rule files |
| `rules_prefix` | string | `condor_job_` | Prefix for rule files |

### `slurm_job_watcher`

Code location: `include/job_watcher/slurm_job_watcher.hpp`

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

Parameters are the same as above; the default `rules_dir` is `{InstallDir}/../config/rules/slurm_jobs`.

---

## Complete Configuration File Example

```yaml
lens_config:
  rpc_socket_path: /var/JobLens/rpc.sock
  lock_path: /var/JobLens/JobLens.lock
  pid_dir: /var/JobLens/node_pids
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

## Command‑Line Startup Arguments

```bash
JobLens - A job monitor system

Usage:
  JobLens [options] [subcommand]

Options:
  -h, --help                    Show help information
  -c, --config <path>           Configuration file path (default: config.yaml)
  -m, --mode <mode>             Running mode (default: starter)
  -e, --exec <executable>       Path to the executable to run (used in starter mode)
  -a, --args <args>             Argument list for the executable
  -v, --version                 Show version information

Running modes:
(Note: This is a historical issue. In the early design phase of joblens, the design goal was to provide fine-grained tracking for a single specified job, so this mode was retained. After discussion, this mode has been deprecated, but the startup option has not been removed yet)

  starter    Start + monitor mode: launch a subprocess and monitor its performance (default)
  service    Pure service mode: start the collection scheduler and run continuously until a shutdown signal is received

Subcommands:
  collector  Manage collectors (list, get help)
  writer     Manage writers (list, get help)

Signal handling:
  SIGINT / SIGTERM              Gracefully shut down the service

Examples:
  # Basic start (starter mode)
  ./JobLens -c config.yaml

  # Service mode
  ./JobLens -m service -c config.yaml

  # Start and monitor a specific process
  ./JobLens -c config.yaml -e /path/to/program -a "arg1 arg2"

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
