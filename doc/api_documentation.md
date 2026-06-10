# JobLens Trigger API Documentation

Base URL: `http://<host>:<port>`

All API routes return JSON responses unless otherwise noted.

---

## 1. Metrics

### `GET /metrics`

Get Prometheus-formatted metrics data from the JobLens writer.

**Response format:** `text/plain` (Prometheus exposition format)

**Status Codes:**
- `200` - Success
- `503` - RPC call failed
- `500` - Unexpected error

### `GET /`

Get basic Trigger service information including version, status, and available endpoints.

**Response body:**

| Field | Type | Description |
|---|---|---|
| `service` | string | Service name (e.g., `"JobLens Trigger"`) |
| `version` | string | Trigger component version |
| `status` | string | Service status (e.g., `"running"`) |
| `endpoints` | object | Key API endpoints reference |

**Status Codes:**
- `200` - Success

### `GET /trigger/health`

Get detailed health status of the Trigger service components, independent of JobLens RPC.

**Response body:**

| Field | Type | Description |
|---|---|---|
| `service` | string | Service name |
| `version` | string | Trigger version |
| `status` | string | Service status |
| `components` | object | Health of internal components (`rpc_client`, `service_registrar`, `config_manager`, `rule_manager`) |
| `joblens` | object | JobLens core version info (`version`, `build_id`, `build_time`) |

**Status Codes:**
- `200` - Success

---

## 2. Health Check

### `GET /joblens/healthy`

Check the health status of the JobLens systemd service.

**Response body (HealthResponse):**

| Field | Type | Description |
|---|---|---|
| `active` | bool | Whether the service is in `active` state |
| `state` | string | Systemd active state (e.g., `"active"`) |
| `sub` | string | Systemd sub-state (e.g., `"running"`) |
| `healthy` | bool | Overall health: `true` when state=`"active"` AND sub=`"running"` |

**Status Codes:**
- `200` - Success
- `400` - Runtime error (e.g., systemctl not available)
- `500` - Internal error

### `GET /joblens/rpc/health`

Check JobLens server health via RPC call, including latency measurement.

**Response body (RPCHealthResponse):**

| Field | Type | Description |
|---|---|---|
| `rpc_latency_ms` | float | RPC round-trip latency in milliseconds |
| `*` | mixed | Additional dynamic fields returned by the `health` RPC call |

**Status Codes:**
- `200` - Success
- `503` - RPC call failed
- `500` - Unexpected error

---

## 3. Job Management

### `POST /joblens/job`

Generic job operation (add or remove).

**Request body (JobRequest):**

| Field | Type | Required | Description |
|---|---|---|---|
| `opt` | string | **Yes** | Operation type: `"add"` or `"remove"` |
| `type` | string | **Yes** | Job type: `"job.condor"`, `"job.slurm"`, or `"job.common"` |
| `JobID` | int | No | Job ID |
| `JobPIDs` | array[int] | No | List of process PIDs |
| `Lens` | array[string] | No | Collector list (default: `["cpumem_collector", "io_collector", "net_collector"]`) |
| `sub_attr` | object | No | Sub-attributes: `condor` type needs `cluster_id`/`proc_id`, `slurm` type needs `job_id`/`step_id` |

**Example request:**
```json
{
  "opt": "add",
  "type": "job.condor",
  "JobID": 1,
  "JobPIDs": [1],
  "Lens": [""]
}
```

**Response body (JobResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status (e.g., `"ok"`) |
| `received` | object | The processed request data |

**Status Codes:**
- `200` - Success
- `400` - Invalid request body or fields
- `500` - Job operation failed

---

### `POST /joblens/condor_job`

Condor job specific operation. Currently only supports `"add"`.

**Request body (CondorJobRequest):**

| Field | Type | Required | Description |
|---|---|---|---|
| `opt` | string | **Yes** | Operation type, only `"add"` is supported |
| `JobID` | int | No | Job ID |
| `slot` | string | **Yes** | Slot name, must start with `"slot"` |
| `Lens` | array[string] | No | Collector list (default: `["proc_collector"]`) |
| `sub_attr` | object | No | Sub-attributes (e.g., `{"cluster_id": 123456, "proc_id": 0}`) |

**Example request:**
```json
{
  "opt": "add",
  "JobID": 1,
  "slot": "slotx",
  "Lens": [""]
}
```

**Response body (JobResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status (e.g., `"ok"`) |
| `received` | object | The processed request data |

**Status Codes:**
- `200` - Success
- `400` - Invalid request body, missing or invalid `slot`, unsupported `opt`
- `500` - Internal error

---

### `POST /joblens/slurm_job`

Slurm job specific operation. Currently only supports `"add"`.

**Request body (SlurmJobRequest):**

| Field | Type | Required | Description |
|---|---|---|---|
| `opt` | string | **Yes** | Operation type, only `"add"` is supported |
| `JobID` | int | **Yes** | Slurm job ID |
| `Lens` | array[string] | No | Collector list (default: `["proc_collector"]`) |
| `sub_attr` | object | No | Sub-attributes (e.g., `{"job_id": 12345, "step_id": 0}`) |

**Example request:**
```json
{
  "opt": "add",
  "JobID": 12345,
  "Lens": [""]
}
```

**Response body (JobResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status (e.g., `"ok"`) |
| `received` | object | The processed request data |

**Status Codes:**
- `200` - Success
- `400` - Invalid request body or unsupported `opt`
- `500` - Internal error

---

### `GET /joblens/jobs/count`

Get the count of currently registered jobs (excluding the easter-egg job with JobID=0).

**Response body (JobCountResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status |
| `job_count` | int | Total number of registered jobs |

**Status Codes:**
- `200` - Success
- `503` - RPC call failed
- `500` - Unexpected error

---

### `GET /joblens/jobs`

List all registered jobs (excluding the easter-egg job with JobID=0).

**Response body (JobsListResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status |
| `jobs` | array[object] | List of job objects (dynamic fields) |

**Status Codes:**
- `200` - Success
- `503` - RPC call failed
- `500` - Unexpected error

---

### `GET /joblens/jobs/{job_id}`

Get detailed information for a specific job by its JobID.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `job_id` | int | The Job ID to query |

**Response body (JobDetailResponse):** Dynamic fields from the RPC result.

**Status Codes:**
- `200` - Success
- `404` - Job not found
- `503` - RPC call failed
- `500` - Unexpected error

---

## 4. RPC Functions

### `GET /joblens/rpc/functions`

List all available RPC methods registered on the server.

**Response body (RPCFunctionsResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Response status (e.g., `"ok"`) |
| `functions` | array[string] | List of available RPC function names |
| `count` | int | Number of available functions |

**Status Codes:**
- `200` - Success
- `503` - RPC call failed
- `500` - Unexpected error

---

## 5. Collector

### `GET /joblens/collectors/perf`

Get performance statistics for all collectors.

**Response body (CollectorsPerfResponse):** Dynamic fields from the RPC result.

**Status Codes:**
- `200` - Success
- `500` - Collector performance data error
- `503` - RPC call failed
- `500` - Unexpected error

---

## 6. Writer

### `GET /joblens/writers/{writer_name}/info`

Get basic information for a specific writer.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `writer_name` | string | Name of the writer (e.g., `"es_writer"`, `"kafka_writer"`, `"file_writer"`) |

**Response body (WriterInfoResponse):** Dynamic fields from the RPC result.

**Status Codes:**
- `200` - Success
- `404` - Writer not found
- `503` - RPC call failed
- `500` - Unexpected error

---

### `GET /joblens/writers/perf`

Get performance statistics for all writers.

**Response body (WritersPerfResponse):** Dynamic fields from the RPC result.

**Status Codes:**
- `200` - Success
- `500` - Writer performance data error
- `503` - RPC call failed
- `500` - Unexpected error

---

## 7. Configuration Management

### `GET /joblens/config/status`

Get configuration manager status.

**Response body (ConfigStatusResponse):**

| Field | Type | Description |
|---|---|---|
| `enabled` | bool | Whether config manager is enabled |
| `running` | bool | Whether config manager is running |
| `mode` | string | Current configuration mode |
| `use_etcd` | bool | Whether etcd is being used |
| `sync_status` | object | Synchronization status details |
| `message` | string | Status message (present when not enabled) |

**Status Codes:**
- `200` - Success

### `POST /joblens/config/update`

Manually trigger a configuration reload (invokes the configuration manager's callbacks).

**Response body (ConfigUpdateResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status (e.g., `"ok"`) |
| `message` | string | Status message |

**Status Codes:**
- `200` - Success
- `503` - ConfigManager not initialized
- `500` - Config update failed

---

## 8. Service Registry

### `GET /joblens/registry/status`

Get service registration status.

**Response body (RegistryStatusResponse):**

| Field | Type | Description |
|---|---|---|
| `enabled` | bool | Whether service registrar is enabled |
| `registered` | bool | Whether service is registered |
| `service_id` | string | Service ID |
| `service_host` | string | Service hostname/IP |
| `service_port` | int | Service port |
| `registry_url` | string | Registry center URL |
| `heartbeat_interval` | int | Heartbeat interval in seconds |
| `version` | string | Version information |
| `etcd_path` | string | etcd key path |
| `etcd_workdir` | string | etcd working directory |
| `etcd_addr` | string | etcd address |
| `etcd_port` | int | etcd port |
| `message` | string | Status message (present when not enabled) |

**Status Codes:**
- `200` - Success

---

### `POST /joblens/registry/register`

Manually trigger service registration.

**Response body (RegistryRegisterResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status (`"ok"` or `"warning"`) |
| `message` | string | Status message |

**Status Codes:**
- `200` - Registration successful
- `202` - Registration failed but service continues (status=`"warning"`)
- `503` - ServiceRegistrar not initialized
- `500` - Registration error

---

## 9. Rule Management

### `GET /joblens/rules`

List all local rule files.

**Response body (RulesListResponse):**

| Field | Type | Description |
|---|---|---|
| `enabled` | bool | Whether rule manager is enabled |
| `count` | int | Number of rules |
| `rules` | array[string] | List of rule filenames |
| `message` | string | Status message (present when not enabled) |

**Status Codes:**
- `200` - Success
- `500` - Failed to get rule list

---

### `GET /joblens/rules/{filename}`

Get the content of a specific rule file.

**Path Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `filename` | string | Rule filename (must end with `.lua`, no path separators) |

**Response format:** `text/plain` (raw Lua file content)

**Status Codes:**
- `200` - Success
- `400` - Invalid filename
- `404` - Rule file not found
- `503` - RuleManager not initialized
- `500` - Read error

---

### `POST /joblens/rules/sync`

Manually trigger full rule synchronization from etcd to local files.

**Response body (RulesSyncResponse):**

| Field | Type | Description |
|---|---|---|
| `status` | string | Operation status (e.g., `"ok"`) |
| `message` | string | Status message |

**Status Codes:**
- `200` - Success
- `503` - RuleManager not initialized
- `500` - Sync failed

---

### `GET /joblens/rules/status`

Get rule manager status.

**Response body (RuleStatusResponse):**

| Field | Type | Description |
|---|---|---|
| `enabled` | bool | Whether rule manager is enabled |
| `running` | bool | Whether rule manager is running |
| `etcd_priority` | bool | Whether etcd has priority |
| `etcd_role_path` | string | etcd role path |
| `etcd_workdir_prefix` | string | etcd working directory prefix |
| `local_rules_dir` | string | Local rules directory path |
| `role_id` | string | Role ID |
| `local_rules_count` | int | Number of local rules |
| `remote_rules_count` | int | Number of remote rules |
| `callbacks_count` | int | Number of registered callbacks |
| `etcd_role_watch_id` | string | etcd role watch ID |
| `etcd_role_info_watch_id` | string | etcd role info watch ID |
| `file_watcher_running` | bool | Whether file watcher is running |
| `syncing` | bool | Whether a sync is in progress |
| `sync_lock_locked` | bool | Whether the sync lock is held |

**Status Codes:**
- `200` - Success

---

## 10. Version & Upgrade

### `GET /joblens/version`

Get version information for both the Trigger and the underlying JobLens service.

**Response body (VersionResponse):**

| Field | Type | Description |
|---|---|---|
| `trigger_version` | string | Trigger component version |
| `joblens_version` | string | JobLens service version |
| `build_id` | string | Build identifier |
| `build_time` | string | Build timestamp |

**Status Codes:**
- `200` - Success (always returns 200, falls back to `"UNKNOWN"` values on error)


## 11. Miscellaneous

### `GET /utils/hardware_info`

Get hardware information including CPU model, memory size, disk info, etc.

**Response body (HardwareInfoResponse):** Dynamic fields from the hardware info utility.

**Status Codes:**
- `200` - Success
- `500` - Failed to get hardware info

---

## Response Status Code Summary

| Code | Description |
|---|---|
| `200` | Success |
| `202` | Accepted (asynchronous operation started) |
| `400` | Bad request (invalid parameters or body) |
| `401` | Unauthorized (missing or invalid auth header) |
| `403` | Forbidden (invalid token) |
| `404` | Resource not found |
| `500` | Internal server error |
| `503` | Service unavailable (RPC failure or dependency not initialized) |
