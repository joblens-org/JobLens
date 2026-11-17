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
| `erase` | bool | No | For `remove` only. `false` or absent keeps the stop tier: collection stops, the job record remains. `true` retires the instance: a collected instance is archived as an `h:` history row, a never-collected instance is deleted |

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
| `mode` | string | For `remove`: `"stop"` or `"erase"` |
| `stopped_count` / `deleted_count` | int | Number of targets handled by the selected tier |
| `stopped_instances` / `erased_instances` | array[string] | Instance handles processed by the selected tier |

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
| `Lens` | array[string] | No | Collector list (default: `["cpumem_collector", "io_collector", "net_collector"]`) |
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
| `Lens` | array[string] | No | Collector list (default: `["cpumem_collector", "io_collector", "net_collector"]`) |
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

## 12. Job List and Collection Control

These routes proxy the Core job/collection contract. Trigger validates and
forwards only: it never allocates or derives internal IDs, and a Core error is
never turned into HTTP 200.

### `GET /joblens/jobs/list`

Bounded, read-only job list snapshot. The old `/joblens/inventory` path is a
deprecated alias served by the Trigger adapter and calls the same Core method.

| Query | Type | Description |
|---|---|---|
| `limit` | int | Page size, 1–1000, default 100 |
| `scheduler_type` | string | `condor` or `slurm` |
| `native_job_id` | string | Native scheduler ID (exact match) |
| `state` | string | `active`, `suspect` or `closing` |
| `cursor` | string | Opaque cursor from the previous page |

**Response (200):**
```json
{
  "status": "ok",
  "snapshot_revision": 1234567890,
  "records": [{
    "instance_id": "42",
    "internal_job_id": "42",
    "schema_version": 2,
    "record_revision": 3,
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

A cursor is only valid while the snapshot is unchanged; otherwise the request
fails with `409 cursor_invalidated`. Internal IDs are decimal strings so
JavaScript consumers keep uint64 precision.

### `GET /joblens/jobs/status`

Source, reconciliation, queue and persistence health (active/suspect/closing
counts, scan timestamps, dropped events, store degradation, incomplete and
quarantined identities, applied/drain state). Served verbatim from the Core;
`200` even when the reported state is `degraded`. The old
`/joblens/inventory/status` path is a deprecated alias.

### `POST /joblens/collection`

Set or reset collection intent for one exact target set.

**Request body:**
```json
{
  "selector": {"scheduler_type": "condor", "cluster_name": "htcondor01",
               "cluster_tag": "schedd-a", "native_job_id": "172.0"},
  "scope": "instance",
  "ensure_registered": true,
  "operation": "set",
  "collectors": ["cpumem_collector"]
}
```

| Field | Required | Description |
|---|---|---|
| `selector` | Yes | Exactly one form: `internal_job_id`, or `scheduler_type` + `cluster_name` + `native_job_id` (+ optional `cluster_tag`, `step_id`) |
| `scope` | No | `instance` (default) or `job`. `internal_job_id` only supports `instance`; Slurm `job` targets every currently observed local step |
| `ensure_registered` | No | Run one authoritative discovery pass before matching |
| `operation` | Yes | `set` or `reset_auto` |
| `collectors` | For `set` | Explicit list; `[]` is a persistent stop. Must be omitted for `reset_auto` |
| `expected_revision` | No | Record-revision guard, single-instance requests only. Omit for no guard; a supplied value must equal the current `record_revision` (0 is not valid for an existing record) |

**Response (200):**
```json
{
  "status": "accepted",
  "operation": "set",
  "scope": "instance",
  "targets": [{
    "instance_id": "42",
    "internal_job_id": "42",
    "native_job_id": "172.0",
    "record_revision": 1,
    "mode": "manual",
    "desired_collectors": ["cpumem_collector"],
    "applied_collectors": ["cpumem_collector"],
    "applied_attach_token": 1,
    "errors": [],
    "changed": true
  }]
}
```

`applied_collectors` is runtime state: it is empty (with an entry in `errors`)
when the Agent has no collection runtime or a collector refused the binding.
`status: accepted` only means the intent is durable.

### `GET /joblens/collection?internal_job_id=...`

Desired/applied selection, mode, revisions, policy revision and per-collector
errors for one instance. `internal_job_id` is the decimal internal ID from the
job list; `instance_id` remains accepted as a deprecated alias by the Trigger
adapter. `404` when it does not name a verified current instance, `409` when it
names a closing generation.

### `GET /joblens/capabilities`

Advertises `api_version`, `collection_control_version`,
`native_identity_selectors`, `job_scope`, `ready` and the DB schema version.
Trigger never declares a capability the Core did not report.

**Status Codes:**
- `200` - Success
- `400` - Invalid request (unknown collector, missing `collectors`, bad selector)
- `404` - No verified current instance
- `409` - Ambiguous selector, stale `expected_revision`, closing generation, invalidated cursor
- `502` - Core returned a response that violates the contract
- `503` - Core RPC unavailable, store unavailable or registry still migrating

---

## 13. v2 Resource API

The v2 surface is resource-oriented and returns the Core `JobRecord` representation.
The v1 routes above remain unchanged; v1 and v2 share the same Trigger adapter and
the same Core methods. The legacy `/joblens/capabilities` response also keeps
`inventory_version` (1) and `inventory_ready` aliases; `/joblens/inventory/status`
keeps `inventory_enabled`.

| Method | Path | Semantics |
|---|---|---|
| `GET` | `/joblens/v2/jobs` | Paginated job records; same `limit`/`cursor`/filters as `/joblens/jobs/list` |
| `GET` | `/joblens/v2/jobs/count` | Active job-record count |
| `GET` | `/joblens/v2/jobs/<internal_job_id>` | One job record |
| `POST` | `/joblens/v2/jobs` | Strict create (`jobs/create`); an existing identity or ID returns 409 |
| `PUT` | `/joblens/v2/jobs` | Identity-resolved upsert (`jobs/upsert`); body has no `opt` field |
| `DELETE` | `/joblens/v2/jobs/<internal_job_id>` | Erase/retire (`jobs/erase`) |
| `POST` | `/joblens/v2/jobs/<internal_job_id>/stop` | Stop collection, retain the record (`jobs/stop`) |
| `GET` | `/joblens/v2/jobs/<internal_job_id>/collection` | Desired/applied collection state |
| `PUT` | `/joblens/v2/jobs/<internal_job_id>/collection` | Replace desired collectors (`{"collectors": [...]}`; `[]` stops) |
| `DELETE` | `/joblens/v2/jobs/<internal_job_id>/collection` | Clear the manual override (reset_auto) |
| `GET` | `/joblens/v2/status` | Same status payload as `/joblens/jobs/status` |
| `GET` | `/joblens/v2/capabilities` | Same capabilities payload; `api_version` is `2` |

The v2 job representation includes `internal_job_id`, `record_revision`, `state`,
`desired_collectors`, `mode`, `processes`, `generation` and `attributes`. The v1
`dump_job` shape is reconstructed by the Trigger adapter from the same record, so
old clients do not need to change.

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
| `409` | Conflict (ambiguous selector, stale revision, closing generation, invalidated cursor) |
| `500` | Internal server error |
| `502` | Core returned a response that violates the contract |
| `503` | Service unavailable (RPC failure, store unavailable or registry initializing) |
