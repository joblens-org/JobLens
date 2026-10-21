# Collector scheduler status (core RPC)

Read-only core Unix-socket methods. Trigger exposes the bounded HTTP bridge
`GET /joblens/collectors/status`, which forwards only
`CollectorScheduler/status` with empty parameters and returns its successful
payload unchanged. It does not proxy arbitrary core methods or configuration.
If the core does not implement this method, the bridge returns HTTP 501 with
`{"status":"error","code":"unsupported","supported":false}`. An unavailable
RPC connection returns HTTP 503; a core error response returns HTTP 502.
Send one JSON object per connection. The server returns the result directly,
without a JSON-RPC `result` envelope, then closes the connection.

```json
{"method":"CollectorScheduler/status","params":{}}
{"method":"CollectorScheduler/collector_status","params":{"name":"proc"}}
```

`status` returns `status: "ok"`, `lifecycle`, `observed_at_unix_ms`,
`collector_count`, `collectors_with_error`, `active_callback_count` and
`collectors` (an array sorted by name), and `scheduler`. `snapshot()` supplies
the same view.
`collector_status` returns `status`, `observed_at_unix_ms`, and `collector`.
An unknown name returns `{"status":"error","code":"unknown_collector",
"msg":"Unknown collector: NAME"}`; absent/non-string names use `invalid_params`.

Global lifecycle: `starting` during construction / before `start()`, `running`
after `start()`, `stopping` before waiting for scheduler locks and worker joins,
`stopped` after joins. Existing system auto-start can execute during `starting`.
Shutdown is terminal; restarting this scheduler is not supported.

Each collector contains `name`, `config`, `scope`, `period_ms`, `job_count`,
`timer_active`, `active_callback_count`, `state`, `has_error`, `last_error`,
`operations`, `current_job`, and `scheduling`. All configured names appear,
including inactive/invalid ones.
An unresolved scope has period 0 (unknown); otherwise the period is derived
from the frequency actually used by the existing scheduler. Job count is
updated under the membership lock, includes synthetic system job 0, and counts
membership rather than registry liveness.

States: `idle`, `initializing`, `scheduled`, `running`, `deinitializing`, `error`.
Active initialization/deinitialization and callbacks take display precedence;
`has_error` remains available independently. Idle is not an unhealthy state.
`timer_active` means registered/not cancelled, not currently executing, and is
cleared after shutdown joins. Cancellation removes queued callbacks; a callback
already running is allowed to finish.

Operations `init`, `deinit`, `callback`, `lookup_job`, `wait_collector_lock`,
`collect`, and `write` each expose
`success_count`, `error_count`, `active_count`, `in_progress_elapsed_ms`
(one elapsed value per overlapping operation), `last_duration_ms`, and nullable
`last_success_unix_ms`. Each also exposes `in_progress` (objects with nullable
`job_id` and `elapsed_ms`) and nullable `last_job_id`. Durations use
`steady_clock`; timestamp fields are
wall-clock Unix milliseconds. Last duration is the most recently completed
operation, including failures. Counters are lifetime counters, not rates.

`callback` measures a whole timer sweep, including lock waiting, registry lookup,
all job collections and writer calls. Repeated ticks do not overlap the same
timer callback. `collect` is one
invocation for one live job; `write` is the entire writer fan-out for that job
(also completes successfully when no writers are configured). Caught collect /
writer errors mark the sweep failed. `write` measures invoking writer callbacks
(usually enqueueing into the existing writer buffers), not the background sink
flush or network delivery. Empty/cancelled sweeps may count as success
but do not clear collector errors. Successful init or writer fan-out clears
current error state, retaining historical counters and last error text.
The last error is bounded to 512 bytes; non-ASCII bytes are replaced with `?`
to keep arbitrary exception messages safe for JSON serialization.

`current_job` is `null` between jobs, otherwise it contains `job_id`, `stage`,
and `elapsed_ms`. Stages are `lookup_job` (registry access),
`wait_collector_lock` (waiting for the membership/lifecycle lock), `collect`,
and `write`. The elapsed time covers the current stage rather than the whole
job sweep. Lock waiting before the job list is copied has no JobID and is
visible in `operations.wait_collector_lock.in_progress`.

Snapshots copy observational data under the runtime mutex and scheduling data
under the scheduler's short-held queue mutex, separately, then build JSON after
releasing those locks. Neither mutex is held while calling a collector or writer.
The response does not acquire a collector membership lock, writer lock, or
registry lock. RPC handlers retain shared ownership of status state and remain
safe after scheduler destruction. Runtime and queue views can describe slightly
different instants. Read cost is proportional to configured collectors and
workers; this is not a real-time guarantee.

## Scheduling and queue visibility

Each timer has at most one callback queued **or** running. A tick encountered
while that timer is busy increments `skipped_runs` instead of creating another
callback. Skipped ticks are not replayed after completion; a future tick resumes
normal sampling. The first repeating callback is still scheduled immediately.
Consequently, pending callbacks cannot grow with the number of elapsed periods:
their count is bounded by registered timers. There is no additional writer queue
and no Job-level parallel collection.

A collector's `scheduling` is `null` without a live timer, otherwise it contains:

| Field | Meaning |
|---|---|
| `task_id`, `name` | Timer registration ID and configured collector name |
| `state` | `idle` (waiting for its deadline), `queued`, or `running` |
| `period_ms` | Configured timer interval |
| `queued_elapsed_ms` | Age of a queued callback, otherwise 0 |
| `running_elapsed_ms` | Duration of the running sweep, otherwise 0 |
| `skipped_runs` | Busy ticks skipped during this timer registration |
| `completed_runs`, `error_count` | Completed callbacks and uncaught callback exceptions |
| `last_queue_wait_ms`, `last_duration_ms` | Most recent worker wait and completed sweep duration |
| `worker_index` | Worker executing the callback, or `null` |
| `cancelled` | Cancellation requested while a callback is still running |

The top-level `scheduler` contains:

- `worker_count`, `currentQueuedTasks`, `maxQueueSize`, and `skippedRuns`.
- `schedulerStats.activeWorkers` and average/min/max worker-queue wait in
  milliseconds. This measures actual time waiting for a worker, rather than
  timer deadline lateness.
- `tasks`: all registered timer states, including queued/running collector names.
- `workerStats`: each worker's `workerIndex`, nullable `task_id`, `name`, `isBusy`,
  `running_elapsed_ms`, completed/error counts, execution durations and load.
- Lifetime totals `totalTasksRegistered`, `totalTasksProcessed`,
  `totalTasksFailed`, `totalQueueOperations`, plus `currentTimerTasks` (future
  deadlines), `schedulerRuntime` in milliseconds, `schedulerWakeups`, and
  `stopped`.

For example, inspect active jobs through the existing HTTP bridge:

```sh
curl -s http://127.0.0.1:7592/joblens/collectors/status | jq '{
  queued: .scheduler.currentQueuedTasks,
  workers: .scheduler.schedulerStats.activeWorkers,
  active_jobs: [.collectors[] | select(.current_job != null) |
    {name, current_job, scheduling}]
}'
```

A `running` timer with `current_job.stage == "collect"` and growing elapsed time
locates a slow job collection. A timer with `state == "queued"` and growing
`queued_elapsed_ms` is waiting for a worker; inspect `workerStats` to find the
collectors occupying the workers. The queue stores collector sweeps, so a queued
sweep does not yet have one selected JobID. Job attribution begins when it
reaches registry lookup during execution.

Counters and current/last operation details are bounded by configured collectors
and workers. No per-job history or sampling-payload queue is retained by this
observability feature. Cancelling a timer drops its registration counters; global
scheduler totals remain for the scheduler lifetime.

This prevents repeated callbacks for one slow collector from exhausting the
pool. A slow job still delays the later jobs within that same collector's sweep;
independently blocked collectors can still occupy every worker. No timeout or
forced cancellation of a running system call is provided. Existing collector
serialization, collector API, and writer behavior remain unchanged. In particular,
writer double buffers are not changed into bounded queues: `buffer_capacity` is
currently an initial vector reservation, not a hard record or memory limit.
Failures swallowed inside collectors/writers are not detectable here. The existing RPC server has a single connection-processing
thread; unrelated slow RPC requests can still delay status requests.

## Regression harness

`tests/integration/collector_scheduler_status_test.cpp` links the normal core
objects except `main.cpp.o`. It uses the actual scheduler, JobRegistry and RPC
server, and synthetic collector functions; no production services are contacted.
The temporary configuration disables job discovery and writers. Kernel PID
tracker initialization may fail on an unprivileged host; this is not required
for the live test process (job 123). The harness removes synthetic job 0 because
PID 1 may not be accessible on the test host. The runner creates a unique temporary directory for its configuration, socket,
and LevelDB database and removes it after the test process exits.

Run the standalone timer regressions and the actual scheduler/RPC harness:

```sh
bash tests/integration/run_scheduler_tests.sh build
```

The runner uses the existing Ninja link command and temporary test binaries. It
covers queued/running tick suppression, cancellation, exception recovery, worker
progress under 4,096 short-period timers, reader lifetime, blocked collect/write
Job attribution, status RPC
latency, and shutdown state. The RPC test requires permission to bind a local
Unix socket. Synthetic error-path tests intentionally emit error logs.
