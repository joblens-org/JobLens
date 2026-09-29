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
`collectors` (an array sorted by name). `snapshot()` supplies the same view.
`collector_status` returns `status`, `observed_at_unix_ms`, and `collector`.
An unknown name returns `{"status":"error","code":"unknown_collector",
"msg":"Unknown collector: NAME"}`; absent/non-string names use `invalid_params`.

Global lifecycle: `starting` during construction / before `start()`, `running`
after `start()`, `stopping` before waiting for scheduler locks and worker joins,
`stopped` after joins. Existing system auto-start can execute during `starting`.
Shutdown is terminal; restarting this scheduler is not supported.

Each collector contains `name`, `config`, `scope`, `period_ms`, `job_count`,
`timer_active`, `active_callback_count`, `state`, `has_error`, `last_error`,
and `operations`. All configured names appear, including inactive/invalid ones.
An unresolved scope has period 0 (unknown); otherwise the period is derived
from the frequency actually used by the existing scheduler. Job count is
updated under the membership lock, includes synthetic system job 0, and counts
membership rather than registry liveness.

States: `idle`, `initializing`, `scheduled`, `running`, `deinitializing`, `error`.
Active initialization/deinitialization and callbacks take display precedence;
`has_error` remains available independently. Idle is not an unhealthy state.
`timer_active` means registered/not cancelled, not currently executing, and is
cleared after shutdown joins. Queued callbacks may outlive timer cancellation.

Operations `init`, `deinit`, `callback`, `collect`, `write` each expose
`success_count`, `error_count`, `active_count`, `in_progress_elapsed_ms`
(one elapsed value per overlapping operation), `last_duration_ms`, and nullable
`last_success_unix_ms`. Durations use `steady_clock`; timestamp fields are
wall-clock Unix milliseconds. Last duration is the most recently completed
operation, including failures. Counters are lifetime counters, not rates.

`callback` measures a whole timer sweep, including lock waiting, registry lookup,
all job collections and writer calls. It can overlap itself. `collect` is one
invocation for one live job; `write` is the entire writer fan-out for that job
(also completes successfully when no writers are configured). Caught collect /
writer errors mark the sweep failed. Empty/cancelled sweeps may count as success
but do not clear collector errors. Successful init or writer fan-out clears
current error state, retaining historical counters and last error text.
The last error is bounded to 512 bytes; non-ASCII bytes are replaced with `?`
to keep arbitrary exception messages safe for JSON serialization.

Snapshots copy only observational data under an independent mutex, then build
JSON after releasing it. No collection, scheduler, writer, or registry lock is
taken by these methods. RPC handlers retain shared ownership of this data and
do not dereference a destroyed scheduler. Read cost is proportional to the
number of collectors and active callbacks; this is not a real-time guarantee.

No queue capacity, rejection, timeout, or worker statistics are claimed.
Existing collection serialization, timer cadence, collector API, and writer
behavior remain unchanged. Failures swallowed inside collectors/writers are
not detectable here. The existing RPC server has a single connection-processing
thread; unrelated slow RPC requests can still delay status requests.

## Regression harness

`tests/integration/collector_scheduler_status_test.cpp` links the normal core
objects except `main.cpp.o`. It uses the actual scheduler, JobRegistry and RPC
server, and synthetic collector functions; no production services are contacted.
The temporary configuration disables job discovery and writers. Kernel PID
tracker initialization may fail on an unprivileged host; this is not required
for the live test process (job 123). The harness removes synthetic job 0 because
PID 1 may not be accessible on the test host. Test databases remain under
`/tmp/opencode`.

After `cmake --build build -j 2`, compile and run on the current development host:

```sh
g++ -std=c++17 -O0 -g -Iinclude -Ibuild/include -I/usr/include/libnl3 -pthread \
  tests/integration/collector_scheduler_status_test.cpp \
  build/CMakeFiles/JobLens.dir/src/{common,core,collector,writer,rule_engine}/*.o \
  -o /tmp/opencode/collector_scheduler_status_test -L/usr/local/lib64 \
  -lspdlog -lfmt -lyaml-cpp -lbpf -lelf -lz -lssl -lcrypto -lcurl \
  -lrdkafka++ -lrdkafka -lsasl2 -lzstd -llz4 -lnl-genl-3 -lnl-3 \
  -lleveldb -ldl -llua5.4 -lxxhash
timeout -k 2s 15s /tmp/opencode/collector_scheduler_status_test
```

The object/library paths are host-specific; no standalone CTest target is added.
