# Collector execution fixtures

These files are **contracts for future implementation tests**, not runnable
collectors and not production configuration. They were added before the unified
execution framework exists so that later tasks assert against a fixed,
reviewable expectation instead of inventing behavior while implementing.

The design is described in:

- `doc/collector_execution_refactor_plan.md`
- `doc/collector_execution_refactor_validation.md`

## Combinations

| File | Scope | Trigger | Planned interface |
| --- | --- | --- | --- |
| `periodic_job.json` | Job | Periodic | `IPeriodicJobCollector` |
| `periodic_system.json` | System | Periodic | `IPeriodicSystemCollector` |
| `event_job.json` | Job | Event | `IEventJobCollector` |
| `event_system.json` | System | Event | `IEventSystemCollector` |

Each file lists the activation rule, invocation shape, required cases,
output-attribution contract, finish boundary, and expected behavior for
`Stream`, `Final`, and `Both`.

## Fixed rules

- A timer never turns a Job collector into a System collector, and a source
  never turns a Periodic collector into an Event collector.
- System output always uses the synthetic Job 0 envelope, even when an event
  payload mentions a real Job.
- A Job collector only emits for bindings it owns. Unknown or stale input is
  counted, never relabeled as System output.
- `Final` consumes and aggregates during the run but emits no stream records.
  `Both` keeps stream output and adds exactly one logical report per applicable
  session or instance run.
- One logical report may require several physical writes. Admission is not a
  durability guarantee.
- A socket EOF, one empty buffer, or a single client end marker cannot certify
  completion of every producer.

## Consumption

Tasks 3, 5, 6, and 7 add executable fixtures that load these files and compare
deterministic fields (scope, trigger, envelope JobID, invocation count, report
count, finish reason). Hardware-dependent eBPF cases are reported separately;
a skipped run is not a pass.
