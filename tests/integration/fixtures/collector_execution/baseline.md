# Execution baseline (M0)

Recorded on 2026-10-23 before the unified execution framework was implemented.
This is evidence for the existing scheduler/writer behavior, not evidence for
any proposed executor, event source, or finalization feature.

## Identity

- Repository: `JobLens`
- Branch: `feature/collector-output-refactor`
- Commit: `d947f4c` (`ci: run unprivileged output regressions`)
- Pre-existing local changes at recording time: `doc/collector_scheduler_status.md`,
  `doc/configuration.md`, `doc/configuration_zh.md`, `tests/integration/README.md`,
  plus untracked design documents. They were not modified by this baseline.
- Build directory: `build/` (Ninja, `RelWithDebInfo`, existing `JobLens` binary)
- Compiler: GCC 11.5.0
- CMake: 3.26.5
- clang: 20.1.8
- libbpf: 1.5.0 (`ring_buffer__consume_n` and `ring_buffer__epoll_fd` available
  in the installed headers)
- bpftool: 7.5.0, kernel BTF present
- Kernel: 6.6.87.2 (WSL2)
- Exchange: `lens_config.max_collector_threads` in the scheduler test is `3`;
  `collectors_config.default_freq` is `20`.

## Commands and results

All commands ran from the repository root with `SKIP_BUILD=1` after the M0
teardown fix, except where noted.

| Command | Exit | Result |
| --- | --- | --- |
| `bash tests/integration/run_scheduler_tests.sh build` | 0 | Timer semantics test passed; real scheduler + Unix-socket RPC boundary test passed; process exited cleanly |
| `bash tests/integration/run_output_tests.sh build` | 0 | Native collector output integration passed |
| `bash tests/integration/run_writer_hot_path_tests.sh build` | 0 | Writer hot-path regression passed |
| `bash tests/integration/run_writer_retry_test.sh build` | 0 | ES writer retry regression passed |
| `bash tests/integration/run_cpumem_test.sh build` | 0 | CPUMem `/proc` sampling and PID-reuse guard passed |
| `bash tests/integration/run_net_usage_test.sh build` | 0 | NetUsage ownership/syscall-count/namespace regressions passed |

Privileged or deployment-dependent suites were not part of this M0 run.

## Pre-fix failure

Before the M0 fix, `run_scheduler_tests.sh` printed its PASS line and then
segfaulted during static destruction (exit 139). Valgrind reported an invalid
read in `WriterManager::shutdown()` logging: the spdlog registry had already
been destroyed because `WriterManager` is constructed by `AUTO_REGISTER_WRITER`
static initializers before the first logging call. The fix makes the spdlog
registry lifetime precede `WriterManager` construction. After the fix, ten
consecutive runs exited 0 and Valgrind reported `0 errors from 0 contexts`.

## Lightweight resource sample

The scheduler status test (`max_collector_threads: 3`) was sampled every 20 ms
through `/proc/<pid>/status`:

- Peak threads: 6 (main + 3 timer workers + timer scheduling thread + RPC loop)
- Peak RSS: ~12 MiB

This is a test-harness sample, not a production load measurement. Tier B/C
measurements remain pending.
