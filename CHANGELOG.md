# JobLens Changelog

## v0.2.1 (2026-07)

### Patch Release - Unified RPM Release and Upgrade Fixes
- Fixed release workflow compatibility with unified RPM packaging:
  - Added root-container `sudo` compatibility for Fedora/AlmaLinux release builds without requiring `sudo` to be installed
  - Generated TGZ artifacts from the built RPM install tree instead of stale build directories, with distro-specific names for matrix builds
  - Moved binary dependency diagnostics to the RPM-extracted tree so release jobs no longer depend on rpmbuild's temporary `BUILD` directory after cleanup
  - Tightened RPM verification for `/usr/bin/JobLens` and real `.bpf.o` objects under `lib`/`lib64` paths
  - Hardened checksum generation to fail when release artifacts are missing instead of producing an empty `SHA256SUMS.txt`
- Fixed unified RPM upgrade migration from the old two-package layout by stopping and disabling any pre-existing `joblens-trigger.service` in `%pre` before installing the unified package
- Kept the legacy `scripts/rpm/preinstall.sh` free of duplicate trigger migration logic; the unified RPM's actual install behavior remains defined in `scripts/rpm/joblens-unified.spec`

---

## v0.2.0 (2026-07)

### Power Monitoring
- **PowerCollector**: Added per-job power and energy attribution based on eBPF `sched_switch` tracing and RAPL energy counters (`c7fa630`)
  - Added `trace_sched_runtime` eBPF program and collector-side runtime accounting for job-scoped CPU time attribution (`0703e37`, `3bb8af2`)
  - Added IPMI-based `ipmi_power_watt` reporting for per-job power context (`6c5b10b`)
  - Added anomaly detection for stale BMC/IPMI readings while preserving measured values with warning logs (`ce16c28`, `ec39114`)
  - Switched BPF map dumping to `bpf_map_lookup_batch` for lower overhead (`43092c9`)
  - Introduced global cache with TTL derived from collector frequency; reduced TTL to 90% of interval to avoid stale deduplication (`132fbbe`, `61270c9`, `957955e`)
  - Fixed use-after-free crash in power energy computation (`dbfdf96`)

### Unified Packaging and Release Flow
- **Unified RPM package**: Consolidated Core and Trigger into one `joblens` RPM package (`cf19376`, `60966bd`)
  - Added unified RPM spec and build/verification scripts (`scripts/rpm/joblens-unified.spec`, `scripts/build-unified-rpm.sh`, `scripts/verify-unified-rpm.sh`)
  - Removed deprecated standalone Trigger RPM packaging files (`5d599bf`)
  - Switched CI/CD and deployment documentation to the unified RPM workflow (`fae27c9`, `1101bac`)
  - Used CMake presets for package builds and improved Fedora/AlmaLinux build environment compatibility (`af28d48`, `11fc00e`)
  - Fixed `bpf_obj` installation paths and systemd path leftovers to avoid `/lib`/`lib64` packaging residue (`f4d29f0`, `e30adbc`, `b9afc68`)
  - Added dynamic version injection for `rpmbuild` and fixed Python virtualenv ELF build-id conflicts (`13bc5a4`, `2b83c54`, `b6884ec`)

### Integration Test Infrastructure
- **AlmaLinux 9 integration framework**: Added Vagrant/libvirt-based dual-node integration test topology for HTCondor, Slurm, Trigger API, FileWriter schema, and performance baseline coverage (`c1fa1e5`, `39c821d`, `617ce92`)
  - Added VM provisioning scripts for common system setup, HTCondor, Slurm, and JobLens deployment (`7a319f0`)
  - Added pytest fixtures, SSH/API helpers, retry utilities, demo jobs, and orchestration Makefile targets (`f431481`, `83c2a52`)
  - Added preset schema, runtime environment loader, Vagrantfile template, and demo-job preflight (`53037f0`, `11d7e5f`)
  - Rewrote `run_preset` as a pure Python orchestrator with preset listing/matching, optional preset selection, diagnostics, log export, and cleanup support (`582230c`, `9917780`, `523d9cf`, `83626a5`)
  - Added manual Trigger tracking tests for HTCondor and Slurm jobs (`14d3c6a`, `7d5ace4`)
  - Added AlmaLinux 9 rule-engine integration preset and core config (`56cf871`)

### Collector and Writer Output
- **FileWriter parser**: Added parser support for multiple collector outputs with structured JSONL records and parser error handling (`7675f71`)
- **Writer parser de-duplication**: Consolidated ESWriter/FileWriter parser logic by delegating common parsing to FileWriter (`bb3bf92`)
- **Flattened writer JSON schema**: Moved job metadata fields to top-level JSON output for easier downstream indexing and validation (`709f501`)
- **Elasticsearch internal TLS option**: Added insecure SSL option for internal Elasticsearch deployments (`9c949d1`)
- **Parser failure handling**: Introduced ES parser-failure skip behavior, then reverted the change to preserve release behavior after validation (`0e8a758`, `d6304fa`)

### Trigger, Configuration, and Runtime Behavior
- **Trigger default behavior**: Added config-file existence checks so service registration and rule manager are enabled only when their required configuration is present (`0d6ad7c`)
- **Trigger route clarity**: Included `status` in `JobsListResponse` for clearer API responses (`848e18c`)
- **Trigger config path fix**: Corrected Trigger configuration path usage (`532d6fb`, `9eaa351`)
- **Core config templates**: Added missing `perf_window_size` and `default_use_writers` defaults to prevent service-mode startup/config errors (`8326909`)
- **Job owner propagation**: Populated job `owner`/`user` from `sub_attr` when adding jobs through RPC (`bcbb512`)
- **Condor BPF path fallback**: Added development-build BPF object path fallback for local Condor runs, later removed from RPM deployment path after unified packaging alignment (`143cc70`, `fb38f99`)
- **Scheduler deduplication**: Deduplicated `jobid` in `addJob2Collector` to avoid empty snapshots (`4a34590`)

### Rule Engine Security
- **Sandbox hardening**: Removed unnecessary headers and strengthened rule-engine sandbox behavior (`3942aa1`)

### CI, Diagnostics, and Developer Experience
- Added comprehensive GitHub Actions integration-test workflow and CI diagnostics for KVM/libvirt, Vagrant, RPM build, and runtime startup failures (`ab4ed16`, `28feba7`, `0a69a9d`, `533ef8c`)
- Added eBPF program loading verification and diagnostics for package/runtime validation (`9d0ea5a`)
- Added JobLens startup and journal log capture for deployment troubleshooting (`8339701`, `7e8cba3`)
- Improved RPM/TGZ verification coverage for system-dependency and self-contained build modes (`b356fb0`, `3f9e3cf`, `1721b1e`, `6040297`)
- Added `BUILD_ID` support to CI/release metadata (`751d847`, `c97ddab`)
- Made dependency/build helper scripts more container-friendly for CI and RPM build environments (`710d873`)

### Documentation
- Updated minimal deployment documentation for unified RPM installation and clarified configuration requirements (`1101bac`, `3376e76`)
- Added Chinese power solution comparison documentation for power attribution design references (`doc/power_solution_comparison_zh.md`)
- Updated project overview, RPM information, and resource requirements in README/deployment docs (`e5eb5bb`)

---

## v0.1.1 (2026-06)

### Patch Release
- Updated project version to `0.1.1` as a focused fix release (`63b6915`)

### Packaging and Upgrade Fixes
- Fixed runtime lookup path for `bpf_obj` files so installed packages can locate eBPF objects correctly (`aa590ce`)
- Fixed CMake packaging logic for install/package generation (`035f5f2`)
- Corrected RPM config-file attributes and upgrade handling so user-managed configuration is preserved during RPM upgrades (`fcc37fd`, `e82db68`)
- Added pre/post-install diagnostic handling for package installation and upgrade troubleshooting (`efd3fc1`)

### Configuration and Documentation
- Simplified `config.example.yaml` to a minimal default example for clearer first-time deployment (`49158db`)
- Updated configuration documentation to match the simplified example and current runtime behavior (`49158db`)

---

## v0.1.0 (2026-06)

### Open Source Release Preparation
- **Open source baseline**: Prepared JobLens for public release with Apache-2.0 license, refreshed English/Chinese README files, and aligned documentation with the open-source project structure (`72ab52d`, `ad1b706`, `30cdc2d`, `649ad11`)
- Removed legacy/private deployment scripts, old test utilities, GitLab-only CI config, and obsolete packaging files from the public tree (`e86863c`, `4dc3c37`)
- Switched default project documentation to English while keeping Chinese documentation available (`23f3588`, `426c649`, `1f03912`)

### Build and Dependency System
- **CMake packaging migration**: Migrated build and packaging flow to CMake/CPack with dedicated install and packaging logic (`75bdfb5`, `8bb4e3b`)
- Added `CMakePresets.json` and standardized local/CI build presets (`75bdfb5`, `8bb4e3b`)
- Replaced SQLite persistence dependency with LevelDB and removed SQLite-related dependency paths from the open-source build (`4dc3c37`)
- Added environment dependency installation script and fixed Ubuntu/AlmaLinux dependency gaps, including LevelDB and `nlohmann_json` discovery (`d9c3768`, `5c0840c`, `3e49c2d`, `9a198ea`)
- Added bpftool-based automatic `vmlinux.h` generation and removed generated `vmlinux.h` from git tracking to reduce repository size (`7b5b596`)
- Fixed CI build type/variable handling and bpftool installation robustness (`bdd7462`, `21127e2`, `76b50df`)

### RPM and Release Automation
- Added GitHub Actions CI/release workflows for building, packaging, uploading, and publishing release artifacts (`481b214`, `cb71539`, `a685ec8`)
- Standardized RPM packaging for Core and Trigger packages, including Python Trigger RPM packaging and RPM-compliant package metadata (`e74cdc2`, `864ff26`, `686f4a6`)
- Packaged default configuration into RPM artifacts and aligned package names/paths with the new naming convention (`c50f28f`, `705b4fc`)
- Added download retry support and release workflow fixes for artifact naming, release content, and package publishing (`c3b88c9`, `062f3b1`, `6b87f36`)
- Added multi-version dependency support in release/CI workflows (`5db12f4`)

### Trigger and Runtime Configuration
- Standardized Trigger startup configuration discovery so Trigger resolves its config path consistently across packaged and local runs (`ae32c04`)
- Updated Trigger README and runtime documentation to match the public package layout (`1f03912`, `1f1446b`)
- Added default config packaging and refreshed example configuration for service-mode deployment (`c50f28f`)

### Documentation
- Added and updated API documentation, configuration manuals, development guide, and minimal deployment suite docs for the open-source release (`30cdc2d`, `649ad11`, `1f1446b`)
- Added architecture and data-flow documentation assets for public project overview (`30cdc2d`)

### CI Stabilization
- Fixed multiple CI/release workflow issues across Ubuntu and AlmaLinux build environments, including package install, artifact verification, release upload, and rerun stability (`5897de5`, `34533f7`, `481537e`, `71e35f1`, `2ca05f6`, `bc322c5`, `e7c7356`, `f45b730`, `8ea9d36`, `bc4daac`)

---

## v0.0.19 (2026-05)

### Bug Fix - Missing sub_attr in requests causing silent collector failure
- **Root Cause**: When adding `job.condor`/`job.slurm` jobs via HTTP API, the `sub_attr` field was missing from requests, causing C++ side `json2JobOpt` to throw an `out_of_range` exception. `addJob` was never called and collectors never started, yet HTTP returned success (`c50c12f`).
- **Fixes**:
  - Added `sub_attr: Optional[Dict]` optional field to `JobRequest`/`CondorJobRequest`/`SlurmJobRequest` schemas (`schemas.py`)
  - `job_handler` auto-fills `sub_attr` defaults for condor/slurm types: condor gets `{cluster_id:0, proc_id:0}`, slurm gets `{job_id:JobID, step_id:0}`
  - `add_condorjob`/`add_slurmjob` include `sub_attr` when constructing requests (`tools.py`)
  - Check `status` field in `job_opt()` RPC return value; return 500 error on failure instead of silent success (`routes.py`)

### Bug Fix - findJob TOCTOU race condition crash
- **Root Cause**: Time window exists between read lock and write lock in `findJob`. With 8 concurrent timer threads, a job could be deleted by another thread, causing `jobs_.at()` to throw `_Map_base::at` exception (`11bb7cc`)
- **Fix**: Use `jobs_.find()` instead of `jobs_.at()` inside write lock section; gracefully returns `std::nullopt` when job is deleted during the gap (`job_registry.cpp`)

### Bug Fix - SQLite concurrent transaction error
- **Root Cause**: Multiple collector threads concurrently calling `findJob` → `delJob` → `end_job_in_db`, starting `SQLite::Transaction` concurrently on the shared `job_db` connection, triggering `cannot start a transaction within a transaction` error (`87f35e0`)
- **Fix**: Added `db_mtx_` mutex to serialize `job_db` transaction operations in `persist_new_job`, `update_job_info`, `end_job_in_db`, and `persist_job_id_counter` (`job_registry.hpp`, `job_registry.cpp`)

### RPM Package Fix
- Added `rm -f /var/JobLens/JobLens.lock` in `%post` phase of `joblens.spec` and `joblens-dev.spec` to clean up stale lock files from abnormal exits, preventing post-install service startup failure (`95d0b6f`)

### Service Registration Optimization
- Re-registration no longer unregisters first then registers; instead filters matching keys in the registry and updates them directly, reducing unnecessary etcd operations (`d1ad30c`)

---

## v0.0.18 (2026-05)

### Trigger Reliability Enhancement
- **Independent helper process architecture**: Refactored Trigger into a fully independent JobLens helper process, ensuring it can always start and serve status queries
  - Added Trigger self-status endpoints (`/` root path, `/trigger/health`) that do not depend on JobLens RPC (`4396b5f`)
  - Independent fault tolerance per component: RPC client, service registrar, Etcd client, ConfigManager, RuleManager — failure of any single component does not block overall startup
  - Components automatically degrade to local mode on failure, preventing cascading failures (`323ddec`)
- **Startup robustness improvement**: RPC connection failure no longer prevents Trigger startup; service remains running even when some features are unavailable
- **Startup fault tolerance**: Added exception handling in `tools.py` for missing config files and incomplete RPC configuration, preventing startup errors (`847ec34`)
- **Systemd policy adjustment**: Trigger startup failure no longer triggers repeated restarts; `Restart` changed from `always` to `no` (`64174b5`)

### Cluster Information Enhancement
- **Full cluster discovery**: Upgraded cluster discovery interface from flat label lists to structured data
  - Added `ClusterDiscoveryEntry` type with `type`, `tag`, `name` fields
  - HTCondor cluster name obtained via `condor_config_val COLLECTOR_HOST` with port number automatically stripped
  - Slurm cluster name obtained via `scontrol show config` `ClusterName`
- **`cluster_name` common attribute**: Added `cluster_name` field to Job struct
  - Condor jobs get cluster name from `COLLECTOR_HOST`
  - Slurm jobs get cluster name from `SLURM_CLUSTER_NAME` environment variable
- **clusterTag logic optimization**: Condor job `clusterTag` changed to parse `scheduler_name` from `GlobalJobId`, more accurately identifying scheduler instances (`5c63a6e`)

### Job Identification Enhancement
- **NativeJobID**: Added `NativeJobID` field to Job struct, storing the scheduler's native job ID
  - Condor format: `cluster_id.proc_id`
  - Slurm format: `job_id.step_id`
  - Database persistence supports `native_job_id` column with automatic migration for legacy table schemas (`1a9ce9f`)

### ES Writer Enhancement
- **Document unique ID**: ES writes generate deterministic `_id` in format `{cluster_name}_{clusterTag}_{NativeJobID}_{timestamp_ns}`, preventing duplicate data (`929a6af`, `e86051f`)

### Configuration and Bug Fixes
- **Config management adjustment**: ConfigManager disabled by default; use Puppet for configuration management instead (`df99a1f`)
- **etcd priority fix**: Fixed issue where `etcd_priority` set to `false` still actively synced etcd configuration, preventing unnecessary remote overwrites (`52e702a`)
- **Trigger version iteration**: Trigger version updated to `0.0.11` (`98227a1`)
- **Default config update**: Condor rule filtering disabled by default; ES index names simplified, `clusterTag` prefix variable removed (`197ee7a`)

---

## v0.0.17 (2026-04)

### GPU Monitoring
- **GPUUsageCollector**: Added GPU utilization collector with dynamic NVML loading for monitoring
  - Supports runtime loading of `libnvidia-ml.so` via `dlopen`, no hard dependency
  - Supports Compute and Graphics process matching
  - Supports SM utilization, memory usage, and GPU utilization collection
  - Supports `summary` mode for cross-process data aggregation
  - Compatible with ESWriter and PrometheusExporterWriter

### Multi-Cluster Support
- **Automatic cluster tag discovery**: Added `cluster_info.py` module for automatic HTCondor/Slurm cluster tag detection
  - Obtains HTCondor Schedd names via `condor_status`
  - Obtains Slurm ClusterName via `scontrol show config`
  - Automatically carries `cluster_tags` metadata during service registration
- **Agent support**: Populates `clusterTag` field in Job struct (`77023fb`)

### Bug Fixes
- **IO rate calculation error**: Fixed operator precedence issue in `io_usage_collector.cpp` causing incorrect IO read/write rate calculation
  - `a - b / c` corrected to `(a - b) / c` (`e138ed8`)
- **ES Writer robustness**: Fixed uninitialized `parse_ret` in `flush_impl` (`2373e87`)
- **Configuration defaults**: Fixed exceptions caused by missing default values in config items
  - `auto_add_condorjob` / `auto_add_slurmjob` default set to `false`
  - Writer performance stats enabled by default, window size defaults to 1000
  - ES `batch_size` defaults to 1, `write_timeout` defaults to 5s, `index_prefix` defaults to "collector"
- **Trigger recursive deadlock fix**: `Lock` replaced with `RLock` in `new_config_manager.py`; mode switching logic moved outside lock scope (`2dfcbfc`)
- **Role info format fix**: Added JSON decoding for `role_info` in `rule_manager.py` (`91c8b36`)
- **Removed mock logic**: Deleted mock restart code (`c9a02c3`)

### Trigger Enhancement
- **RPM packaging refactor**: Trigger packaging migrated to Python 3.13 venv architecture
  - Creates venv during build, packages with `venv/bin/shiv`
  - Creates venv in `%post` phase at runtime; systemd uses venv Python interpreter
- **Dependency updates**: Adjusted `requirements.txt` dependency versions; added `grpcio`; constrained `protobuf`
- **Removed debug output**: Deleted extraneous `print(response)` in `rpc_client.py`

### Configuration Changes
- **RPC path adjustment**: Unix socket path migrated from `/tmp/JobLens/` to `/var/JobLens/` (`658279b`)
- **Rule directory consolidation**: Condor and Slurm rule directories merged into unified `/etc/JobLens/rules/`
- **Fixed config file being emptied**: Restored valid content in RPM default config
  - When `config.yaml` becomes `{}` after upgrade, reinstalling this RPM overwrites with the package's full config
  - **Next version** will add back `%config(noreplace)` flag to avoid conflicts with Puppet-managed config

### Code Cleanup
- Removed extraneous debug prints and verbose log output
- Deleted residual mock test utility code
- Fixed netlink config file defaults

---

## v0.0.16 (2026-04)

### Rule Engine
- **Rule Engine implementation**: Implemented rule engine and invocation interface with Lua-based custom rules
  - Added RuleManager with support for referencing collector data in rule conditions
  - Directory watching for automatic rule file loading
  - Provided Condor job utility Lua scripts
  - Added RuleManager unit test framework
  - Fixed RuleManager initialization parameter name, using `etcd_workdir` instead of `etcd_rules_prefix`

### Job Monitoring Enhancement
- **Slurm job auto-monitoring**: Automatic Slurm job monitoring via eBPF
  - Added `trace_slurm_stepd` eBPF program
  - Added SlurmJobWatcher for automatic Slurm job discovery
  - Provided Slurm job rule config examples (default, partition filter, user filter)
- **Condor job enhancement**:
  - Added automatic JobID generation; supports using JobID 0 for auto-generation
  - Supports job lookup by `sub_attr` matching
  - Added interface to manually add all Condor jobs
  - Enriched Condor job attribute collection
  - Migrated auto-trigger job addition logic, reserving extension patterns for more computing systems

### Trigger Architecture Refactor
- **Architecture upgrade**: Large-scale Trigger architecture refactor
  - Added `app_factory` application factory pattern
  - Added `etcd_client` for etcd interaction
  - Added `service_registrar` service registration component
  - Added `email_notifier` email alert module
  - Added hardware info retrieval endpoint
- **ConfigManager refactor**: Rewrote ConfigManager with etcd raw content management and sync status features
- **Mock environment**: Added JobLens Trigger mock tool and simulation data
  - Added `Dockerfile.trigger.mock` and `docker-compose.mock.yml`
  - Supports simulated RPC and health check testing

### Stability and Operations
- Added post-install automatic service restart mechanism for improved robustness
- Provided virtual execution environment support
- Fixed compilation warnings
- Optimized robustness checks when no CollectorNames are present

---

## v0.0.15 (2026-03)

### Configuration Management
- **Config management RPC methods**: Added RPC interface supporting YAML-to-JSON conversion
- **Config update**: Updated default config to disable auto-adding condorjob

### Data Persistence
- **Database recovery optimization**: Updated JobRegistry's `addJob` method to support job recovery from database

### Code Quality
- **Type consistency fix**: Modified `addJob2Collector` method, updated `jobid` type to `size_t` for consistency

### CI/CD
- Updated `.gitlab-ci.yml` with build rules supporting execution on `main` and `develop` branches

---

## v0.0.13 (2026-01)

### Build and Packaging
- **RPM package build support**: Full RPM package build support
  - Added `joblens.spec` and `joblens-trigger.spec`
  - Added `build-rpm.sh` build script
  - Added `joblens-dev.spec` and `joblens-trigger-dev.spec` supporting different packages for `develop` and `main` branches
- **GitLab CI enhancement**: Updated CI config to support RPM build, upload, and dependency installation

### Trigger Deployment Optimization
- **shiv packaging support**: Added `entrypoint.py` and `setup.py` for shiv packaging
- **Version management**: Added `version.py` for unified Trigger version management
- **Deployment script updates**: Optimized trigger deployment and installation process

### Stability and Bug Fixes
- **Database persistence fix**: Fixed database persistence related bugs
- **CPU collection fix**: Fixed `cpupercent` collection bug and lrucache creation bug
- **Performance fix**: Fixed idle CPU resource consumption bug
- **Core dump optimization**: Modified post-critical handling logic to directly throw exceptions for easier coredump acquisition
- **Auto-delete Job**: Added automatic Job deletion logic

### Documentation
- Updated README documentation
- Added JobLens Trigger Service README documentation
- Added development collaboration guidelines (`develop_guide.md`)

### Miscellaneous
- Provided upgrade interface
- Supported manual config sync disable
- Reduced info log output

---

## v0.0.12 (2025-12)

### eBPF Collector Enhancement
- **Fine-grained file IO monitoring**: Added eBPF-based single file content reading for fine-grained file IO monitoring
  - Added job_fd_basic.h header defining file descriptor tracking data structures
  - Implemented job_fd_basic.bpf.c kernel-side code supporting file read/write event tracing
  - Refactored job_fd_rw_stat.bpf.c code, optimized IO statistics logic
  - Updated io_usage_collector to support new eBPF file monitoring features
- **Performance optimization**: Added spin_lock.hpp spinlock implementation for improved concurrency performance
- **Data structure optimization**: Updated bpf_types.h, optimized eBPF data structure definitions

### Job Management Optimization
- **Job ID parsing optimization**: Modified job ID parsing logic in condor_job.hpp for improved job identification accuracy
- **Job update interface enhancement**: Improved CollectorScheduler's job update interface for more flexible job config updates
  - Added CollectorScheduler::updateJob method implementation
  - Optimized job status update flow

### Data Persistence and Storage
- **SQLite database persistence**: Implemented SQLite database persistence for service registration data
  - Implemented DatabaseManager class in scripts/JSRC.py
  - Supports CRUD operations for service information
  - Added periodic backup mechanism, defaulting to every 24 hours
  - Supports automatic service info recovery after system restart
- **Elasticsearch optimization**: Added index sharding support, optimized ES write performance
  - Implemented index sharding logic in es_writer.cpp

### Performance Monitoring and Statistics
- **Scheduler performance statistics**: Added performance statistics to TimerScheduler
  - Added performance counters to monitor scheduler runtime status
  - Implemented statistics collection and reporting
  - Optimized timer scheduling algorithm

### Prometheus Integration Optimization
- **Metric format optimization**: Updated Prometheus metric format in trigger/tools.py
  - Optimized joblens_format_metrics function for more standard Prometheus text protocol
  - Unified HELP and TYPE declarations to avoid duplicate output
  - Supports multi-dimensional labels including jobid, pid, name, etc.
  - Added thread count metric (slurm_job_threads_count)

### Concurrency and Stability
- **Trigger concurrency fix**: Fixed concurrency errors in trigger/app.py
  - Optimized gunicorn.conf.py config, adjusted worker count and work mode
  - Fixed multi-worker concurrent access issues
- **Process concurrency bug fix**: Fixed process concurrency bugs in scripts/JSRC.py
  - Optimized concurrency control for service registration and heartbeat checks

### Deployment and Operations
- **Deployment script optimization**: Updated installation and deployment scripts
  - Supports version selection and base URL construction
  - Added version info output
  - Optimized trigger deployment process
- **Error handling enhancement**: Optimized error handling and log output
  - Modified error reporting logic for improved error message readability
  - Removed BPF module debug prints to reduce log noise

### Bug Fixes
- Fixed parse_uint64 function logic error ensuring correct handling of post-decimal characters
- Fixed JobLens and JobLens-Trigger download link and package name symbol issues
- Fixed trigger concurrent access errors
- Fixed process concurrency bugs
- Fixed job ID parsing logic issues

---

## v0.0.11 (2025-12)

### Core Feature Enhancement
- **Prometheus metrics integration**: Added ability to retrieve Prometheus-type metric values via JobLens, supporting export of collected data in Prometheus format
  - Added Prometheus data parsing functionality
  - Optimized Prometheus exporter output format
- **Job update interface**: Added job update interface for dynamic job config and status updates
- **Rule engine**: Implemented Lua-based rule engine for custom collection and processing rules
- **Auto config update**: Implemented automatic config update mechanism for runtime dynamic config updates

### Collector Optimization
- **Collector type setting**: Added method to set collector type for more flexible collector configuration
- **Run frequency optimization**: Modified run frequency retrieval for more precise frequency control
- **Basic info collection**: Added lightweight basic info collection using the lightest netlink method
- **IO collection enhancement**: Added file IO read/write detail functionality (kernel-side eBPF implementation)
  - Implemented IO collection initialization eBPF content
  - Implemented IO BPF kernel-side code
- **Auto job addition**: Added automatic Condor Starter launch job addition

### System Architecture Improvements
- **SO version dependency resolution**: Resolved dynamic library version dependency issues for improved system compatibility
- **Service registry**: Added service registry with interactive CLI
- **Database error handling**: Added database access error handling for improved system robustness
- **Deployment script updates**: Updated deployment scripts, optimized deployment flow

### Bug Fixes
- Fixed parse_uint64 function logic error ensuring correct decimal point handling
- Fixed JobLens and JobLens-Trigger download link and package name symbol issues
- Fixed multiple issues discovered during debugging

---

## v0.0.10 (2025-11)

### Configuration Management Enhancement
- **Default config support**: Added default config file support to simplify deployment
- **ES password authentication**: Added Elasticsearch password authentication for improved security
- **Auto install update**: Updated auto-install content for optimized installation experience
- **Timezone handling**: Restored timezone config to ensure time data accuracy

### Interactive Experience Improvements
- **Terminal-like usage support**: Added Ctrl+C and Ctrl+D terminal-like usage support
- **Version retrieval optimization**: Modified version retrieval to include more information
- **Deployment script enhancement**: Updated deployment scripts with debug info

### Bug Fixes
- Fixed multiple issues discovered during debugging

---

## v0.0.9 (2025-10)

### Performance Optimization
- **String split optimization**: Optimized string splitting implementation for improved parsing performance
- **Mount point lookup optimization**: Optimized mount point lookup logic with caching for better performance
- **Memory management optimization**: Added BitmapU64 class to optimize pid_inode_dict memory management
- **Inode management optimization**: Refactored NetUsageCollector::ParseProcNetFile method, using unordered_set instead of BitmapU64 for better performance

### Code Quality Improvement
- **Null-safe handling**: Updated JobRegistry::findJob method to return std::optional<Job> type, resolving dangling pointer issues
- **Return type optimization**: Modified flush_impl return type to bool for success/failure status feedback
- **Concurrent write fix**: Fixed concurrent write bug for improved data consistency

### Stability Enhancement
- **PID detection**: Added pre-collection PID detection for increased system robustness
- **Log output optimization**: Reduced unnecessary log output for improved performance
- **Deployment script optimization**: Modified deployment scripts with debug info

### Rollback Operations
- Rolled back BitmapU64 class related implementation
- Rolled back NetUsageCollector refactoring
- Rolled back string split optimization implementation

---

## v0.0.8 (2025-10)

### Core Features
- **Service registry**: Added service registry with interactive CLI interface
- **Job info persistence**: Added Job info persistence for easier system recovery
- **Basic info collection**: Added lightweight basic info collection using the lightest netlink method

### Configuration and Deployment
- **Log level adjustment**: Adjusted log level to info, optimized log output
- **Core dump limit**: Added core dump limits to service deployment scripts
- **Server address update**: Modified server addresses and target paths in push scripts
- **Runtime path optimization**: Modified runtime file paths for optimized file management
- **SQL statement optimization**: Modified SQL statement format for improved database operation efficiency

### Trigger Features
- **Job slot interface**: Added job slot interface for adding jobs
- **Trigger updates**: Updated trigger functionality, optimized job management
- **Auto-start**: Added auto-start functionality

### Bug Fixes
- Fixed job_restore related issues
- Fixed multiple issues discovered during debugging

---

## v0.0.7 (2025-09)

### Performance Monitoring
- **Performance stats**: Added perfcount to Writer for performance counting
- **Collector performance collection**: Added collector performance collection for monitoring collector runtime status
- **Performance counter refactor**: Moved perf_counter to a single file for optimized code structure

### CI/CD Integration
- **GitLab CI config**: Comprehensively updated .gitlab-ci.yml for complete CI workflow
- **CI deployment**: Added CI deployment support for automated deployment
- **CMake enhancement**: Updated CMake config for optimized build flow

### Code Quality
- **Header includes**: Added necessary header includes to fix compilation issues
- **Debug optimization**: Continued debugging optimization for improved system stability

---

## v0.0.6 (2025-09)

### Build System
- **CMake fully automated build**: Enhanced CMake for fully automated build flow
- **Auto dependency handling**: Added automatic dependency handling to simplify compilation

### IO Statistics Optimization
- **IO stats method change**: Modified IO statistics approach to avoid Linux kernel zombie process IO stats affecting collection
- **Execution logic strengthening**: Strengthened execution logic for improved system stability

### Child Process Management
- **Child process update rewrite**: Rewrote child process update method, optimized child process management
- **Auto child process update**: Added automatic child process update

### Testing and Debugging
- **Test file updates**: Updated test files for increased test coverage
- **New test cases**: Added new test cases
- **Concurrency bug fix**: Resolved multi-threaded concurrency bugs

### Configuration Management
- **Version config added**: Added version configuration management
- **Auto-start**: Added auto-start functionality

---

## v0.0.5 (2025-09)

### Data Types and Safety
- **JobID type update**: Updated JobID to uint64 type for larger job range support
- **Null-safe handling**: Updated JobRegistry::findJob method using std::optional<Job> type for safer null handling
- **Data summary support**: Updated CPUMemCollector and IOUsageCollector config params with summary option for cross-process data aggregation

### Monitoring and Alerting
- **JobLensWatchdog**: Added JobLensWatchdog class for monitoring JobLens service status
- **RPC client**: Added RPCClient class for C++ RPC server communication, supporting service restart and job list management
- **Job serialization**: Added dump_job function for Job object serialization; updated regRPChandle method to return job info

### Time Format
- **UTC+8 time format**: Updated format_utc8 function using standard format for UTC+8 time output
- **ESWriter time handling**: Updated ESWriter with format_utc8 function for UTC+8 time format handling

### Job Management Tools
- **Job find script**: Added find_job.sh script, optimized child process acquisition
- **Job add script**: Added add_job.py script for test job addition simulation

### Deployment Scripts
- **push2server script update**: Updated push2server.sh script with modified server address and script copy plus permission setup
- **deploy script optimization**: Updated deploy.sh script using wildcards for JobLens package matching, simplified file path definitions
- **Target path refactor**: Refactored push2server.sh script, unified target path variables, simplified code structure

---

## v0.0.4 (2025-09)

### Performance Optimization
- **Thread pool adjustment**: Adjusted max_collector_threads to 8, optimized concurrency performance
- **TimerScheduler optimization**: Adjusted TimerScheduler default worker thread count to 8
- **IOUsageCollector optimization**: Optimized IOUsageCollector result processing logic, switched to push_back for better performance
- **CollectorScheduler fix**: Fixed initialization logic in CollectorScheduler constructor

### Memory Management
- **Physical memory retrieval**: Added physical memory retrieval function, optimized process info parsing logic
- **Memory percentage calculation**: Supported memory percentage calculation and summary data processing

### Testing
- **Job simulation test**: Added simulated job testing, supporting load simulation via child process and writing job info to config file

### Data Storage
- **ES index optimization**: Updated ESWriter, changed index prefix to "joblens", optimized default index warning logic to avoid duplicate warnings

### Configuration Management
- **Frequency calculation fix**: Fixed frequency retrieval logic in collector config, supporting frequency calculation from period
- **Summary mode support**: Added summary field to connection struct, supporting summary mode during initialization and collection

---

## v0.0.3 (2025-09)

### RPC Support
- **RPC functionality added**: Added RPC support and timeout config, optimized job processing logic
- **Kafka integration**: Added KafkaWriter for writing data to Kafka

### Feature Enhancement
- **Summary feature**: Added summary functionality for data aggregation
- **Auto child process update**: Added automatic child process update for optimized process management
- **Log level config**: Added log level configuration for flexible log output control

### Testing and Debugging
- **ES server test**: Added ES server connectivity test and timeout functionality
- **Related tests added**: Added related test files for improved test coverage

### Project Structure
- **Directory coupling removal**: Modified project structure to remove directory coupling
- **Documentation updates**: Updated partial README documentation

### Bug Fixes
- **Netlink fix**: Fixed netlink bugs
- **Collection frequency fix**: Fixed collection frequency bugs
- **ES Writer fix**: Fixed ES Writer bugs
- **Exit bug fix**: Fixed incorrect exit behavior

---

## v0.0.2 (2025-09)

### Stress Testing
- **Disk stress test**: Added disk read/write stress test script with integrated test invocation
- **Network collection test**: Added network collection test tool

### Configuration Management
- **Frequency parameter optimization**: Changed freq to floating point for more precise frequency control

### RPC Communication
- **RPC bug fix**: Fixed RPC bugs for improved communication stability

### Deployment Optimization
- **Server config**: Modified server and deployment script config
- **Package deployment**: Modified packaging and deployment scripts, optimized deployment flow

---

## v0.0.1 (2025-09)

### Initial Release

#### Core Architecture
- **Collector registry**: Added collector registry with automatic collector registration mechanism
- **System collector types**: Added system-level collector framework supporting multiple collector types
- **Writer manager**: Implemented Writer manager supporting multiple data output methods

#### Collector Implementation
- **CPU/Memory collector**: Implemented cpumem_collector for CPU and memory usage collection
- **IO usage collector**: Implemented io_usage_collector for IO usage statistics
- **Network usage collector**: Implemented net_usage_collector for network traffic collection
- **Process collector**: Rewrote proc_collector, optimized process info collection

#### Writer Implementation
- **Elasticsearch Writer**: Implemented es_writer for writing data to Elasticsearch
- **File Writer**: Implemented file_writer for writing data to files
- **Prometheus exporter**: Added Prometheus exporter writer for Prometheus-format data export

#### Job Management
- **Job registry**: Implemented JobRegistry for managing job lifecycle
- **Job info persistence**: Added job info persistence for system recovery
- **Condor job support**: Differentiated job types, added CondorJob auto PID update

#### RPC Communication
- **Local RPC implementation**: Implemented local RPC using Unix sockets as the external interaction interface
- **RPC Python client**: Added RPC Python client and config
- **FIFO RPC removal**: Removed FIFO RPC implementation, unified to Unix socket RPC

#### Configuration Management
- **Config file support**: Added YAML format config file support
- **Command-line arguments**: Beautified program CLI argument format for enhanced usability
- **Help documentation**: Added Collector and Writer help documentation viewing

#### Deployment and Operations
- **Deployment scripts**: Added deployment scripts for automated deployment
- **JobLens Trigger**: Added joblens-trigger component providing Flask interface
- **Service monitoring**: Added job status monitoring
- **Auto directory creation**: Added automatic directory creation for improved usability

#### Testing
- **Smoke test**: Completed smoke testing for basic functionality verification
- **Remote testing**: Added remote testing functionality
- **Stress testing**: Added stress test scripts

#### Code Quality
- **Auto-registration**: Added automatic Collector and Writer registration
- **Log standardization**: Standardized log functions, modified collection function call signatures
- **Project structure optimization**: Modified project format, removed directory coupling

#### Bug Fixes
- Fixed multiple bugs discovered during debugging
- Fixed job addition validation issues
- Fixed Writer related bugs
- Fixed collector initialization issues
