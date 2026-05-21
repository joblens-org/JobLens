# JobLens 版本更新日志

## v0.0.19 (2026-05)

### Bug修复 - 请求缺失 sub_attr 导致采集器静默失败
- **核心问题**：通过 HTTP API 添加 `job.condor`/`job.slurm` 作业时，请求中缺少 `sub_attr` 字段，导致 C++ 端 `json2JobOpt` 抛出 `out_of_range` 异常，`addJob` 从未被调用、采集器从未启动，但 HTTP 仍返回成功（`c50c12f`）
- **修复措施**：
  - `JobRequest`/`CondorJobRequest`/`SlurmJobRequest` schema 添加 `sub_attr: Optional[Dict]` 可选字段（`schemas.py`）
  - `job_handler` 对 condor/slurm 类型自动补全 `sub_attr` 默认值：condor 补 `{cluster_id:0, proc_id:0}`，slurm 补 `{job_id:JobID, step_id:0}`
  - `add_condorjob`/`add_slurmjob` 构造请求时补上 `sub_attr`（`tools.py`）
  - 检查 `job_opt()` RPC 返回值 `status` 字段，失败时返回 500 错误而非静默成功（`routes.py`）

### Bug修复 - findJob TOCTOU 竞态崩溃
- **核心问题**：`findJob` 读锁与写锁之间存在时间窗口，8 线程并发定时器触发时，job 可能被其他线程删除，导致 `jobs_.at()` 抛出 `_Map_base::at` 异常（`11bb7cc`）
- **修复措施**：写锁段内使用 `jobs_.find()` 替代 `jobs_.at()`，job 在间隙中被删除时优雅返回 `std::nullopt`（`job_registry.cpp`）

### Bug修复 - SQLite 并发事务错误
- **核心问题**：多个采集器线程并发调用 `findJob` → `delJob` → `end_job_in_db`，在共享 `job_db` 连接上并发开始 `SQLite::Transaction`，触发 `cannot start a transaction within a transaction` 错误（`87f35e0`）
- **修复措施**：添加 `db_mtx_` 互斥锁，序列化 `persist_new_job`、`update_job_info`、`end_job_in_db`、`persist_job_id_counter` 四处的 `job_db` 事务操作（`job_registry.hpp`、`job_registry.cpp`）

### RPM 安装包修复
- 在 `joblens.spec` 和 `joblens-dev.spec` 的 `%post` 阶段添加 `rm -f /var/JobLens/JobLens.lock`，清理上次异常退出残留的锁文件，避免安装后服务启动失败（`95d0b6f`）

### 服务注册优化
- 重新注册时不再先注销再注册，改为直接在注册中心筛选匹配的 key 进行更新，减少不必要的 etcd 操作（`d1ad30c`）

---

## v0.0.18 (2026-05)

### Trigger 可靠性增强
- **独立辅助进程架构**：将 Trigger 重构为完全独立的 JobLens 辅助进程，保证任何情况下都能启动并对外提供状态查询
  - 新增 Trigger 自身状态接口（`/` 根路径、`/trigger/health`），不依赖 JobLens RPC（`4396b5f`）
  - 各组件独立容错：RPC 客户端、服务注册器、Etcd 客户端、ConfigManager、RuleManager 任一组件失败不影响整体启动
  - 组件失败时自动降级为本地模式，避免级联故障（`323ddec`）
- **启动鲁棒性提升**：RPC 连接失败不再阻止 Trigger 启动，部分功能不可用时服务仍保持运行
- **启动容错**：`tools.py` 增加配置文件缺失及 RPC 配置不完整时的异常处理，防止启动报错（`847ec34`）
- **Systemd 策略调整**：Trigger 启动失败不再反复重启，`Restart` 从 `always` 改为 `no`（`64174b5`）

### 集群信息增强
- **完整集群发现**：升级集群发现接口，从扁平标签列表升级为结构化数据
  - 新增 `ClusterDiscoveryEntry` 类型，包含 `type`、`tag`、`name` 字段
  - HTCondor 集群名称通过 `condor_config_val COLLECTOR_HOST` 获取并自动剥离端口号
  - Slurm 集群名称通过 `scontrol show config` 获取 `ClusterName`
- **`cluster_name` 公共属性**：Job 结构新增 `cluster_name` 字段
  - Condor 作业从 `COLLECTOR_HOST` 获取集群名称
  - Slurm 作业从 `SLURM_CLUSTER_NAME` 环境变量获取集群名称
- **clusterTag 逻辑优化**：Condor 作业的 `clusterTag` 改为从 `GlobalJobId` 解析 `scheduler_name`，更准确标识调度器实例（`5c63a6e`）

### 作业标识增强
- **NativeJobID**：Job 结构新增 `NativeJobID` 字段，存储调度器原生作业 ID
  - Condor 格式：`cluster_id.proc_id`
  - Slurm 格式：`job_id.step_id`
  - 数据库持久化支持 `native_job_id` 列，并兼容旧表结构自动迁移（`1a9ce9f`）

### ES Writer 增强
- **文档唯一 ID**：ES 写入时生成确定性 `_id`，格式为 `{cluster_name}_{clusterTag}_{NativeJobID}_{timestamp_ns}`，避免重复数据（`929a6af`、`e86051f`）

### 配置与 Bug 修复
- **配置管理调整**：默认关闭 ConfigManager 启动，改为使用 Puppet 管理配置（`df99a1f`）
- **etcd 优先级修复**：修复 `etcd_priority` 为 `false` 时仍主动同步 etcd 配置的问题，避免不必要的远程覆盖（`52e702a`）
- **Trigger 版本迭代**：Trigger 版本更新至 `0.0.11`（`98227a1`）
- **默认配置更新**：Condor 规则过滤默认关闭，ES 索引名称简化，去除 `clusterTag` 前缀变量（`197ee7a`）

---

## v0.0.17 (2026-04)

### GPU 监控
- **GPUUsageCollector 采集器**：新增 GPU 使用率采集器，通过动态加载 NVML 实现监控
  - 支持通过 `dlopen` 运行时加载 `libnvidia-ml.so`，无硬依赖
  - 支持 Compute 和 Graphics 进程匹配
  - 支持 SM 利用率、显存使用量、GPU 利用率采集
  - 支持 `summary` 模式跨进程数据汇总
  - 兼容 ESWriter 和 PrometheusExporterWriter

### 多集群支持
- **集群标签自动发现**：新增 `cluster_info.py` 模块，自动探测 HTCondor/Slurm 集群标签
  - 通过 `condor_status` 获取 HTCondor Schedd 名称
  - 通过 `scontrol show config` 获取 Slurm ClusterName
  - 服务注册时自动携带 `cluster_tags` 元数据
- **Agent 支持**：在 Job 结构中填充 `clusterTag` 字段（`77023fb`）

### Bug 修复
- **IO 速率计算错误**：修复 `io_usage_collector.cpp` 中运算符优先级导致的 IO 读写速率计算错误
  - 原先的 `a - b / c` 被修正为 `(a - b) / c`（`e138ed8`）
- **ES Writer 鲁棒性**：修复 `flush_impl` 中 `parse_ret` 未初始化问题（`2373e87`）
- **配置默认值**：修复配置项缺少默认值导致的异常
  - `auto_add_condorjob` / `auto_add_slurmjob` 默认值设为 `false`
  - Writer 性能统计默认启用，窗口大小默认为 1000
  - ES `batch_size` 默认为 1，`write_timeout` 默认为 5s，`index_prefix` 默认为 "collector"
- **Trigger 递归死锁修复**：`new_config_manager.py` 中 `Lock` 替换为 `RLock`，模式切换逻辑移出锁范围（`2dfcbfc`）
- **Role 信息格式修复**：`rule_manager.py` 中 `role_info` 增加 JSON 解码（`91c8b36`）
- **移除 Mock 逻辑**：删除 mock 重启代码（`c9a02c3`）

### Trigger 增强
- **RPM 打包重构**：Trigger 打包迁移至 Python 3.13 venv 架构
  - 构建时创建 venv 环境，使用 `venv/bin/shiv` 打包
  - 运行时 `%post` 阶段创建 venv，systemd 使用 venv Python 解释器
- **依赖更新**：调整 `requirements.txt` 依赖版本，新增 `grpcio`，限制 `protobuf`
- **移除调试输出**：删除 `rpc_client.py` 中的多余 `print(response)`

### 配置变更
- **RPC 路径调整**：Unix Socket 路径从 `/tmp/JobLens/` 迁移至 `/var/JobLens/`（`658279b`）
- **规则目录合并**：Condor 和 Slurm 规则目录合并为统一 `/etc/JobLens/rules/`
- **修复配置文件被置空的问题**：恢复 RPM 包默认配置的有效内容
  - 升级后 config.yaml 变成 `{}` 时，重新安装此 RPM 会以包内的完整配置覆盖
  - **下个版本**将加回 `%config(noreplace)` 标记，避免与 Puppet 管理的配置冲突

### 代码清理
- 移除多余 debug 打印和详细日志输出
- 删除 mock 测试工具残留代码
- 修复 netlink 配置文件的默认值

---

## v0.0.16 (2026-04)

### 规则引擎
- **Rule Engine 实现**：实现规则引擎和调用接口，支持基于 Lua 的自定义规则
  - 新增 RuleManager，支持规则条件中引用采集器数据
  - 支持目录监听自动加载规则文件
  - 提供 Condor 作业工具类 Lua 脚本
  - 新增 RuleManager 单元测试框架
  - 修正 RuleManager 初始化参数名称，使用 `etcd_workdir` 替代 `etcd_rules_prefix`

### 作业监视增强
- **Slurm 作业自动监控**：通过 eBPF 自动监控 Slurm 作业
  - 新增 `trace_slurm_stepd` eBPF 程序
  - 新增 SlurmJobWatcher 实现 Slurm 作业自动发现
  - 提供 Slurm 作业规则配置示例（默认、分区过滤、用户过滤）
- **Condor 作业增强**：
  - 支持自动生成 JobID，允许使用 JobID 0 自动生成
  - 支持通过 `sub_attr` 匹配查找作业
  - 添加手动添加所有 Condor 作业的接口
  - 丰富 Condor 作业属性采集
  - 迁移自动触发添加作业逻辑，预留扩展更多计算系统的模式

### Trigger 架构重构
- **架构升级**：大规模重构 Trigger 架构
  - 新增 `app_factory` 应用工厂模式
  - 新增 `etcd_client` 实现 etcd 交互
  - 新增 `service_registrar` 服务注册组件
  - 新增 `email_notifier` 邮件告警模块
  - 新增硬件信息获取接口
- **ConfigManager 重构**：重写 ConfigManager，支持 etcd 原始内容管理和同步状态功能
- **Mock 环境**：新增 JobLens Trigger Mock 工具和模拟数据
  - 新增 `Dockerfile.trigger.mock` 和 `docker-compose.mock.yml`
  - 支持模拟 RPC 和健康检查测试

### 稳定性与运维
- 新增安装后自动重启服务机制，提高鲁棒性
- 提供虚拟执行环境支持
- 修复编译警告
- 优化没有 CollectorNames 时的鲁棒性检查

---

## v0.0.15 (2026-03)

### 配置管理
- **配置管理 RPC 方法**：新增支持 YAML 到 JSON 转换的 RPC 接口
- **配置更新**：更新默认配置，禁用自动添加 condorjob

### 数据持久化
- **数据库恢复优化**：更新 JobRegistry 的 `addJob` 方法，支持从数据库恢复作业

### 代码质量
- **类型一致性修复**：修改 `addJob2Collector` 方法，更新 `jobid` 类型为 `size_t`，确保一致性

### CI/CD
- 更新 `.gitlab-ci.yml`，添加构建规则以支持在 `main` 和 `develop` 分支上执行

---

## v0.0.13 (2026-01)

### 构建与打包
- **RPM 包构建支持**：全面支持 RPM 包构建
  - 新增 `joblens.spec` 和 `joblens-trigger.spec`
  - 新增 `build-rpm.sh` 构建脚本
  - 新增 `joblens-dev.spec` 和 `joblens-trigger-dev.spec`，支持 `develop` 和 `main` 分支生成不同包
- **GitLab CI 增强**：更新 CI 配置，支持 RPM 构建、上传和依赖安装

### Trigger 部署优化
- **shiv 打包支持**：新增 `entrypoint.py` 和 `setup.py`，支持 shiv 打包
- **版本管理**：新增 `version.py`，统一管理 Trigger 版本
- **部署脚本更新**：优化 trigger 部署和安装流程

### 稳定性与 Bug 修复
- **数据库持久化修复**：修复数据库持久化相关 bug
- **CPU 采集修复**：修正 `cpupercent` 采集 bug 和 lrucache 创建 bug
- **性能修复**：修复空闲时占用 CPU 资源的 bug
- **核心转储优化**：修改 critical 后处理逻辑，直接抛出异常以方便获取 coredump
- **自动删除 Job**：添加自动删除 Job 的逻辑

### 文档
- 更新 README 文档
- 新增 JobLens Trigger Service 的 README 文档
- 新增开发协作规范文档（`develop_guide.md`）

### 其他
- 提供升级接口
- 支持手工关闭配置同步
- 减少 info 日志输出

---

## v0.0.12 (2025-12)

### eBPF采集器增强
- **文件IO细粒度监控**：新增使用eBPF读取单个文件内容的功能，实现对文件IO的细粒度监控
  - 新增job_fd_basic.h头文件定义文件描述符追踪数据结构
  - 实现job_fd_basic.bpf.c内核态代码，支持文件读写事件追踪
  - 重构job_fd_rw_stat.bpf.c代码，优化IO统计逻辑
  - 更新io_usage_collector采集器，支持新的eBPF文件监控功能
- **性能优化**：添加spin_lock.hpp自旋锁实现，提升并发性能
- **数据结构优化**：更新bpf_types.h，优化eBPF数据结构定义

### 作业管理优化
- **作业号解析优化**：修改condor_job.hpp中的作业号解析逻辑，提升作业识别准确性
- **作业更新接口增强**：完善collector_scheduler的作业更新接口，支持更灵活的作业配置更新
  - 新增CollectorScheduler::updateJob方法实现
  - 优化作业状态更新流程

### 数据持久化与存储
- **SQLite数据库持久化**：实现SQLite数据库持久化功能，支持服务注册信息的持久化存储
  - 在scripts/JSRC.py中实现DatabaseManager类
  - 支持服务信息的增删改查操作
  - 添加定期备份机制，默认24小时备份一次
  - 支持服务信息在系统重启后自动恢复
- **Elasticsearch优化**：添加索引分片支持，优化ES写入性能
  - 在es_writer.cpp中实现索引分片逻辑

### 性能监控与统计
- **调度器性能统计**：为TimerScheduler添加性能统计信息
  - 新增性能计数器，监控调度器运行状态
  - 实现统计信息收集和报告功能
  - 优化定时器调度算法

### Prometheus集成优化
- **指标格式优化**：更新trigger/tools.py中的Prometheus指标格式
  - 优化joblens_format_metrics函数，支持更规范的Prometheus文本协议
  - 统一HELP和TYPE声明，避免重复输出
  - 支持多维度标签，包括jobid、pid、name等
  - 新增线程数指标(slurm_job_threads_count)

### 并发与稳定性
- **Trigger并发修复**：修复trigger/app.py中的并发错误
  - 优化gunicorn.conf.py配置，调整worker数量和工作模式
  - 修复多worker并发访问问题
- **进程并发Bug修复**：修复scripts/JSRC.py中的进程并发bug
  - 优化服务注册和心跳检测的并发控制

### 部署与运维
- **部署脚本优化**：更新安装和部署脚本
  - 支持版本选择和基础URL构建
  - 添加版本信息输出功能
  - 优化触发器部署过程
- **错误处理增强**：优化错误处理和日志输出
  - 修改报错逻辑，提升错误信息可读性
  - 删除BPF模块的debug打印，减少日志噪音

### Bug修复
- 修复parse_uint64函数中的逻辑错误，确保正确处理小数点后续字符
- 修复JobLens和JobLens-Trigger的下载链接和包名符号问题
- 修复trigger并发访问错误
- 修复进程并发bug
- 修复作业号解析逻辑问题

---

## v0.0.11 (2025-12)

### 核心功能增强
- **Prometheus指标集成**：新增使用JobLens获取Prometheus类型指标数值的功能，支持将采集数据导出为Prometheus格式
  - 新增普罗米修斯数据解析功能
  - 优化普罗米修斯导出器导出内容格式
- **作业更新接口**：新增作业更新接口，支持动态更新作业配置和状态
- **规则引擎**：实现基于Lua的规则引擎，支持自定义采集和处理规则
- **自动配置更新**：编写自动配置更新机制，支持运行时动态更新配置

### 采集器优化
- **采集器类型设置**：添加设置采集器类型的方法，支持更灵活的采集器配置
- **运行频率优化**：修改获取运行频率的方式，支持更精确的频率控制
- **基础信息采集**：添加轻量级基础信息采集功能，使用最轻量的netlink方式
- **IO采集增强**：添加文件IO具体情况读写功能（内核态eBPF实现）
  - 编写IO采集初始化eBPF内容
  - 实现IO BPF内核侧代码
- **任务自动添加**：添加自动添加Condor Starter启动任务的功能

### 系统架构改进
- **SO版本依赖解决**：解决动态库版本依赖问题，提升系统兼容性
- **服务注册中心**：添加服务注册中心功能，支持交互式CLI
- **数据库错误处理**：添加数据库访问错误处理机制，提升系统健壮性
- **部署脚本更新**：更新部署脚本，优化部署流程

### Bug修复
- 修复parse_uint64函数中的逻辑错误，确保正确处理小数点
- 修复JobLens和JobLens-Trigger的下载链接和包名符号问题
- 修复多个调试阶段发现的问题

---

## v0.0.10 (2025-11)

### 配置管理增强
- **默认配置支持**：支持默认配置文件，简化部署流程
- **ES密码认证**：添加Elasticsearch密码认证功能，提升安全性
- **自动安装更新**：更新自动安装内容，优化安装体验
- **时区处理**：恢复时区配置，确保时间数据准确性

### 交互体验改进
- **终端用法支持**：添加对Ctrl+C和Ctrl+D的类似终端用法支持
- **版本号获取优化**：修改版本号获取方式，添加更多信息
- **部署脚本增强**：更新部署脚本，添加调试信息

### Bug修复
- 修复多个调试阶段发现的问题

---

## v0.0.9 (2025-10)

### 性能优化
- **字符串分割优化**：优化字符串分割实现，提升解析性能
- **挂载点查找优化**：优化挂载点查找逻辑，增加缓存机制以提高性能
- **内存管理优化**：添加BitmapU64类优化pid_inode_dict的内存管理
- **Inode管理优化**：重构NetUsageCollector::ParseProcNetFile方法，使用unordered_set替代BitmapU64提高性能

### 代码质量提升
- **空值安全处理**：更新JobRegistry::findJob方法，改为返回std::optional<Job>类型，解决dangling pointer问题
- **返回类型优化**：修改flush_impl方法的返回类型为bool，支持成功与否的状态反馈
- **并发写入修复**：修复并发写bug，提升数据一致性

### 稳定性增强
- **PID检测**：采集前检测PID，增加系统鲁棒性
- **日志输出优化**：减少不必要的日志输出，提升性能
- **部署脚本优化**：修改部署脚本，添加调试信息

### 回退操作
- 回退BitmapU64类相关实现
- 回退NetUsageCollector重构内容
- 回退字符串分割优化实现

---

## v0.0.8 (2025-10)

### 核心功能
- **服务注册中心**：添加服务注册中心，提供交互式CLI界面
- **作业信息持久化**：添加Job信息持久化功能，方便系统恢复
- **基础信息采集**：添加轻量级基础信息采集，使用最轻量的netlink方式

### 配置与部署
- **日志级别调整**：调整日志级别为info，优化日志输出
- **核心转储限制**：增加服务部署脚本的核心转储限制
- **服务器地址更新**：修改推送脚本中的服务器地址和目标路径
- **运行时路径优化**：修改运行时文件路径，优化文件管理
- **SQL语句优化**：修改SQL语句格式，提升数据库操作效率

### 触发器功能
- **作业槽接口**：添加使用作业槽添加作业的接口
- **触发器更新**：更新trigger功能，优化作业管理
- **自动启动**：添加自动启动功能

### Bug修复
- 修复job_restore相关问题
- 修复多个调试阶段发现的问题

---

## v0.0.7 (2025-09)

### 性能监控
- **性能统计功能**：给Writer添加perfcount功能，支持性能计数
- **采集器性能收集**：添加采集器性能收集功能，可监控采集器运行状态
- **性能计数器重构**：将perf_counter移至单一文件中，优化代码结构

### CI/CD集成
- **GitLab CI配置**：全面更新.gitlab-ci.yml文件，完善持续集成流程
- **CI部署**：添加CI部署功能，支持自动化部署
- **CMake增强**：更新CMake配置，优化构建流程

### 代码质量
- **头文件包含**：添加必要的头文件包含，修复编译问题
- **调试优化**：持续进行调试优化，提升系统稳定性

---

## v0.0.6 (2025-09)

### 构建系统
- **CMake全自动构建**：增强CMake功能，实现全自动构建流程
- **依赖自动处理**：添加自动处理依赖功能，简化编译过程

### IO统计优化
- **IO统计方式修改**：修改IO统计方式，避免Linux内核回收僵尸进程IO统计影响采集
- **执行逻辑加强**：加强执行逻辑，提升系统稳定性

### 子进程管理
- **子进程更新重写**：重写更新子进程的方式，优化子进程管理
- **子进程自动更新**：添加自动更新子进程功能

### 测试与调试
- **测试文件更新**：更新测试文件，增加测试覆盖
- **新测试样例**：添加新的测试样例
- **并发Bug修复**：解决多线程并发bug

### 配置管理
- **版本配置添加**：添加版本配置管理
- **自动启动**：添加自动启动功能

---

## v0.0.5 (2025-09)

### 数据类型与安全
- **JobID类型更新**：更新JobID为uint64类型，支持更大的Job范围
- **空值安全处理**：更新JobRegistry::findJob方法，使用std::optional<Job>类型支持更安全的空值处理
- **数据汇总支持**：更新CPUMemCollector和IOUsageCollector配置参数，添加summary选项支持跨进程数据汇总

### 监控与告警
- **JobLensWatchdog**：新增JobLensWatchdog类监控JobLens服务状态
- **RPC客户端**：添加RPCClient类实现与C++ RPC服务端通信，支持服务重启和作业列表管理
- **作业序列化**：新增dump_job函数序列化Job对象，更新regRPChandle方法返回作业信息

### 时间格式
- **UTC+8时间格式**：更新format_utc8函数，使用标准格式输出东八区时间
- **ESWriter时间处理**：更新ESWriter，添加format_utc8函数处理东八区时间格式

### 作业管理工具
- **作业查找脚本**：新增find_job.sh脚本，优化子进程获取方式
- **作业添加脚本**：新增add_job.py脚本，支持添加作业测试模拟

### 部署脚本
- **push2server脚本更新**：更新push2server.sh脚本，修改服务器地址并添加脚本复制与权限设置
- **deploy脚本优化**：更新deploy.sh脚本，使用通配符匹配JobLens包名，简化文件路径定义
- **目标路径重构**：重构push2server.sh脚本，统一目标路径变量，简化代码结构

---

## v0.0.4 (2025-09)

### 性能优化
- **线程池调整**：调整max_collector_threads为8，优化并发性能
- **TimerScheduler优化**：调整TimerScheduler默认工作线程数为8
- **IOUsageCollector优化**：优化IOUsageCollector收集结果处理逻辑，改用push_back提高性能
- **CollectorScheduler修复**：修复CollectorScheduler构造函数中的初始化逻辑

### 内存管理
- **物理内存获取**：添加物理内存获取函数，优化进程信息解析逻辑
- **内存百分比计算**：支持内存百分比计算和汇总数据处理

### 测试功能
- **作业模拟测试**：添加模拟作业测试功能，支持通过子进程执行负载模拟并将作业信息写入配置文件

### 数据存储
- **ES索引优化**：更新ESWriter，修改索引前缀为"joblens"，优化默认索引警告逻辑避免重复警告

### 配置管理
- **频率计算修复**：修复采集器配置中的频率获取逻辑，支持通过周期计算频率
- **汇总模式支持**：添加连接结构体中的汇总字段，在初始化和收集过程中支持汇总模式

---

## v0.0.3 (2025-09)

### RPC支持
- **RPC功能添加**：添加RPC支持和超时配置，优化作业处理逻辑
- **Kafka集成**：添加KafkaWriter，支持将数据写入Kafka

### 功能增强
- **Summary功能**：添加summary功能，支持数据汇总
- **子进程自动更新**：添加自动更新子进程功能，优化进程管理
- **日志等级配置**：增加log等级配置，支持灵活的日志输出控制

### 测试与调试
- **ES服务器测试**：增加测试ES服务器联通功能以及超时功能
- **相关测试添加**：添加相关测试文件，完善测试覆盖

### 项目结构
- **目录耦合去除**：修改项目结构，去除目录耦合
- **文档更新**：更新部分README文档

### Bug修复
- **Netlink修复**：修复netlink的bug
- **采集频率修复**：修复采集频率bug
- **ES Writer修复**：修正ES Writer的bug
- **退出Bug修复**：修正不能正确退出的bug

---

## v0.0.2 (2025-09)

### 压力测试
- **硬盘压力测试**：新增硬盘读写压力测试脚本，并在测试中集成调用
- **网络采集测试**：新建测试网络采集的工具

### 配置管理
- **频率参数优化**：修改freq为浮点数，支持更精确的频率控制

### RPC通信
- **RPC Bug修复**：修复RPC的bug，提升通信稳定性

### 部署优化
- **服务器配置**：修改服务器以及部署脚本配置
- **打包部署**：修改打包部署脚本，优化部署流程

---

## v0.0.1 (2025-09)

### 初始版本发布

#### 核心架构
- **Collector注册器**：添加Collector注册器，实现采集器自动注册机制
- **系统采集器种类**：添加系统级采集器框架，支持多种采集器类型
- **Writer管理器**：实现Writer管理器，支持多种数据输出方式

#### 采集器实现
- **CPU/Memory采集器**：实现cpumem_collector，支持CPU和内存使用采集
- **IO使用采集器**：实现io_usage_collector，支持IO使用统计
- **网络使用采集器**：实现net_usage_collector，支持网络流量采集
- **进程采集功能**：重写proc_collector，优化进程信息采集

#### Writer实现
- **Elasticsearch Writer**：实现es_writer，支持数据写入Elasticsearch
- **文件Writer**：实现file_writer，支持数据写入文件
- **Prometheus导出器**：添加Prometheus exporter writer，支持Prometheus格式数据导出

#### 作业管理
- **作业注册表**：实现JobRegistry，管理作业生命周期
- **作业信息持久化**：添加作业信息持久化功能，支持系统恢复
- **Condor作业支持**：细分任务种类，添加CondorJob自动更新PID功能

#### RPC通信
- **本地RPC实现**：使用Unix套接字实现本地RPC，作为与外界交互接口
- **RPC Python客户端**：添加RPC Python客户端及配置
- **FIFO RPC移除**：删除FIFO RPC实现，统一使用Unix套接字RPC

#### 配置管理
- **配置文件支持**：支持YAML格式配置文件
- **命令行参数**：美化程序命令行参数格式，增强可用性
- **帮助文档**：添加Collector和Writer的帮助文档查看功能

#### 部署与运维
- **部署脚本**：添加部署用脚本，支持自动化部署
- **JobLens Trigger**：添加joblens-trigger组件，提供Flask接口
- **服务监控**：添加作业状态监控功能
- **自动创建目录**：添加自动创建目录功能，提升易用性

#### 测试
- **冒烟测试**：完成冒烟测试，验证基本功能
- **远程测试**：增加远程测试功能
- **压力测试**：添加压力测试脚本

#### 代码质量
- **自动注册功能**：添加Collector和Writer自动注册功能
- **日志规范**：规范日志函数，修改采集函数调用签名
- **项目结构优化**：修改项目格式，去除目录耦合

#### Bug修复
- 修复多个调试阶段发现的bug
- 修复作业添加校验问题
- 修复Writer相关bug
- 修复采集器初始化问题
