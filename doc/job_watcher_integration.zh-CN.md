# Job Watcher 接入指南

本指南描述如何向作业注册表接入一种**新的调度器类型**——例如一个
`ssh` 会话源：写一个 *job watcher*（发现源）和一个 *scheduler adapter*
（调度器适配器）。其余所有环节（`JobStore` 的 merge 语义、心跳的退休证据
规则、采集绑定与对账、RPC 面、内核归因）对新类型零改动直接生效。English
version: [job_watcher_integration.md](job_watcher_integration.md)

参考实现：`condor` / `slurm`
（[include/job_watcher/](../include/job_watcher/)）。下文的示例用 `ssh`——
一个没有内建调度器 schema 的源。

---

## 1. 你要添加什么

接入一种调度器 = **两个类 + 一行注册**，外加一个链接入口，使 TU 作用域的
注册器进入 Agent 二进制（在已编译的 TU 中 include adapter 头，或把它的 TU
加入构建）：

```
include/job_watcher/ssh_job_watcher.hpp   # IDiscoverySource 实现（watcher）
include/job_watcher/ssh_adapter.hpp       # ISchedulerAdapter 实现 + 注册行
src/ebpf/trace_ssh_session.bpf.c          # 可选：exec-hint 事件源
```

你免费得到的东西：

- 持久存储（`JobRecord`）、identity 索引、generation 冲突处理；
- 心跳：权威 manager pass、缺席计数、退休证据规则、逐记录执行证据刷新；
- desired-vs-applied 采集对账与 `CollectionStatus`；
- `jobs/*` 与 `jobs/collection/*` RPC 方法及 trigger 代理；
- 内核归因（`pid2job` / `cgroup2job`）——**当且仅当**你的 adapter 声明
  `kernel_attribution` trait；
- 无特权测试路径（fixture 文件树、注入的探针/源）。

## 2. 这条线如何运转（回顾）

1. **注册（`main()` 之前）：** TU 作用域的 `REGISTER_SCHEDULER_ADAPTER("ssh",
   SshSchedulerAdapter)` 把工厂函数插入 `SchedulerAdapterRegistry`——和
   `AUTO_REGISTER_COLLECTOR` 同一套约定。
2. **装配（`JobRegistry::armDiscovery()`，由注册表构造函数调用）：** 配置解析出的
   `maintenance.types` 逐项通过注册表构建，每种类型恰好一个 adapter。
   adapter 是**唯一**知道该调度器属性 schema、身份格式和能力的东西。
3. **运行：** 心跳驱动 adapter 的 `IDiscoverySource`：`enumerateManagers()`
   枚举一遍 pass 里的全部存活 per-job manager（不读 job ad、不读成员），
   `observe(manager)` 为本 pass 尚未跟踪的 manager 构造 Job，此外还有由
   eBPF ringbuf exec 事件喂给的 O(job) `observeForHint(pid)` 定向观测。
   发现的 **Job** 自带 typed facts 和一组 execution 证据，由 watcher 在
   解析源的地方就地填好；registry 在准入时从 Job 派生 selector/identity。
   没有第二个信封。

两条贯穿始终的结构性规则：

- 一个发现的 **Job** 要么 ready（canonical identity 可解析 + 完整
  execution generation，见 `discovery_job_ready`），要么**显式
  incomplete 并把原因写在 `job.execution->incomplete_reason`**。不存在
  "尽力而为"的中间态。
- **失败或不完整的 manager pass 永远不代表"其余的都结束了"**：
  `ManagerSet::complete` 只说明这次 pass 是否覆盖了配置的源；消费方必须
  先检查它才能把"缺席"当作终止证据。

### exec 快速通道（作业启动 → 采集）

延迟敏感的发现永远不等心跳 pass：

1. 内核送达 exec 事件的瞬间，watcher 轮询线程即被唤醒
   （`ring_buffer__poll` 阻塞在 libbpf 自己的 epoll 里；中间没有任何
   轮询切片）。
2. 轮询线程自己解析 pid（`observeForHint`），并**同线程同步**通过
   `sink.on_job(job)` 登记发现的 Job：store upsert → 生命周期/attach。
   没有队列、没有 worker；心跳只保留 manager pass、退休与对账。
3. exec 时刻调度器状态可能还没写好（job ad、spool 目录）：**不完整的发现
   永远不建记录**；watcher 请求一次合并的权威 pass，下一遍 manager pass
   在数十毫秒内收敛。
4. `joblens/status` 保留 `exec_ingest_depth`、`queue_depth`
   与 `events_dropped` 作为废弃的恒零键：ingest worker 与 observation
   队列已不存在。

已知限制：进程第一条指令到用户态
`pid2job` 播种之间的事件，对 syscall-tracepoint 采集器仍然不可见。由
exec tracer 自己写入 provisional `pid2job` 条目可以关掉这个窗口——但那会
把"捕获起点"与 collector attach 解耦，从而改变 collector 的生命周期契约。

## 3. 你的 watcher 必须遵守的契约

### 3.1 `IDiscoverySource` 生命周期

| 方法 | 契约 |
|---|---|
| 构造函数 | 只收 `DiscoveryOptions`。**不读 Config、不加载 eBPF、不开线程。** 永远不被 start 的 adapter 零成本。 |
| `start(sink)` | **先装 sink**（装好之前任何东西都不能回调），然后按需加载事件源、启动轮询。没有可轮询的东西（无事件源）时返回 `false`——不建线程，`enumerateManagers()` / `observe()` / `observeForHint()` 继续可用，发现由 registry 的 manager pass 驱动。 |
| `stop()` | 幂等；join 轮询线程、释放事件源。延迟有界（切片等待，绝不无限 join）。 |
| `enumerateManagers()` | 一次 registry pass 的权威 manager 枚举：每个存活的 per-job manager 一项，不读 job ad、不读成员。内部用锁串行化（`pass_mutex_`）；它拥有本次 pass 的 `SourceHealth` 更新（起始时间戳、消费 rescan 请求、按覆盖情况置 `Healthy`/`Degraded`）。 |
| `observe(manager)` | 为一个已知 manager 构造完整 Job——身份、初始 facts 与 execution 证据，只读该执行的文件（O(job)）。只为本 pass 尚未跟踪的 manager 调用。`nullopt` 表示"不可观测"（进程已死、PID 被复用、发现不完整），绝不是猜出来的 Job。 |
| `health()` | 第一遍 pass 结束前是 `Initializing`；最近一遍覆盖了全部源是 `Healthy`；失败或部分覆盖是 `Degraded`。Degraded 期间消费方不得把缺席当终止。 |
| `requestRescan()` | 只设置一个由 owner 消费的标志，绝不内联跑 pass。 |
| `observeForHint(pid)` | 为一个 exec 事件 pid 做定向观测——O(job)，只读该执行的文件。解析不出来（进程已死、parent 不符）返回 `nullopt`；调用方等下一遍权威 manager pass。 |

### 3.2 线程模型与 sink

- 轮询线程抽干 ring buffer，把每个 pid 解析成 Job（`observeForHint`），
  再通过 `sink.on_job` 登记。登记写盘不带 fence，回调不等持久化——但发现
  I/O 与 upsert 就是内联做的，这是刻意为之。
- sink 回调是跑在**你的**线程上的调用方代码：用 try/catch 包住，消费方
  抛异常不能带塌 watcher。
- 发现 I/O（`/proc`、ad/spool 读取）发生在构造 Job 的地方：
  `enumerateManagers()` 与 `observe(manager)` 在 registry 的 pass 线程上，
  `observeForHint()` 在轮询线程上。

### 3.3 构造发现的 Job

Job 就是登记单元：typed facts 加一组 `execution` 证据，在源解析输入的
地方就地填好。`JobID`、`JobPIDs`、`CollectorNames` 保持为空——它们归
store/intent 所有。

- 身份字段（`subtype_name`、`cluster_name`、`clusterTag`、`NativeJobID`、
  `execution->step_id`）：源真正提供的原生字段，逐字保留。绝不合成数值。
- 集群命名空间（如 `COLLECTOR_HOST`、`SLURM_CLUSTER_NAME`）：显式配置
  优先；否则从源环境推导。**绝不从原生作业编号瞎猜。** 推不出来 ⇒
  incomplete 发现并写明原因。
- `execution->generation`：manager pid + manager start ticks + boot id，
  三者齐备才算完整。
- `execution->members`：优先读执行 cgroup 的 `cgroup.procs`——**读成功
  （包括空列表）即为权威**（`members_authoritative = true`）。cgroup 不可
  读才退化为枚举 manager 的子进程，且标记列表为部分
  （`members_authoritative = false`）。start ticks 读不出来的成员保留在
  列表里、`start_ticks == 0`，让缺失的证据保持可见。
- 收尾调 `finalize_discovery_job(job, reasons)`：它按身份/代次检查算出
  ready 与否（`discovery_job_ready`），把每个缺失环节写进
  `execution->incomplete_reason`（当且仅当 ready 时为空）。注意事项进
  `execution->notes`。

### 3.4 Adapter 的职责（`ISchedulerAdapter`）

- `discoveryOptions(settings)`：把 `DiscoveryTypeSettings` 映射到你的
  `DiscoveryOptions`（事件源开关、集群命名空间）。
- （发现期 facts 由 watcher 自己填写；没有经过中立字段的
  `encodeFacts` 中转。）
- `decodeFacts(row)` / `legacySelector(job)`：为历史行做尽力而为
  的重建。可以不完整；不可以瞎猜。
- `traits()`：如实声明。
  `cgroup_scoped_membership = true` 仅当 `cgroup.procs` 是你的执行实例的
  权威成员证据；`kernel_attribution = true` 仅当你的 manager 语义支持
  pid/cgroup 归因。缺 trait 是正常状态、绝不是错误：没有该 trait 的
  adapter，其作业只是不参与基于 cgroup 的退休证据和内核 map。

## 4. 分步操作

### 第 1 步 — 最小共享接线

调度器逻辑只住在 watcher 和 adapter 里。adapter 名字就是处处通行的调度器
身份：配置 `types` 是启动时对注册表校验的名字，`<name>_job_watcher` 配置节
按约定解析，native-id 规则自注册（`REGISTER_NATIVE_ID_RULE`），非内置调度器
的 facts 走 `SubAttr` 的 JSON 通道（配 `subtype_name`），身份/cgroup 推导经
`ISchedulerAdapter` 接口分派。

唯一的共享修改是让注册器进入 Agent 二进制：在已编译的 TU 中 include
adapter 头（内置 adapter 由 `job_registry.hpp` include），或把 adapter 的
TU 加入构建。另一个可选修改是给 `DiscoveryEnvironment` 加一个特殊根字段
（如 Slurm 的 spool 目录）；通用根（`proc_root`、`cgroup_root`）已经存在。

### 第 2 步 — 写 watcher

Header-only，仿照 `condor_job_watcher.hpp`：

```cpp
// include/job_watcher/ssh_job_watcher.hpp
class SshJobWatcher final : public IDiscoverySource {
public:
    explicit SshJobWatcher(DiscoveryOptions options);   // 只存 options
    ~SshJobWatcher() override { stop(); }

    const std::string& name() const override { return name_; }

    bool start(const DiscoverySink& sink) override;     // 先 sink 后轮询
    void stop() override;                               // 幂等、有界
    ManagerSet enumerateManagers() override;            // 权威 pass
    std::optional<Job> observe(const ProcessIdentity& manager) override;
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Job> observeForHint(pid_t hint_pid, pid_t hint_ppid = 0) override;

private:
    void poll_loop();                                   // 事件阻塞（epoll）
    Job observe_session(...);                           // 你的 observe_starter
    bool init_event_source();                           // 可选 eBPF
    ...
};
```

实现清单（以下每一条在 Condor watcher 里都是承重的——抄的是模式，
不只是形状）：

- `poll_loop()`：**事件阻塞**而非切片轮询——单次
  `ring_buffer__poll(rb, timeout)` 直接阻塞在 libbpf 自己的 epoll 里，exec
  事件以内核唤醒的速度送达；超时只用来限定 `stop()` 最长多久被察觉
  （100ms 节拍）。旧切片设计的延迟来自"先在条件变量上睡觉、再做非阻塞
  抽干"——不要再引入这种拆分。这个循环永不跑发现 pass、永不按定时器扫描：
  节奏归 registry 心跳管。
- `enumerateManagers()`：先检查 `proc_root` 可读（失败 ⇒ `complete=false`
  并置 `Degraded`）；读 `boot_id`；从进程表（或 tier-1 子树）枚举候选
  manager；进程表里读不出来的条目只计数加 note，**绝不伪造 Job**；pass
  进行中退出的 manager 以不完整身份留在集合里，由调用方的 `observe()`
  报为不可观测。用同一个健康更新路径收尾（`finish_pass` 模式）。
- `observe(manager)`：只读该 manager 的 ad 与成员集合，校验读到的 manager
  身份确实是被请求的那一个（被复用的 pid 不是这次执行），然后构出一个
  Job。绝不更新 `SourceHealth`：读不出来是重试，不是 pass 失败。
- `observeForHint(pid)`：读 parent，**校验 parent 的 cmdline 确实是预期
  manager**（eBPF 过滤对真实事件有保证，但被复用的 pid 不能伪造发现），
  然后以 hint pid 优先构出一个 Job。

### 第 3 步 — 写 adapter

```cpp
// include/job_watcher/ssh_adapter.hpp
class SshSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit SshSchedulerAdapter(const DiscoveryTypeSettings& settings)
        : watcher_(discoveryOptions(settings)) {}

    const std::string& name() const override { return name_; }
    std::string configSection() const override { return "ssh_job_watcher"; }
    IDiscoverySource& source() override { return watcher_; }
    DiscoveryOptions discoveryOptions(const DiscoveryTypeSettings&) const override;
    Job decodeFacts(const nlohmann::json& row) const override;
    NativeSelector legacySelector(const Job&) const override;
    AdapterTraits traits() const override;   // ssh：两项都是 false

private:
    std::string name_{"ssh"};
    SshJobWatcher watcher_;
};

REGISTER_SCHEDULER_ADAPTER("ssh", SshSchedulerAdapter)
```

### 第 4 步 — 把注册链接进二进制

注册表靠静态初始化填充，所以这个 TU 必须被链接进 `JobLens`。两个等价
做法；代码库目前用 (a)：

- **(a)** 在 `include/core/job_registry.hpp` 的 condor/slurm include 旁边
  加 `#include "job_watcher/ssh_adapter.hpp"`；
- **(b)** 把 `REGISTER_SCHEDULER_ADAPTER` 行放进一个小的
  `src/job_watcher/ssh_adapter.cpp`——`GLOB_RECURSE src/*.cpp` 会自动
  收编。

### 第 5 步 — 可选的 eBPF 事件源

如果 exec 事件线索对你的调度器有意义，在 `sys_exit_execve` tracepoint 上
加 `src/ebpf/trace_<manager>.bpf.c`：读 `real_parent->comm`，与你的
manager 名比较，向名为 `exec_events` 的 `BPF_MAP_TYPE_RINGBUF`（256KB
是惯用大小）reserve + submit `{pid, ppid, comm}`。遵守
[src/ebpf/AGENTS.md](../src/ebpf/) 的约定：共享头 include 放在**最后**，
共享结构放 `include/ebpf/<name>.h` 并用 `#ifndef __VMLINUX_H__` 保护，
构建会把 `src/ebpf/*.bpf.c` 自动编译成 `build/bpf_obj/*.bpf.o`。用户态
用 `EbpfCommon::resolve_bpf_obj("trace_<manager>.bpf.o")` 加载——任何失败
都处理为"pass 上的一条 note"，绝不是启动错误。

没有事件源你的 adapter 就不带 exec hint 运行：`start()` 返回 `false`、
不建轮询线程、不产生 hint，而 registry 的 manager pass 照常维护作业记录。

### 第 6 步 — 配置

在 `config/config.example.yaml` 加一节（生产环境真正的 `config/config.yaml`
是 gitignore 的）：

```yaml
ssh_job_watcher:
  auto_collect: true          # 或 use_rules + rules_dir 走 Lua 规则引擎
  use_rules: false
  auto_add_collectors: [cpumem_collector]
  cluster_name: ""            # 空：从源环境推导
```

并在 `job_registry_config.maintenance.types: [condor, slurm, ssh]` 里列出
该类型。不列出则该类型保持休眠；心跳仍会维护该类型的既有记录。

### 第 7 步 — 测试

照抄现成的两个层级；都不需要 root、真实调度器或 eBPF：

1. **Watcher 层** — fixture 文件树 + 真 watcher，参照
   `tests/integration/run_watcher_scan_test.sh`：搭一棵假
   procfs/cgroup/job-ad 树，把 `DiscoveryEnvironment` 指过去，断言
   `enumerateManagers()`/`observe()` 的结果、health 迁移、`start/stop`
   生命周期，以及"事件源不可用是一条显式 note"。脚本复用 Ninja 的
   `JobLens` 链接命令，生产目标文件按 shipped 状态被演练。
2. **Registry 层** — 通过构造接缝注入假 source + 探针，参照
   `tests/integration/job_maintenance_test.cpp`：

   ```cpp
   JobRegistryOptions options;
   options.db_path            = (tmp / "db").string();
   options.register_rpc       = false;
   options.test_sources       = {{"ssh", &fake_source}};
   options.test_process_probe = probes.process_probe();
   options.test_cgroup_probe  = probes.cgroup_probe();
   options.test_heartbeat     = true;
   JobRegistry registry(options);
   registry.runPass(/*do_scan=*/true, /*wait=*/true);   // 直接驱动 pass
   ```

   不变量（I1–I8）在这一层钉死：merge 语义、generation 冲突、
   缺席/suspect 迁移、退休证据、closing 语义。

依赖 eBPF 的测试，照 `tests/integration/run_net_usage_test.sh` 的方式先
探测 namespace 可用性、不可用就干净跳过。

## 5. 文件清单（汇总）

| 新增 | 用途 |
|---|---|
| `include/job_watcher/<t>_job_watcher.hpp` | `IDiscoverySource` 实现 |
| `include/job_watcher/<t>_adapter.hpp` | `ISchedulerAdapter` + `REGISTER_SCHEDULER_ADAPTER` |
| `src/ebpf/trace_<manager>.bpf.c`（+ `include/ebpf/<manager>.h`） | 可选 exec-hint 事件源 |
| watcher / registry 层测试 | fixture 树、假 source pass |

| 修改 | 用途 |
|---|---|
| `include/core/job_registry.hpp`（或任一已编译 TU） | include adapter 头，使其 `REGISTER_SCHEDULER_ADAPTER` 注册器被链接 |
| `include/core/job_discovery.hpp` | 可选：特殊 `DiscoveryEnvironment` 根字段（如 spool 目录） |
| `config/config.example.yaml` | `<t>_job_watcher` 配置节 + `maintenance.types` 一项 |

`collector_type.h`、`job_identity`、`maintenance_config`、`JobStore`、
心跳/退休逻辑、`KernelAttribution`、RPC 方法、trigger 均**不需要改动**：
身份、配置节、native-id 规则、facts（JSON 通道）、匹配与 cgroup 推导全部
由 adapter 驱动。

## 6. 实现不变量

- 构造函数：只存 options——不读 Config、不加载 eBPF、不开线程。
- `start()` 在任何可能的回调之前装好 sink。
- 轮询线程把 exec hint 经 `on_job` 同步登记；发现 pass 跑在 registry 的
      线程上，绝不在轮询线程上跑。
- `stop()` 幂等、有界；从析构函数调用安全。
- `enumerateManagers()` 只在 pass 级失败时报 `complete=false`；进程表里
      读不出来的条目只计数加 note，manager 读不出来是重试、绝不是缺席。
- 发现的 Job 要么 ready 要么显式不完整
      （`execution->incomplete_reason`）；集群命名空间绝不从原生 id
      瞎猜。
- cgroup 成员：读成功（含空列表）即权威；退化路径标记
      `members_authoritative=false`。
- 构造方让 `JobID`/`JobPIDs`/`CollectorNames` 保持为空（store 与
      intent 所有）。
- `traits()` 如实；缺失的能力是正常状态。
- eBPF 失败把发现留给 manager pass，并留一条显式 note。
- 两个测试层级都在无特权环境通过；新不变量在 registry 层覆盖。
