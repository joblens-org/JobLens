# Job Watcher 扩展设计

本文说明调度器扩展点的设计约束：共享代码不感知具体调度器，调度器知识只住在
watcher 与 adapter 中。接入步骤与接口契约见
[job_watcher_integration.zh-CN.md](job_watcher_integration.zh-CN.md)。

## 1. 扩展契约

接入一种调度器需要：

- 一个 `IDiscoverySource` 实现（watcher）；
- 一个 `ISchedulerAdapter` 实现，并用 `REGISTER_SCHEDULER_ADAPTER` 注册；
- 一条 `maintenance.types` 配置；
- 一个链接入口：在已编译 TU 中 include adapter 头，或把它的 TU 加入构建，
  使注册器进入 Agent 二进制。

除链接入口外，以下文件不需要改动：`job_identity.{hpp,cpp}`、
`maintenance_config.{hpp,cpp}`、`job_registry.{hpp,cpp}`、`job_store.{hpp,cpp}`、
`kernel_attribution.{hpp,cpp}`、`job_rpc.cpp`、`collector_type.h`、任何
collector 与 trigger。

## 2. 设计原则

- **名字即身份**：adapter 的注册名（字符串）是共享代码中唯一的调度器身份。
  `JobRecord.adapter` 保存该名字，配置节、规则前缀、日志与 RPC 都使用它。
- **调度器知识只住在 adapter 里**：native-id 校验、facts 编码、cgroup 提取、
  legacy 行解码都通过接口或自注册规则表进入，共享代码不按类型分支。
- **约定优于登记**：配置节 `<name>_job_watcher`、规则目录
  `config/rules/<name>_jobs`、规则前缀 `<name>_job_` 由 adapter 名字推导。
  只有内置类型的历史 `auto_add_*` 键保留为显式兼容特例。
- **缺失即显式**：身份或执行证据不完整时，发现结果标记为 incomplete 并记录原因，
  不做猜测、不生成半可信记录。

## 3. 身份

`NativeSelector` / `CanonicalIdentity` 使用 `adapter` 字符串，不再使用
`SchedulerType` 枚举。通用逻辑（命名空间拼接、key 组装、原样保留 native 字符串）
在 `job_identity.cpp`；每个调度器的 native-id 规范化规则通过
`REGISTER_NATIVE_ID_RULE` 自注册。

身份分层保持独立：

- 内部存储键：`uint64` 的 JobID，0 预留给系统哨兵；
- 逻辑调度器身份：`NativeSelector` / `CanonicalIdentity`；
- 本地执行身份：`ExecutionGeneration` + `ProcessIdentity`；
- 对外实例句柄：内部 ID 的十进制字符串。

调度器原生编号、调用方哈希或裸 PID 都不是内部 ID，也不能单独构成完整身份。
查不到 native-id 规则时，`canonicalize()` 返回 `nullopt`，调用方按不完整观测处理。

## 4. facts 表示

`SubAttr` 是以下 variant：

```cpp
using SubAttr = std::variant<CondorJobAttr, CommonJobAttr, SlurmJobAttr, nlohmann::json>;
```

内置调度器保持强类型 attrs，现有 collector 与 `SubAttrMatcher` 不受影响。
后注册的调度器把 typed facts 作为 JSON 对象写入第四个分支，由 adapter 自己定义
字段、演进与解码方式。`Job.subtype_name` 保存 adapter 名字；序列化同时接受旧的
数字 subtype，新写入只写名字。

选择 JSON 而不是继续扩展枚举的原因是：新增调度器不应修改共享类型定义，
也不应让所有读取强类型 attrs 的 collector 重新编译或改写。

## 5. 发现与装配

1. `REGISTER_SCHEDULER_ADAPTER("ssh", SshSchedulerAdapter)` 在 TU 作用域把工厂
   注册进 `SchedulerAdapterRegistry`。
2. `JobRegistry` 启动时按 `maintenance.types` 构建 adapter；adapter 是唯一知道该
   调度器属性模式、身份格式与能力的组件。
3. 心跳驱动 adapter 的 `IDiscoverySource`：`enumerateManagers()` 枚举一遍 pass
   的存活 per-job manager，`observe(manager)` 为本 pass 尚未跟踪的 manager 构建
   Job，以及由 exec 事件触发的 O(job) `observeForHint(pid)`。
4. watcher 在解析源的位置直接构造 `Job`：typed facts + 一个 execution 证据组。
   注册表在准入时派生 selector / identity，不存在第二层传输信封。

发现结果只有两种状态：可解析身份且执行代次完整（ready），或显式 incomplete
并在 `job.execution->incomplete_reason` 写明原因。部分 pass 不得被解释为
“其余作业都已结束”；消费者必须先检查 `ManagerSet::complete`。

## 6. 配置与归因

- `DiscoveryTypeSettings` 按 adapter 名字键控，并始终包含全部已注册类型。
  未注册的名字在启动时报错并列出可用名字。
- `ISchedulerAdapter::discoveryOptions()` 把配置映射为 `DiscoveryOptions`。
- `applyIdentityToFacts()` 在 upsert 时把解析后的身份写回 typed facts；
  watcher 已填好的字段保持不变。
- `cgroupPathOf()` 返回该作业的执行 cgroup 路径，供内核归因使用。
  `traits()` 必须如实声明：`cgroup_scoped_membership` 表示 `cgroup.procs`
  是否为权威成员来源，`kernel_attribution` 表示是否参与 pid/cgroup 归因。
  缺少 trait 是正常状态，不是错误。

## 7. 兼容性

- 存量 LevelDB 行的 `scheduler_type` 与数字 `subtype` 必须原样可读；新行只写
  adapter 名字。
- 解码顺序：先由 adapter 的 `decodeFacts()` / `legacySelector()` 认领；
  无人认领时按 `CommonJobAttr` 通用行处理。
- 新调度器的 facts 使用 JSON 分支，不改变 schema 版本，也不改变既有行布局。

## 8. 示例：ssh 会话源

```cpp
// include/job_watcher/ssh_job_watcher.hpp
class SshJobWatcher final : public IDiscoverySource {
public:
    explicit SshJobWatcher(DiscoveryOptions options);   // 只存 options
    const std::string& name() const override { return name_; }
    bool start(const DiscoverySink& sink) override;     // 先装 sink，再启动轮询
    void stop() override;
    ManagerSet enumerateManagers() override;            // 权威枚举
    std::optional<Job> observe(const ProcessIdentity& manager) override;
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Job> observeForHint(pid_t, pid_t = 0) override { return std::nullopt; }
private:
    std::string name_{"ssh"};
    DiscoveryOptions options_;
};

// include/job_watcher/ssh_adapter.hpp
class SshSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit SshSchedulerAdapter(const DiscoveryTypeSettings& settings)
        : watcher_(discoveryOptions(settings)) {}

    const std::string& name() const override { return name_; }
    std::string configSection() const override { return "ssh_job_watcher"; }
    IDiscoverySource& source() override { return watcher_; }
    DiscoveryOptions discoveryOptions(const DiscoveryTypeSettings&) const override;

    Job decodeFacts(const nlohmann::json&) const override;   // ssh 无存量行
    NativeSelector legacySelector(const Job&) const override { return {}; }
    AdapterTraits traits() const override { return {}; }

private:
    std::string name_{"ssh"};
    SshJobWatcher watcher_;
};
REGISTER_SCHEDULER_ADAPTER("ssh", SshSchedulerAdapter)
REGISTER_NATIVE_ID_RULE("ssh", [](const NativeSelector& s) {
    return s.native_job_id.find_first_not_of("0123456789") == std::string::npos
        ? std::optional<CanonicalParts>(CanonicalParts{s.native_job_id, s.cluster_name, {}})
        : std::nullopt;
});
```

```yaml
job_registry_config:
  maintenance:
    types: [condor, slurm, ssh]
ssh_job_watcher:
  auto_add: default
  auto_add_collectors: [cpumem_collector]
```
