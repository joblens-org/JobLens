# Job Watcher-Only 扩展设计（去除 SchedulerType/InventoryType 硬编码）

**状态：** **已实施（阶段 1a + 1b，2026-10-29，commits 76dc9ea..）**。
F 区采用方案一（variant+JSON）并落地；`SchedulerType`/`InventoryType`
枚举已删除，验收标准达成（接入新调度器 = watcher/adapter 文件 + 注册行
+ 配置，共享代码零改动；接入文档的文件清单已同步）。方案二（全开放
JSON facts）仍为可选的后续项。
**审计依据：** 2026-10-29 会话的硬编码依赖盘点（SchedulerType/JobSubType/InventoryType 全量扫描）。
**目标引用：** doc/job_inventory_v2_architecture.md §1 G4（"No enum, no registry member, no switch in shared code"）。

## 0. 验收标准

接入一种新调度器（例如 ssh 会话源）＝ **两个新增头文件（watcher + adapter）+ 一行 `REGISTER_SCHEDULER_ADAPTER` + 一条 `inventory.types` 配置**。以下文件零改动：

`job_identity.{hpp,cpp}`、`inventory_config.{hpp,cpp}`、`job_registry.{hpp,cpp}`、`job_store.{hpp,cpp}`、`kernel_attribution.{hpp,cpp}`、`inventory_rpc.cpp`、`collector_type.h`、任何 collector、trigger。

验收方式：以 ssh 假想 adapter 走通"配置 → 发现 → 建档 → 自动采集 → 退休"全链路，且 `git diff --stat` 只含新增文件与配置示例。

## 1. 现状（为什么还做不到）

审计发现的硬编码分四类，其中三类阻断 watcher-only：

1. **身份枚举**：`SchedulerType`（job_identity.hpp）、`InventoryType`（inventory_config.hpp）、`JobSubType`（collector_type.h）都是封闭枚举，新类型无法命名自己。
2. **共享代码 binary 分派**（最危险，静默出错）：
   - job_store.cpp:584 `apply_identity_to_facts`：`if (Condor) … else 一律当 Slurm`，**在 upsert 活路径上**；
   - job_identity.cpp:225 `canonicalize`：else 分支按 Slurm 规则 canonical 化；
   - kernel_attribution.cpp:29：按 JobSubType 提取 cgroup 路径。
3. **配置解析硬编码**：inventory_config.cpp 4 处 `{Condor, Slurm}` 字面迭代、节名/规则目录三元表达式、legacy auto_add 键。
4. **枚举键控容器**：`auto_policies_`、`source_complete`（evaluateRetirements 的 coverage 查询对新类型静默 miss → 永不缺席退休）。

合法保留区（不动）：adapter 自身、watchers、eBPF 程序、`CondorJobAttr`/`SlurmJobAttr` 定义。

## 2. 设计原则

- **名字即身份**：adapter 注册名（字符串）是共享代码里唯一的调度器身份。`JobRecord.adapter` 已是名字，本设计让下游全部跟上。
- **调度器知识只住在 adapter 里**：native-id 规则、facts 编码、cgroup 提取、legacy 解码，通过接口或自注册规则表进入，共享代码零分支。
- **约定优于登记**：配置节 `<name>_job_watcher`、规则目录 `config/rules/<name>_jobs`、规则前缀 `<name>_job_` 由约定生成；仅 condor/slurm 的 legacy `auto_add_*` 折叠保留为显式特例。

## 3. 改造区

### A. 身份层（job_identity）

- `NativeSelector.scheduler_type: SchedulerType` → `adapter: std::string`。序列化沿用原键名：写 `"adapter": "condor"`；读兼容旧值（`scheduler_type: "condor"`）。`scheduler_type_from_string` 降级为 legacy 解码垫片（容忍大小写）。
- `CanonicalIdentity` 同步改名字字段；`key()` 用名字参与总序。
- `canonicalize(selector)` 保留通用部分（命名空间拼接、key 组装），**native-id 校验/规范化规则外置**：新增自注册规则表
  `REGISTER_NATIVE_ID_RULE("condor", fn)`（与 `REGISTER_SCHEDULER_ADAPTER` 同一 TU 作用域约定）。condor 注册 `"d.d"` 严格十进制规则，slurm 注册数字去前导零规则；`canonicalize` 按名字查表，查不到 → 不完整（显式，不猜测）。

### B. 发现源（job_discovery）

- Observation 中间态已删除（2026-11-04）：watcher 直接构造带 `execution`
  证据组的 Job，typed facts 在解析源的地方就地填写；readness 由
  `discovery_job_ready` 判定，缺失原因记在
  `job.execution->incomplete_reason`（`finalize_discovery_job` 收尾）。
- `IDiscoverySource::schedulerType()` 退役；`DiscoveryEnvironment` 中 per-scheduler 命名空间字段（`condor_cluster_name`/`slurm_cluster_name`）合并为通用 `cluster_name`（各自 adapter 只读自己的键——通过 `InventoryTypeSettings.cluster_name` 已有通路）。

### C. 配置解析（inventory_config）

- 删除 `InventoryType` 枚举与 `adapter_name_of`。
- `InventoryMaintenanceConfig.types: std::vector<std::string>`（注册名直填）；`settings: std::map<std::string, InventoryTypeSettings>` 按名字键控，**始终包含全部已注册类型**（解析阶段对 `SchedulerAdapterRegistry` 校验，未知名启动报错并列出注册名——行为与现状 build 步骤一致，只是提前）。
- 节名/规则目录/前缀按 §2 约定生成；legacy `auto_add_condorjob`/`auto_add_slurmjob` 折叠保留显式特例。

### D. Registry 运行时（job_registry）

- `auto_policies_: std::map<std::string, AutoCollectorPolicy>`，武装时直接用 settings 的名字键（删除 job_registry.cpp:984 的 InventoryType→SchedulerType else）。
- `source_complete: std::map<std::string, bool>` 按源名字键控；`evaluateRetirements` 用 `record.adapter` 查询——**修复新类型永不缺席退休的静默缺陷**。
- `apply_auto_policy(job, SchedulerType)` → 名字参数；`update_job`/refreshJobFacts 的 subtype 分派（1585-1591）改经 adapter（按 record.adapter 查注册表调 facts 刷新钩子；无 adapter 的手工记录走 CommonJobAttr 通用路径）。

### E. Store（job_store）

- **删除 `apply_identity_to_facts` 的类型分派**：discovery upsert 的 facts 由 watcher 在解析源时直接填完（Observation/encodeFacts 中转已删除）；身份推导经 registry 按 `record.adapter` 查 adapter 的 `applyIdentityToFacts` 完成，store 保持零分支。
- legacy 行解码（`decode_legacy_job`/`selector_from_legacy_job`）：遍历注册 adapter 的 `decodeFacts`/`legacySelector`（对外来类型已返回"不认识"），认领者处理；无人认领 → CommonJobAttr 通用行。
- `record_json` 对 facts 的序列化随 F 区方案走。

### F. facts schema（collector_type.h）——两案

- **方案一（推荐，阶段一）**：`SubAttr = std::variant<CondorJobAttr, CommonJobAttr, SlurmJobAttr, nlohmann::json>`。内置两类型保持强类型（存量 collector、SubAttrMatcher 强类型匹配零改动）；**新调度器的 typed facts 以 JSON 进 variant**（watcher 构造 Job 时自由填写、`factsSchema()` 可选声明）。`Job` 增加 `subtype_name: std::string`（wire 新旧并存：旧数字 subtype 照读，新行只写名字）。匹配器对 JSON 成员做字段级匹配。
- **方案二（阶段二，可选）**：`sub_attr` 彻底 JSON 化。收益是完全开放；代价是所有读强类型 attr 的 collector 全改。本设计不阻塞、不要求。

### G. KernelAttribution

- `ISchedulerAdapter` 增加 `std::string cgroupPathOf(const Job&) const`（condor 返回 `slots_cgroup_path`，slurm 返回 `cgroup_path`，其余空）。kernel_attribution.cpp 的 subtype 分派改调此接口（经 registry 按 `record.adapter` 查）。traits 门（`kernel_attribution`）已有，语义不变。

### H. RPC / 手工路径（inventory_rpc）

- `json2JobOpt`：`"adapter": "ssh"` 名字 + 可选 JSON facts；subtype 由名字推导（新类型 → JSON 分支）。
- 匹配器解析（639-640）接受任意 subtype **名字**字符串。
- legacy selector 重构（353/534-545）走 E 区的 adapter 认领路径。

## 4. 阶段划分

| 阶段 | 内容 | 效果 |
|---|---|---|
| 1a | A + B + C + D（名字化 + 配置泛化） | 发现/身份/配置/退休/自动选择全部调度器无关；新类型不再被误判 |
| 1b | E + G + H（store/RPC 去分支） + F 方案一 | **达成验收标准：watcher-only**（新类型 facts 走 JSON variant） |
| 2 | F 方案二 | facts 类型系统完全开放（可选，另行立项） |

每阶段全部存量测试（identity/store/intent/maintenance/lifecycle/autopolicy）必须不改动断言即通过——它们是行为不变的回归网。

## 5. 示意 watcher（ssh，阶段 1b 完成后）

```cpp
// include/job_watcher/ssh_job_watcher.hpp —— 新增文件①：发现源
class SshJobWatcher final : public IDiscoverySource {
public:
    explicit SshJobWatcher(DiscoveryOptions options);   // 只存 options，不开线程不碰 Config
    const std::string& name() const override { return name_; }
    bool start(const DiscoverySink& sink) override;     // 先装 sink；无 eBPF，返回 false 走纯扫描
    void stop() override;
    ScanResult scan() override;          // 枚举 /proc 中带 JOBLENS_SSH_JOB 标记环境变量的会话进程
    SourceHealth health() const override;
    void requestRescan() override;
    std::optional<Job> observeForHint(pid_t) override { return std::nullopt; }  // 无 exec 事件源
private:
    std::string name_{"ssh"};
    DiscoveryOptions options_;
    ...
};

// include/job_watcher/ssh_adapter.hpp —— 新增文件②：适配器
class SshSchedulerAdapter final : public ISchedulerAdapter {
public:
    explicit SshSchedulerAdapter(const InventoryTypeSettings& settings)
        : watcher_(discoveryOptions(settings)) {}

    const std::string& name() const override { return name_; }
    std::string configSection() const override { return "ssh_job_watcher"; }  // 约定，无需登记
    IDiscoverySource& source() override { return watcher_; }
    DiscoveryOptions discoveryOptions(const InventoryTypeSettings&) const override;

    // 发现期 facts 由 watcher 自己构造（不再有 encodeFacts 中转）；此处只保留
    // 存量行解码与身份推导。
    Job decodeFacts(const nlohmann::json&) const override;   // ssh 无存量行，返回通用 Job
    NativeSelector legacySelector(const Job&) const override { return {}; }
    AdapterTraits traits() const override { return {}; }     // 无 cgroup 证据、无内核归因

    // 新增接口（G 区）：cgroup 路径提取归 adapter
    std::string cgroupPathOf(const Job&) const override { return {}; }

private:
    std::string name_{"ssh"};
    SshJobWatcher watcher_;
};
REGISTER_SCHEDULER_ADAPTER("ssh", SshSchedulerAdapter)
REGISTER_NATIVE_ID_RULE("ssh", [](const std::string& raw) {   // A 区：native-id 规则自注册
    return raw.find_first_not_of("0123456789") == std::string::npos
        ? std::optional<std::string>(raw) : std::nullopt;     // ssh 会话 id：纯数字
});
```

```yaml
# config/config.example.yaml —— 仅加一节 + types 一项
job_registry_config:
  inventory:
    types: [condor, slurm, ssh]     # 直接写注册名
ssh_job_watcher:                    # 约定节名，resolver 泛化识别
  auto_collect: true
  auto_add_collectors: [cpumem_collector]
```

退休/缺席/对账/内核归因（traits 全关）自动按通用规则工作，无需任何额外接线。

## 6. 风险与开放问题

1. **序列化兼容**：存量 LevelDB 行的 `scheduler_type`/数字 `subtype` 必须原样可读（A/F 的双读垫片）；schema 仍为 2，不加版本。
2. **`SubAttr` 加 JSON 成员**：`std::visit` 全路径要审计（SubAttrMatcher 的 variant 遍历已有 `if constexpr` 结构，新增分支集中一处）。
3. **规则表单例**：`REGISTER_NATIVE_ID_RULE` 与 adapter 注册表同级；注意静态初始化顺序（规则表只被 canonicalize 按需查询，不依赖跨 TU 顺序）。
4. **legacy 认领遍历**（E 区）：注册 adapter 数量小（≤3），逐个询问成本可忽略。
5. **测试**：`FakeSource` 构造不再传 SchedulerType；maintenance 套件的观测构造辅助同步改名。断言语义不变。
