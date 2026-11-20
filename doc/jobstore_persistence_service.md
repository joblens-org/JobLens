# JobStore 持久化服务边界

## 1. 定位

`JobStore` 是 LevelDB 之上的纯持久化服务，只负责：

- 键空间布局（`j:` 记录、`i:` 身份索引、`h:` 归档、`c:` 自动 ID 高水位、`m:` 元数据）；
- 行编解码、批量写原子性、durability 分层、恢复围栏与自动 ID 分配。

所有业务判定都在 `JobRegistry` 完成：记录合并、恢复验证、选择策略、归档与删除、
移除时机。`JobRegistry` 在记录锁内完成“读事实 → 判定 → 变换”，然后调用
`JobStore::commit()` 写入结果；`JobStore` 不解释这些行。

## 2. 持久化范围

持久化只服务两个需求：

1. **重启恢复采集**：按记录恢复内核归属与采集选择。
2. **自动 JobID 分配**：`c:` 高水位跨重启单调，已分配的 ID 永不复用。

以下运行时投影不持久化，重启后由发现与重新观察重建：

- 内核归因中动态增删的 PID；
- 运行时的 `presumed_live` 判据（`jobs/status` 的 `suspect` 计数即 `!presumed_live`）；
- 采集器实际挂载集合（`applied_collectors`）。

## 3. 行格式与字段

`j:` 行格式冻结在 schema 5。保留字段及理由：

| 字段 | 保留理由 |
|---|---|
| `job` 事实（含 `sub_attr`） | 恢复后 collector 复挂、内核归属准入的数据源 |
| `selection` | 恢复采集的核心；manual 覆盖与显式停止跨重启存活 |
| `state`（active / closing） | 中断的 closing 不得复活为 active |
| `index_key` + `i:` 索引 | 重启后再观察时的身份解析（逻辑身份只从 facts 派生，不落行） |
| `manager` / `pid_identities` / `pids` | 防 PID 复用的恢复验证证据：`manager` 三元组、成员身份三元组（`pid` + `start_ticks` + `boot_id`），以及由 `pid_identities` 派生的裸 pid 投影 `pids`；三者都落行。`pid_identities_authoritative` 与 `read_order` 不落盘，恢复后分别为 `false` / `0`。判定逻辑在 registry |
| `revision` | 变更定序与 CAS |
| `first_seen_us` / `last_seen_us` | 移除判定依据 |
| `quarantined`（不落盘） | 当前没有隔离层：行内不写该键，健康计数 `quarantined_identities` 恒为 0 |

采集选择不单独携带 revision：`JobRecord::revision` 是唯一单调计数器，选择命令的
CAS 也由它定序。

## 4. 接口

```cpp
class JobStore {
public:
    struct Options {
        std::string db_path;                 // 空值使用 job_registry_config.job_db_path
    };

    explicit JobStore(const Options& options);
    ~JobStore();

    // --- 健康 / 恢复 ---------------------------------------------------
    StoreHealth health() const;
    bool ready() const;
    bool recoveryRequired() const;

    // 显式完成恢复：采用未知结果的持久行、重写脏集、清除围栏。
    bool recoverPending(std::vector<JobRecord>& adopted,
                        std::vector<uint64_t>& removed,
                        std::string& error);

    // --- 启动装载 -------------------------------------------------------
    std::vector<JobRecord> loadAll();                  // 原始 j: 行，不做业务验证
    std::optional<JobRecord> readRowByIdentity(const SchedulerKey&);

    // --- 自动 ID --------------------------------------------------------
    uint64_t nextJobId();
    bool reserveId(uint64_t& id, uint64_t& durable_counter,
                   bool& durable_exhausted, std::string& error);

    // --- 唯一写原语 -----------------------------------------------------
    struct WriteOp {
        enum Kind {
            PutRecord, DeleteRecord,          // j:
            PutIdentity, DeleteIdentity,      // i:
            PutHistory,                       // h: 归档 = 最终记录 + applied 快照 + 结束时间
            ReserveCounter,                   // c: 与 create 同批的 ID 高水位
        } kind = PutRecord;
        uint64_t job_id = 0;
        std::optional<JobRecord> record;
        std::optional<SchedulerKey> key;              // i: operations
        std::string history_key_suffix;
        std::vector<std::string> applied_collectors;
        std::string history_provenance;
        uint64_t durable_counter = 0;
        bool counter_exhausted = false;
    };
    bool commit(std::vector<WriteOp> ops, bool durable,
                std::vector<uint64_t>& touched, const char* reason,
                std::string& error);

    // --- 检查点 / 关停刷写 ----------------------------------------------
    bool flushDirty(const char* reason);

    // --- 测试故障注入 ---------------------------------------------------
    void injectFaultForTest(StoreFaultMode mode, size_t skip = 0, size_t count = 1);
    void clearFaultsForTest();
};
```

写路径约束：

- 一批 `WriteOp` 原子提交，批次内容与顺序由 `JobRegistry` 决定；
- `durable=false` 只进入脏集，由维护 pass 的 checkpoint 或关停刷写合并 fsync；
  注册热路径不得触发 fsync；
- `touched` 列出本批重写的实例；写入结果不确定时，恢复围栏只对这些实例生效；
- 纯删除批次用“行不存在”自证结果，不需要额外围栏；
- `PutIdentity` 拒绝仍被其他实例持有的 live 身份，过期索引项可以覆盖；
- 归档路径（写过 `h:`）的歧义恢复使用 ArchiveFence 围栏。

## 5. 职责边界

| 业务动作 | 责任方 |
|---|---|
| upsert 合并、事实更新、身份解析 | `JobRegistry` |
| 恢复验证、suspect 标记、移除判定 | `JobRegistry` |
| manual / automatic 选择策略与 CAS | `JobRegistry` |
| 归档分流（采集过 → `h:`，未采集 → 删除） | `JobRegistry` 判定，`JobStore` 执行 |
| 内核归因 PID 增删、运行中执行验证 | `JobRegistry` / `KernelAttribution` |
| 键布局、行编解码、批量原子性、durability | `JobStore` |

## 6. 不变量

1. **单一事实来源**：运行时记录 map、身份索引和业务判定都在 `JobRegistry`；
   `JobStore` 只持有持久化镜像。
2. **恢复不降级**：写结果不确定后，禁止业务变更、应用与检查点，直到
   `recoverPending()` 显式完成恢复。
3. **选择命令原子**：一次选择命令的所有目标在同一批内全成或全不成。
4. **ID 单调**：自动 ID 高水位与 create 同批提交，已分配的 ID 永不复用。
5. **快照只读**：其他模块只拿 `JobRecord` 投影或只读指针，不直接修改持久化镜像。

## 7. 存储格式生命周期（准入缝）

`admit_database()` 是唯一的格式准入决策点，`init_db` 只执行其结论：

| 磁盘状态 | 处置 |
|---|---|
| `m:schema_version` = 当前版本 | 正常打开 |
| 标记非规范 / 标记其他版本 | 拒绝打开（只读关闭，不动任何字节） |
| 无标记、空库 | 初始化新 schema 库 |
| 无标记、有数据（旧格式库）且 `<db>.legacy` 不存在 | **隔离**：只读关闭 → 改名为 `<db>.legacy` → 新库另起 → 从 `.legacy` 导入分配水位线（`c:job_id` 与最大 `j:` 键 + 1 取大，sync 写入） |
| 无标记、有数据、`.legacy` 已存在 | 拒绝（隔离槽单用，不链式覆盖，交运维裁决） |

要点：

- 隔离**只改文件名，不改内容**：旧库每个键值保持原样；行数据永不自动转换
  （行内容转换需要 adapter，属 `JobRegistry` 层，store 只在键层面迁移）。
- **水位线与 schema 标记同一 synced 批写入**：崩溃只会留下「整批丢失（无标记）」
  或「标记+水位俱全」两种状态，不存在「有标记无水位」。已标记库在打开时仍会
  尝试从 `.legacy` 补收水位（幂等）；`.legacy` 不可读且本库尚无 `c:job_id` 时
  拒绝就绪（`legacy_watermark_unreadable`），本库已有计数器则视为残留放行。
- 水位线取 `max(c:job_id, 最大 j: 行号 + 1, 最大 h: 行号 + 1)`（饱和：最大行号
  为 UINT64_MAX 时直接写 MAX，计数器初始化按既有保守规则停止分配）；旧库是外来
  数据，畸形 `j:/h:` 键跳过并告警，不解析、不抛异常。
- 回滚到旧版：停服务 → `mv <db> <db>.v5` → `mv <db>.legacy <db>` → 安装旧
  版 RPM。旧二进制可直接读回（新库最多写过一个它本就认识的 `c:job_id`）。
  **注意**：若新版运行期间已发出过 JobID（这些 ID 只存在于 `<db>.v5`），换回后
  旧库计数器不包含它们，可能重发；回滚前记录 `<db>.v5` 的 `c:job_id`，必要时
  将旧库 `c:job_id` 推进到不小于该值再启动。
- 未来 schema bump 需要保行数据时，在 `admit_database()` 加一个转换臂即可，
  `init_db` 控制流不变。
