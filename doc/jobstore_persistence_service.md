# JobStore 持久化服务边界重构方案

状态:方案已评审定稿(2026-11-03),待实施。
分支:`feature/collector-output-refactor`(在途 collector 改动由维护者自理后开工)。

## 1. 背景与诊断

JobStore 现状(`include/core/job_store.hpp` / `src/core/job_store.cpp`)是"单一持久化表示 + 半个业务引擎":

- **业务判定住在 store 里**:`upsert()`/`merge_existing()` 内嵌库存合并策略;`restore()` 内嵌恢复验证策略(boot 比对、manager start ticks、member 重查),并为此注入与准入同一个 `identity_probe`;`mutateSelections()` 内嵌选择策略(manual 恒压 auto、结果分类);`setIdentityApplier()` 把适配器派生回调拉进 store;`persistState()` 写业务生命周期词汇。
- **运行时状态寄生在 store 的内存镜像里**:`mutateRecord()` 供 registry 改内核归因 PID 增删、内存态 state、`execution_verified`,不落盘也不标 dirty;而 `flushDirty()` 落盘写的是内存镜像(cpp:1055-1063),`updateRecord()` 又以 DB 镜像整体替换内存(cpp:2180-2182)——内存镜像与持久化镜像边界模糊。
- **接口过宽**:约 30 个公开方法中 8 个无外部调用者(`isClosing`/`activeCount`/`dbUuid`/`bootId` 零调用;`restore`/`allocateId`/`updateRecord`/`merge_existing` 仅有类内调用);唯一生产消费者是 `JobRegistry`(job_registry.cpp:117 唯一构造点),测试全部经 registry facade。
- **头文件引用失效**:job_store.hpp:15 引用的 `doc/job_inventory_v2_architecture.md` 不存在。

## 2. 目标架构

### 2.1 定性

- **JobStore = 纯持久化服务**:LevelDB 封装(键布局、行编解码、批量事务、durability 分层、恢复围栏、ID 高水位)。不含任何业务判定、验证策略或生命周期词汇。
- **JobRegistry = 业务 + 唯一事实来源**:持有 `JobRecord` 本体(in-memory map 与身份索引从 store 迁入);一切判定(合并、恢复验证、选择策略、归档分流、退休)在 registry 完成;**其他模块一律拿投影(快照拷贝)或只读指针**。
- **变更原子性由 registry 记录锁串行化保证**(决策已定):业务变更序列"读事实来源 → 判定 → 变换 → 调 store 写行"全程持锁,CAS/revision 比较在锁内;store 不提供事务回调原语。现状 `upsert_mtx_` 即此串行化点,迁回 registry 语义不变、无并发回归。

### 2.2 持久化只服务两个需求

1. **重启恢复采集**:重启后按记录恢复内核归属与采集选择。
2. **自动 JobID 分配**:`c:` 高水位跨重启单调。

其余持久化内容一律不写。运行时投影(内核归因 PID 动态增删、运行中 `execution_verified`、内存态 state)**不持久化**——重启即失,由 BPF 事件与重新观察重建。

### 2.3 字段保留集(j: 行格式冻结,schema 4 不动)

| 字段 | 保留理由 |
|---|---|
| `job` 事实(含 sub_attr) | 恢复后 collector 复挂、内核归属准入的数据源 |
| `selection` | 恢复采集的核心(manual 覆盖/停止跨重启存活) |
| `state`(active/closing) | 中断的 closing 不得复活为 active |
| `identity` + `i:` 索引 | 重启后再观察的身份解析 |
| `generation`/`members`/`members_authoritative`/`execution_observed` | 防 PID 复用的恢复验证证据(判定逻辑在 registry) |
| `revision` | 变更定序与 CAS |
| `first_seen_us`/`last_seen_us` | 退休时限判定 |
| `quarantined` | 准入策略输入 |

### 2.4 归档分流(事实口径,决策在 registry)

- **判定**:registry 在退休决策点(drain 之前,runtime 尚挂载)用 `actualCollection()` 取**真实挂载集**;非空 = "采集过"。
- **采集过** → 写 `h:` 归档行,内容重新定义为:`final record 全量 + applied collector 快照 + 结束时间戳`(现行 `h:` 内容无业务读方,可自由重定义;线上 debug 依据)。
- **未采集过** → 持久删除(`j:` 行 + `i:` 索引项),不写 `h:`。
- 恢复语义:纯删除自证(`j:` 行在则重删,不在则完成);写 `h:` 的归档路径歧义恢复仍用 ArchiveFence 围栏(该机制保留,仅服务此路径)。

## 3. JobStore 最终接口

```cpp
class JobStore {
public:
    struct Options { std::string db_path; };   // 删除 identity_probe(归 registry)

    explicit JobStore(const Options&);
    ~JobStore();

    // --- 健康 / 生命周期 --------------------------------------------
    StoreHealth health() const;
    bool ready() const;
    bool recoveryRequired() const;
    // 恢复完成后通过 adopted 返回采用的真值行,registry 据此校正自己的 map
    bool recoverPending(std::vector<JobRecord>& adopted, std::string& error);
    bool flushDirty(const char* reason);

    // --- 读取(启动装载 / 恢复采用;运行期事实在 registry) -----------
    std::vector<JobRecord> loadAll();                  // j: 行全量(原 restore 的装载半)
    std::optional<JobRecord> readRow(uint64_t id);     // 恢复采用时读真值

    // --- 写入(全部由 registry 在其锁内调用;单批原子) ---------------
    struct WriteOp {
        enum Kind { PutRecord, DeleteRecord,          // j:
                    PutIdentity, DeleteIdentity,      // i:
                    PutHistory,                        // h:(采集过的归档)
                    ReserveCounter } kind;
        uint64_t internal_id = 0;
        std::optional<JobRecord> record;              // PutRecord / PutHistory
        std::optional<CanonicalIdentity> identity;    // i: 操作
        std::string history_key_suffix;               // PutHistory: 结束时间
        std::vector<std::string> applied_collectors;  // PutHistory: 事实快照
        uint64_t durable_counter = 0;                 // ReserveCounter
    };
    // durable=false 进入 dirty set(热路径);touched 用于歧义恢复围栏
    bool commit(std::vector<WriteOp> batch, bool durable,
                std::vector<uint64_t>& touched, const char* reason, std::string& error);

    // --- 测试故障注入(经 registry facade 转发) ----------------------
    void injectFaultForTest(StoreFaultMode mode, size_t skip = 0, size_t count = 1);
    void clearFaultsForTest();
};
```

消失的接口与去处:

| 现接口 | 去处 |
|---|---|
| `upsert`/`merge_existing`/`reserveIdInMemory` | registry:锁内"查索引 → 合并判定 → commit(PutRecord + ReserveCounter + PutIdentity)" |
| `mutateSelections` | registry:锁内 map CAS + 变换 + commit(单批,保持"全成或全不成") |
| `persistState`/`updateRecord`/`refreshFacts` | registry:锁内变换 + commit(PutRecord) |
| `insertManual`/`allocateId` | registry:构造记录 + commit(单批含 ReserveCounter,不再需要先独立提交拿号) |
| `archive` | registry 判定分流后:采集过 → commit(PutHistory + DeleteRecord + DeleteIdentity);未采集 → commit(DeleteRecord + DeleteIdentity) |
| `restore` | 拆为 store.`loadAll()` + registry 验证(probe 归 registry)+ 写回调整行 |
| `record`/`records`/`recordByIdentity`/`findActive`/`hasActive`/`closingCount`/`instanceId`/`selection`/`nextJobId`/`mutateRecord` | registry 自家 map 操作;对外仍走既有 registry facade(测试不动) |
| `setIdentityApplier` | 删除;派生在 registry 的合并序列内调用 |
| `dbUuid`/`bootId`/`isClosing`/`activeCount`(getter) | 删除(零调用);`db_uuid_`/`boot_id_` 成员留内部(instance_id 派生、启动读取) |

## 4. 迁移步骤(按功能点提交,每步可编译、套件全绿)

### 步骤 0 — `docs:` 方案文档
本文件 + job_store.hpp:15 死引用改指本文档。

### 步骤 1 — `refactor(core):` 零行为清理
删除零调用 getter(`isClosing`/`activeCount`/`dbUuid`/`bootId`);`restore`/`allocateId`/`updateRecord`/`merge_existing` 转 private;修正 mutateRecord 注释中失效的 `persistRecord` 指称。
**验证**:重编译 + store / lifecycle / collection-rpc 三套件。

### 步骤 2 — `feat(core):` 归档分流
registry 退休决策点取 `actualCollection()`;store 新增 `discard(id)`(纯删除)与带 applied 快照的归档写;`h:` 行内容重定义;纯删除路径自证恢复。
**验证**:三套件 + 重点核对 store 套件归档/恢复用例;交付前加跑 scheduler / output / watcher-scan。

### 步骤 3 — `refactor(core):` 事实来源迁移(核心步)
`records_` map、`identity_index_`、`upsert_mtx_` 语义迁入 registry;registry 记录锁建立;store 查询接口删除,registry facade 改读自家 map;`mutateRecord` 删除——归因 PID/`execution_verified`/内存态成为 registry 自家 map 上的不落盘变更;`restore` 拆分(probe 随迁);`setIdentityApplier` 拆除,适配器派生进 registry 合并序列。
**验证**:三套件 + scheduler / output / watcher-scan 全量;`hot_path_registration_does_not_fsync` 必须绿(upsert 热路径保持 durable=false + dirty set)。

### 步骤 4 — `refactor(core):` store 接口最终收敛
写路径(选择命令、状态写、手工注册、upsert)全部改走 `commit(WriteOp...)`;专用写接口删除;头文件契约注释按"纯持久化服务"重写;恢复接口改为 `recoverPending(adopted...)` 外馈真值。
**验证**:全量无特权套件;护栏断言绿。

## 5. 护栏与明确不做的事

**护栏**
- 注册热路径不得新增 fsync(`hot_path_registration_does_not_fsync`,store 套件)。
- j: 行序列化格式冻结,schema 4 不动(开发期本可无负担 bump,但保留集已核定,无字段可删)。
- 恢复语义不降级:歧义写后"禁止变更/应用/检查点直至显式恢复"的围栏行为保持;步骤 3/4 后 `recoveryRequired` 期间 registry 必须同等拒绝业务变更。
- 选择命令的"单批全成或全不成"原子性保持。

**明确不做**
- 不做 schema 迁移/升级工具(store 对旧 schema 仍只读拒绝)。
- 运行时投影不拆散归各子系统(先集中于 registry;有需求再议)。
- 不持久化 applied 集(归档行的一次性快照除外)。
- 不改对外 RPC / 测试 facade 的形状(测试全部经 `JobRegistry` 走,预期零测试改动;受步骤 2 归档行为影响的用例除外)。
