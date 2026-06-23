# Slice: T1.4 — ReadView + MVCC 一致性读（消费版本链）

- 日期：2026-06-22
- 关联设计：`innodb-transaction-mvcc-design.md` §5.4（ReadView 与五条可见性规则）、§7.1（creator id 生命周期）、§8.1–8.2（快照创建与版本遍历）、§17（短锁与 latch 边界）；`innodb-undo-log-purge-design.md` §7.2（INSERT undo 可复用）、§7.5（MVCC 旧版本遍历）、§8（undo S latch 立即释放）、§14.1（一致性读旧版本构造）、§15.2（consistent read 不依赖 INSERT undo）。
- 前置：T1.3c（聚簇记录写入真实 `DB_TRX_ID`/`DB_ROLL_PTR`）、T1.3e（UPDATE undo 全量旧 image；记录版本链为 `DB_ROLL_PTR → update undo → oldHidden.dbRollPtr`）；`TransactionSystem`/`TransactionManager`；btree `lookup` 返回已物化最新版本。
- 定位：T1 epic 首个可见性切片。创建 RR/RC ReadView，按隐藏列判断可见性，并消费既有 UPDATE 版本链；不扩大到 delete-mark、purge 或生产 SQL 接线。

## 1. 范围

做：
- `storage.trx.ReadView`（不可变 Snapshot）：保存 `creatorTrxId`、`upLimitId`、`lowLimitId`、`activeIds`；防御性复制集合并校验 `up <= low`、active id 全部位于 `[up, low)`。`isVisible(recordTrxId)` 顺序为 `==creator` 可见、`<up` 可见、`>=low` 不可见、active 集合内不可见、其余可见；记录 writer id 为 NONE 时拒绝为损坏输入。
- `TransactionSystem.openReadViewSnapshot(Transaction)`（包内）：在同一 `ReentrantLock` 临界区内，给尚无 id 的**非只读**事务分配并登记 creator id，再原子捕获 `{activeIds, nextTransactionId}`；只读事务 creator 保持 NONE。`lowLimitId=nextTransactionId`，`upLimitId=min(activeIds)`，集合空时 `up=low`。
- `ReadViewManager.openReadView(Transaction)` 只接受 ACTIVE：RR 惰性创建并缓存到 `Transaction.readView`，RC 每次返回新对象且不缓存；RU/SERIALIZABLE 明确抛 `TransactionStateException`，避免把未实现隔离级别静默当作 RR/RC。
- `ReadViewManager.release(Transaction)` 幂等清除 RR 缓存，允许事务处于 ACTIVE、COMMITTING 或 ROLLING_BACK；`TransactionManager.commit/finishRollback` 在移出 active table 后、进入终态前调用。RC 未注册 active-view list，本片 release 为无缓存可清理。
- `Transaction` 增加惰性 `readView` 字段及包内绑定/清理方法，仅 `ReadViewManager` 修改；`TransactionManager` 显式注入同一 `ReadViewManager`，不隐藏全局单例。
- `UndoLogSegmentAccess.readRecordByRollPointer(mtr, undoSpace, rollPtr, keyDef, schema)`：按单 undo space 的 pageNo+offset 直接以 S latch 打开 UNDO 页，不依赖 segment 首页；校验非 NULL、页类型、**真实 record 槽边界**、指针 insert 位与 record type、`record.indexId==keyDef.indexId`，格式/解码异常统一为 `UndoLogFormatException`。
- `MvccReader` 注入 `MiniTransactionManager`、btree、undo access、undo space 和有限 `maxVersionHops`；`read(readView, clusteredIndex, key)` 自主管理物理短 MTR，不接收调用方 MTR：一个 MTR 只物化 btree 当前版本并释放 index latch，之后每个 UPDATE undo 各用独立只读 MTR，构造旧版本后立即释放 undo latch。
- 遍历规则：可见版本直接返回；不可见且 roll pointer 为 NULL 或 `insert()==true` 时返回 empty（不得读取可复用 INSERT undo）；UPDATE 指针读回后校验 writer、indexId、cluster key 与当前链节点一致，用 `oldColumnValues + oldHidden` 构造上一版本。用 visited pointers + `maxVersionHops` 拒绝环和异常长链。

不做（→ 后续片）：
- DELETE-mark/DELETE undo（T1.3f）；现有 `btree.lookup` 会过滤 delete-mark，T1.3f 必须增加能物化 delete-mark 当前版本的聚簇读取 API；二级索引命中必须等回表 MVCC 接线。
- READ_UNCOMMITTED 最新读、SERIALIZABLE locking read、current/locking read、MVCC 唯一检查；本片对未支持入口显式失败。
- purge、oldest-ReadView/active-view list、transaction-no low limit、ReadViewId/对象池；INSERT undo 回收由后续片实现，但本片算法已不依赖其存活。
- 生产 DML/SQL facade、改聚簇主键、external undo payload、在线 DDL 跨 schema-version 旧版本；本片按 index 当前固定 schema 解码全量旧 image，并在注释中声明该简化。

## 2. 关键决策
1. **两条 undo 链不混用**：MVCC 版本链沿 `oldHidden.dbRollPtr`；`prevRollPointer` 仍是本项目 rollback 的事务链。
2. **读路径不反转写路径 latch 顺序**：写路径在同一 MTR 内为 undo→index；一致性读必须先结束 index MTR，再打开 undo MTR，任何时刻不同时持有 index 与 undo page latch。
3. **INSERT 指针是版本链终点**：其 insert bit 足以表达“更早版本不存在”，consistent read 不解码 INSERT undo。
4. **creator 分配区分 readOnly 与尚未写**：只读事务可保持 NONE；可写事务首次建 ReadView 即取得 writer id，保证之后的自身写命中 creator 规则。
5. **RR/RC 只共享可见性算法**：RR 缓存事务级快照，RC 由每次 consistent-read 调用创建新快照；语句边界由未来 executor 负责。
6. **损坏快速失败**：错槽、错类型、错索引/键/writer、环和超限链都抛领域异常，不返回拼错的历史行，也不无限遍历。

## 3. 验收测试
- `ReadViewTest`：五规则边界、activeIds 不可变、构造不变量、NONE record writer 拒绝；只读 creator=NONE 不误命中规则1。
- `ReadViewManagerTest`：RR `assertSame`、RC `assertNotSame`；非只读首次 open 原子分配 creator id，随后 `assignWriteId` 幂等；“先开 RR→首次写→读自身写”可见；RU/SERIALIZABLE 拒绝。
- `TransactionSystemReadViewTest`：空/非空 active 集合的 up/low 边界；并发 allocate/commit 与 snapshot 竞态下，每个快照保持 active id 位于 `[up, low)`，不存在拆分读取状态。
- `TransactionManagerTest`：commit 与无 undo rollback 自动清理；`RollbackService` 成功 finish 后清理，单条 undo 失败停在 ROLLING_BACK 时不提前宣称终态。
- `UndoLogSegmentAccessTest`：跨事务/跨段直读 UPDATE（另保留 INSERT 物理读取测试供 rollback 基座）；NULL、非 UNDO 页、越界、槽内 offset、insert/type 不符、indexId 不符均抛 `UndoLogFormatException`。
- `MvccReaderTest`：未提交 INSERT 不读 undo 即返回 empty；当前版本可见；单次/多次 UPDATE 链可回到 v1 或中间版本；自身写可见；RR 不漂移、RC 第二次读看到新提交。
- `MvccReaderCorruptionTest`：writer/key/index 不匹配、NULL hidden、指针环、超过 hop 上限快速失败；非聚簇索引拒绝。
- `MvccReaderConcurrencyTest`：受控 writer 按 undo→index 更新时，reader 在超时内完成且观测不到同时持有 index+undo latch 的反向顺序。
- 回归：固定 JDK/Gradle 全量 `test`，测试数不得低于实现前 552；既有 lookup、rollback、undo 编解码路径继续通过。

## 4. current map 更新（实现后）
- **Transaction Layer**：按源码新增 ReadView 创建/复用/释放、可写事务 creator 分配、RR/RC 行为与 lifecycle 接线；只有真实存在的调用边才画实线。
- **Undo Layer**：新增跨 segment 的 direct roll-pointer read 与精确槽校验；修正受影响的旧描述（包括 UPDATE_ROW 已实现、INSERT/UPDATE type 字节权威），不得保留“only INSERT_ROW”陈述。
- **数据链**：记录 `MvccReader.read → [index read MTR 完成并释放] → ReadView.isVisible → [逐条 undo read MTR] → reconstruct`，明确 INSERT bit 直接终止，禁止画成一个 MTR 同持两类页 latch。
- **缺口/Reserved**：MVCC RR/RC 为 test-wired；保留 delete-mark、secondary、RU/SERIALIZABLE、locking read、purge/oldest view、schema evolution 和生产 facade；`MvccReader`/`ReadViewManager` 无生产组合根时进入 Reserved/Unwired，并写清下一步 executor/storage facade 接线。
