# Slice: T1.3e — UPDATE undo 写路径 + rollback（版本链结构建立）

- 日期：2026-06-22
- 关联设计：`innodb-undo-log-purge-design.md` §7.3（UPDATE undo 写流程）、§6.5 第389行（UPDATE_ROW undo 字段）+ 第397行（phase-1 允许存全量旧 image）、§7.5（MVCC 旧版本遍历，本片只建链不读）、§7.4（commit:insert undo REUSABLE、update undo 入 history）、§5.3（lastRollPointer=事务回滚链入口，≠记录版本链入口）。
- **厚设计歧义校正（高风险点）**：厚设计 §381 称 `prevRollPointer`=事务回滚链，但 §441/§475 称其=记录版本链（指上一版本 DB_ROLL_PTR）——二者矛盾。本片明确**本工程模型**（并修 §381，见下）：① `prevRollPointer`=**事务 undo 回滚链**（RollbackService 走指针，非顺序扫描）；② **记录版本链 = `oldHiddenColumns.dbRollPtr()`**（update undo 内存的旧 DB_ROLL_PTR），T1.4 MVCC 用它遍历旧版本（**不是** §475 字面的 prevRollPointer）。即把 InnoDB 单一 roll_ptr 拆成两条显式链。current map 记此偏离。
- 前置：T1.3c（`beforeInsert`/`UndoContext`/`RollbackSegmentSlotManager`/单事务 undo 段）；T1.3d（`RollbackService` 反向走链、`deleteClustered`、`onCommit` 释放 slot、`begin/finishRollback`）；record `RecordPageUpdater`（三模式 update，test-only）、`RecordCursor.dbTrxId/dbRollPtr`、`RecordEncoder`。
- 定位：T1.3 epic 第五片 = **UPDATE undo 写 + rollback 恢复**，并建立记录版本链结构供 T1.4 MVCC 读。

## 1. 范围

做：
- `storage.undo` `UndoRecord`：扩展为类型判别命令对象——加可空字段 `oldColumnValues`(全行旧列值)、`oldHiddenColumns`(旧 dbTrxId/dbRollPtr)；compact ctor 校验 INSERT_ROW→两者必 null、UPDATE_ROW→必非 null。静态工厂 `insert(undoNo,txnId,tableId,indexId,clusterKey,prevRollPointer)`（固定 INSERT_ROW）、`update(undoNo,txnId,tableId,indexId,clusterKey,oldColumnValues,oldHiddenColumns,prevRollPointer)`（固定 UPDATE_ROW，调用方不再传 type）。`UndoRecordType` 放开 `UPDATE_ROW`（仍拒 `DELETE_MARK`→T1.3f）。
- `storage.undo` `UndoRecordCodec`：按 type 分支。UPDATE_ROW 额外编码全列旧 image（按 schema 全列）+ 两隐藏列（TransactionId + RollPointer 字节）。损坏防护：截断、列数/schema 不匹配、未知 type → 抛 `UndoLogFormatException`。
- `storage.undo` `UndoLogSegment.append`：**RollPointer.insert 由记录类型决定**（INSERT_ROW→true、UPDATE_ROW→false），不再恒 true（修当前 `:125` 硬编码）。**单事务混合 undo 段**（insert+update 同段）：段头 `UndoLogKind` 仅反映创建时类型、**非权威**；每条记录的 `UndoRecordType` 首字节才是权威类型（T1 简化，注释/current map 标明；独立 insert/update undo log 留 purge 片）。
- `storage.trx` `UndoContext`：`insertUndoFirstPageId` **重命名 `undoFirstPageId`**（混合段持全部 undo，去误导名）；加 `hasUpdateUndo` 标志（仅 UndoLogManager 改）。`lastUndoNo`/`lastRollPointer` 仍事务级。
- `storage.trx` `UndoLogManager.beforeUpdate(txn, mtr, tableId, indexId, clusterKey, oldColumnValues, oldHiddenColumns, keyDef, schema) -> RollPointer`：ensureUndoContext（复用 `undoFirstPageId` 续 append）→ undoNo+1 → `UndoRecord.update(…, prevRollPointer=ctx.lastRollPointer)` → append（返回 insert=false 的 rp）→ 推进 ctx → 标 `hasUpdateUndo` → 返回 rp。`onCommit` 改为**仅 `!hasUpdateUndo` 才 release slot**（含 update 的已提交事务段保留，供 T1.4/purge）。
- `storage.btree` `SplitCapableBTreeIndexService.replaceClustered(mtr, index, key, newRecordWithHidden, expectedTrxId, expectedRollPtr) -> BTreeUpdateResult(replaced)`：导航 leaf(X)→findEqual→**所有权校验**(当前 `RecordCursor.dbTrxId/dbRollPtr`==expected，不符=replaced=false 幂等)→`RecordPageUpdater.update(newRecordWithHidden)`；`REQUIRES_REINSERT`(改聚簇 key)→抛 `BTreeUnsupportedStructureException`(T1.3e 不做改 PK)。前向更新与回滚恢复**共用**(仅 newRecord/expected 不同)。不 import trx/undo。`BTreeUpdateResult(boolean replaced)` 放 `storage.btree`。
- `storage.trx` `RollbackService.applyUndoRecord` 加 `UPDATE_ROW`→重建 `new LogicalRecord(schema.schemaVersion(), rec.oldColumnValues(), false, RecordType.CONVENTIONAL, rec.oldHiddenColumns())`（schema 稳定假设）→`replaceClustered(restore, expected=(rec.transactionId(), 当前 rp))`；`INSERT_ROW` 不变；`DELETE_MARK` 仍抛（T1.3f）。RollbackService 单段走链不变（混合段）。
- orchestration（test-wired，§7.3）：① `lookup` 读旧 image（SHARED 单独 MTR，T1 无并发旧值稳定）② updateMtr：`beforeUpdate`(写 UPDATE undo) ③ `replaceClustered`(新列 + `HiddenColumns(txnId,newRollPtr)`，expected=旧 hidden)；同 MTR commit（WAL）。

不做（→ 后续片）：
- DELETE-mark undo + `deleteMarkClustered` + rollback 取消标记（T1.3f）。
- ReadView / MVCC 旧版本读 `buildPreviousVersion` / 可见性（T1.4，本片只建版本链结构不遍历）。
- **external undo payload**（§379）：全量旧 image 超单页容量时 `UndoLogSegment.growAndAppend` 抛 `UndoPageOverflowException`（不支持 extern 页，失败边界有测试）。
- changed-columns-only 旧 image、改聚簇 PK、独立 insert/update undo 段（留 purge 片）、history list/purge/undo 页回收、恢复期 rollback、二级索引、生产 DML facade。

## 2. 关键决策
1. **两条显式链**（修厚设计 §381 歧义）：`prevRollPointer`=事务回滚链（RollbackService 走）；记录版本链=`oldHiddenColumns.dbRollPtr()`（T1.4 用）。无新指针字段。
2. **全量旧 image**（§6.5 第397行 phase-1 允许）：rollback 整记录恢复，简化 vs InnoDB changed-cols diff；超单页抛溢出（不做 extern）。
3. **单混合 undo 段 + 类型权威在记录字节**：`append` 按记录类型设 RollPointer.insert；段头 `UndoLogKind` 非权威；`undoFirstPageId` 去误导名；`onCommit` 按 `hasUpdateUndo` 条件释放（含 update 段不回收=缺口，purge 片处理）。独立 insert/update undo log 留 purge 片。
4. **`replaceClustered` 前向/回滚合一**：本质都是"所有权校验→整记录替换"，复用 `RecordPageUpdater`；幂等收敛（不匹配=no-op），改 PK 抛 `BTreeUnsupportedStructureException`。
5. **WAL**：beforeUpdate 与 replaceClustered 同 MTR；旧 image 读在前置 SHARED MTR（T1 无并发，文档化）。

## 3. 验收测试
- `UndoRecordCodecTest`(+)：UPDATE_ROW 往返（全列旧 image + 旧隐藏列 + clusterKey + prevRollPointer 等值）；ctor old* 可空性校验；**损坏**：UPDATE payload 截断、列数/schema 不匹配、未知 type → `UndoLogFormatException`；**INSERT_ROW 固定 golden bytes 断言**（证明字节不变，非仅 round-trip）。
- `UndoLogManagerTest`(+)：`beforeUpdate` 首/续写 undoNo 递增、prevRollPointer 串事务链、**返回 rp.insert()==false**、`hasUpdateUndo`；`onCommit` 含 update 不释放 slot、insert-only 仍释放；**混合 INSERT+UPDATE 段重开**(新 store/pool)按各自 rp 读回正确类型与内容。
- `BTreeReplaceClusteredTest`：前向替换后 lookup 见新值 + 新 DB_TRX_ID/DB_ROLL_PTR；所有权不匹配=replaced=false 且记录不变（幂等）；改聚簇 PK 抛 `BTreeUnsupportedStructureException`；level-1 跨 leaf 替换。
- `RollbackServiceTest`(+)：insert+update 混合事务 rollback 反向恢复（update 先恢复旧值、insert 后物理删，混合段单 walk）；**同行二次 update** rollback 链式恢复到原值（每步所有权 expected 链对）；未命中替换幂等。
- 失败边界：**超单页全量旧 image** `beforeUpdate` 抛 `UndoPageOverflowException`（不静默损坏）。
- 回归：全量 Gradle `test` 不倒退（当前 532）；INSERT 写/rollback、非聚簇路径不受影响；既有 INSERT undo 编解码字节不变（golden 钉死）。

## 4. current map 更新（实现后）
- **Record/B+Tree slice**：新增 `replaceClustered`（findEqual→所有权校验→RecordPageUpdater）数据链；`RecordPageUpdater` 由 test-only 改为**经 `replaceClustered` 被 `RollbackService` 调用（src/main 源码链，test-wired）**——非"0 prod"，仅前向 `beforeUpdate` orchestration 为 tests-only；标注无改 PK/无 merge。
- **Transaction/Undo slice**：`UndoRecord`/`UndoRecordCodec` 支持 UPDATE_ROW + 全量旧 image；`UndoLogSegment.append` RollPointer.insert 按类型；`beforeUpdate`/`UndoContext.hasUpdateUndo`/`undoFirstPageId` 重命名/`onCommit` 条件释放；`RollbackService` 消费 UPDATE_ROW；**记录版本链结构已建（DB_ROLL_PTR→update undo→`oldHidden.dbRollPtr`），MVCC 读 T1.4**。
- **缺口校正/新增**：UPDATE undo 写+rollback 已做（原 T1.3e）；DELETE-mark undo→T1.3f；混合 undo 段（段头 kind 非权威，记录 type 字节权威）+ 独立 insert/update undo log 留 purge 片；含 update 已提交事务 slot/段不回收（无 purge）；全量旧 image 简化 + 无 extern undo payload（超页抛）；无 ReadView/MVCC 读；无改 PK。**厚设计 §381 已校正**（prevRollPointer 语义对齐版本链目标，实现偏离记此处）。
- **Reserved / Unwired**：`BTreeUpdateResult` + 前向 `beforeUpdate` orchestration test-wired（生产组合根 0）。
