# Slice: T1.3f — DELETE-mark undo（写 + rollback + MVCC 可见性）

- 日期：2026-06-22
- 关联设计：`innodb-undo-log-purge-design.md` §7.3（DELETE 先 delete-mark 再写 undo，物理删除归 purge）、§6.5 第390行（DELETE_MARK undo 字段含 old delete flag）、§7.5（MVCC 旧版本遍历）、§14.1；`innodb-transaction-mvcc-design.md` §7.3（rollback）、§8.2（MVCC 可见性，含 delete-marked 行）、§11（purge 物理清理）、§16.3（DELETE 流程）。
- 前置：T1.3e（UPDATE undo 全量旧 image + `UndoRecord.update`/codec 尾部 + `replaceClustered`）、T1.4（`MvccReader`/`ReadView`/`UndoLogSegmentAccess.readRecordByRollPointer`）；record `RecordPage.setDeleted`（外科改 delete 位）/`RecordEncoder` 写 deleted 位 + 尾部隐藏列/`materialize().deleted()` 携带删除位；btree `findEqual`（含 delete-marked）/`lookup`（过滤 delete-marked）。
- 定位：T1.3 epic 收尾片 = **delete-mark 端到端**。DELETE 接成 logical delete-mark：写 DELETE_MARK undo、btree 置删除位 + 盖新 DB_TRX_ID/DB_ROLL_PTR、rollback 取消标记、MvccReader 按 ReadView 判 delete 可见性。**物理移除归 purge（非目标）。**

## 1. 范围

做：
- `storage.undo` `UndoRecord`：放开 `DELETE_MARK`——compact ctor 改为 INSERT_ROW→old* 必 null、**UPDATE_ROW/DELETE_MARK→old* 必非 null**。工厂 `deleteMark(...)`（旧 image=删除前**存活**版本：列不变 + 旧隐藏列）。**不存 old delete flag（阶段性差异 vs §6.5 第390行）**：delete-mark 限定 live↔deleted 翻转，旧 delete flag 隐含 false（见决策 1）。
- `storage.undo` `UndoRecordCodec`：UPDATE_ROW **和** DELETE_MARK 共用尾部（旧隐藏列 + 全列旧 image）；decode 放开 DELETE_MARK。`append` insert 位按类型（DELETE_MARK→false）。
- `storage.trx` `UndoLogManager.beforeDelete(...)`：与 `beforeUpdate` 同构，写 `UndoRecord.deleteMark`，返回 insert=false rp，标 `hasUpdateUndo`（delete undo 须存活至 purge）。
- `storage.record.page` `RecordPage.writeHiddenColumns(offset, HiddenColumns)`：外科修补尾部 15B 隐藏列（`offset + recordHeaderAt(offset).recordLength() - HIDDEN_BYTES`），不动其余字节。
- `storage.btree`：
  - `setClusteredDeleteMark(mtr, index, key, deleted, newHidden, expectedTrxId, expectedRollPtr) -> BTreeDeleteMarkResult(changed)`：**plan-then-execute**（决策 4）——plan：导航 leaf(X)→`findEqual`(含已标记)→所有权校验(当前 dbTrxId/dbRollPtr==expected，不符=changed=false 幂等)→**校验翻转合法**(当前 `deletedFlag != deleted`，否则抛领域异常=损坏/非法重复)→算 hidden 尾偏移；execute：`setDeleted(offset, deleted)` + `writeHiddenColumns(offset, newHidden)`（两步皆纯写、不抛，杜绝"删除位已改/隐藏列未改"半状态）。前向删除 deleted=true、expected=存活 hidden；回滚取消标记 deleted=false、expected=(txnId, delRp)。不 import trx/undo。`BTreeDeleteMarkResult(boolean changed)` 放 `storage.btree`。
  - `lookupIncludingDeleted(mtr, index, key) -> Optional<BTreeLookupResult>`：同 `lookup` 但**不过滤** delete-marked（`materialize().deleted()` 携带标志）。普通 `lookup` 仍过滤。
- `storage.trx` `RollbackService.applyUndoRecord` 加 `DELETE_MARK`→`setClusteredDeleteMark(deleted=false, newHidden=rec.oldHiddenColumns(), expected=(rec.transactionId(), rp))`。
- `storage.trx` `MvccReader`（在 T1.4 基础上加 delete-mark + 两处加固）：
  - 用 `lookupIncludingDeleted` 取当前版本；携带 `deleted` 标志（当前=`record.deleted()`，重建旧版本恒 false）；**可见 + deleted → empty**，可见 + 非 deleted → 返回；遍历接受 UPDATE_ROW **和** DELETE_MARK undo（构造存活旧版本）。
  - **加固 A（所有权链校验，决策 2）**：每跳校验 `undo.transactionId() == 当前版本 trxId`（undo 由产生该版本的事务写），不符抛 `UndoLogFormatException`，防串错 roll pointer 拼伪造历史。UPDATE_ROW/DELETE_MARK 均校。
  - **加固 B（MTR 不泄漏，决策 3，修 T1.4 既有缺陷）**：index 读与每条 undo 读的 MTR 用 try + 异常 `rollbackUncommitted` 后再抛，避免抛异常后线程仍绑 MTR 致后续 `begin()` 失败。
- orchestration（test-wired，§16.3）：delete = `lookup` 取存活当前版本 → `beforeDelete`(旧列+旧 hidden) → `setClusteredDeleteMark(deleted=true, (txnId,delRp), expected=旧 hidden)`，同 MTR commit；**`changed==false` 必须按失败处理（不得当删除成功）**（决策 5）。

不做（→ 后续片）：
- purge 物理移除 delete-marked + 二级索引项 + undo 回收；二级索引 delete-mark；locking/current read（并发 DELETE 冲突检测 + 行锁）；RU/SERIALIZABLE；生产 DML/SQL facade；恢复期 rollback。

## 2. 关键决策
1. **不存 old delete flag → 限定翻转**：阶段性差异（§6.5 第390行要求存）。delete-mark 仅 live(false)→deleted(true)、rollback 仅 deleted(true)→live(false)；旧 delete flag 隐含 false。已 delete-marked 记录：orchestration 经 `lookup`（过滤删除）取不到→不重复删除；`setClusteredDeleteMark` 所有权校验 + 翻转合法校验双重拒绝错向。补非法状态测试。
2. **版本链所有权校验**：MvccReader 每跳校 `undo.transactionId()==当前版本 DB_TRX_ID`（+ 既有 indexId/insert 位/cluster key/type 校验），杜绝伪造历史。
3. **读路径 MTR 异常清理**：MvccReader 每个 MTR try/异常 `rollbackUncommitted`，修 T1.4 既有泄漏（抛异常后 `begin()` 失败）。
4. **delete bit + hidden 两写运行时原子**：MTR rollback 不撤页内容，故全部校验 + 算偏移在首次写前完成，两步纯写不抛（plan-then-execute）。
5. **TOCTOU 与孤立 undo**：旧 image 在前置 SHARED MTR 读，写 undo + 删除在后 MTR；并发更新可致 `changed=false` 而 undo 已追加（孤立 undo）。本片：`changed=false` 报失败、孤立 undo/语句回滚未解决（同 T1.3c orphan）、生产并发 DELETE 须 current-read + 行锁片。
6. **外科对称不走 replaceClustered**：`RecordPageUpdater` 拒绝 delete-marked 旧记录无法 un-mark，故用 `setDeleted` + `writeHiddenColumns`（列/长不变）。

## 3. 验收测试
- `UndoRecordTest`/`UndoRecordCodecTest`(+)：DELETE_MARK 工厂 + ctor old* 校验；DELETE_MARK 往返；INSERT golden bytes 不变。
- `UndoLogManagerTest`(+)：`beforeDelete` undoNo/prevRollPointer 链、rp.insert()==false、`hasUpdateUndo`、onCommit 不回收 slot。
- `RecordPage`/btree：`writeHiddenColumns` 外科改 hidden 不动列/头其余位；`setClusteredDeleteMark` 标记后 `lookup` 过滤、`lookupIncludingDeleted` 见到且 deleted=true+新 hidden；取消标记后 `lookup` 复现；所有权不匹配 changed=false 不动记录；**非法翻转**（对已标记记录前向删除 / 对存活记录回滚 un-mark）抛领域异常；level-1 跨 leaf。
- `RollbackServiceTest`(+)：insert+delete-mark rollback → 取消标记还原存活 + 删 insert；insert+update+delete 混合逐级还原。
- `MvccReaderTest`(+)：删除事务可见→行消失(empty)；删除前建的旧 ReadView→见删除前版本；自身 delete→self 读消失；committed delete 后 RC 新 ReadView 见消失、老 RR ReadView 见旧版本；**所有权链不匹配**（undo.transactionId≠当前版本 trxId，UPDATE_ROW/DELETE_MARK 各一）抛异常；**MTR 异常后可再开**（损坏读抛异常后同 mgr 再 `read` 成功）。
- 回归：全量 Gradle `test` 不倒退（当前 585）；既有 lookup/update/rollback/MVCC 路径不受影响。

## 4. current map 更新（实现后）
- **Record/B+Tree slice**：新增 `RecordPage.writeHiddenColumns`、`setClusteredDeleteMark`（plan-then-execute）、`lookupIncludingDeleted` 数据链；`lookup` 过滤 delete-marked 标注。
- **Transaction/Undo slice**：`UndoRecord`/codec 支持 DELETE_MARK（不存 old delete flag=阶段差异）；`beforeDelete`；`RollbackService` 消费 DELETE_MARK；**`MvccReader` delete-mark 可见性已接** + 所有权链校验 + MTR 异常清理（修 T1.4 缺陷）。
- **缺口校正/新增**：DELETE-mark undo 写+rollback+MVCC 可见性已做；保留 purge 物理移除 + undo 回收、二级索引 delete-mark、locking read/并发 DELETE、孤立 undo 清理、生产 DML facade；标注 old delete flag 阶段性未存。
- **Reserved / Unwired**：`BTreeDeleteMarkResult`/`beforeDelete`/`setClusteredDeleteMark`/`lookupIncludingDeleted` test-wired（0 prod 组合根）。
