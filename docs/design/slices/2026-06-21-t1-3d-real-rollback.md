# Slice: T1.3d — 真 rollback（INSERT undo 反向走链 + btree 物理删除 + slot 回收）

- 日期：2026-06-21
- 关联设计：`innodb-undo-log-purge-design.md` §7.6（full rollback 流程：从 `lastRollPointer` 反向、INSERT_ROW 删除未提交插入、每条独立 MTR、走到 prev=NULL、释放 slot、ROLLING_BACK→ROLLED_BACK）、§11.2（RollbackService.rollback/applyUndoRecord）、§14.4（rollback 数据流）、§7.7（purge/物理清理约束，作对照）、§10（rollback 单条 undo 失败处理）、§9.3（锁序）。
- 前置：T1.3c（`UndoLogManager.beforeInsert` 写 INSERT undo + 真 `DB_ROLL_PTR`、`UndoContext` 挂 `Transaction`、`RollbackSegmentSlotManager.claim`、`UndoLogSegmentAccess.open/readRecord`）；record 层 `RecordPageSearch.findEqual`、`RecordCursor.dbTrxId/dbRollPtr`、`RecordPageDeleter.deleteMark`、`RecordPagePurger.purge`；btree `SplitCapableBTreeIndexService` 导航（level 0/1）。
- 定位：T1.3 epic 第四片 = **首次消费 undo**。把 T1.3c 写下的 insert undo 链真正回放：反向走链物理删除未提交聚簇行，回收内存 slot，commit 也回收 slot。

## 1. 范围

做：
- `storage.btree`：`SplitCapableBTreeIndexService.deleteClustered(mtr, index, SearchKey, TransactionId expectedTrxId, RollPointer expectedRollPtr) -> BTreeDeleteResult(removed)`，与 `insertClustered` 对称的**具体方法**（不进 `BTreeIndexService` 接口，`LeafOnlyBTreeIndexService` 不动），不 import trx/undo（只收 domain 值对象）。导航到 leaf（level 0=root leaf / level 1=chooseChild，X）→ `findEqual`：
  - 未命中 → `removed=false`（幂等）。
  - 命中但 `cursor.dbTrxId()!=expectedTrxId` 或 `cursor.dbRollPtr()!=expectedRollPtr` → `removed=false`（**所有权校验**：不是本 undo 插入的行，绝不误删同 key 记录）。
  - 命中且匹配：未 delete-marked 则 `deleteMark` 后 `purge`；**已 delete-marked 则跳过 deleteMark 直接 purge**（幂等/可重试），返回 `removed=true`。
  - 同 MTR 产 PAGE_BYTES redo。不做 merge / node-pointer 维护 / 空页回收。
- `storage.trx`：`RollbackService`（rollback 执行器，固定放 `storage.trx`，直 import btree=设计 §94）。**构造依赖（显式注入，禁全局/临时 new）**：`SplitCapableBTreeIndexService`、`UndoLogSegmentAccess`、`RollbackSegmentSlotManager`、`TransactionManager`、`MiniTransactionManager`。`rollback(Transaction, BTreeIndex clusteredIndex) -> RollbackSummary`：
  - `txnMgr.beginRollback(txn)`（ACTIVE→ROLLING_BACK）。
  - ctx 存在则从 `ctx.lastRollPointer` 反向走链，**每条独立 MTR**：`undoAccess.open(SHARED)`+`readRecord`→取 `prev`→`applyUndoRecord`→`mtr.commit`。`applyUndoRecord` 按 `rec.type()` 分派：`INSERT_ROW`→断言 `rec.indexId()==index.indexId()`→`deleteClustered(key=rec.clusterKey(), expectedTrxId=rec.transactionId(), expectedRollPtr=当前 rp)`；`UPDATE_ROW`/`DELETE_MARK`→抛 `DatabaseRuntimeException`（T1.3e+ 保留）。
  - **失败语义（§10）**：`slotManager.release` 与 `finishRollback` **仅在 while 走到 prev=NULL 后执行**；任一条 undo 抛异常即向上传播，事务停在 `ROLLING_BACK`、slot 不释放、活跃表不变，保持可重试。
  - `slotManager.release(ctx.slotId())` → `txnMgr.finishRollback(txn)`（removeActive + ROLLING_BACK→ROLLED_BACK）。
  - **单聚簇索引假设**：用传入 index 的 keyDef/schema 解码所有 undo（T1 无 data dictionary）。
- `storage.trx`：`TransactionManager.rollback()` 拆为包内 `beginRollback`(ACTIVE→ROLLING_BACK) + `finishRollback`(removeActive + →ROLLED_BACK)，public `rollback()`=两者组合（行为不变，老测试不动）。
- `storage.trx`：`RollbackSegmentSlotManager.release(UndoSlotId)`（锁内 `slots[idx]=null; activeCount--`，校验已占用）；`UndoLogManager.onCommit(Transaction)`：有 undoContext 则 release slot（insert undo 提交即可复用，对齐 `trx_undo_insert_cleanup`）。
- **commit 编排（不改 public 自动行为）**：`TransactionManager.commit()` 仍为纯内存状态、**不**自动调 onCommit；提交编排（当前 test-wired，未来 DML facade）约定调 `undoMgr.onCommit(txn)` 作为 commit 流程一部分（与 `txnMgr.commit(txn)` 无状态依赖，二者顺序不敏感）。
- 值对象：`storage.btree` 加 `BTreeDeleteResult(boolean removed)`（对齐 `BTreeInsertResult` 包位置）；`storage.trx` 加 `RollbackSummary(int undoRecordsApplied)`。**不放 domain**（domain 只放跨模块通用值对象）。

不做（→ 后续片）：
- UPDATE/DELETE undo、savepoint/statement rollback、history list/purge、MVCC 旧版本/ReadView。
- btree merge / 空 leaf 回收 / node-pointer 删除维护；undo record/page/segment 物理回收与 undo tablespace truncation。
- 恢复期 rollback（crash 后未提交事务）、多 rseg/多 undo 空间、多索引/二级索引删除、生产 DML facade（orchestration 仍 test-wired）。

## 2. 关键决策
1. **首次消费 undo**：物理删除走 `deleteMark`+`purge` 复用已测 record 算子，不新写物理 remove 路径。
2. **依赖方向 trx→btree**（设计 §94）：RollbackService 在 trx 持 rollback 语义并直调 btree `deleteClustered`；btree 仍不 import trx/undo，无环。
3. **状态机两阶段**：走链处于真正的 `ROLLING_BACK`（忠实 §7.6），TransactionManager 仅加两个包内方法，无重复状态逻辑。
4. **所有权校验 + 幂等**：`deleteClustered` 用 `dbTrxId`+`dbRollPtr` 确认是本 undo 插入的行才删，未命中/不匹配/已标记均安全收敛；MTR 无 content undo 的失败插入由 full rollback 幂等清理（不建 statement-id/savepoint 机制）。
5. **每条 undo 独立 MTR**（§7.6 step 6）：大事务可分批、可恢复；复用 D3/D4 物理 redo，不新增 redo 类型/恢复编排。失败只回滚当前 MTR，不污染已撤销部分。
6. **slot 双路径回收**：commit（`UndoLogManager.onCommit`）与 rollback（RollbackService 成功收尾）都 release 内存 slot；undo 页不回收（无 purge/truncation，已知缺口）。

## 3. 验收测试
- `BTreeDeleteClusteredTest`：匹配命中删除后 lookup 空；未命中 `removed=false` 不抛（幂等）；**所有权不匹配**（dbTrxId/dbRollPtr 不符）`removed=false` 且记录仍在；**已 delete-marked 命中**直接 purge（`removed=true`、lookup 空、不抛）；level-1 跨 leaf 删除；删后空 leaf 不影响其余 key 查询。
- `RollbackSegmentSlotManagerTest`（+）：release 后槽可重认领、release 未占用抛领域异常、activeCount 回落。
- `RollbackServiceTest`：单行 insert→rollback→lookup 空 + slot 已释放 + 状态 ROLLED_BACK + summary=1；多行 insert→rollback 反向走链全删（undoNo 递减消费）；**orphan undo 幂等**（undo 链在但对应行已不在 → rollback 不抛、slot 释放、summary 计数走满链）；ctx=null（只读/未写）rollback 仅翻状态、不动 slot。失败路径不变量（失败保留 ROLLING_BACK + slot 不释放）由 release/finishRollback 置于 while 之后的代码结构保证；若能廉价构造（手工 append `UPDATE_ROW` undo 触发 `applyUndoRecord` 抛错）补一条断言测试。
- `UndoLogManagerTest`（+）：`onCommit` 后 slot 释放、可被新事务重认领。
- 回归：全量 Gradle `test` 不倒退（当前 515）；非聚簇/insert 路径不受影响；既有 `TransactionManager` rollback 测试行为不变。

## 4. current map 更新（实现后）
- **Record/B+Tree slice**：新增 `deleteClustered`（findEqual→所有权校验→delete-mark+purge）当前数据链；`RecordPageDeleter`/`RecordPagePurger` 由「test-only」改为「经 `deleteClustered`（test-wired）被调用」；标注无 merge/node-pointer 维护/空页回收。
- **Transaction/Undo slice**：新增 `RollbackService`、`TransactionManager.begin/finishRollback`、`RollbackSegmentSlotManager.release`、`UndoLogManager.onCommit`；缺口表：「rollback 不消费 undo」→已接、「无 slot 回收」→commit+rollback 已接。
- **缺口校正**：current map 现把 `UndoRecordType.UPDATE_ROW/DELETE_MARK` 标为 T1.3d/e，本片明确只做 INSERT rollback，须改为 **T1.3e+ / future**；新增缺口：undo 页/段不回收、btree 无 merge/空页回收、无恢复期 rollback、无多索引解析、无生产 DML facade（commit/rollback orchestration 仍 test-wired）。
- **Reserved / Unwired**：`RollbackService` test-only（`new RollbackService` = 0 prod matches）；trx→btree 新边记入依赖小节。
