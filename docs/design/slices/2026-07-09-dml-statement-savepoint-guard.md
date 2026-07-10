# Slice: DML statement guard / savepoint facade v1

- 日期：2026-07-09
- 关联设计：`innodb-undo-log-purge-design.md` §5.8/§7.6（statement/savepoint rollback），`mysql-sql-executor-storage-api-design.md` §9.4/§12（statement 失败清理）。
- 前置：1.4 storage 内部 savepoint rollback；2.1 单聚簇 DML facade；T1.3d/e/f INSERT/UPDATE/DELETE_MARK undo rollback。
- 定位：把已有 storage 内部 `RollbackService.createSavepoint/rollbackToSavepoint` 暴露到 DML facade 的显式 statement guard，先解决 storage API 调用者可在单语句失败时回退本语句修改。

## 1. 范围

做：
- `storage.api.dml` 新增显式 statement guard：调用方在语句开始调用 `ClusteredDmlService.beginStatement(txn, clusteredIndex)`。
- 若事务已有 `UndoContext`，guard 创建 storage 内部 savepoint；失败时先精确命中其 `RollPointer + UndoNo` 再回滚边界后的记录，成功关闭时释放该 guard savepoint。
- 若事务尚未首写，guard 持有 service/transaction 归属且一次性的 `EmptyUndoBoundary`；失败时把本语句首写后产生的全部当前逻辑 undo 反向应用，并把 `UndoContext` 逻辑链头退回空边界。
- rollback 前先逐 pointer 用短只读 MTR 精确预扫描到边界，再逐条短读并执行反向命令；边界断裂不得先修改部分记录再报错，也不得长持整条 undo 页链。
- partial rollback 后事务保持 `ACTIVE`，不释放 undo slot、ReadView 或事务级 row locks，不写 `TRX_STATE_DELTA(ROLLED_BACK)`。
- rollback 中途失败时 Guard 终止且事务标记 rollback-only：拒绝后续 DML/commit，但允许 full rollback 收尾。
- DML facade 继续只处理单聚簇索引，调用方仍显式传 `Transaction`、`BTreeIndex`、row/key。

不做：
- 不把 SQL/session/executor 的 statement 生命周期自动接上来；本片只提供 storage DML 显式 guard。
- 不实现 SQL `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` 语法和命名保存点。
- 不做 savepoint 后行锁精细释放；当前仍由事务 commit/full rollback `releaseAll` 收尾。
- 不做 rolled-back undo record 的持久 marker；崩溃恢复仍以现有 undo/rseg 状态为权威。
- 不修复 MTR content undo；本片通过 statement guard 回退已提交短 MTR 的事务 undo 语义修改。

## 2. 关键决策

1. guard 是 storage API 显式对象，而不是在每个 `insert/update/delete` 内部隐式包一层；这样未来 executor 可覆盖多行 statement，且不会把单行 DML 调用误等同完整 SQL statement。
2. 空边界不伪造 `TransactionSavepoint`，而由 `RollbackService` 在首写前铸造一次性 `EmptyUndoBoundary`；rollback/close 都校验 service+transaction ownership，禁止任意 ACTIVE 事务无令牌截断整条逻辑链。
3. 成功关闭 guard 只消费运行期 savepoint/空边界令牌，不改变 undo 链；失败 rollback 才移动 `UndoContext.logicalLastUndoNo/lastRollPointer`。
4. `UndoContext.lastUndoNo` 是 append 高水位，空边界或 savepoint rollback 都不倒回，防止后续 undoNo 复用。
5. savepoint 到达必须同时精确匹配 `RollPointer + UndoNo`；rollback 异常后 guard 进入失败终态并把事务标为 rollback-only，避免提交 outcome-uncertain 结果。
6. row lock scope 暂保持事务级，符合当前 `LockManager.releaseAll(txnId)` 已接状态；精细锁释放另列后续。

## 3. 验收测试

- DML facade：已有 undo context 时，guard 内 update+insert 失败 rollback 只撤销 guard 后的修改，事务保持 `ACTIVE`，guard 前记录仍存在。
- DML facade：首写前创建 guard，guard 内 insert 后 rollback 删除该插入，事务保持 `ACTIVE`，当前逻辑 undo 链为空但 append 高水位不复用。
- DML facade：guard 成功关闭不回滚修改，后续 full rollback 仍能撤销 guard 内成功写入。
- `RollbackService`/`UndoContext`：空边界 rollback 后保存点栈被清空，`lastRollPointer=NULL`，`logicalLastUndoNo=NONE`，`lastUndoNo` 保留高水位。
- 边界防护：断链 savepoint 不得因较小 undoNo 被误接受；空边界令牌不得在首写后创建或重复消费；guard rollback 失败后不得重试。
- 失败保护：带较新记录的断链必须在零数据修改时拒绝；rollback-only 事务不得继续写入或提交，但 full rollback 可完成。
- 资源边界：小 Buffer Pool 重开后，跨越池容量的多页 statement undo 仍可完成边界扫描和 rollback。
- 回归：相关 DML/rollback/undo 测试与全量 Gradle `test` 通过。

## 4. current map 更新（实现后）

- `storage.api.dml` 包状态加入 `DmlStatementGuard`，说明它是 production-held DML facade 返回的显式 statement 边界。
- Transaction known gap 从“DML facade 未接 savepoint / 首写前空边界未表达”更新为“storage DML 显式 guard 已接；SQL/session 自动 statement guard、锁 scope 精细释放、持久 marker 仍缺”。
- `storage-backlog.md` 的 1.4 剩余项同步缩小到 SQL/session SAVEPOINT/statement abort、锁精细释放、持久 rolled-back marker。
