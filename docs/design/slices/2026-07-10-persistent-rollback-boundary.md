# Slice: Persistent logical undo head / rollback boundary v1

- 日期：2026-07-10
- 关联设计：`innodb-undo-log-purge-design.md` §5.8/§7.6（部分回滚与持久逻辑标记）、§8（purge）；`innodb-crash-recovery-design.md`（未提交事务回滚）。
- 前置：T1.3b 持久 undo segment、T1.3d/e/f 真实 rollback、R1.2 formal undo rollback、1.4 DML statement guard。
- 定位：让部分回滚后的逻辑 undo 链头跨 crash/reopen 生效，避免 recovery/purge 再消费物理仍存在、但已被回滚的分支。

## 1. 范围

做：
- 在 undo log first-page header 持久化成对的 `logicalLastUndoNo + RollPointer`；空头必须同时编码 `UndoNo.NONE + RollPointer.NULL`。
- 新建 undo 页写显式格式版本；本片扩展 log header 并移动 record area，遇到旧版/未知版本 fail-closed，不做隐式迁移。
- append 在同一 MTR 内写 record、物理 `LOG_LAST_UNDO_NO` 高水位和新的持久逻辑头，三者具有同一提交结果。
- partial rollback 先完成全部反向操作，再以独立短写 MTR 把持久逻辑头退到 savepoint/空边界；持久成功后才更新内存 `UndoContext`。
- rollback marker 写失败时不移动内存逻辑头；statement guard 沿用 rollback-only 保护，禁止提交结果不确定的事务。
- recovery rollback 与 purge 从持久逻辑头开始沿 `prevRollPointer` 反向遍历，只消费当前逻辑链，不再按物理槽扫描整段。
- 逻辑链遍历每次只在短只读 MTR 中读取一条 record，进入 B+Tree 反向操作前释放 undo page latch/fix。

不做：
- 不实现 SQL/session 自动 statement 生命周期、命名 `SAVEPOINT` 或 savepoint 后行锁精细释放。
- 不回收已回滚分支占用的物理 undo 槽；空间仍随整段 purge/truncate 回收。
- 不持久化 full rollback 的逐条进度；marker 未提交前 crash 允许 recovery 幂等重做已完成的反向操作。
- 不兼容或在线迁移本项目旧版 undo 页；测试/教学数据需重建，未来迁移另立切片。
- 不扩展多 rollback segment、多索引 purge 或 prepared/XA transaction recovery。

## 2. 关键决策

1. `UndoLogicalHead` 是不可变值对象并集中校验 pair invariant，避免 undoNo 与 pointer 半更新或一空一非空。
2. `lastUndoNo` 继续表达不可回退的 append 高水位；`logicalLastUndoNo/lastRollPointer` 表达可回退、可持久化的当前链头。
3. 持久头字段复用 `UNDO_LOG_HEADER_FIELD` metadata delta，不新增 redo record 类型；redo replay 必须可恢复完整 pair。
4. 部分回滚的数据修改与 marker 不在同一 MTR：先逆操作、后 marker，保证 marker 永不领先；marker 落后只会触发可重复的幂等 undo。
5. rollback 后的新 append 使用已回退的内存链头作为 `prevRollPointer`，并原子覆盖持久头，使被抛弃分支不会重新接回。
6. recovery/purge 对持久头、undoNo 单调下降、segment 归属和 record/pointer 类型做快速失败校验，损坏链不得降级为物理扫描。

## 3. 验收测试

- 页格式：新版本 header 初始空头、pair 编解码、非法 pair/未知版本拒绝，record area 新边界一致。
- append/reopen：跨 store/pool reopen 仍读到最新持久头；record、高水位、逻辑头由同一 MTR redo 恢复。
- partial rollback/reopen：savepoint 与首写前空边界均持久；marker 写失败时内存头不提前移动且事务不可提交。
- rollback→新写→reopen：新 record 的 predecessor 指向回退边界，物理旧分支不进入当前逻辑链，undoNo 不复用。
- crash recovery：marker 已持久时只回滚逻辑链；逆操作完成但 marker 未持久时可安全重复，最终结果一致。
- purge：已提交段只遍历持久逻辑链，rolled-back `DELETE_MARK/UPDATE` 分支不产生 purge 动作。
- 资源边界：逻辑链超过 Buffer Pool 容量仍可 rollback/recovery/purge，且 undo latch 不跨 B+Tree 修改持有。
- 回归：相关 undo/rollback/recovery/purge 测试与全量 Gradle `test` 通过，测试数不倒退。

## 4. current map 更新（实现后）

- Undo/Purge/Recovery 真实链路改为“first-page persistent logical head → prevRollPointer short-MTR traversal”。
- `UndoPageLayout` 记录新格式版本、兼容性限制和 record area；Reserved/Unwired 表按真实生产接线复核。
- `storage-backlog.md` 从 1.4 剩余项删除 persistent rolled-back marker，仅保留上层 statement/SAVEPOINT 与锁 scope 等缺口。
- 记录五遍复核结论：语义/格式、WAL/crash、资源边界、生产调用链/current map、测试/静态扫描。
