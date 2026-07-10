# Slice: Persistent full rollback progress v1

- 日期：2026-07-10
- 关联设计：`innodb-undo-log-purge-design.md` §7.6/§14.4/§14.5；`innodb-crash-recovery-design.md` §7.6。
- 前置：1.4b first-page persistent logical head、live/recovery real rollback、幂等聚簇 inverse。
- 定位：让完整回滚按 undo record 持久推进，避免每次崩溃都从整条逻辑链头重做。

## 1. 范围

做：
- live full rollback 与 recovery rollback 每成功提交一条 inverse 后，用独立短 MTR 后退持久 `UndoLogicalHead`。
- 当前 record 与前驱分别用短只读 MTR 物化，进入 B+Tree inverse 前释放全部 undo latch/fix。
- 前驱 undoNo 从真实 `prevRollPointer` record 读取；支持 savepoint 后新写产生的 `3 -> 1` 非连续链。
- marker 复用 `UndoLogSegment.updateLogicalHead` 的 CAS 与 `UNDO_LOG_HEADER_FIELD` after-image，不新增页格式或 redo 类型。
- live marker commit 后才发布 `UndoContext` 逻辑头；物理 `lastUndoNo` 高水位不回退。
- 到 EMPTY 后才写 rollback-complete diagnostic redo、释放内存 slot、完成 `ROLLED_BACK`。
- 增加包内 fault injector，只在 inverse/marker MTR 成功提交后模拟 crash。

不做：
- 不持久化 page3 slot release，不解决 purge/history/drop 的跨 crash 原子边界。
- 不实现 PREPARED/RECOVERED_ACTIVE、正式 transaction recovery table、多索引或 DD。
- 不为每条 marker 单独 fsync；继续服从现有 redo、pageLSN、WAL 与 recovery flush。
- 不修复全局 MTR COMMITTING 异常语义；该异常仍按 fail-stop、重启恢复处理。
- 不改变 partial rollback、命名 SAVEPOINT、savepoint 后行锁释放或 undo page format。

## 2. 关键决策

1. 顺序固定为：读 current/前驱 -> inverse commit -> marker commit -> live context publish。
2. marker 永不领先 inverse；marker 落后时只会幂等重复最后一条 inverse。
3. predecessor 在 inverse 前验证，损坏 pointer、错误事务/索引或非严格下降不得留下新的聚簇修改。
4. 事务层解释 rollback 链，undo 层只执行调用方给出的物理 logical-head CAS；不新增公开 undo API。
5. fault hook 位于成功 commit 之间，不伪造 MTR 内部提交失败或改变生产控制流。
6. `RollbackSummary` 只统计本次调用实际执行的 inverse，不跨失败调用累计。

## 3. 验收测试

- live：inverse 后 crash 保持旧头；marker 后 crash 从新头继续；EMPTY marker 后重试只做终态收尾。
- recovery：中途 marker 后重新构造 service 只消费剩余链；旧头场景允许幂等重复当前 record。
- 分支：`rp3(undoNo=3) -> rp1(undoNo=1)` 直接后退到 1，不复活 detached rp2。
- 错误：stale CAS、损坏前驱、错误事务/索引、非严格下降均 fail-closed，非终态资源不释放。
- 资源：4-frame Buffer Pool 可处理超过容量的多页 undo 链，undo latch 不跨 index inverse。
- 恢复：ACTIVE 多记录事务恢复后再次重启，结果仍稳定且 persistent head 为 EMPTY。
- 回归：partial rollback、statement guard、purge、engine recovery 与全量 Gradle tests 通过，测试数不倒退。

## 4. current map 更新

- live/recovery rollback 链路改为 per-record persistent logical-head progress。
- Known Gaps 删除 full rollback 逐条进度缺口，保留 persistent slot release 与正式 trx recovery table。
- `storage-backlog.md` 同步 1.4 状态和下一推荐项，不修改全局目标架构图。
