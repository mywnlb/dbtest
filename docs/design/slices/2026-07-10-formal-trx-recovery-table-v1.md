# Slice: Formal transaction recovery table v1

- 日期：2026-07-10。
- 关联设计：`innodb-crash-recovery-design.md` §6.1/§7.1/§7.6/§8.5/§10；
  `innodb-transaction-mvcc-design.md` §5.1–5.3/§7/§12；`innodb-redo-log-design.md` §12–14/§20.5；
  `innodb-undo-log-purge-design.md` §5.1/§6.3/§14.5；`mysql-data-dictionary-ddl-design.md` §11/§15.4。
- 前置：`TRX_STATE_DELTA`、page3 owner CAS、atomic undo finalization、formal UNDO_ROLLBACK/RESUME_PURGE。
- 目标：跨 fuzzy checkpoint 与 redo ring 回收恢复事务高水位、终态证据和 recovered-active 集合。

## 1. 范围

做：
- 新增双槽 CRC `TransactionRecoveryCheckpointStore`，持久 `{checkpointLsn,nextTrxId,nextTrxNo}`。
- checkpoint 先短锁快照事务计数并 force sidecar，再持久 redo label，最后才推进 redo reclaim boundary。
- 新增 `CheckpointMetadataParticipant` 端口；默认 no-op，StorageEngine 注入事务基线参与者。
- 新增恢复线程独占的 `RecoveredTransactionTable`，合并 checkpoint 基线、顺序 redo delta 与 page3 slot。
- `TransactionStateRedoHandler` 经 `TransactionStateDeltaSink` 交付 delta，不反向依赖 trx/recovery 实现。
- `TransactionUndoRecoveryParticipant` 消费不可变 recovery snapshot，交叉校验 page3 后恢复 counter/history。
- ACTIVE page3 且无终态冲突时登记 `RECOVERED_ACTIVE`，完整 rollback 后写 recovery rollback delta。
- 识别稳定 `PREPARED` 状态码，但 v1 无 XA coordinator，NORMAL/FORCE 启动 fail-closed。

不做：
- 不持久化完整逐事务明细表，不修改 page3/undo page 布局，不新增事务系统页。
- 不实现 XA commit/rollback 决议、DD/tablespace discovery、多索引 rollback 或 DDL recovery。
- 不让 redo handler 执行事务状态机、undo rollback、MVCC 可见性或普通锁等待。
- `READ_ONLY_VALIDATE` 只诊断文件，不发布 recovery table、counter 或用户写流量。

## 2. 关键决策

1. sidecar 基线必须覆盖 redo checkpoint；更新于 redo label 之前，允许更“新”但禁止落后。
2. sidecar 路径由 `EngineConfig.redoControlFile()` 派生同目录固定文件名，不扩张配置 record 组件。
3. checkpoint 计数快照可保守偏大，绝不能偏小；高水位过估只跳号，不会复用旧事务标识。
4. checkpoint 后同一事务的精确重复 terminal delta 幂等；终态、commitNo 或 reason/state 冲突即 fatal。
5. COMMITTED page3 无 post-checkpoint delta 时，只有基线已覆盖 creator/commitNo 才接受其页上状态。
6. ACTIVE page3 与 COMMITTED/ROLLED_BACK/PREPARED redo 证据冲突；不得进入 rollback 或开放流量。
7. 非零 redo checkpoint 缺失/损坏 sidecar 无法证明纯 INSERT 高水位，旧教学实例明确拒绝并要求重建。
8. recovery rollback terminal delta 使用独立 reason，下一次 crash 可在 page3 已清后保留 transaction-id 证据。

## 3. 验收测试

- sidecar：双槽往返、torn/CRC 损坏回退、单调选择、落后 checkpoint 拒绝。
- checkpoint：sidecar force 失败时 redo label/reclaim 均不推进；成功顺序为 sidecar→label→reclaim。
- table：顺序 last-state、精确重复幂等、冲突终态/commitNo/reason、PREPARED 全部 fail-closed。
- cross-check：ACTIVE/COMMITTED page3 与 baseline/delta 的合法组合及矛盾组合。
- engine：纯 INSERT commit→checkpoint→旧 redo 回收→重启后新 id/no 不复用。
- recovery：ACTIVE rollback、COMMITTED history、rollback terminal delta、恢复中再次 crash 后幂等续作。
- 兼容：checkpoint=0 且 sidecar 缺失从初始高水位启动；非零 checkpoint 缺 sidecar 拒绝。
- 回归：固定 Gradle/JDK 全量测试通过，测试数不倒退，静态并发/异常规则通过。

## 4. 文档更新

- 更新 `current-implementation-map.md` 的 redo replay、checkpoint、transaction/recovery 数据链与缺口。
- `storage-backlog.md` 将 1.10 标为完成；下一优先级重新依据剩余 crash-safety 缺口排序。
- 不修改全局目标架构图，不生成持久 implementation plan 或 `docs/superpowers` 文档。
