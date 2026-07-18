# MiniMySQL XA / PREPARED Transaction Resolution v1

## 1. 目标

本切片在现有 `Transaction`、双 undo log、事务状态 redo、page3 恢复和
`RecoveryTrafficGate` 上实现存储引擎 resource-manager participant：

- 可写事务经过 phase one 后持久进入 `PREPARED`，prepare 返回前 redo 必须 fsync。
- live prepared transaction 保留 write id、active-table 身份和全部事务锁，只允许显式决议。
- 支持 live `COMMIT PREPARED` 与 `ROLLBACK PREPARED`。
- crash recovery 能从 redo + page3 + undo first-page 识别 prepared transaction，并在开放流量前消费上层决议。
- 未提供决议、证据冲突或决议执行失败时保持 recovery gate 关闭。

本切片不是完整 MySQL XA server：

- 不增加 `XA START/END/PREPARE/COMMIT/ROLLBACK/RECOVER` SQL grammar、协议包或 Session 分支注册表。
- 不在 storage 内重复持久化 XID；上层协调器拥有 XID→`TransactionId` 映射和最终决议。
- v1 只 prepare 已产生普通 INSERT/UPDATE undo 的写分支；无写分支由上层按 read-only/one-phase 完成。
- 不在开放流量后长期悬挂 recovered prepared transaction；v1 未重建其 record/gap locks，因此必须在 recovery gate
  关闭期间决议，`UNRESOLVED` 直接阻止 OPEN。

## 2. 权威身份与 API 边界

storage participant 的稳定分支身份是 `TransactionId`。完整 XID 仍由未来 SQL/server XA coordinator
管理；协调器必须在确认 prepare 成功前持久保存 XID 与 `TransactionId` 的映射，并在重启时通过
`PreparedTransactionDecisionProvider` 返回：

- `COMMIT`
- `ROLLBACK`
- `UNRESOLVED`

Provider 只能读取已经持久化的协调器状态，不能在持有 page latch、事务系统锁或 history transition lease
时阻塞等待远端。默认 provider 恒返回 `UNRESOLVED`，保持旧引擎遇到 prepared transaction 时 fail-closed。

live storage API 位于 `storage.api.trx`：

- `prepare(transaction, timeout)`：phase one，固定要求 redo fsync，不接受弱 durability policy。
- `commitPrepared(transaction, timeout)`：phase two commit，固定要求 terminal redo fsync。
- `rollbackPrepared(transaction, clusteredIndex, timeout)`：legacy 单聚簇兼容入口。
- `rollbackPrepared(transaction, timeout)`：DD exact-version resolver 入口，不向上暴露 B+Tree/page。

普通 `ClusteredDmlService.commit/rollback` 不接受 `PREPARED`，防止普通事务终态绕过 XA 决议入口。

## 3. 状态机

运行时状态增加：

```text
ACTIVE --prepare--> PREPARED --commit decision--> COMMITTING --> COMMITTED
                         \
                          --rollback decision--> PREPARED_ROLLING_BACK --> ROLLED_BACK
```

约束：

- `ACTIVE -> PREPARED` 只能在全部 undo first-page 与 prepare delta 已由同一 MTR 持久化后发布。
- `PREPARED` 不允许 DML、ReadView、savepoint、普通 commit 或普通 rollback。
- `PREPARED_ROLLING_BACK` 是显式重试态；失败重试仍知道 first-page 期望状态为 PREPARED，不能退化成普通
  `ROLLING_BACK`。
- prepared transaction 在 phase two 成功前一直留在 `ActiveTransactionTable`，因此 purge eligibility
  不会越过其 creator。
- prepare 时释放事务级 ReadView，但不移出 active table、不释放行锁。
- phase two 物理终态提交后才移出 active table并释放锁。

## 4. 持久格式

### 4.1 Undo first-page state

`UndoPageLayout.STATE` 已是一个独立 u8，本切片只追加稳定值：

| code | state |
| --- | --- |
| 0 | ACTIVE |
| 1 | COMMITTED |
| 2 | CACHED |
| 3 | FREE |
| 4 | PREPARED |

不修改 v3 header 长度、record area 或 `RollPointer`，因此不提升 undo page format version。旧文件的
0..3 语义保持不变，未知值继续 fail-closed。PREPARED 必须满足：

- creator transaction id 非 NONE；
- commit no 为 NONE；
- history prev/next 均为空；
- page3 active slot 仍指向该 first page；
- INSERT/UPDATE 两个 log 如果同时存在，必须在一个 prepare MTR 中一起改为 PREPARED。

### 4.2 Transaction-state redo

稳定状态 code 6 已保留给 PREPARED。本切片向 reason enum 末尾追加：

| code | reason | legal tuple |
| --- | --- | --- |
| 4 | PREPARE | ACTIVE → PREPARED, transactionNo=NONE |
| 5 | PREPARED_COMMIT | PREPARED → COMMITTED, transactionNo assigned |
| 6 | PREPARED_ROLLBACK | PREPARED → ROLLED_BACK, transactionNo=NONE |

既有 reason code 1..3 和所有普通 commit/rollback tuple 不变。Recovery table 把 PREPARED 当作可继续转换的
稳定态，而不是终态；COMMITTED/ROLLED_BACK 仍只接受完全相同 record 的幂等重放。

## 5. Live prepare 数据流

1. API 校验 recovery gate 为 OPEN、事务为可提交 ACTIVE 写事务、已经持有至少一个普通 undo log。
2. finalizer 分别用短只读 MTR 核对内存 slot、page3 owner、first-page creator/kind/ACTIVE 状态，返回前释放 latch。
3. 单个写 MTR 按 PageId 排序固定全部 first page，先全量复核，再把状态统一写成 PREPARED，并追加
   `ACTIVE -> PREPARED` logical redo。
4. MTR commit 后，内存事务发布 PREPARED并释放 ReadView；active membership 与事务锁保持不变。
5. 强制 `redo.flush()` 并带 timeout 等待 prepare LSN durable。超时或 fsync 失败时事务仍为 PREPARED，
   调用方只能执行显式 phase-two 决议；若进程崩溃则由 recovery provider 继续裁决。

崩溃窗口：

- 步骤 3 前崩溃：first page 仍 ACTIVE，按 recovered-active rollback。
- 步骤 3 后、prepare 应答前崩溃：恢复识别 PREPARED，协调器因尚未记录成功可安全选择 rollback。
- prepare 应答后崩溃：redo 已 durable，上层持久 XID 映射给出最终决议。

## 6. Phase-two commit

1. `PREPARED` 事务预留新的 `TransactionNo`，但仍保留 active membership。
2. UPDATE undo 以 PREPARED 为期望状态挂入持久 history，并转为 COMMITTED；INSERT undo 在同一 MTR
   drop segment + clear page3 slot。
3. 同一 MTR 追加 `PREPARED -> COMMITTED` redo。MTR commit 后才发布 history/slot 内存投影。
4. 内存状态经过 COMMITTING 到 COMMITTED，移出 active table并释放 ReadView。
5. 强制 terminal redo fsync；durable 后才释放全部事务锁。若 durability 确认失败，事务保持 COMMITTED
   且继续持锁，相同 commit 决议可幂等重试 durability/锁清理，不能改走 rollback。

简化点：prepared INSERT undo 在 phase two 一律 drop，不进入 cache/free reuse。这样 prepared commit
不需要把 PREPARED owner 暂时伪装成 ACTIVE；后续可在独立优化切片扩展 prepared→cache/free 原子转移。

## 7. Phase-two rollback

1. `PREPARED -> PREPARED_ROLLING_BACK`，active membership 和锁保持。
2. 复用现有双 logical-head 全局 undoNo 归并；每条 inverse、LOB free 和 persistent head marker 仍使用独立短 MTR。
3. 两条 logical head 都到 EMPTY 后，同一 finalization MTR drop 全部 prepared segment、clear 全部 page3 slot，
   并追加 `PREPARED -> ROLLED_BACK` redo。
4. MTR commit 后发布 slot 释放，再进入 ROLLED_BACK、移出 active table并释放事务锁。

实际 facade 在步骤 4 后还会强制 terminal redo fsync，成功后才释放锁；durability 确认失败时相同
ROLLED_BACK 决议可重试确认，不能改走 commit。

简化点同 prepared commit：prepared rollback 不把空 segment放入 cache/free。中途失败保留
`PREPARED_ROLLING_BACK`、first-page PREPARED 和锁，可从持久 logical head 幂等重试。

## 8. Crash recovery

恢复顺序仍为 doublewrite → redo → undo transaction resolution → purge：

1. `RecoveredTransactionTable` 接受并验证三种 prepared tuple。
2. page3 scan 把 first-page PREPARED 读为 `RecoveredUndoState.PREPARED`；同一 creator 最多各一条
   INSERT/UPDATE log，所有 log 必须同态。
3. reconciler 要求 PREPARED page3 与 post-checkpoint redo PREPARED 一致；若 prepare delta 已被 checkpoint
   回收，则要求 baseline next-transaction-id 覆盖 creator，并以 page3 + checksum-protected first-page 为权威证据。
4. persistent history 先重建到运行时投影；ACTIVE/PREPARED first page 都必须未链接 history。
5. 恢复 counters 与 slot owner后，按 `TransactionId` 升序查询 decision provider：
   - COMMIT：重建最小 prepared transaction/undo context，扫描 UPDATE logical chain 投影 affected tables，
     然后复用 prepared commit。
   - ROLLBACK：重建最小 prepared transaction/undo context，然后复用 prepared rollback。
   - UNRESOLVED：抛 `TransactionRecoveryException`，gate 保持关闭。
6. prepared 全部决议后再 rollback recovered ACTIVE、resume purge、flush/force恢复写，最后开放流量。

recovered transaction 只在 gate 关闭期间注册到 active table，phase two 结束立即移除；不进入普通 LockManager
等待图，也不伪造已经丢失的 live lock handles。

## 9. 并发、锁序与失败语义

- live Transaction 仍是单 owner；不支持同一 transaction 并发 prepare/commit/rollback。
- prepare 预检不持 page latch跨 MTR；最终批次只按 PageId 顺序固定 first page。
- phase two 不在 `TransactionSystem` 短锁、HistoryList Java lock或 row-lock table lock内执行 page/FSP IO。
- 事务锁等待发生在 DML 阶段；prepare/phase two 不新增无界等待。redo 与 history transition均有显式 timeout。
- first-page/page3/redo 任一证据冲突都 fail-closed，不能猜测决议。
- phase-two 物理 MTR 已提交但内存发布失败属于 fail-stop；重启依赖 redo/page3重新裁决。

## 10. 验收测试

- 状态机：合法 prepared 路径、普通 commit/rollback 拒绝 PREPARED、prepared rollback 重试态。
- 物理格式：ACTIVE→PREPARED、双 first-page 原子标记、unknown state拒绝、旧 0..3 兼容。
- redo table：prepare→commit、prepare→rollback、重复重放、非法 reason/transactionNo和终态冲突。
- reconciler：单/双 prepared slot、混合 ACTIVE/COMMITTED/PREPARED、baseline覆盖和 redo冲突。
- live API：prepare 强制 fsync、超时后保持 PREPARED与锁、commit/rollback后才释放锁。
- purge：prepared creator仍 active且 prepared undo不进 history/purge。
- restart：provider COMMIT、ROLLBACK、UNRESOLVED；双 log、affected-table history与二次崩溃幂等。
- 全量回归：固定 Java 25 / Gradle 9.5.1，测试数量不得倒退，并执行禁止 monitor/裸运行时异常/越层 import扫描。
