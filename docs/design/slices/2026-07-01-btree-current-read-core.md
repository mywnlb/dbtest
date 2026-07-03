# Slice: B+Tree current-read 点读与 unique 检查（2.7a）

依据：`innodb-btree-design.md` §9.4、§10、§14、§15；
`innodb-transaction-mvcc-design.md` §10、§17、§19；
`mysql-lock-observability-deadlock-design.md` §5.5、§7、§9.3。
前置：0.17 `storage.trx.lock.LockManager` 已实现 record/gap/next-key/insert-intention、wait queue、timeout、deadlock snapshot。

## 目标

- 新增 B+Tree current-read 点查协调器，让 B+Tree 开始使用 `LockManager`。
- 定位 record/gap 后构造 `RecordLockKey`、`GapLockKey`、`NextKeyLockKey`、`InsertIntentionLockKey`。
- 等事务锁前释放 page latch、RecordCursor、buffer fix；授锁后重新定位并校验锁落点。
- 支持 point `FOR_SHARE` / `FOR_UPDATE` 与 unique insert 物理重复检查。
- 由 `StorageEngine` 持有共享 `LockManager` 和 `BTreeCurrentReadService`，但不接 SQL/session DML。

## 关键决策

- 不改 legacy `lookup/scan/insert` 语义；新增 `BTreeCurrentReadService` 作为独立入口。
- current-read 服务内部开启短 MTR；调用方不能在同线程已有 active MTR 时调用。
- 成功授予的事务锁不自动 close，后续由事务/DML facade 调 `LockManager.releaseAll(TransactionId)`。
- RC point miss 不加 gap lock；RR point miss 按模式加 `GAP_S/GAP_X`。
- existing point：`FOR_SHARE` -> `REC_S`，`FOR_UPDATE` -> `REC_X`。
- unique check：命中物理 duplicate 先加 `REC_S` 并重定位确认；miss 加 `INSERT_INTENTION` 到目标 gap。
- delete-marked duplicate 仍算物理重复；MVCC 逻辑唯一留后续片。
- 重定位失败会释放 stale lock 并重试，超过预算抛 `BTreeCurrentReadRelocationException`。

## 非目标

- 不实现 RR range scan 的 next-key/gap 全覆盖。
- 不实现 executor/session `SELECT ... FOR UPDATE`、UPDATE/DELETE/INSERT facade。
- 不实现 TransactionManager commit/rollback 自动释放锁。
- 不实现 SERIALIZABLE 普通 SELECT、MVCC 逻辑唯一、server.lockobs adapter。
- 不修改 redo/undo/page 格式。

## 验收测试

- point FOR UPDATE 等待 record lock 时不持 page latch/buffer fix。
- 等待期间 root split/heapNo 改变后，授锁方释放 stale lock 并重新锁新 `RecordLockKey`。
- RC point miss 不持 gap lock；RR point miss 持 gap lock。
- unique insert miss 被 gap lock 阻塞，释放后获得 insert-intention 并返回 available。
- unique insert duplicate 被 record lock 阻塞，释放后返回 duplicate。
- `LockWaitTimeoutException` / `DeadlockDetectedException` 由 `LockManager` 原样传播。
- 授锁后若 B+Tree 重定位失败，释放刚授予的事务锁，避免 statement 失败后泄漏。

## current map 更新要求

- B+Tree 小节标明 `BTreeCurrentReadService` 已 production-held by `StorageEngine`。
- Transaction 缺口更新为：LockManager 已接 B+Tree point current-read，但 DML facade/自动 release/range scan 未接。
- Reserved/Unwired 表保留 server.lockobs adapter、range current-read 和 DML facade 缺口。
- 完成后按源码调用链复核，不能把 range scan 或 SQL 层标成已实现。
