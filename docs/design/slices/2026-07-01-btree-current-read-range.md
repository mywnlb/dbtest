# Slice: B+Tree current-read 范围锁定读（2.7b）

依据：`innodb-btree-design.md` §5.4、§7.2、§7.3、§15.2；
`innodb-transaction-mvcc-design.md` §8.3、§10；
`mysql-lock-observability-deadlock-design.md` §15.1。
前置：0.17 `LockManager` 与 2.7a point/unique current-read 已实现。

## 目标

- 在 `BTreeCurrentReadService` 增加 range current-read 入口。
- 复用短 MTR 定位，返回不可变 range position，不泄露 `RecordCursor`、page latch 或 buffer fix。
- 等事务锁前释放所有 page latch/fix；授锁后重新扫描 range 并校验锁落点。
- RC 范围锁定读只锁返回记录的 `REC_S/REC_X`。
- RR 范围锁定读锁返回记录的 `NEXT_KEY_S/NEXT_KEY_X`，并锁终止 gap。
- 任一尝试发生 timeout/deadlock/重定位变化/重定位异常时，释放本次尝试已授予的锁。

## 关键决策

- 不改 legacy `scan` 语义；新增 `lockRange` 作为 current-read 专用入口。
- 本片采用批量 range 结果，不实现长期游标或逐行 fetch。
- `BTreeScanRange.limit` 仍是硬上限；测试使用足够大 limit 覆盖完整范围。
- RR 终止 gap 用当前页级 gap 表达，可能比 SQL 谓词略宽，后续可用显式 global gap ref 收窄。
- 成功授予的锁仍不自动 close，由事务/DML facade 后续调用 `releaseAll(TransactionId)`。
- delete-marked 记录仍按 legacy scan 过滤；MVCC 逻辑可见性与唯一性留后续片。

## 非目标

- 不实现 executor/session `SELECT ... FOR UPDATE/FOR SHARE` 接线。
- 不实现 UPDATE/DELETE DML facade 或事务结束自动释放锁。
- 不实现 NOWAIT、SKIP LOCKED、SERIALIZABLE 普通 SELECT。
- 不重写 B+Tree read-path crab/SX latch，也不修改 redo/undo/page 格式。
- 不实现 global gap 边界值对象或非唯一索引 duplicate gap 精确锁。

## 验收测试

- RR range `FOR_UPDATE` 对返回记录持 next-key X，并对终止 gap 持 gap X。
- RR 空范围返回 empty，但持目标 gap 锁并阻塞同 gap insert intention。
- RC range `FOR_UPDATE` 只持返回记录 X，不持 next-key/gap 锁。
- range 等待 next-key/gap 冲突时不持 page latch/buffer fix。
- 等待期间 range 结果变化后，释放 stale locks，重扫并返回新结果。
- range 锁等待 timeout/deadlock 时释放本次尝试已授予的前序锁。

## current map 更新要求

- B+Tree 小节把 2.7b 标为 range current-read 已接入 `BTreeCurrentReadService`。
- Transaction 缺口改为：LockManager 已接 point/unique/range current-read，但 DML facade 与事务自动 release 未接。
- Known gaps 保留 SQL/session 接线、MVCC 逻辑唯一、global gap 精确化、server.lockobs adapter。
- 完成后按源码调用链复核，不能把 executor/DML 或事务生命周期释放写成已实现。
