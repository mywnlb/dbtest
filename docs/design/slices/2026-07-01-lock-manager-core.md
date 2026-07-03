# Slice: 最小 LockManager 内核（0.17）

依据：`storage-backlog.md` 0.17；`innodb-transaction-mvcc-design.md` §10、§19、§20；
`innodb-btree-design.md` §9.4、§10、§14、§15；`mysql-lock-observability-deadlock-design.md` §5.5、§7、§9.3。
当前实现以 `current-implementation-map.md` 为准：事务已有 `TransactionManager`、ReadView、MVCC、undo/rollback/purge，
但无 `LockManager`、record-lock、gap/next-key lock，B+Tree 也尚未接 current-read 锁等待与重定位。

## 目标

- 新增 `cn.zhangyis.db.storage.trx.lock`，实现 storage 内可单测闭环的事务锁内核。
- 支持 record lock、gap lock、next-key lock、insert intention lock。
- 支持 wait queue、等待 timeout、事务持锁集合、`releaseAll(TransactionId)`。
- 支持 row-lock wait-for graph 与 bounded deadlock detector。
- 提供轻量 snapshot/edge 查询口，供后续 `server.lockobs` 适配；本片不实现 Performance Schema 视图。

## 关键决策

- `LockManager` 是 row/table transaction lock 的真相来源；B+Tree 只在后续 current-read 片构造锁落点。
- 锁 owner 使用 `TransactionId`，锁内核维护 `TransactionId -> held locks`，不把锁集合塞进现有 `Transaction` 对象。
- 本片不修改 `TransactionManager.commit/rollback` 自动释放锁；后续 DML facade 接入时统一调用 `releaseAll`。
- 等待必须有 `Duration timeout`，使用 `ReentrantLock`/`Condition`，禁止 `synchronized`、`wait`、`notify`。
- 死锁检测只覆盖 row-lock graph；Buffer Pool page latch、MTR latch、redo wait、file lock 不进入检测图。
- victim 第一版只标记等待请求、移除等待边并唤醒抛 `DeadlockDetectedException`，不自动执行 rollback。
- 后续实现必须遵守 `AGENTS.md` 的 TDD 流程：先写/调整锁兼容、等待、timeout、死锁测试，再实现生产代码。

## 锁模型

- `REC_S` 与 `REC_S` 兼容；`REC_X` 与同 record 其它 record 锁冲突。
- `GAP_S`/`GAP_X` 表达 gap 保护，主要阻止插入，不阻止已有 record 的普通 record S 读。
- `INSERT_INTENTION` 在同 gap 多事务间兼容，但与覆盖该 gap 的 `GAP_S/GAP_X` / `NEXT_KEY_S/NEXT_KEY_X` 冲突。
- `NEXT_KEY_S` / `NEXT_KEY_X` 按 record lock + preceding gap lock 的组合语义判断兼容性。
- 锁 key 使用现有值对象：record 锁基于 `RecordRef(indexId,pageId,heapNo)`；gap 锁基于 `indexId + left SearchKey + right SearchKey`。

## 非目标

- 不实现 SQL/session/executor DML facade。
- 不实现 B+Tree current-read lock ref、等待后重定位、MVCC 逻辑唯一检查。
- 不实现 MDL、table DDL 排他锁、SERIALIZABLE 普通 SELECT 接线。
- 不实现完整 `server.lockobs` 包、`data_locks`、`data_lock_waits`、deadlock report repository。
- 不改 redo/undo/page 格式，不新增持久化锁状态。

## 验收测试

- 兼容矩阵：record S/X、gap、next-key、insert intention 覆盖 grant/conflict。
- 等待与释放：T1 持冲突锁，T2 等待；T1 `releaseAll` 后 T2 被授予。
- timeout 清理：等待超时后 wait queue 与 wait-for edge 均清理，不留下悬空 waiter。
- 死锁检测：两事务和三事务环均能选 victim，victim 抛 `DeadlockDetectedException`，非 victim 可继续等待或获锁。
- insert intention：同 gap 多插入意向兼容；被 gap/next-key X 阻塞后释放可唤醒。
- snapshot：能读取当前 granted locks 与 wait edges，且不暴露内部可变集合。

## current map 更新要求

- 实现完成后更新 Transaction 小节：`LockManager` core 已实现，current-read/DML 仍未接线。
- B+Tree 缺口保留：仍缺 current-read lock ref、等待前释放 page latch、等待后重定位。
- Reserved/Unwired 表若出现只由测试覆盖的 lock snapshot/observer 类型，必须标明保留理由和下一步接线。
- 完成后按 `current-implementation-map.md` 检查清单核对源码真实调用链，不能只按本 slice 判断已完成。
