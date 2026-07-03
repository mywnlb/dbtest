# Slice: server.lockobs row-lock 观测快照（2.8a）

依据：`mysql-lock-observability-deadlock-design.md`、`innodb-btree-design.md`、`current-implementation-map.md`。
前置：0.17 `LockManager` 与 2.7a/2.7b B+Tree point/unique/range current-read 已实现。

## 目标

- 新增 `cn.zhangyis.db.server.lockobs` 第一阶段观测层。
- 定义 lockobs API、thread/event id、wait slot、snapshot row、deadlock report。
- 从 `LockManager.snapshot()` 生成 Performance Schema 风格 `data_locks` 与 `data_lock_waits` 当前快照。
- 为 row-lock 请求记录 request id、engine lock id、thread id、event id、transaction id。
- 保存最近 row-lock deadlock report，供诊断查看 victim 与等待边。
- `StorageEngine` 持有共享 lockobs 服务，并提供只读诊断快照入口。

## 关键决策

- `LockManager` 仍是 row-lock 真相来源；lockobs 只接收不可变事件和只读 snapshot。
- `LockManager` 增加可选 `LockObservationService`，旧构造器走 no-op，生产 `StorageEngine` 注入 `DefaultLockObservationService`。
- `GrantedLockSnapshot` / `WaitingLockSnapshot` / `WaitForEdgeSnapshot` 增加 request id 与 thread/event id。
- `data_lock_waits` 使用 wait edge 中的 requesting/blocking request id 连接 `data_locks`，不反向扫描 LockManager 内部队列。
- session/statement 尚未生产接线，wait slot 中 `sessionId/statementId` 第一阶段为 0。
- DD 未接，`OBJECT_SCHEMA/OBJECT_NAME` 为空，`INDEX_NAME` 暂用 `index#<indexId>`。
- `LOCK_DATA` 只作诊断摘要，不作为应用协议或测试稳定字符串结构。
- row-lock deadlock victim 策略仍沿用 LockManager 当前等待请求为 victim 的简化。

## 非目标

- 不实现 SQL 可查询 Performance Schema / Information Schema 表。
- 不实现 `metadata_locks`、MDL graph、MDL victim callback。
- 不接 Buffer Pool latch、file latch、mutex、condition 等物理等待采集。
- 不实现 executor/session DML、`SELECT ... FOR UPDATE` SQL 层入口。
- 不实现 `TransactionManager.commit/rollback` 自动释放 row lock。
- 不改变 B+Tree current-read 的等待前释放 latch、授锁后重定位语义。

## 验收测试

- `LockObservationServiceTest` 覆盖 granted/waiting `data_locks`、一对一和一对多 `data_lock_waits`。
- wait slot 在 row-lock 等待中可见，在 grant、timeout、deadlock victim 后清理。
- deadlock victim 被记录到最近 deadlock report，报告包含 victim 与等待边。
- timeout 后 waiting row、wait edge、wait slot 均不残留。
- `StorageEngineTest.engineExposesLockDiagnosticSnapshotFromSharedLockManager` 验证 engine 注入真实 observer，event id 非 0。

## current map 更新要求

- Transaction 小节标明 `server.lockobs` 已生产接线 row-lock snapshot，不参与授锁/rollback。
- Known gaps 改为：MDL、物理 latch/condition、SQL 视图、DML facade、事务自动 release 仍未接。
- Reserved/Unwired 表中不再把 server.lockobs row-lock adapter 标为未接；后续保留物理等待与 SQL view 缺口。
