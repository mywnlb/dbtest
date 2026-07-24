# Slice: SQL Calcite-Lite M4（Pull-based SELECT 执行树）

依据：`mysql-query-optimizer-design.md` §3/§7、
`mysql-sql-executor-storage-api-design.md` §3/§5/§7/§8，以及
`2026-07-23-calcite-lite-sql-planning.md` ADR。

## 目标

把 SELECT 的扁平物理计划与一次性 Data Port 读取改为：

`PhysicalProject -> PhysicalFilter -> PhysicalAccess
-> ProjectionNode -> FilterNode -> AccessNode -> SqlStorageCursor`。

Optimizer 只生成不可变物理树；Executor 为每次语句创建有状态 PlanNode tree，
通过 `open/advance/current/close` 拉取，并只在公开 `QueryResult` 边界物化。

## 关键决策

- M4 不增加 SQL 语法，只迁移现有 point、secondary-prefix、comparison/full-scan SELECT。
- `PhysicalQuery` 固定根形状为 `project(filter(access))`，完整 residual 只保存在 Filter。
- point/secondary key 的必要条件证明由 PhysicalQuery 跨算子构造校验闭合。
- PlanNode 显式维护 NEW、OPEN、EXHAUSTED、CLOSED，close 幂等且打开失败收口子资源。
- `current()` 返回 cursor-owned `SqlRowView`；下一次 advance 或 close 后旧视图必须失效。
- Filter 通过统一 `ExpressionEvaluator` 执行 SQL 三值逻辑与 AND/OR 短路。
- Projection 按需访问列；external LOB 只在投影或未来表达式真正取值时 hydrate。
- Storage cursor 只发布完整聚簇候选，不执行 residual 或公开 projection。
- comparison range 每批最多 256 个 physical candidate，并以完整 physical key 续扫。
- RC ReadView 与 opaque handle operation lease 由 cursor 持有到 close；RR view 归事务终态。
- locking cursor 等待前不持 page latch/fix，取得锁后仍按既有链路重定位。
- 公开 QueryResult 继续 eager，Filter 后最多 4096 行；DML 路径与原子语义不变。
- 旧扁平 SELECT 计划和 `selectPoint/selectRange` Data Port 双轨删除。

## 非目标

- 不实现 join、aggregate、sort、limit、union、subquery、memo 或 cost optimizer。
- 不把范围 UPDATE/DELETE 改成逐行流式 mutation，继续先物化 identity 防 Halloween。
- 不改变 parser/binder 语法、事务状态机、锁协议、MVCC 可见性或持久格式。
- 普通二级 prefix 底层 reader 本片仍可返回稳定列表；对 Executor 的边界必须是 cursor。

## 验收测试

- Optimizer/Compiler 测试逐层断言 Project、Filter 与三类 Access。
- 物理树拒绝不完整 point proof、LOB key、非法 range endpoint 与错配 metadata。
- PlanNode 测试覆盖合法生命周期、重复 close、旧 row view 失效及打开/拉取失败收口。
- 表达式测试覆盖 UNKNOWN、AND/OR 短路及 NULL test 不触发值 hydration。
- Executor 测试覆盖 point/range、多行、过滤、投影顺序、容量与 close failure。
- adapter 集成覆盖 RR/RC/RU、primary/secondary、locking read、LOB 与 deadline。
- 静态扫描确认旧扁平 SELECT 类型/方法零引用，SQL 层不泄露 storage internal 类型。
- 固定 JDK 25.0.2 + Gradle 9.5.1 全量测试通过且测试数不倒退。

## Current map 更新要求

- SQL flow 改为 PhysicalQuery -> PlanNode tree -> SqlStorageCursor。
- 标明公开 QueryResult eager、range physical batch、RC/RR view 与 cursor 资源所有权。
- secondary-prefix 底层稳定列表必须标为 partial，不能误画成真实分页 reader。
- 新生产类型逐项核对调用方；无生产调用者必须进入 Reserved / Unwired 表。
