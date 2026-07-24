# Slice: SQL 二表 INNER JOIN v1

依据：`mysql-parser-binder-design.md`、`mysql-query-optimizer-design.md`、
`mysql-advanced-executor-operators-design.md` 与
`mysql-sql-executor-storage-api-design.md`。

## 目标

支持如下教学型二表连接：

`SELECT ... FROM left [AS] l [INNER] JOIN right [AS] r
 ON l.key = r.key WHERE ... [ORDER BY ...] [LIMIT ...]`。

Parser、Binder、逻辑计划、物理计划和 pull executor 必须形成真实生产接线；
同一语句的多个 storage cursor 共享 transaction operation lease、ReadView 和 deadline。

## 关键决策

- v1 只接受恰好两个输入、等值 `INNER JOIN`，不把 JOIN 降格为 WHERE 笛卡尔积。
- 列引用支持 `column` 与 `qualifier.column`；alias 存在时 qualifier 必须使用 alias。
- 未限定列只在唯一输入含有该列时成功；零命中与多命中分别报告 unknown/ambiguous。
- Binder 先规范化两个表名，再按 canonical table key 顺序获取 MDL/DD lease。
- Bound 列引用携带 relation ordinal、stable column id、local ordinal 与 exact DD type。
- ON 两侧必须是 exact-type scalar；v1 不做跨类型隐式转换或 collation 合并。
- LogicalJoin 独立保存 ON，LogicalFilter 独立保存 WHERE，Project 使用连接行的扁平 schema。
- outer 固定为 SQL 左表；右表存在可用单列完整索引时选择 Index Nested Loop Join。
- 右侧聚簇/唯一索引用 point probe，普通单列二级索引用 prefix probe。
- 无安全右侧索引时使用普通 Nested Loop Join，并为每个 outer row 重开右侧全扫 cursor。
- JoinNode 只依赖 PlanNode/row-view/data-port，不读取 DD repository、B+Tree 或 record 内部类型。
- joined row 在任一 child 前进后失效；Sort/公开 QueryResult 需要跨前进保存时立即物化。
- JOIN 暂不支持 locking clause；普通一致性读的全部 child cursor 共享一个 statement ReadView。
- SqlCursorScope 独占一个 transaction operation lease，可管理多个 child cursor，并统一关闭。
- RC ReadView 由 scope 关闭一次；RR ReadView 仍由事务终态关闭；RU 不创建 ReadView。
- 单表查询继续兼容原 cursor API，但生产 Executor 统一通过 statement cursor scope 执行。
- JOIN 的 ORDER BY 在扁平连接行上执行；小窗口 Top-N，其余使用既有分堆归并。
- 公开结果允许同名列，列顺序与投影顺序一致，行宽仍须严格匹配。

## 非目标

- 不实现 LEFT/RIGHT/FULL/CROSS JOIN、多于两表、USING/NATURAL JOIN。
- 不实现 Hash Join、Merge Join、join reorder、统计代价或 bushy tree。
- 不实现表达式投影、列别名、聚合、子查询、GROUP BY/HAVING 或 join DML。
- 不新增持久格式，不改变 MVCC 可见性、事务锁或 B+Tree 编码语义。

## 验收测试

- Parser 覆盖 INNER 可选词、AS/隐式 alias、限定投影/WHERE/ORDER BY 和 ON 等值。
- Binder 覆盖 unknown qualifier、ambiguous column、重复 alias、ON 类型不一致及 canonical lease。
- Optimizer 覆盖 point/prefix Index NLJ 与无索引普通 NLJ，WHERE residual 不丢失。
- JoinNode 覆盖 NULL key、零/多匹配、inner 重开、旧 row 失效和异常逆序关闭。
- Executor 覆盖 JOIN 后 filter、sort、limit、projection 与重复结果列名。
- Adapter 覆盖同一 RC scope 双 cursor、共享 ReadView、terminal 拒绝与幂等关闭。
- 固定 JDK 25.0.2、Gradle 9.5.1 全量测试通过且测试数不倒退。

## Current map 更新要求

- 把 SELECT 生产链扩展为二表 LogicalJoin、PhysicalNestedLoopJoin 和 NestedLoopJoinNode。
- 标明普通 NLJ 与 Index NLJ 的选择边界、右侧 probe 重开行为和扁平 joined schema。
- 标明 SqlCursorScope/ReadView/operation lease 的 owner、关闭顺序和单表兼容路径。
