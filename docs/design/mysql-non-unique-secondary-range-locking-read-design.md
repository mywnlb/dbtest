# Non-Unique Secondary Range / Locking Read Design

## 1. 目标与依据

本设计收束 SQL 读主线中“完整 non-unique secondary logical key 等值，映射为物理 B+Tree prefix range”的
端到端能力。依据：

- `mysql-parser-binder-design.md`
- `mysql-sql-executor-storage-api-design.md`
- `innodb-secondary-index-mvcc-purge-design.md`
- `innodb-transaction-mvcc-design.md`
- `innodb-btree-design.md`
- `slices/2026-07-01-btree-current-read-range.md`

目标链路：

```text
Session
  -> Parser: SELECT ... WHERE indexed_col = literal [FOR SHARE | FOR UPDATE]
  -> Binder: point primary/unique 或 non-unique logical-prefix range
  -> Executor
  -> SqlStorageGateway
  -> SecondaryMvccReader / SecondaryCurrentReadService
  -> secondary prefix scan -> clustered MVCC/current read -> LOB hydrate -> projection
```

这里的 range 是一个真实的物理范围：二级完整 physical key 为
`logical secondary key + clustered key suffix`。因此同一 logical key 的多行在树中形成连续 prefix range，
即使 SQL 表面仍是等值谓词，也不能按 unique point lookup 处理。

## 2. 本次范围

- 完整、无 prefix、单 key-part 的普通 non-unique secondary 等值查询，返回 0..N 行。
- 普通一致性读与 `FOR SHARE`、`FOR UPDATE`。
- RC/RR ReadView 生命周期、secondary candidate 回聚簇版本链、可见完整行重算谓词、聚簇身份去重。
- locking read 使用当前版本，不读取历史版本；logical-prefix 锁等待后重新扫描，再逐行取得聚簇 record lock。
- external LOB hydration 与公开投影仍在同一 ReadView 或 current-read 锁保护范围内完成。
- autocommit 与显式事务终态统一释放 logical-prefix/clustered 锁。
- DML 发布、换 key、DELETE 标记 secondary entry 前取得同一 logical-prefix X 锁，闭合读写冲突。

## 3. 明确非目标

- `<`、`<=`、`>`、`>=`、`BETWEEN`、`OR`、任意 residual predicate 与 full table scan。
- composite/prefix/LOB/JSON secondary key；本版只接受单 key-part、`prefixBytes == 0`。
- range UPDATE/DELETE、主键更新、Halloween protection。
- ORDER BY、LIMIT、长期 cursor、流式结果协议；本版使用有界批量物化。
- NOWAIT、SKIP LOCKED、SERIALIZABLE、RU、optimizer、prepared statement、网络协议。
- 把现有 page-local `GapLockKey` 扩张为 global precise gap。logical-prefix 锁是本版的稳定教学抽象。

这些限制必须由 Binder fail-closed，不能由 Gateway 猜测或静默退化为全表扫描。

## 4. Parser 与 AST

`SelectStatementNode` 增加 `SelectLockingClause`：

- `NONE`
- `FOR_SHARE`
- `FOR_UPDATE`

语法只允许一个尾部 locking clause，且必须位于 WHERE 谓词之后、分号之前。现有 equality predicate AST 保持不变，
避免 UPDATE/DELETE 与既有 point SELECT 被迫迁移到尚未实现的通用表达式树。

Parser 接受 clause 不代表 Binder 必须接受所有组合。当前 Binder 只为 non-unique secondary range 发布 locking plan；
point locking、无索引形状或额外谓词均明确拒绝。

## 5. Binder 与稳定访问计划

新增 `BoundSecondaryRangeSelect`，携带：

- exact-version `TableDefinition`
- projection ordinals
- access index stable id
- 按 logical key-part 顺序转换后的 equality values
- `SelectLockMode`：`CONSISTENT` / `FOR_SHARE` / `FOR_UPDATE`

访问路径选择保持确定性：

1. `NONE` 下完整主键优先，其次完整无 prefix unique secondary，保持现有 point 行为。
2. point 路径均不匹配时，选择完整、无 prefix、单 key-part的 non-unique secondary。
3. 多个 non-unique 候选取最小 stable index id。
4. locking clause 当前只允许第 2 步的 range plan。
5. 任何额外谓词、缺列、重复谓词、prefix key 或 LOB/JSON key 都 fail-closed。

locking plan 以 `TableAccessIntent.WRITE` 固定 metadata；它不表示修改 DD，只表示语句会持事务行锁，不能进入
read-only autocommit transaction。

`column = NULL` 保留 SQL 三值语义：Binder 可完成类型转换，Gateway 直接返回空列表且不申请 logical-prefix 锁。

## 6. 一致性 range MVCC

`SecondaryMvccReader.readRange` 数据流：

1. 校验 exact-version table/index 归属、non-unique 属性与完整 logical key。
2. 在独立只读 MTR 中执行 including-deleted prefix scan，物化 secondary candidate 后释放全部页资源。
3. 对每个 candidate 提取 clustered key，并调用 `MvccReader` 选择同一 ReadView 下可见的完整行。
4. 从可见行重建 secondary logical key，用与 B+Tree 相同的 type/prefix/collation/order comparator 复核等值谓词。
5. 按 clustered key comparator 去重，保持首次有效 candidate 的物理索引顺序。
6. Gateway 在 ReadView 仍登记期间 hydrate 全部 LOB、投影全部行；RC 最后在 `finally` 注销 view。

delete-marked entry 只是导航候选，不直接决定行可见性。旧 ReadView 需要的旧 entry 在 purge low-water 越过前仍存在；
新 ReadView 则会在回聚簇后拒绝不再匹配的旧 key。

批量实现设置显式 candidate 上限，并多取一个候选检测溢出。超过上限抛领域异常，不允许截断结果冒充完整查询。
这是与 MySQL 流式 cursor 的明确简化，后续可替换为有界 fetch protocol。

## 7. Logical-Prefix 事务锁

现有 `SecondaryUniqueKeyLockKey` 泛化为 `SecondaryLogicalKeyLockKey`。identity 为：

```text
index stable id + SecondaryLogicalKeyLockTokenFactory(type/prefix/collation normalized values)
```

允许 `REC_S` / `REC_X`：

- consistent read：不申请。
- `FOR SHARE`：申请 logical-prefix `REC_S`。
- `FOR UPDATE`：申请 logical-prefix `REC_X`。
- INSERT、secondary key UPDATE 的新/旧 key、DELETE 的旧 key：申请 logical-prefix `REC_X`。
- SQL NULL equality：不申请，因为谓词结果为空；DML 的 NULL key 保持既有“允许多个 NULL”语义。

同 identity 上 S/S 兼容，S/X 与 X/X 冲突。锁进入既有分片锁表、wait-for graph、timeout/deadlock detection，并由事务
commit/rollback `releaseAll` 收尾。它比物理 next-key 更粗，但不会依赖 page/heapNo，也不会因 split/merge 失效。

这是一项显式教学简化：对 exact logical prefix，它提供正确的无幻读和读写串行化；代价是同 prefix 的并发插入彼此
串行。后续实现任意有界比较范围时，再引入 comparator-aware global range key，不能复用 raw `SearchKey.equals` 猜测重叠。

## 8. Locking Current Read

`SecondaryCurrentReadService.readRange` 数据流：

1. 为事务分配真实 write id，构造带 deadline 的 current-read request。
2. 在不持 page latch/MTR 时申请 logical-prefix S/X。
3. 授锁后用 including-deleted prefix scan 重新物化候选。
4. 逐 candidate 提取 clustered key，调用 `BTreeCurrentReadService.lockPoint` 取得 `REC_S/REC_X`，等待后由该服务重定位。
5. 从授锁后的 clustered current row 重算 logical key；不匹配、已删除或重复 clustered identity 均过滤。
6. Gateway hydrate/投影后返回；锁保持到事务终态。

锁顺序是 logical-prefix → clustered record。DML 的既有顺序可能先取得 clustered X 再申请 logical-prefix X，因此环由
既有 deadlock detector 选择当前等待者为 victim；任何路径都不得在锁等待时持 secondary/clustered/undo page latch。

## 9. Session 与事务语义

- 普通 SELECT 继续调用 `prepareData(true)`；autocommit 可创建只读事务。
- 带 locking clause 的 SELECT 调用 `prepareData(false)`；read-only handle 不得执行锁定读。
- autocommit locking SELECT 在结果完整构造后 commit，随即释放锁。
- 显式事务中的锁定读保留锁到 COMMIT/ROLLBACK。
- 语句失败沿既有策略：autocommit full rollback；显式事务进入 rollback-only。
- statement absolute deadline 同时覆盖 handle wait、logical-prefix wait、clustered record wait 与 LOB hydration。

## 10. 验收

- Parser/Binder：clause、确定性 access path、READ/WRITE MDL、unsupported shape、NULL。
- MVCC：同 key 多行、update/delete 历史、old/new ReadView、collation、去重、candidate overflow。
- LockManager：logical-prefix S/S、S/X、X/X、timeout、deadlock snapshot。
- DML 协作：locking reader 阻塞同 prefix insert/update/delete；等待时可取得 page latch；终态释放。
- Gateway/Executor：0..N 结果、投影顺序、LOB hydration、RC view close、deadline。
- Session E2E：autocommit `FOR SHARE`、显式 `FOR UPDATE`、COMMIT/ROLLBACK 后解除阻塞。
- 完整 Gradle 回归、`git diff --check`、源码调用链复核与 `current-implementation-map.md` 小节更新。
