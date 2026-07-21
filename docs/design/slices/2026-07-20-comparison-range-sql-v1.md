# Comparison / Composite Range SQL v1

## 目标

- 把 SQL WHERE 从 equality-only 扩为 AND 连接的 `=,<,<=,>,>=,BETWEEN`。
- 支持聚簇/二级复合索引的连续前缀 range，以及无可用索引时的聚簇 full scan。
- 支持 point/range `FOR SHARE`、`FOR UPDATE` 与 range UPDATE/DELETE。
- 保持 existing point/non-unique equality 行为、MVCC/LOB 生命周期与锁等待边界。

## 关键决策

- Parser 以 sealed `PredicateNode` 表达 comparison/between，不把 SQL operator 塞入 storage。
- Binder 对同列约束求交；NULL comparison 为 UNKNOWN；矛盾约束产生 empty plan。
- access path 顺序：clustered point、unique point、最长 continuous index prefix、clustered full scan。
- 同分数按 stable index id；剩余谓词由 Executor/adapter 在完整行上按 DD 类型重新求值。
- storage range 使用 unbounded/open/closed bound，物理 suffix sentinel 不编码进页。
- scan 每批 256；单语句最多 4096 个 row identity、16384 个 physical candidate。
- 超限不返回 partial result；range DML 超限早于第一笔 row mutation。
- range DML 先锁定/物化 clustered identity，再复用 point DML，避免 Halloween。
- 等锁前释放 page latch/fix，成功后重定位复核；所有等待共享 statement absolute deadline。
- RR 保留 next-key/terminal gap，RC 不保留 gap并释放未匹配行锁。

## 非目标

- OR、IN、IS NULL、LIKE、NOT、表达式、函数、JOIN、ORDER BY、LIMIT。
- 成本统计、Index Merge、并行 scan、长期 cursor 或网络流式结果。
- 主键更新、partial LOB update、prefix-index SQL range。

## 验收测试

- lexer/parser 覆盖五种比较、BETWEEN、AND、非法/截断 operator。
- binder 覆盖约束求交/矛盾、composite prefix、residual、stable tie-break、full scan。
- storage 覆盖 open/closed/unbounded、ASC/DESC、NULL/collation 和分页 continuation。
- MVCC 覆盖 old-view secondary key change、delete-mark、LOB hydration 与 candidate 去重。
- locking 覆盖 point/range SHARE/UPDATE、等待期间无 page latch、RC/RR gap 差异。
- DML 覆盖多行 update/delete、索引 key change、Halloween、防 partial 与 statement rollback。
- 固定 Java 25 / Gradle 9.5.1 执行定向和全量测试，数量不得倒退。

## Current map 更新

- 更新 SQL/Session、B+Tree current-read、transaction/MVCC 和 known gaps 小节。
- 只有完成生产调用的类型可标 Implemented；分页 seam 若仅测试使用必须进入 Reserved / Unwired。
