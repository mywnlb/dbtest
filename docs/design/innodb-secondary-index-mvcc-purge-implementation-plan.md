# Secondary Index MVCC / Purge Production Closure Implementation Plan

版本：2026-07-17

> 本计划按用户明确要求持久化，配套 `innodb-secondary-index-mvcc-purge-design.md`。它不是当前实现状态；只有源码、
> 测试和 current map 的生产调用链可证明任务完成。本模块不拆分 slice spec。

**Goal:** 完成二级索引紧凑格式、多索引 DML/undo/rollback、回表 MVCC、secondary purge 和 purge-aware DROP。

**Architecture:** DD exact-version metadata 映射为 table/index runtime snapshot；一个行级逻辑 undo 覆盖多个短 MTR；
secondary consistent read 必须回表；purge 以聚簇版本链证明物理删除安全；persistent history 是 DROP barrier 的恢复依据。

## Global Constraints

- 每个任务严格 TDD：先写目标测试并确认 RED，再实现到 GREEN；不得弱化既有断言。
- 禁止 GitNexus；调用链和 blast radius 只用设计文档、源码、测试、`rg` 与 Git。
- 不使用 Java monitor、全局大锁或无界等待；显式锁必须 `try/finally`/Guard 释放。
- SQL/session 不 import storage internals；DD 只依赖稳定 storage API；B+Tree/Record 不依赖事务业务语义。
- 高扇出 record component/API 编辑前报告直接调用方、磁盘/API 风险和回归范围。
- 复杂核心方法补齐阶段化中文 Javadoc 与方法体编号，说明锁、redo、dirty、失败释放和恢复不变量。
- 固定 JDK 25.0.2、Gradle 9.5.1；最终 `test --rerun-tasks`，测试数不得下降。

## Delivery Boundaries

| 阶段 | 任务 | 可独立验收结果 |
| --- | --- | --- |
| A：物理模型 | 0-4 | 紧凑 layout、物理唯一、root snapshot、secondary BTree 原语与锁边界闭环 |
| B：原子写入 | 5-7 | undo tail、表级 DML、statement/full/recovery rollback 闭环 |
| C：读取 | 8-9 | back-to-cluster MVCC 与唯一二级 SQL 点查生产接线 |
| D：回收 | 10-11 | version-safe purge 与 purge-aware DROP/recovery 闭环 |
| E：收尾 | 12-14 | 故障/并发/恢复、current map/backlog、全量回归与五遍复核 |

任何中间阶段都不能把 backlog 2.2 标为完成。

## Current Execution Status（2026-07-17）

- Task 0-13 已落地：紧凑 secondary layout、`physicalUnique`、root page header 快照、secondary B+Tree 原语、
  logical-unique transaction X lock、DML/purge row guard、secondary undo tail 与表级 INSERT/UPDATE/DELETE 已闭环。
- statement/full/recovery rollback 按 secondary index-id 顺序逐棵逆操作，clustered inverse 最后，logical marker 再后；
  已覆盖 secondary durable、clustered inverse durable 与 marker durable 故障边界，重试以精确旧状态收敛。
- `SecondaryMvccReader` 已在唯一二级点查中执行 back-to-cluster 版本复核；SQL INSERT 与 primary/unique-secondary
  point SELECT 已走 exact DD metadata、table DML、ReadView 和 LOB hydration 的生产链。
- purge 已按聚簇版本链证明 secondary 物理删除安全，DELETE 固定 secondary-first；恢复阶段真实运行
  `RESUME_PURGE`。persistent history 发布 affected-table runtime projection，普通 DROP 与 DROP_PENDING recovery
  都必须等待 `TablePurgeBarrier`。
- Task 14 已完成：固定 JDK 25.0.2 + Gradle 9.5.1 fresh 全量回归为 275 suites / 1476 tests，
  0 failure/error/skip；五遍复核结果已写入 `current-implementation-map.md`。
- 本节是执行进度快照，不替代 `current-implementation-map.md`；源码调用链、测试与 current map 共同构成完成证据。

## Task 0: Baseline 与 Blast Radius

- [x] 记录 `git status --short --branch`、最近提交和用户改动归属。
- [x] 固定全量 suites/tests/failure/error/skip 基线。
- [x] 扫描 `BTreeIndex`、`UndoRecord`、DML command/service、`HistoryEntry`、resolver、SQL bound types 的生产/测试调用点。
- [x] 向用户报告磁盘兼容、API 迁移、恢复和测试风险后才编辑。

## Task 1: Secondary Layout 与 DD 校验

- [x] RED：紧凑字段顺序、重复附加主键、ASC/DESC/prefix/NULL/collation、defensive copy、错误映射。
- [x] RED：DD/mapper 拒绝 LOB/JSON secondary key，空 root 可映射，非法 metadata fail-closed。
- [x] 实现 `SecondaryIndexLayout`、`SecondaryIndexMetadata`、`TableIndexMetadata` 和 mapper。
- [x] 定向运行 record schema、DD mapper、DDL/catal​og 回归。

## Task 2: 物理唯一与 Root Snapshot

- [x] RED：logical unique=false 的 secondary 仍拒绝重复完整 physical key；不同主键可共存。
- [x] RED：跨独立语句 root split 后，新快照读取真实 level并正确继续 split/merge。
- [x] `BTreeIndex.unique` 迁移为 `physicalUnique`，更新全部调用点，不保留含混别名。
- [x] 实现 root-level snapshot 服务，structural budget/operation 必须消费刷新快照。

## Task 3: Secondary BTree 原语

- [x] RED：insert、revive、delete-mark、rollback physical delete、purge marked、live-conflict、absent idempotence。
- [x] RED：scan including deleted 与普通 scan 过滤语义互不污染，跨 sibling 保序。
- [x] 实现非聚簇专用 API和明确结果枚举；复用现有 split/merge/root shrink，不复制结构算法。
- [x] 定向运行全部 B+Tree/record/redo recovery 回归。

## Task 4: Unique Prefix Lock 与 Row Guard

- [x] RED：两个事务不同主键并发写同 unique key，只有一个越过检查；NULL 不冲突。
- [x] 历史 v1 RED：其它主键 delete-marked entry 保守重复；2026-07-23 current-read 切片已替换为“等待事务终态后重扫，
  其它主键 marked 历史可复用、同主键 marked 可复活”，本文件不再作为当前实现依据。
- [x] RED：DML guard 有界等待，purge zero-wait busy；用户输入 collation 等价时使用物化行主键 guard。
- [x] 实现 including-deleted unique-prefix current-read；logical unique identity 在统一 `LockManager` 中持事务级 X 锁，
  1024 个公平 stripe 仅承担 DML/purge 同行物化 guard，不冒充 next-key/gap 锁。

## Task 5: Secondary Undo Tail

- [x] 再次报告 `UndoRecord`/codec blast radius。
- [x] RED：领域 action/state/index 排序与类型约束。
- [x] RED：旧 golden、新 secondary tail、LOB+secondary 双尾、identity peek、未知/截断/尾随损坏。
- [x] 实现 mutation 值对象、`UndoRecord` component、codec 和 write-plan/redo workload。
- [x] 运行 undo/MVCC/rollback/purge/recovery 定向回归。

## Task 6: Table-level DML

- [x] RED：一聚簇多 secondary INSERT、UPDATE key/no-change、DELETE、unique/null/prefix。
- [x] RED：secondary 已提交与下一 secondary MTR 尚未开始两类 statement 故障边界均由 guard 回滚；事务终态前不提前释放锁。
- [x] 重构为 `TableDmlService` 和表级 command；生产 SQL 固定走表级接口，底层 clustered facade 仅保留显式 legacy/教学兼容入口。
- [x] 按设计固定聚簇首 MTR、secondary index-id 顺序和 update new-before-old 顺序。

## Task 7: Multi-index Rollback

- [x] RED：INSERT/UPDATE/DELETE 的 statement、full、recovery rollback。
- [x] RED：secondary inverse、cluster inverse、logical marker 的 durable 边界 crash 幂等。
- [x] resolver 返回表级 metadata；Guard 使用 resolved savepoint rollback。
- [x] 单条 inverse 重构为多个 MTR，cluster inverse 最后；marker-lag 重试执行精确旧状态校验。

## Task 8: Secondary Back-to-cluster MVCC

- [x] RED：未提交 INSERT/UPDATE/DELETE、RR/RC、A->B、A->B->A、marked candidate、去重。
- [x] RED：secondary/cluster/undo latch 不重叠，ReadView 覆盖 LOB hydration。
- [x] 实现 `SecondaryMvccReader`、版本候选复核、unique-visible corruption 检测。

## Task 9: SQL Production Wiring

- [x] RED：主键优先、唯一二级候选、最小 index id、NULL 空结果、不支持形状。
- [x] RED：真实 Session INSERT 后按唯一二级索引 SELECT 返回完整投影/LOB。
- [x] 泛化 bound point select、executor gateway；SQL INSERT 改走 table-level DML。
- [x] Parser 语法不扩张，SQL/session 继续零 storage internal import。

## Task 10: Version-safe Secondary Purge

- [x] RED：UPDATE 旧 key、DELETE 全索引、A->B->A、新版本仍需要旧 entry、最终后续回收。
- [x] RED：guard busy、链不到 target、live-conflict、task 中断与幂等重试不移动 history。
- [x] 实现 safety checker，PurgeCoordinator 收集 UPDATE/DELETE task；DELETE secondary 先于 clustered。
- [x] 扩展 `PurgeSummary` 诊断 clustered/secondary removed 数。

## Task 11: Table Purge Barrier 与 DROP

- [x] RED：HistoryEntry table 集合、commit/purge 发布计数、recovery rebuild。
- [x] RED：DROP 等待、timeout 保持 ACTIVE、purge 唤醒、DROP_PENDING recovery 不越过 barrier。
- [x] 实现稳定 `TablePurgeBarrier` API、history runtime projection 和 DD/DDL/recovery 接线。
- [x] 不新增独立持久计数；persistent history 始终是恢复真相。

## Task 12: Redo、Buffer Pool 与故障注入

- [x] 对已接生产 secondary operation 验证结构预算不低估，并复用现有 MTR/WAL/pageLSN/dirty publish 路径。
- [x] 小 Buffer Pool、多层树、split/merge/root shrink、多 secondary 压力测试。
- [x] 覆盖 statement 的 next-MTR/secondary-commit、rollback 的 secondary/cluster/marker commit、purge task commit
  未 finalization、history finalization 与 DROP/DROP_PENDING barrier 边界，证明恢复与重试一致。

## Task 13: 生产调用链与文档

- [x] 从源码核对 SQL -> gateway -> table DML/MVCC 与 purge/DDL recovery 调用链。
- [x] 只更新 `current-implementation-map.md` 受影响小节；test-only/unwired fault seam 已进入保留表。
- [x] 全部功能验收后将 backlog 2.2 标为 complete，并保留非唯一 range MVCC、多 worker purge 等真实非目标。
- [x] 同步相关厚设计交叉引用，不生成 slice spec。

## Task 14: 静态检查、全量回归与五遍复核

- [x] 扫描 Java monitor、裸异常、占位词和 SQL/storage 反向依赖。
- [x] `git diff --check`，确认无 build/IDE/临时数据。
- [x] 固定 Gradle/JDK 全量 `test --rerun-tasks`：275 suites / 1476 tests，较 Task 0 增加 35 tests，
  0 failure/error/skip。
- [x] 按生产链、格式、并发、恢复、文档五个维度复核并记录修正。

## Final Acceptance Checklist

- [x] 二级索引物理格式、logical unique 与 root-level 权威边界一致。
- [x] INSERT/UPDATE/DELETE、statement/full/recovery rollback 多索引闭环。
- [x] 唯一二级 SQL SELECT 使用真实 back-to-cluster MVCC。
- [x] Purge 不误删仍被较新版本需要的 entry，history 只在全部 task 后推进。
- [x] DROP 不越过 persistent history，重启可重建 barrier。
- [x] current map/backlog 与源码一致，固定全量回归通过且测试数不下降。
