# DD、表空间迁移与 Blocking ALTER v1

## 目标

- 为表元数据持久保存 comment、默认 charset/collation 和列默认值语义。
- SQL 接入受控目录内的 `ALTER TABLE ... DISCARD/IMPORT TABLESPACE`。
- 支持一次语句内按顺序组合 ADD/DROP COLUMN、ADD/DROP secondary index、
  RENAME、COMMENT、DEFAULT CHARACTER SET/COLLATE 和 CONVERT。
- 结构变更只执行一次 table-X 保护的 shadow rebuild，并支持崩溃收敛。

## 关键决策

- `TableOptions` 与 `ColumnDefaultDefinition` 是 DD 权威状态，并进入 catalog 与 SDI。
- 旧 catalog 的表 charset/collation 从 schema 补齐；nullable 列推导隐式 NULL。
- Binder 只产生逻辑 `BoundAlterTable`；DD 在 table X 下重验并顺序应用 action。
- DISCARD/IMPORT 必须独立成句，文件只在实例受控 transfer 目录中原子移动。
- IMPORT 校验 page0、SDI、table/space identity，成功后递增 space version。
- 普通结构 ALTER 分配新 SpaceId，旧表 binding 在 DD 提交前始终保持权威。
- rebuild 以 256 行 continuation batch 扫描聚簇树，不物化整表。
- 每行先按 source schema hydrate 外部 LOB，再按 target schema 重新编码。
- target external LOB 在 shadow LOB segment 中重新分配，不复制旧页引用。
- clustered 与所有 secondary 都按 target definition 重建并重新执行 unique 校验。
- 新空间、redo 与 SDI durable 后才允许原子提交 DD 新 binding。
- DD 提交后只允许前滚：发布 cache，随后 drain 并删除旧 tablespace。
- `REBUILD_TABLE` marker 保存 old/new space/path 和 DD version；恢复以 committed DD
  binding 裁决删除 shadow 或旧空间。
- v1 marker 未单独保存 schema digest；target SDI 由 committed DD 做 exact reconcile。
- catalog append 一旦开始，返回异常视为 outcome unknown，不能擅自删除 shadow。
- shadow 创建后的失败用 `TableRebuildException` 把精确 binding 交还 DD 清理。

## 非目标

- online DDL row log、并行复制、external sort、断点续传和 binlog participant。
- 主键或列类型修改、foreign key、generated column、FULLTEXT/SPATIAL。
- prefix key part 新语法、跨设备 rename、任意外部路径 IMPORT。
- marker schema digest；后续格式升级必须保持旧 marker 可恢复。

## 验收

- catalog/SDI 新旧格式、table options、三种列 default 语义往返。
- DISCARD/IMPORT 正常、非法路径/identity/SDI 和重启收敛。
- 多 action 顺序、FIRST/AFTER、跨 schema rename、charset convert。
- unique 冲突、编码失败、非法 drop 在 DD 发布前回滚 shadow。
- 超过 256 行的 continuation rebuild 不重不漏。
- external LOB 在新空间重分配，旧空间删除后仍可读且重启一致。
- ENGINE_DONE 对旧 DD 回滚 shadow；DD 新 binding 后崩溃完成旧空间删除。
- 固定 Java 25 / Gradle 9.5.1 定向与全量测试通过。

## Current map 更新

- 更新 DD/DDL、SQL Session、storage DDL 和 recovery 的生产实线。
- 删除 DISCARD/IMPORT 与已支持 blocking ALTER 缺口。
- 保留 online DDL、主键/类型变更、marker digest 和 catalog B+Tree/redo 化。
