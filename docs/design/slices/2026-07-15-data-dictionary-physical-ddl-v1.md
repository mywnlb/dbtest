# Data Dictionary + Physical DDL v1

## Goal

- 提供可持久化、可恢复的 schema/table/index 字典最小闭环。
- 用 MDL + cache pin 保证 statement 打开表和 DDL 发布的并发边界。
- 让 CREATE/DROP TABLE 真正创建/删除 tablespace、segment 和 B+Tree root。
- 让公共 `DatabaseEngine` 重启时从 DD 发现表空间并完成 DDL cleanup。
- 让 rollback/purge 按 undo 中的 tableId/indexId 解析目标索引。

## Decisions

- `mysql.dd.ctrl` 使用两个 4 KiB CRC32C slot 保存 ID/version high-water。
- `mysql.ibd` v1 是页对齐 append catalog，frame CRC + batch SHA 保护原子发布。
- catalog 只接受严格递增 version；崩溃丢失的保留号允许形成间隙。
- startup 以已提交 catalog 反向抬高 control high-water，防止损坏槽回退后 ID 复用。
- schema MDL 先于 table MDL；DDL 再进入 tablespace X lease 和 MTR。
- statement 打开表同时持有 schema/table MDL 与 cache pin，由 lease 统一逆序释放。
- CREATE 先持久化物理对象，再发布 ACTIVE 字典版本。
- CREATE 的 DD publish 报错属于提交结果不确定：保留物理文件；重启后已提交 catalog 继续使用，未提交文件由 orphan discovery 清理。
- DROP 在 table X 下先建立 cache 准入屏障，再发布 `DROP_PENDING`；publish 结果不确定时保持 fail-closed 到重启。
- DROP 再写 durable DISCARDED page0 marker、刷页、invalidate buffer pool、删文件。
- DROP 执行前必须复核 binding path 与已打开 tablespace 路径一致。
- 物理删除完成后发布 `DROPPED`；startup 对 `DROP_PENDING` 幂等续作。
- DD 只返回稳定 storage API binding；公共 engine 组合根负责 recovery config 和 `BTreeIndex` 转换。

## Explicit Simplifications

- v1 catalog 不是 InnoDB system-table B+Tree，不经 Buffer Pool/MTR/redo。
- `DdlId` 只作日志诊断且尚无独立持久 `dd_ddl_log`；最新 control 槽损坏时允许其回退复用，恢复不得依赖它。
- 不实现 SDI、binlog、online DDL、ALTER TABLE、CREATE INDEX 语句、foreign key。
- v1 每表一个 file-per-table GENERAL tablespace，且要求恰好一个聚簇主键。
- DROP 已等待 metadata pin，但尚无“表级 purge history 已清空”持久屏障。
- orphan cleanup 只删除 `tables/` 下符合受控命名规则的未引用文件。

## Acceptance Tests

- 值对象、版本、catalog codec、双槽 control 损坏回退。
- repository commit/rollback/reopen、版本间隙与 lifecycle 查询。
- cache single-flight、pin/stale/invalidate，MDL 六模式/FIFO/upgrade/timeout/deadlock。
- 真实 CREATE 初始化多索引 segment/root，真实 DROP 持久 marker 并删文件。
- durable DISCARDED 后故障注入保留文件，重试 DROP 幂等续作。
- catalog 已 durable 后注入 publish 异常，CREATE 保留物理文件且重启恢复 ACTIVE。
- DROP_PENDING publish 结果不确定时，当前进程阻止旧快照重载，重启后续作删除。
- CREATE/open/drop 端到端，`DROP_PENDING` 故障注入后重启续作。
- 重启无手工 recovery list 发现 tablespace，control 回退不复用 SpaceId。
- 受控命名 orphan 清理，ACTIVE 字典缺文件时 fail-closed。
- rollback/purge 对 undo identity 解析 table/index，legacy 单索引构造保持兼容。

## Current Map Update

- 增加 DD/catalog/cache/MDL/DDL/public engine 生产调用链。
- 更新 GENERAL DISCARDED、discovery、DDL recovery 与 resolver 状态。
- 把 `DdlId`/DDL_LOG 和 purge-aware DROP 明确列为 reserved/partial，不画成已接线。
