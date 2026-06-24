# Slice: 持久 rollback segment header + 恢复扫描（backlog 0.3）

依据：`innodb-undo-log-purge-design.md` §5.4（RollbackSegment/UndoSegment）、§6.2（Undo Tablespace）、§6.3
（Rollback Segment Header）、§14.5（Crash Recovery）；`storage-backlog.md` 0.3。当前 `RollbackSegmentSlotManager`
为内存 `slots: PageId[]`，崩溃后无法找到 active 事务的 undo segment——本片把 slot 目录持久化并在恢复期重建。

## 目标

> **不变量**：rseg slot 目录（slot → insert-undo segment 首页）持久在 undo 表空间 **page3**（redo 保护）；
> 崩溃重启后恢复期扫 page3 重建内存 slot 目录，使 active 事务的 undo segment 首页可被找回。这是 crash-recovery
> ROLLBACK_TRX（R 1.2）/ RESUME_PURGE（R 1.3）的硬前置。

## 关键决策

- **固定 UNDO page3 为 rseg header 页**：`reserveSystemExtent` 已把 page0..3 标为系统保留（不会被 undo segment
  分配占用），UNDO 表空间 page3 改作 rseg header；GENERAL 表空间 page3 仍保持 SDI/保留语义，**不改 page0 布局**。
- **新 `PageType.RSEG_HEADER`（code 7）** + page3 格式：magic / format / `rollbackSegmentId` / `slotCapacity`
  + slot array（每槽 8B pageNo，`FIL_NULL`=空；同 undo 空间只存 pageNo）。打开时校验 page type、format、rseg id、
  `slotCapacity` 与配置一致，slot 数不能超过页容量；`EngineConfig.undoSpaceInitialPages` 至少覆盖 page0..3。
- **`RollbackSegmentHeaderRepository`（`storage.undo`）**：经 MTR 读/写 page3，redo 保护——`format` / `writeSlot`
  / `readSlots`，形态仿 `SpaceHeaderRepository`。
- **`RollbackSegmentSlotManager` 保持纯内存**（§9.3 不做 IO），只加 `restore(slot, firstPageId)` 供恢复重填；
  claim/release 签名不变。持久化由 `UndoLogManager` 编排：claim 后**同一 MTR** `writeSlot(firstPageId)`；
  `onCommit`/`RollbackService`/`PurgeCoordinator` 释放时补短 MTR `writeSlot(NULL)`，提交成功后才释放内存 slot。
  claim **和** release 都持久化
  （否则 page3 slot 永不清→跨重启 slot 耗尽）。
- **engine 接线**：fresh 建 undo 表空间后同 boot MTR `headerRepo.format(page3, empty)`；existing open 在
  `crashRecoveryService.recover`（redo 已重放 page3）**之后**用只读 MTR 扫 page3 → `slotManager.restore` 重建。
  扫描必须早于后台 worker 与 `StorageEngine` 发布 OPEN；正式 `UndoRecoveryService` recovery stage 留后续。
- **truncate/rebuild 接线**：`UndoTablespaceFspRebuilder` 物理重建 UNDO 时必须同 MTR 格式化空 page3；truncate 前已要求无
  active slot，重建后 page3 为空目录。

## 非目标（明确推迟）

- 不做 active-vs-committed 判定、不做实际 rollback / purge resume（R 1.2/1.3）；恢复只重建 slot 目录。
- 不持久 history list base / cached·free segment list / lastTransactionNo（§6.3 富字段）；不做多 rseg / 多 undo 表空间。
- 不把恢复扫描做成正式 `UndoRecoveryService` recovery stage（先放 engine 后恢复步）。
- 整条 trx/undo 仍 test-only（无生产 DML driver）；本片让 slot 目录可持久/可恢复，不接生产 DML。

## 验收测试

- `RollbackSegmentHeaderRepository`：format→writeSlot→readSlots round-trip；经 redo replay 后 slot 一致（幂等）。
- claim/release 持久化：`beforeInsert` 后 page3 该 slot=首页；`onCommit`（纯 insert 事务）后该 slot 清空。
- **crash-recovery money test**：claim slot（写 undo）→ **跳过 release 直接 close/丢弃**（模拟 active 事务崩溃）→
  重开 → 恢复扫描后 `engine` 的 slotManager 有该 slot 且首页正确（`activeSlotCount>=1`）。
- 配置/格式防线：slotCapacity 超页容量、page3 类型不符、磁盘 slotCapacity 与配置不一致均拒绝；UNDO truncate rebuild
  后 page3 为空 rseg header。
- 回归：trx/undo/engine/recovery/undo-truncate 全量不倒退；`PageTypeTest` 加 RSEG_HEADER code 钉死。

## 文档更新要求

- Undo/Transaction 数据链：slot claim/release 行补"持久化到 page3 rseg header（redo 保护）"；新增 rseg header
  数据链 + 恢复扫描 step。
- Package Status：`storage.undo` 新增 `RollbackSegmentHeaderRepository` + page 格式；`storage.page` `PageType` 加 RSEG_HEADER。
- Undo 缺口/Transaction 缺口：把"持久 rseg header / 恢复期 active slot 扫描仍缺"改为"slot 目录已持久 + 恢复重建；
  active-vs-committed 判定 + 实际 rollback/purge resume（R 1.2/1.3）+ history/cached segment 仍缺"。
- Undo truncate/current map：补 page3 会随 UNDO rebuild 重新格式化为空 rseg header。
- `storage-backlog.md`：0.3 标 ✅；R 1.2/1.3 依赖 0.3 已满足；推荐路线下一片改 R 1.2 或并行项。
