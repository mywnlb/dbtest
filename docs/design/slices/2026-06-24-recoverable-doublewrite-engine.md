# Slice: recoverable doublewrite 接进 engine（backlog 0.2）

依据：`innodb-flush-checkpoint-doublewrite-design.md` §7.2（doublewrite）、§8.4（与 recovery）；
`innodb-crash-recovery-design.md` §7.3（doublewrite repair）；`storage-backlog.md` 0.2。组件
（`RecoverableDoublewriteStrategy`/`DoublewriteFileRepository`/`DoublewriteRecoveryScanner`）已存在且测试覆盖，
engine 仍用 `NoDoublewriteStrategy` 且 recovery 不配 scanner/page 列表——本片把它们接进生产组合根。

## 目标

打开真正的 torn-page 防护：

> **不变量**：engine 正常运行时脏页落 data file 前先写整页副本到 doublewrite 文件并 fsync（`FlushCoordinator`
> 走 `RecoverableDoublewriteStrategy`）；崩溃重启时恢复期用 doublewrite 副本修复 checksum 损坏的 data 页，
> 干净重启不修复（`repaired=0`）。

## 关键决策

- **默认常开、不加 config 开关**：engine 始终用 recoverable doublewrite。`config.doublewriteFile()` 用**派生路径
  访问器**（`baseDir/doublewrite.dwb`），不给 15-arg `EngineConfig` record 加组件（同 redoFlushInterval 的
  blast-radius 决策）。
- **唯一新逻辑 `DoublewriteFileRepository.pageIds()`**：扫描 doublewrite 文件，返回有有效 slot（CRC + 页
  `PageImageChecksum` 通过）的去重 `PageId` 集合，作为恢复期"待检查页列表"来源（现 repo 仅 `latestCopy(pageId)`，
  无法枚举）。复用既有 slot 扫描循环，无效/尾部截断 slot 跳过。
- **前向接线**：engine 构造期 `DoublewriteFileRepository.open(config.doublewriteFile(), pageSize)`；`FlushCoordinator`
  注入 `RecoverableDoublewriteStrategy(dwRepo)`；`close()` 把 dwRepo 加进 `closeQuietly` 释放链。
- **恢复接线**：`recoverExisting` 构造 `DoublewriteRecoveryScanner(dwRepo, store, pageSize)` +
  `request.withDoublewriteRepair(scanner, dwRepo.pageIds())`。dwRepo 早于 FlushCoordinator 打开、文件跨进程持久，
  故恢复枚举到的是上一进程的副本。
- **UNDO 过滤协作核对**：接真 scanner 后 `repairDoublewritePages` 首次真正跑——`undoTablespaceRecovery
  .prepareDoublewrite(scanner)`（非 null）+ 逐页 `shouldRepairDoublewritePage` 过滤须与全量 `pageIds()` 列表正确协作
  （越界/TRUNCATING 边界页跳过，已是 scanner/participant 既有语义）。

## 非目标（明确推迟）

- 不做 doublewrite slot 回收（仍 append-only 无界增长，backlog 0.5）；不做 `DETECT_ONLY` 模式。
- 不做全空间 checksum discovery（页列表来自 doublewrite 文件枚举，不是 DD/tablespace discovery）。
- 不改 `FlushCoordinator`/scanner 既有逻辑（只换注入的策略 + 配页列表）。

## 验收测试

- `DoublewriteFileRepository.pageIds()`：append 多页副本（含重复页）→ 返回去重 `PageId` 集；torn/无效 slot 跳过。
- **e2e torn-page 恢复**（StorageEngine 集成）：写+flush 一页（产生 doublewrite 副本 + 有效 data 页）→ close →
  裸写损坏该 data 页 → 重开（recovery）→ 该页被修复，读回 close 前内容；`RecoveryReport.repairedPages>=1`。
- 干净重启回归：无损坏时 `repaired=0`，DOUBLEWRITE_REPAIR 阶段仍跑、不误修。
- 回归：engine/flush/recovery/doublewrite 全量不倒退（常开后每次 flush 多一次 doublewrite fsync，功能无害）。

## 文档更新要求

- Flush doublewrite 数据链/Package Status：`StorageEngine` 改注入 `RecoverableDoublewriteStrategy` + 配
  `DoublewriteRecoveryScanner`；新增 `DoublewriteFileRepository.pageIds()` 说明。
- Recovery 数据链 DOUBLEWRITE_REPAIR 行：从"占位"改为"engine 配 scanner + `dwRepo.pageIds()`，真正修复 torn 页"。
- Flush 缺口/Recovery 缺口：移除"doublewrite 生产未接线/NoDoublewrite"，保留 slot 回收无界增长（0.5）、DETECT_ONLY、discovery 仍缺。
- `current-implementation-map.md` Reserved/Unwired 里 recoverable doublewrite 栈改为 production-wired。
- `storage-backlog.md`：0.2 标 ✅；推荐路线下一片改 0.3 持久 rseg。
