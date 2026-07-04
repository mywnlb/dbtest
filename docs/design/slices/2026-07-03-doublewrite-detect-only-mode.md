# Doublewrite DETECT_ONLY Mode

## Goal

- 补齐 `DoublewriteMode.DETECT_ONLY`：flush 前写 page id、pageLSN、checksum 等 metadata，用于恢复期检测 torn/corrupt data page。
- recovery 扫描 detect-only slot 时只报告可疑页，不用副本修复 data file；`DETECT_AND_RECOVER` full-copy 行为保持不变。
- 让 recovery report 能区分 `detectedOnlyPageCount` 与 `repairedPageCount`，为后续 `READ_ONLY_VALIDATE`/诊断模式铺路。
- 保持 bounded slot reuse、WAL gate、doublewrite-before-data-file 顺序不变。

## Non-goals

- 不实现完整 `RecoveryMode.READ_ONLY_VALIDATE` 或 `FORCE_SKIP_CORRUPT`。
- 不做全空间 discovery；待检查 page list 仍来自 doublewrite 文件枚举和当前已打开/配置空间。
- 不改 redo replay 幂等、checkpoint、flush policy 或 PageStore 写盘顺序。
- 不做 FlushList/LRU 双 doublewrite 文件拆分，也不改 slot 回收策略。
- 不迁移 legacy `BufferPool.flush/flushAll`。

## Key Decisions

- `DoublewriteMode` 增加 `DETECT_ONLY`，新增 `DetectOnlyDoublewriteStrategy`；`NoDoublewriteStrategy` 和 `RecoverableDoublewriteStrategy` 保持现有语义。
- `DoublewriteFileRepository` 支持两类 slot：`FULL_COPY` 和 `DETECT_ONLY_METADATA`。slot 大小仍固定为 `header + pageSize`，detect-only 的 payload 区只写 `metadataLength + pageChecksum/pageLsn/pageId` 并清零剩余区域，不保存完整 page image，保持 bounded reuse 简单。
- 为避免破坏既有 full-copy 文件，scanner 同时识别现有 v1 full-copy slot 和新增 v2 typed slot；新写入统一使用 v2 typed slot。
- 仓储新增只读枚举能力 `scanEntries()`，返回 `pageId/pageLsn/mode/checksum/hasFullCopy`；`latestCopy(pageId)` 只从 full-copy slot 选副本。
- 既有 `pageIds()` 改为枚举 full-copy 和 detect-only 两类有效 slot 的去重页号，保证恢复期会检查 detect-only 覆盖的页。
- `DoublewriteRecoveryScanner` 新增检测结果对象：data page checksum 无效 + detect-only metadata 命中时返回 `DETECTED_ONLY`；full-copy 命中并写回时返回 `REPAIRED_FROM_COPY`；有效页/无条目返回 `CLEAN_OR_NOT_COVERED`。
- `RecoveryReport` 增加 `detectedOnlyPageCount`；`CrashRecoveryService` 统计 detect-only suspicious pages，但不把它们计入 `repairedPageCount`。本片只暴露报告，不新增 fail-closed 决策。

## Data Flow

- flush：`FlushCoordinator` snapshot -> strategy.beforeDataFileWrite -> detect-only strategy 写 metadata slot + force -> data file write/force -> strategy.afterDataFileWrite 释放 in-flight slot。
- recovery full-copy：scanner 读 data page checksum 失败 -> `latestCopy(pageId)` 有 full-copy -> 写回 data file -> report `REPAIRED_FROM_COPY`。
- recovery detect-only：scanner 读 data page checksum 失败 -> 无 full-copy 但有 detect-only metadata -> 不写 data file -> report `DETECTED_ONLY(pageId,pageLsn)`。
- recovery clean：data page checksum 有效时不使用 doublewrite slot，避免旧 slot 误报。

## Acceptance Tests

- `detectOnlyWritesMetadataWithoutFullPagePayload`：detect-only strategy 写 slot 后，repo 枚举能看到 metadata，`latestCopy(pageId)` 为空。
- `detectOnlyRecoveryReportsTornPageButDoesNotRepair`：损坏 data page + detect-only metadata -> scanner 返回 detected-only，data page 内容未被替换。
- `detectAndRecoverStillRepairsFromFullCopy`：现有 full-copy 路径仍能修复 torn page，`repairedPageCount` 增加。
- `mixedSlotsPreferFullCopyForRepair`：同一 page 同时有 detect-only 和 full-copy 时，恢复使用最高 LSN 的有效 full-copy 修复，detect-only 不阻断修复。
- `storageEngineReportSeparatesDetectedOnlyFromRepaired`：恢复报告中 `detectedOnlyPageCount` 与 `repairedPageCount` 分开统计。

## Current Map Update

- Flush/Doublewrite 数据链补充：`DoublewriteMode.DETECT_ONLY` 与 `DetectOnlyDoublewriteStrategy` 已接测试路径，生产默认仍可保持 recoverable full-copy。
- Recovery 数据链补充：`DoublewriteRecoveryScanner` 区分 repaired-from-copy 与 detected-only report。
- `storage-backlog.md` 0.7 项标记 `DETECT_ONLY` 完成；保留 RecoveryMode 扩展、legacy flush 去留等其它碎片。

## Verification

- 按 TDD 先新增 doublewrite detect-only 仓储/strategy/scanner 测试并确认 RED。
- 实现后运行 `gradle test --tests cn.zhangyis.db.storage.flush.doublewrite.*`。
- 再运行 `gradle test --tests cn.zhangyis.db.storage.recovery.* --tests cn.zhangyis.db.storage.engine.*`。
- 最后运行全量 `gradle test`。
- 静态扫描生产代码不新增 `synchronized/wait/notify/notifyAll` 或裸 `IllegalArgumentException/RuntimeException`。

## Fifteen-pass Self-check

- Pass 1 scope: 只做 DETECT_ONLY metadata + recovery report，不做完整 RecoveryMode。
- Pass 2 design source: 对齐 flush 设计 §7.2/§8.4 的 detect-only 定义。
- Pass 3 current code: 保留现有 v1 full-copy slot 读取能力，避免误删 `latestCopy/pageIds` 行为。
- Pass 4 slot size: detect-only 仍占固定 slot，维持 bounded reuse 和 batch 分配简单性。
- Pass 5 repair semantics: detect-only 永不写 data file，只有 full-copy 可以 repair。
- Pass 6 reporting: `detectedOnlyPageCount` 与 `repairedPageCount` 分开，不混淆恢复结果。
- Pass 7 WAL/checkpoint: 不改变 redo durable gate、checkpoint 或 data file write 顺序。
- Pass 8 discovery: 不做全空间扫描，仍依赖打开空间和 doublewrite page list。
- Pass 9 compatibility: 新 scanner 读 v1/v2，新写统一 v2 typed slot。
- Pass 10 layering: doublewrite 不读取 BufferFrame/Redo internals；Recovery 通过 scanner 端口消费结果。
- Pass 11 failure: 无 full-copy 时不伪修复，报告后交给 redo/recovery 既有 fail-closed 语义。
- Pass 12 tests: 仓储、strategy、scanner、mixed-slot、engine report 均有验收测试。
- Pass 13 docs: current map/backlog 更新点明确，不把 RecoveryMode 扩展写成完成。
- Pass 14 implementation size: 可拆成仓储/策略/报告三小步，适合 TDD。
- Pass 15 placeholders: 文档无 TBD/TODO/待定，所有决策可执行。
