# Slice: redo 文件轮转 + checkpoint 回收（0.18a）

依据：`storage-backlog.md` 0.18；`innodb-redo-log-design.md` §5.7（RedoFileRepository）、§10（checkpoint 回收）；
0.1 已落的 `closedLsn` checkpoint 边界。

## 背景

当前 `RedoLogFileRepository` 是单文件、从 offset 0 无界 append-only；checkpoint 推进后旧 redo 区间无法回收。
0.1 接上后台 `RedoFlushWorker` 后 redo 被长期周期性驱动，单文件会无界增长——长跑必须有界化。

## 目标

- 单文件 → 固定 N 个等容量 redo 文件的环：目录下 `redo-NNNNNN.log`，每文件带头 `{magic, formatVersion, fileId, startLsn}`。
- active 文件写满（下一批次放不下剩余容量）→ 轮转到下一个文件；**单批次不跨文件**（沿用 `MAX_PAYLOAD_BYTES ≤ 文件容量`）。
- 恢复：读所有文件头、按 `startLsn` 排序，从含 `checkpointLsn` 的文件起跨文件顺序扫描到 torn tail；replay 保持幂等。
- 回收：轮转取下一文件时，只能复用「最高 LSN < checkpoint 回收边界」的文件，复用即重写文件头 `startLsn`。
- 环满（无可回收文件）→ fail-closed 抛 `RedoLogCapacityExceededException`（`DatabaseRuntimeException`，调用方可在驱动 checkpoint 后重试），**绝不静默覆盖未 checkpoint 的 redo**。

## 关键决策

- LSN→(fileId, offset) 由文件头 `startLsn` + 文件内顺序定位；不引入独立 control 文件，checkpoint label 仍复用现有 `RedoCheckpointStore` 两槽。
- 回收边界 = 已持久化 `checkpointLsn`，由调用方（engine/`CheckpointCoordinator`）注入文件集合；redo.file **不反向依赖** Buffer Pool / flush list（守 §3 依赖方向）。
- `ioLock` 仍串行 write/force/scan；轮转在 `ioLock` 内原子切换 active 指针，不引入第二把锁顺序。
- batch 不跨文件：剩余空间放不下整批就 rotate（必要时尾部留空），文件容量默认配置 ≥ 单批上限。
- `restoreRecoveredBoundary` 后：新进程 open 时按文件头 + checkpoint 定位 active 文件与续写 offset，从 `recoveredTo` 连续续写。
- 不用 `synchronized`；容量压力走显式异常，不无界等待。

## 非目标

- 不做 0.6 adaptive flush / 容量 throttle（sync→等 checkpoint、hard→暂停 reservation）；环满只 fail-closed。
- 不做 0.20 log block header/trailer/checksum；仍用现有 batch frame + CRC32。
- 不做动态 resize、文件数在线增减、多 writer。
- 不改 redo record 格式，不引入逻辑 redo handler（0.19）。
- 不把 checkpoint 改成后台线程（沿用 0.1 现状）。

## 验收测试

- `rotatesToNextFileWhenActiveFull`：写满 active 文件后续写进入下一文件，LSN 连续。
- `recoveryScansAcrossRotatedFiles`：跨多文件写入后恢复，replay 覆盖全部文件且幂等。
- `reclaimsFileOnlyBehindCheckpoint`：checkpoint 推进后旧文件可复用；未推进时不复用。
- `refusesAppendWhenRingFullWithoutCheckpoint`：环满且无可回收文件 → 抛 `RedoLogCapacityExceededException`，旧 redo 不被覆盖。
- `recoveryStartsFromCheckpointFile`：从含 `checkpointLsn` 的文件起扫描，忽略已回收的更早区间。
- `restoreRecoveredBoundaryContinuesInActiveFile`：恢复后续写 LSN 连续、active 文件/offset 定位正确。
- 回归：`RedoLogManagerTest`、`RedoRecoveryReader` 相关、`CheckpointCoordinatorPersistentTest`、engine recovery/flush 测试通过。

## 文档更新要求

- `current-implementation-map.md`：Redo 数据链与 file 仓储从「单 append-only 文件」改为「固定文件环 + checkpoint 回收」；恢复链补跨文件扫描；Outstanding Gaps 标 0.18 部分完成，容量 throttle(0.6)/log block(0.20) 仍缺。
- `storage-backlog.md`：0.18 标记轮转 + 回收已落（0.18a），保留容量 throttle、动态 resize、log block 校验为后续。
- 代码注释说明：回收边界必须 ≤ 持久 `checkpointLsn`，否则覆盖恢复所需 redo；batch 不跨文件的简化点与 MySQL 差异。
