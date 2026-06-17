# Spec: R2 - persistent checkpoint label + recovery startup

- 日期：2026-06-16
- 关联设计：`docs/design/innodb-redo-log-design.md` §5.9、§5.10、§10-§13、§17、§20-§24；`docs/design/innodb-flush-checkpoint-doublewrite-design.md` §7.3、§8.4、§11.4；`docs/design/innodb-crash-recovery-design.md` §3、§7.1-§7.4、§8.1-§8.2、§11。
- 前置：R1 redo runtime/recovery page replay；F1 WAL-gated flush、doublewrite recovery、in-memory checkpoint。
- 状态：把 R1/F1 的内存恢复边界推进为可持久化 checkpoint label，并提供最小 crash recovery startup facade。

## 1. 范围

**做：**

- 新增 redo control/checkpoint label 文件：保存 checkpoint LSN、checkpoint 时的 current LSN、格式版本、时间戳和 checksum。
- `CheckpointCoordinator` 在安全边界推进时可选持久化 checkpoint label；默认构造器保持 F1 内存语义。
- `RedoRecoveryReader` 支持从 checkpoint LSN 过滤 redo 批次：`range.end <= checkpointLsn` 的批次不进入 replay。
- 新增 redo capacity pressure 策略：根据 `currentLsn - checkpointLsn` 与 capacity 阈值返回 `NONE/ASYNC_FLUSH/SYNC_FLUSH/HARD_LIMIT`。
- 新增 `storage.recovery` 最小启动编排：关闭 gate，先执行 doublewrite repair，再按 checkpoint 扫描并 replay redo，成功后开放 gate，失败时 fail closed。

**不做：**

- 不做循环 redo 文件和旧 redo 文件物理回收。
- 不做后台 writer/flusher、page cleaner 或 adaptive flush worker。
- 不做 tablespace discovery、DDL recovery、transaction rollback、purge resume 的真实实现；R2 facade 只保留阶段边界和可测试跳过结果。
- 不做 SQL/session 接入；Recovery gate 只作为存储层启动门控对象。

## 2. 关键决策

1. **Checkpoint label 独立于 redo data file**：R1 redo data file 仍保持 append-only batch frame，R2 新增 control file，避免破坏现有 redo 文件扫描测试。
2. **双槽 label，读取最新有效槽**：写 checkpoint 时交替写两个完整 slot。恢复读取时校验 magic/version/checksum，选择 checkpoint LSN 最大的有效 slot；单槽损坏不阻断 NORMAL 恢复。
3. **Checkpoint 过滤只跳过完整旧批次**：如果某批 `end <= checkpointLsn`，该批已被 checkpoint 覆盖，可以跳过；如果 `start < checkpoint < end`，仍保留该批，由 pageLSN 幂等逻辑兜底。
4. **Capacity pressure 只计算，不阻塞 append**：R2 先提供可测试 policy，让后续后台 flush/redo reservation 接入；本片不改 `RedoLogManager.append()` 行为。
5. **Recovery facade 只编排已存在能力**：doublewrite repair 通过 `DoublewriteRecoveryScanner`，redo replay 通过 `RedoRecoveryReader` + `RedoApplyDispatcher`，facade 不解析 redo payload、不访问 BufferPool 内部。

## 3. 数据流

Checkpoint:
`CheckpointCoordinator.advanceCheckpoint()` -> compute safe LSN -> if advanced, write `RedoCheckpointLabel(checkpointLsn, redo.currentLsn(), createdAt)` -> persist control slot.

Recovery:
`CrashRecoveryService.recover(request)` -> gate closed -> repair requested pages with doublewrite -> read checkpoint label -> `RedoRecoveryReader.withCheckpoint()` scans redo and filters old batches -> dispatcher applies page redo -> report recoveredToLsn -> gate open.

Capacity:
`RedoCapacityPolicy.evaluate(currentLsn, checkpointLsn)` -> checkpoint age -> pressure level and recommended target LSN for flush/checkpoint consumers.

## 4. 测试

- `RedoCheckpointStoreTest`：写入两个 label 后重开选择 LSN 更大的 label；破坏一个 slot 后仍读取另一个有效 label；空 control 返回 0。
- `RedoCheckpointRecoveryReaderTest`：checkpoint LSN 覆盖第一批时 reader 只返回后续批次，`recoveredToLsn` 仍为最后完整批次 end。
- `CheckpointCoordinatorPersistentTest`：safe checkpoint 推进后写 label；safe 不推进时不倒退 label。
- `RedoCapacityPolicyTest`：固定阈值下区分 none/async/sync/hard，非法容量或阈值拒绝。
- `CrashRecoveryServiceTest`：恢复阶段顺序为 gate close -> doublewrite repair -> redo replay -> gate open；redo 损坏时 gate fail closed。

## 5. 简化点与后续

- R2 control file 不做 fileId+offset 映射；恢复仍从 redo 文件头顺序扫描并按 LSN 过滤。
- R2 recovery request 由测试/启动编排显式传入需要 doublewrite 检查的 page id；后续 tablespace discovery 落地后由 registry 枚举候选页。
- R2 capacity pressure 不阻塞写入；后续 F2/page cleaner 接入后再驱动 flushList 和 foreground throttling。
