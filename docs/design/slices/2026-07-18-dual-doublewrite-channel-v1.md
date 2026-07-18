# Dual Doublewrite Channel v1

- 目标：为 FlushList/LRU 批量刷脏提供独立 bounded doublewrite 文件，并让 engine 配置选择 OFF、DETECT_ONLY、DETECT_AND_RECOVER。
- 依据：`innodb-flush-checkpoint-doublewrite-design.md` §7.2、§8.4、§9；`innodb-crash-recovery-design.md` §7.3。
- 依赖：现有 `DoublewriteFileRepository`、`DoublewriteBatch`、WAL-safe `FlushCoordinator`、`DoublewriteRecoveryScanner`。
- 关键决策：FlushList 使用 `doublewrite-flush-list.dwb`；LRU/single-page 使用 `doublewrite-lru.dwb`；旧 `doublewrite.dwb` 只作为恢复输入兼容。
- 关键决策：source 信息携带在 Flush 批次边界，不让 Buffer Pool 依赖 doublewrite 包；每个物理 channel 独立 slot 游标、锁和 force。
- 关键决策：恢复合并两个 channel，按 pageLSN 选择最新 full-copy；同 LSN 不同 payload fail-closed；detect-only 永不写回。
- 关键决策：data-file 部分失败释放所有 reservation，但保留磁盘副本；避免 in-flight slot 泄漏。
- 非目标：全空间 checksum discovery、动态 slot/IO 配置、旧文件在线迁移、DROP INDEX、temporary undo。
- 验收：双文件隔离/复用、source 路由、torn-page 双文件恢复、detect-only/off/read-only 生命周期、旧单文件兼容、全量 Gradle 回归。
- current map：更新 `storage.flush.doublewrite` 生产接线、reserved gap 和 engine mode 状态。
