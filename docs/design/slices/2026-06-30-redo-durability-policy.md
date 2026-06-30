# Slice: redo DurabilityPolicy（commit 刷盘策略，0.20a）

依据：`storage-backlog.md` 0.20（DurabilityPolicy 部分，= 0.1 剩余 commit durable policy）；
`innodb-redo-log-design.md` §5.5（append/write/flush 三阶段）、§8（Durability Policy）、§9（Writer/Flusher）。

## 背景

当前 commit 不按策略等待 redo durable：`MiniTransaction.commit` append redo + 发布 dirty + `markClosed` 后即返回，
事务层无 commit durability 选择。`RedoLogManager` 只有 `flush()`（同步 write+fsync）+ `waitFlushed`，**缺** write-without-fsync
路径、`writtenToDiskLsn()` 和 `waitWritten`，无法表达「commit 只等写到 OS cache、不等 fsync」。

## 目标

- `DurabilityPolicy` 枚举：`FLUSH_ON_COMMIT` / `WRITE_ON_COMMIT` / `BACKGROUND_FLUSH`，每策略一个
  `awaitCommitDurable(RedoLogManager, Lsn commitLsn, Duration timeout) -> boolean`（Strategy via enum，§8）。
- `RedoLogManager` 补三阶段中的「write」：`write()`（drain pending 到 OS cache，不 fsync）、`writtenToDiskLsn()`、
  `waitWritten(target, timeout)`；`flush()` 复用同一 drain 再额外 fsync（行为不变）。
- 三策略单测：FLUSH→`flushedLsn` 覆盖 commitLsn；WRITE→`writtenLsn` 覆盖但 `flushedLsn` 不变；
  BACKGROUND→两者都不立即推进（靠后台 `RedoFlushWorker`）。

## 关键决策

- 策略决策属 commit 层（§8：「redo 只提供 waitWritten/waitFlushed，不决定事务是否对外提交成功」）。本片只到
  **redo 原语 + 策略选用 + 单测（test-wired）**；生产 `TransactionManager.commit`/DML facade 接线 = 2.1，显式延后。
- `write()`/`flush()` 共用 `drainPendingToWritten`；`writtenToDiskLsn` 单调，state lock 保护 + 新 `writtenAdvanced` Condition；
  `ioLock` 仍串行文件 write/fsync（沿用 0.1 拆锁，append 不被 fsync 阻塞）。
- 内存模式 `write()`/`waitWritten` 与 `flush()`/`waitFlushed` 对称：不伪造 durable，`writtenToDiskLsn` 恒 0。
- 不用 `synchronized`，所有等待带 timeout（不无界等待）。
- `SYNC_EVERY_N_MS` 暂不单列：等价于 `BACKGROUND_FLUSH` + 现有 `RedoFlushWorker` 周期 flush；需要独立等待语义再加。

## 非目标

- 不接生产 `TransactionManager.commit` / `MiniTransaction.commit`（无 DML facade，2.1）。
- 不做 LogBlock header/trailer/checksum（0.20b，独立 epic，与当前 batch-frame 格式不同）。
- 不改 redo 文件格式、不动 0.18 文件环、不做 group commit / 多 writer 线程。

## 验收测试

- `flushOnCommitMakesCommitLsnDurable`：FLUSH_ON_COMMIT 后 `flushedToDiskLsn >= commitLsn`，返回 true。
- `writeOnCommitAdvancesWrittenButNotFlushed`：WRITE_ON_COMMIT 后 `writtenToDiskLsn >= commitLsn` 且 `flushedToDiskLsn` 未变。
- `backgroundFlushDoesNotWaitOnCommit`：BACKGROUND_FLUSH 立即返回 true，written/flushed 未推进；手动 flush 后才 durable。
- `waitWrittenReturnsWhenWritten` / `waitWrittenTimesOutWhenNoWriter`：边界与 timeout。
- `writtenToDiskLsnMonotonicAcrossWriteThenFlush`：write 后 flush，writtenLsn 不回退、flushedLsn 跟上。
- 回归：`RedoLogManagerTest`、`RedoRuntimeRecoveryTest`、`CheckpointCoordinatorTest`（flush/durable 行为不变）。

## 文档更新要求

- `current-implementation-map.md`：redo core 增 `DurabilityPolicy` + `write()`/`waitWritten()`/`writtenToDiskLsn()`；
  Known Gap「commit 不等 redo durable」改为「DurabilityPolicy 已抽象 + 三阶段原语就绪，生产 trx-commit 接线待 2.1」。
- `storage-backlog.md`：0.20 标 DurabilityPolicy(0.20a) 已落，剩 LogBlock checksum(0.20b)。
- 代码注释说明：`write` 只到 OS page cache（崩溃可能丢），`flush` 才 fsync durable；WRITE_ON_COMMIT 的宕机丢失风险。
