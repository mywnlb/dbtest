# Slice: Purge 驱动 Undo Tablespace 自动截断

- 日期：2026-07-22
- 关联设计：`innodb-undo-log-purge-design.md` §6.2、§7.7–7.8、§8；`innodb-transaction-mvcc-design.md` purge 后台协作。
- 前置：bounded multi-worker purge、persistent page3 history/cache/free、可恢复 `UndoTablespaceTruncationService` 均已生产接线。
- 定位：补齐当前单 rseg、单系统 undo space 的 purge→truncate 在线调度，不扩展持久身份模型。

## 1. 范围与默认策略

做：
- 复用 `PurgeDriverWorker` 线程；每次成功 purge batch 后执行有冷却的 maintenance callback，不新建后台线程。
- 默认启用；增长至少 1 extent，且距上次检查至少 30 秒时才尝试。
- maintenance 使用公平、立即返回的 tablespace X lease；普通访问或排队 owner 存在时返回 deferred，不阻塞用户流量。
- page3 history/active slot/reuse transition busy 是可重试 deferred；cache/free owner 由既有 drain 协议回收后继续。
- recovery 与 live scheduler 共享同一 truncate service、registry、redo、flush、reuse directory 和 access controller。
- 提供检查、跳过、deferred、完成、失败、回收页数、最后 epoch/结果/失败的只读 metrics。

不做：
- 不实现多 rseg、多 undo tablespace 候选选择、blocked-head 调度或 temporary undo。
- 不新增持久 `TRUNCATE_CANDIDATE`；候选只在内存中判断，磁盘状态仍从 `ACTIVE/INACTIVE` 进入 `TRUNCATING`。
- 不改变 page0 lifecycle、page3 rseg、redo 或 `RollPointer` 格式。
- 不把 truncate 放进 purge worker；worker 只处理 history log，dispatcher finalization 完成后才允许 maintenance。

## 2. 数据流与并发

1. purge target 返回稳定 `PurgeSummary`；此时 worker token、row guard、history lease 与 MTR 均已释放。
2. driver 在内部锁下线性化 maintenance claim；若 `STOPPING` 已发布则跳过，否则本轮维护归 driver owner。
3. scheduler 用单调时钟检查 cooldown；到期后零等待尝试 X lease，失败返回 `DEFERRED_ACCESS_BUSY`。
4. X lease 内读 persisted lifecycle 与物理大小；`TRUNCATING` 必须续作，稳定状态按 persisted initial size 计算阈值。
5. page3 history/active/reuse gate 不满足时无物理副作用返回 deferred；满足后 drain cache/free 并复核 inode 全空。
6. 复用 marker durable→flush/checkpoint→buffer invalidate→truncate→FSP/page3 rebuild→final state 发布协议。
7. shutdown 不打断已 claim 的 truncate；close 只按共享 deadline 等待，不能在线程位于物理阶段时强制中断。

## 3. 失败、观测与验收

- threshold 不足记 skipped；access/history/active/reuse busy 记 deferred，均不让 purge driver 进入 `FAILED`。
- orphan inode、owner mismatch、格式损坏、IO/WAL/flush 错误继续抛出；scheduler 先记录失败，再让 driver `FAILED`。
- marker durable 前失败不得缩短文件；marker durable 后失败保持 `TRUNCATING`，重启由既有 recovery 幂等续作。
- 只有 shutdown 中的 `PurgeWorkerStoppedException` 可作为正常取消；其它 maintenance 失败不得因 `STOPPING` 被吞掉。
- TDD 覆盖配置、零等待公平 lease、所有 attempt 状态、cooldown/metrics、driver stop 竞态和 engine 自动回收/重启续作。
- 实现后更新 `current-implementation-map.md`、`storage-backlog.md` 与两份 undo/transaction 厚设计，并运行固定工具链全量测试。
