# 0.23b MTR/Redo 剩余纪律

## 目标

本 slice 收敛 `storage-backlog.md` 中 0.23a 之后剩余的 MTR/Redo 纪律：

- savepoint 只能释放未触碰持久页的资源。
- MTR commit 必须在 dirty/pageLSN 发布后才推进 closed LSN。
- redo collector 增加本地分类接缝，为后续 MLOG 细化留下审计边界。

参考文档：

- `innodb-record-design.md`：Record 不直接写 redo 文件，只表达页内修改。
- `innodb-btree-design.md`：B+Tree 结构修改必须在 MTR 内完成，恢复只重放物理页修改。
- `innodb-disk-manager-design.md`：FSP/page allocation 的 redo、latch、memo 释放顺序必须由 MTR 约束。
- `innodb-redo-log-design.md`：closed LSN 只能覆盖已经发布 dirty view 的 redo range。

## 关键决策

- 不新增持久 redo tag，不修改 `RedoBatchFrameCodec`，恢复路径仍只识别 `PAGE_INIT` / `PAGE_BYTES`。
- `MtrRedoCategory` 只存在于 MTR collector 内部诊断条目：`PAGE_INIT`、`PAGE_BYTES_GENERIC`、`RECORD_PAGE_BYTES`、`BTREE_STRUCTURE_BYTES`、`FSP_METADATA_BYTES`、`UNDO_PAGE_BYTES`。
- `MiniTransaction.enterRedoCategory(category, reason)` 使用 try-with-resources 管理嵌套分类；scope 必须 LIFO 关闭。
- `PAGE_INIT` 只能由 `MiniTransaction.newPage` 产生，普通分类 scope 只影响后续 `PAGE_BYTES`。
- read-only / empty MTR 的空 redo range 不得关闭前面未发布的 redo gap。

## 非目标

- 不实现完整逻辑 MLOG、多 redo apply handler 或 B+Tree 专用 redo。
- 不实现 MTR content undo；`rollbackUncommitted` 仍只释放 memo。
- 不改变 `DurabilityPolicy` 消费位置，commit 刷盘等待仍由 storage DML facade 等上层编排。
- 不把分类写入 redo 文件，也不让 recovery 依赖分类信息。

## 验收测试

- savepoint 后 touched page rollback 必须失败。
- savepoint 前 touched page 在 commit 后仍盖 batch end LSN。
- 多页 MTR 的所有 touched page 使用同一个 end LSN。
- read-only MTR 不推进 closed LSN，不关闭外部未发布 redo range。
- `newPage` 产生 `PAGE_INIT` 分类；普通写默认 `PAGE_BYTES_GENERIC`。
- 分类 scope 嵌套后按 LIFO 恢复；乱序关闭抛 `MtrStateException` 且不污染后续正确关闭。
- 分类不改变 `RedoRecordType`、redo 编码长度或 replay handler。

## current map 更新要求

- `Buffer Pool + MiniTransaction Slice` 标明 0.23b savepoint/commit/collector 分类已接。
- `Redo Log Layer Slice` 标明 collector 有本地 `MtrRedoEntry` 分类诊断，但持久 redo 仍只有两种物理 record。
- `Known Implementation Gaps` 中只保留完整逻辑 MLOG、多 handler、MTR content undo 等真实剩余缺口。

## 复核

- 已按 backlog、Record、B+Tree、Disk Manager、Redo/current map、源码测试六遍核对。
