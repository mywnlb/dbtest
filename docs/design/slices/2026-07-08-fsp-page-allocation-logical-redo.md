# 0.19b FSP Page Allocation Logical Redo

## 目标

本 slice 在 0.19a dispatcher registry 之后，落第一个可持久化的 FSP/page allocation 逻辑 redo。

- 新增 `FSP_PAGE_ALLOC` 持久 record，表达某个 page 被分配给某个 segment 的 allocation intent。
- 新增 FSP redo apply handler，并注册进生产 recovery dispatcher。
- recovery handler 只做恢复期安全副作用：按目标页号 `ensureCapacity(space, pageNo+1)`，为后续 `PAGE_INIT` / `PAGE_BYTES` 建立文件容量前提。
- 现阶段仍保留 page0/page2/XDES/INODE 的物理 `PAGE_BYTES` redo 作为权威元数据恢复来源。

## 关键决策

- `FspPageAllocationRecord` 字段为：`PageId allocatedPageId`、`int inodeSlot`、`SegmentId segmentId`、`boolean autoExtendRetry`。
- `autoExtendRetry` 只表达本次页号是否来自 `DiskSpaceManager` autoextend 后的第二次 allocator 尝试；handler 不重新执行分配决策。
- record 编码追加新 tag，必须更新 `RedoRecord` sealed permits、`RedoRecordType`、`RedoBatchFrameCodec` 和 `byteLength()`。
- `DiskSpaceManager.doAllocatePage` 改为返回内部 allocation result（页号 + 是否 autoextend retry），在 `initAllocatedPage` 前收集该 record；同一 MTR 内仍会产生后续 `PAGE_INIT(ALLOCATED)`。
- 新 `MiniTransaction` 追加逻辑 redo 的公开方法只负责把 record 交给 collector，不抑制 PageGuard 产生的物理字节 redo，不改变 pageLSN 盖戳模型。
- handler 不写 page0/page2，不 stamp pageLSN；批末 pageLSN 仍由 `PageRedoApplyHandler` 处理物理页。
- `RedoApplyDispatcher.pageDispatcher()` 作为兼容生产入口保留名字，但内部默认注册 FSP handler + page handler；是否后续改名另列清理。
- `affectedPages` 至少返回 allocated page；这样 `FORCE_SKIP_CORRUPT_TABLESPACE` 会在触碰 `PageStore` 前跳过该空间。

## 非目标

- 不实现 `FREE_PAGE`、`UPDATE_XDES`、`SEGMENT_INODE_UPDATE`、`SPACE_HEADER_UPDATE`。
- 不删除或压缩现有 FSP metadata `PAGE_BYTES`。
- 不让 recovery 重新运行 allocator、free-list 选择、segment extent 策略或 B+Tree split/merge 决策。
- 不修改 log block checksum/header-trailer，也不引入 per-record LSN。
- 不把 0.23b 的 `MtrRedoCategory` 写入 redo 文件；它仍只是本地诊断。

## 验收测试

- codec round-trip：`FspPageAllocationRecord` 编码/解码后字段和 `byteLength()` 精确一致。
- dispatcher registry：生产 `pageDispatcher()` 同时注册 FSP handler 与 page handler，旧 PAGE_INIT/PAGE_BYTES replay 继续通过。
- handler 行为：只含 `FSP_PAGE_ALLOC` 的 batch 可幂等 `ensureCapacity` 到目标页后一页，不读写数据页内容。
- FORCE_SKIP：skip predicate 命中该 space 时，不打开 FSP handler session，也不调用 `PageStore.ensureCapacity`。
- DiskSpaceManager 集成：一次 `allocatePage` 的 redo batch 中，`FSP_PAGE_ALLOC` 出现在 `PAGE_INIT(ALLOCATED)` 之前。
- 现有恢复回归继续通过：page allocation crash replay、extend-on-demand、space file reconcile、redo 文件环恢复。

## current map 更新要求

- Redo collect 行说明 MTR collector 已可追加持久 FSP allocation 逻辑 record。
- Redo replay 行说明 `pageDispatcher()` 生产默认注册 `FspPageAllocationRedoHandler` + `PageRedoApplyHandler`。
- Known gaps 中把“仅 2 种持久 redo 记录类型”改为“已有 FSP_PAGE_ALLOC，仍缺 XDES/INODE/SPACE_HEADER/FREE_PAGE 等完整 MLOG”。
- Disk/FSP 小节说明 0.19b 只是 allocation intent，不替代 page0/page2 物理元数据 redo。

## 复核

- 已按 backlog、redo 设计、disk manager 设计、btree 边界、current map、源码六处核对。
