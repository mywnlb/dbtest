# 0.19c FSP Metadata Delta Logical Redo

## 目标

本 slice 接在 0.19b `FSP_PAGE_ALLOC` 之后，为 FSP 元数据写入补持久化、可重放的逻辑 delta。

- 覆盖 page0 space header、page0 XDES、page2 segment inode、FLST base/node、page free intent。
- 新增 FSP metadata delta record，使 redo 文件能表达“空间账本字段变更”，不只依赖裸 `PAGE_BYTES`。
- 新 record 必须能在没有对应 `PAGE_BYTES` 的合成恢复测试中独立把目标页 patch 到 after image。
- 生产收集阶段暂时保留现有 `PAGE_BYTES`，本片不做物理监听抑制，避免一次切片同时改写 MTR 收集模型。

## 关键决策

- 新增通用 `FspMetadataDeltaRecord(PageId pageId, FspMetadataDeltaKind kind, long subjectId, int subIndex, int offset, byte[] afterImage)`。
- `kind` 使用稳定磁盘码，首批包含 `SPACE_HEADER_FIELD`、`XDES_FIELD`、`XDES_BITMAP_BYTE`、`INODE_SLOT_IMAGE`、`INODE_FIELD`、`INODE_FRAGMENT_SLOT`、`FLST_BASE_FIELD`、`FLST_NODE_FIELD`。
- `subjectId` 表达 extentNo、inodeSlot 或 0；`subIndex` 表达 bitmap byte、fragment slot 或字段内序号；二者用于审计和边界校验，不参与 Java 对象图恢复。
- 新增 `FspPageFreeRecord(PageId freedPageId, int inodeSlot, SegmentId segmentId)`，表达 free page intent；真实账本变化仍由 metadata delta record 承载。
- `PageRedoApplyHandler` 扩展为统一 page patch session，支持 `PAGE_INIT`、`PAGE_BYTES`、`FspMetadataDeltaRecord`；所有同页 patch 进入同一个 batch cache，批末只写回一次并盖 batch end LSN。
- FSP intent handler 支持 `FSP_PAGE_ALLOC` 的 `ensureCapacity` 与 `FspPageFreeRecord` 的 no-op/affectedPages；metadata delta 不另建会写 page0/page2 的独立 handler，避免 handler finish 顺序造成双写覆盖。
- FSP repository 在每次写 `PageGuard` 前后追加逻辑 delta：记录 after image，同时现有写监听继续收集相同字节的 `PAGE_BYTES`。
- 逻辑 delta 的 offset/length 必须落在对应 FSP 元数据区域内：space header 固定区、XDES entry、inode entry、FLST base/node 或 fragment slot。
- record 编码追加 tag，不改已有 tag；`byteLength()` 必须精确等于落盘编码长度。
- MTR `MtrRedoCategory.FSP_METADATA_BYTES` 继续用于 `PAGE_BYTES` 诊断；逻辑 delta 用同一分类写入 `MtrRedoEntry`。

## 非目标

- 不删除 FSP metadata `PAGE_BYTES`，不抑制 `PageGuard` 写监听；替代物理字节 redo 留给 0.19d。
- 不引入 dispatcher 跨 handler 共享 page cache；本片通过扩展 page patch handler 避免该需求。
- 不重跑 allocator、free-list 选择、segment extent 选择或 B+Tree split/merge 决策。
- 不实现 undo record、btree page op、trx state redo，也不改 log block checksum/header-trailer。
- 不扩大 XDES 到独立管理页；仍只覆盖当前代码支持的 page0 首批 XDES 区域。
- 不把 lifecycle truncate marker 迁到逻辑 delta；该路径暂由现有 `PAGE_BYTES` 保护。

## 验收测试

- codec round-trip：`FspMetadataDeltaRecord` 和 `FspPageFreeRecord` 字段、payload、`byteLength()` 精确一致。
- handler replay：只含 metadata delta、无 `PAGE_BYTES` 的 batch 能 patch page0/page2 并把 pageLSN 盖到 batch end。
- mixed replay：同一 batch 同页同时含 metadata delta 与对应 `PAGE_BYTES` 时，最终页内容一致且只写回一次。
- FORCE_SKIP：skip predicate 命中 metadata delta 或 free intent 的 space 时，不读写、不 ensure 该表空间。
- FSP integration：`createSegment`、`allocatePage`、`freePage`、`dropSegment`、`reserveSpace` 的 metadata 写入能看到对应 delta 诊断条目。
- 异常边界：非法 kind、越界 offset、过长 payload、inode/fragment/bitmap subIndex 越界均抛 redo/FSP 领域异常。
- 回归：0.19a/0.19b redo apply、autoextend crash-safe、FSP allocation、free/drop segment 既有测试继续通过。

## current map 更新要求

- Redo collect 行补充 FSP metadata delta 与 page free intent 已进入持久 redo record 集合。
- Redo replay 行说明 `PageRedoApplyHandler` 统一承载物理 page patch 与 FSP metadata delta patch，避免 handler 双写。
- Disk/FSP 缺口改为：FSP metadata logical delta 已可 replay，但生产仍并存 `PAGE_BYTES`；0.19d 再做物理 redo 抑制/替代。
- Backlog 0.19 剩余改为 undo/btree/trx 逻辑 redo、FSP metadata `PAGE_BYTES` 去重、可能的独立 XDES 管理页。

## 复核

- 已按 storage-backlog、redo 设计、disk manager 设计、btree 依赖边界、current map、当前源码六处核对；关键风险是双 handler 写同页，设计用统一 page patch handler 避开。
