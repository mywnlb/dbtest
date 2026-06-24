# Slice: page0 FSP_HDR envelope validation (D-next)

依据：`innodb-disk-manager-design.md` §5.3（物理页信封）+ `current-implementation-map.md` Disk Manager 缺口
“page0 still lacks FSP_HDR envelope validation”。

## 目标

让 page0 携带统一 FilePageHeader 信封并在打开/恢复时校验，关闭“page0 物理信封无校验”这一独立缺口：

- `SpaceHeaderRepository.initialize` 在 page0 写入 FSP_HDR 信封头（spaceId / pageNo=0 / prev=next=FIL_NULL / pageType=FSP_HDR）。
- `PageZeroTablespaceMetadataLoader` raw 读 page0 后，先做信封校验：`pageType==FSP_HDR` 且 `pageNo==0`，
  不通过抛 `TablespaceCorruptedException`，阻止把绑定错误/损坏的 page0 注册成可用表空间。

## 关键决策

- **写入唯一收口在 `initialize`**：它是 `DiskSpaceManager.createTablespace` 与 `UndoTablespaceFspRebuilder.rebuild`
  共同的 page0 初始化点。在其已持的 page0 X guard 上加一次 `PageEnvelope.writeHeader`，两条路径一并修复；
  随即移除 rebuilder 中现在重复的 page0 `writeHeader`（保留 `newPage(FSP_HDR)` 的清零 + PAGE_INIT redo）。
- **信封落盘走 MTR/PageGuard**：writeHeader 经 page guard 产生 PAGE_BYTES redo，replay 能重建信封；loader 读侧仍走
  现有 raw + 共享 lease 路径，只新增纯字节偏移比较（offset 8 = PAGE_NO、offset 28 = PAGE_TYPE）。
- **损坏用领域异常**：信封不符抛 `TablespaceCorruptedException`（与 reconcile 路径一致），不抛裸校验异常；
  既有 spaceId mismatch 校验保持不变（不在本切片改其异常类型）。

## 非目标（明确推迟）

- **checksum / trailer 校验不做**：当前 `flushAll`/淘汰写回路径不调用 `PageImageChecksum.stamp`（只有
  `FlushCoordinator` 盖 checksum），loader 又是 raw 直读，合法 page0 的 checksum 仍为 0；现在校验会误判损坏。
  待“写盘统一盖 checksum / page0 读经校验路径”落地后再单独接入。
- 不改 `PageStore` 的 registry-free/state-free 边界；不接 tablespace discovery；不动普通 lifecycle 持久化。
- 不引入新页类型校验以外的 FSP 语义校验（FLST 一致性仍只走 PageGuard 路径，raw loader 不复刻）。

## 验收测试

- `SpaceHeaderRepositoryTest.initializeStampsFspHdrEnvelopeOnPageZero`：initialize 后 page0 信封 = FSP_HDR / pageNo=0 / spaceId。
- `PageZeroTablespaceMetadataLoaderTest.rejectsPageZeroWithNonFspHdrPageType`：pageType 被改成 ALLOCATED → load 抛 corrupted。
- `PageZeroTablespaceMetadataLoaderTest.rejectsPageZeroWithWrongPageNo`：PAGE_NO 被改非 0 → load 抛 corrupted。
- 回归：`rebuildsMetadataFromDiskPageZero`（page0 现带 FSP_HDR 仍正常 load）、SpaceHeaderRepository 既有 round-trip、
  UNDO truncation 全套（rebuilder 改动后仍重建出合法 page0）。

## current map 更新要求

- Disk Manager 缺口表：把 “page0 still lacks FSP_HDR envelope validation” 改为已校验（pageType+pageNo），
  并新增一行记录 checksum/trailer 校验 deferred 及原因。
- Disk Manager 数据链：Create / Open / Recovery open 三行补“page0 FSP_HDR 信封写入 + 打开时信封校验”。
- Package Status：`PageZeroTablespaceMetadataLoader` 与 `SpaceHeaderRepository` 注记补 FSP_HDR 信封职责。
