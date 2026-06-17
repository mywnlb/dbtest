# Spec：物理页 envelope（FilePageHeader/Trailer + checksum）— innodb-disk-manager-design §5.3

- 日期：2026-06-11
- 关联设计：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-disk-manager-design.md`（§5.3 Page envelope、§6.5 IndexPage 区域、§14 ChecksumStrategy）
- 上游依赖：buf（PageGuard）、fsp（PageLayouts.FIL_PAGE_DATA=38、FileAddress）
- 状态：brainstorming 评审通过——做 envelope 类型 + checksum 工具；**不**接入 PageStore IO、**不**回填现有 fsp 页；redo/pageLSN 暂停（pageLsn 字段恒 0）。

## 1. 背景与目标

设计 §5.3 要求所有磁盘页共享统一 envelope：页首 `FilePageHeader`（checksum/spaceId/pageNo/prev/next/pageLsn/pageType）+ 页尾 `FilePageTrailer`（checksumTrailer/low32Lsn）。此前各 fsp 页只预留了 `FIL_PAGE_DATA=38` 字节并填零，envelope 一直未实现。本片把它做出来，作为记录层 `IndexPage`（§6.5）的物理外壳，也为将来 flush/redo 提供 pageLSN/checksum 地基。

## 2. 范围与非目标

**做**：`PageType` 枚举、`FilePageHeaderLayout`/`FilePageTrailerLayout` 偏移、`FilePageHeader` record（编解码）、`PageChecksum`（compute/stamp/verify）。全部位于 `cn.zhangyis.db.storage.fsp`。

**不做**（注释标注）：
- 不接入 `PageStore` 读写 IO 的 checksum 校验（属 flush/recovery，暂停）。
- 不回填现有 SpaceHeader/XDES/Inode/系统页的头部（无 redo/pageLSN 时收益小、侵入大）。记录层 `IndexPage` 是第一个真正用 envelope 的页。
- redo/pageLSN 暂停：`pageLsn` 字段就位但恒写 0；trailer `low32Lsn` 由 stamp 同步为 pageLsn 低 32 位（现 0）。
- 不实现 ChecksumStrategy 接口族（YAGNI）：`PageChecksum` 直接给 CRC32 实现；将来 flush 需要多策略再抽接口。

## 3. 组件（`storage.fsp`）

### 3.1 `PageType`（枚举）
带稳定 int code（落盘，测试钉死，改序破坏已写盘页）：`ALLOCATED=0`（空闲/未用，零初始化页天然解码为它）、`FSP_HDR=1`、`IBUF_BITMAP=2`、`INODE=3`、`SDI=4`、`INDEX=5`。
- `int code()`；`static PageType fromCode(int)`：未知 code → `FspMetadataException`（页上类型损坏）。

### 3.2 `FilePageHeaderLayout`（页首 0..37，= FIL_PAGE_DATA）
| 字段 | 偏移 | 字节 |
| --- | --- | --- |
| CHECKSUM | 0 | int 4 |
| SPACE_ID | 4 | int 4 |
| PAGE_NO | 8 | int 4 |
| PREV_PAGE_NO | 12 | int 4 |
| NEXT_PAGE_NO | 16 | int 4 |
| PAGE_LSN | 20 | long 8 |
| PAGE_TYPE | 28 | int 4（PageType.code）|
- 30..37 预留（实际 fields 到 32）；`SIZE = PageLayouts.FIL_PAGE_DATA = 38`。
- 简化点：pageType 用 4 字节 int（设计/InnoDB 为 2 字节；PageGuard 无 2 字节访问器，用 int 省去新增 API）；pageNo/prev/next 用 4 字节（InnoDB 风格；教学页号在 int 范围；与 FileAddress 的 8 字节 pageNo 是不同结构）。

### 3.3 `FilePageTrailerLayout`（页尾 8 字节，起于 `pageSize-8`）
| 字段 | 条内偏移 | 字节 |
| --- | --- | --- |
| CHECKSUM_TRAILER | 0 | int 4 |
| LOW32_LSN | 4 | int 4 |
- `SIZE = 8`；`static int offset(PageSize)` = `pageSize.bytes() - SIZE`。

### 3.4 `FilePageHeader`（record）
`(SpaceId spaceId, long pageNo, long prevPageNo, long nextPageNo, long pageLsn, PageType pageType)`。checksum 不入 record（派生值，由 `PageChecksum` 盖）。
- `static final long FIL_NULL = 0xFFFFFFFFL`：prev/next 无邻居哨兵。
- 构造校验：spaceId/pageType 非空；pageNo ≥ 0；prevPageNo/nextPageNo 为 FIL_NULL 或 ≥0；pageLsn ≥ 0。
- `void writeTo(PageGuard g)`（要求 X）：写 SPACE_ID/PAGE_NO/PREV/NEXT（`(int)` 截断，FIL_NULL→0xFFFFFFFF）、PAGE_LSN、PAGE_TYPE(code)；**不**写 checksum。
- `static FilePageHeader readFrom(PageGuard g)`：读各字段；prev/next 用 `readInt(...) & 0xFFFFFFFFL` 还原（-1→FIL_NULL）；pageType 用 `PageType.fromCode`。

### 3.5 `PageChecksum`
CRC32 over 页体 `[4, pageSize-8)`（排除 4 字节头 checksum 与 8 字节 trailer）。
- `static int compute(PageGuard g, PageSize pageSize)`：`readBytes(4, pageSize-12)` → CRC32 → `(int) value`。
- `static void stamp(PageGuard g, PageSize pageSize)`（要求 X，"封页"）：读 header.PAGE_LSN → 计算 checksum → 写 header.CHECKSUM、trailer.CHECKSUM_TRAILER（均 = checksum）、trailer.LOW32_LSN（= pageLsn 低 32 位）。
- `static boolean verify(PageGuard g, PageSize pageSize)`：重算 checksum，与 header.CHECKSUM 且 trailer.CHECKSUM_TRAILER 同时相等才返回 true。

## 4. 数据流与并发

- 典型"封页"：`mtr.getPage(X)` → `header.writeTo(g)` → 写 body → `PageChecksum.stamp(g, ps)`；读校验 `PageChecksum.verify(g, ps)`。本片只提供工具，不自动接入 IO。
- 全部经 PageGuard 绝对读写；写要求 X latch（PageGuard 自校验）。no-redo：不产 redo、不声明 crash-safe。

## 5. 异常

- `FspMetadataException`：`PageType.fromCode` 未知 code（页上类型损坏）。
- `DatabaseValidationException`：FilePageHeader 构造非法值、PageChecksum/header 的 null/越界（经 PageGuard 透传）。

## 6. 测试

- `PageTypeTest`：code 钉死（ALLOCATED=0、FSP_HDR=1…INDEX=5）；`fromCode` 往返；未知 code→FspMetadataException。
- `FilePageHeaderTest`（真实 PageGuard）：writeTo→readFrom 往返（含 prev/next=FIL_NULL 与具体值）；构造非法值拒绝。
- `PageChecksumTest`：stamp 后 verify=true；篡改页体一字节后 verify=false；stamp 写入的 trailer LOW32_LSN = header pageLsn 低 32 位；4K 与 16K 页 trailer 偏移正确（stamp/verify 都过）。

## 7. 简化点（注释标注）

- pageType 4 字节、pageNo/prev/next 4 字节；pageLsn 恒 0（redo 暂停）；low32Lsn 由 stamp 同步。
- checksum 用 CRC32、不接 IO、无 ChecksumStrategy 接口；不回填现有 fsp 页。
- envelope 放 `storage.fsp`（与 PageLayouts/FileAddress 同处）；记录层将引用之。

## 8. 后续衔接

- 记录层（innodb-record-design.md）：`IndexPage` 用 `FilePageHeader`（pageType=INDEX、prev/next 维护 leaf 兄弟链）框定页，body 内再放记录层 PageHeader/Infimum/Supremum/UserRecords/PageDirectory。
- redo/flush 切片：MTR commit 盖 pageLsn；flush 前 `PageChecksum.stamp`、读盘 `verify`；接入 PageStore IO；按需抽 ChecksumStrategy。
