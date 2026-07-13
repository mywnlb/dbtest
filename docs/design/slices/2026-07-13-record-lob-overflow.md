# 0.21h Record LOB / overflow slice

## 目标

- 补齐 TEXT/BLOB 家族和 JSON 的 Record 物理类型。
- 记录内支持 inline payload 或稳定 external reference。
- 通过 `storage.api.lob.LobStorage` 写、读、释放 off-page 页链。
- 复用 FSP `SegmentPurpose.LOB` 与 `SpaceReservationKind.BLOB`。
- 复用 PAGE_INIT/PAGE_BYTES/FSP redo，使通用恢复可重放 LOB 页。

## 关键决策

- `record` 只认识 `LobReference`，不依赖 Buffer Pool/FSP。
- `LobStorage` 是跨 record、buffer、FSP 的稳定存储门面。
- LOB 页追加 `PageType.BLOB=8`，既有落盘 code 不变。
- 每个页保存 chain index、page count、总长、segment identity 与整值 CRC32。
- FIL_PAGE_PREV/NEXT 是页链权威链接；末页 NEXT=FIL_NULL。
- external reference 保存 space/first page/length/page count/segment/CRC32。
- 记录保留最多 32B inline prefix，供受限前缀索引比较。
- TEXT/BLOB/JSON 的记录编码进入变长目录，storage kind 为 OVERFLOW_CAPABLE。
- 记录 inline payload 上限为 256B；更大逻辑值必须先由 LobStorage externalize。
- TINY/LONG 上限采用 MySQL 字节级量级；LONG 在 Java 中封顶 Integer.MAX_VALUE。
- JSON v1 以严格 UTF-8 文本存储，校验语法，不实现 MySQL binary JSON。
- JSON 不进入核心索引比较；TEXT/BLOB 只允许显式 prefix key。
- Unicode TEXT prefix 不截断 UTF-8 code point。
- 页链写入在一个 MTR 内完成，保证引用发布前整链 redo 原子可见。
- 教学简化：单 MTR 会同时固定所有新 LOB 页，超大值后续改为 staged chain protocol。
- free 先完整校验页链，再更新 FSP 并把释放页重新初始化为 ALLOCATED。
- 不在持 page latch 时等待事务 row lock；本切片没有事务锁等待。

## 非目标

- SQL parser/binder 的类型语法与 4GiB Java 流式值。
- MySQL binary JSON、partial update、JSON path index。
- locale tailoring、UCA contractions/expansions。
- 自动把 DML 行值 externalize 或把引用发布到聚簇记录。
- undo/purge 自动回收已被行版本替换的 external reference。
- 跨 MTR staged/incomplete LOB chain 的恢复协议。

## 验收

- 类型工厂、边界、inline/external 编解码和损坏 envelope 有单测。
- JSON 非法文本、非前缀索引和超 inline 值 fail-closed。
- 多页写读往返、整值 CRC、链链接、segment identity 有协作测试。
- 释放后旧引用不可读取，BLOB reservation 与 FSP free redo 可观察。
- PAGE_INIT(BLOB)+PAGE_BYTES 可由既有 redo dispatcher 重放。
- StorageEngine 生产组合根暴露同实例 LobStorage。
- 更新 current implementation map、backlog 和 reserved/unwired 状态。
- 固定 JDK/Gradle 全量测试通过，测试数不得少于 1210。
