# B+Tree Node / Root Structure Redo Slice

## 目标

- 把 internal node pointer 页体与 root level/index header 的最终 after-image 接入 `BTreePageDeltaRecord`。
- 恢复期继续只做 page-local patch，不重新执行 split、merge、redistribute 或 root shrink。
- 精确过滤被结构 delta 最终 after-image 覆盖的 `PAGE_BYTES`，保留未覆盖或中间态不同的物理 redo。
- 不改变 B+Tree 对外 API、页格式、redo tag、LSN、pageLSN、WAL 与 latch 顺序。

## 关键决策

- 复用已稳定落盘的 `NODE_POINTER_AREA(3)` 与 `ROOT_LEVEL_OR_HEADER(4)` kind，不新增 record type/tag。
- node pointer snapshot 只用于 `PAGE_LEVEL > 0` 的 internal/root 页，leaf row bytes 仍走 `PAGE_BYTES`。
- 每次完整结构动作结束后按页最多收集三个非重叠 patch，而不是每插入一个 pointer 捕获整页。
- patch 1 为 INDEX header `[38,66)`；root 使用 `ROOT_LEVEL_OR_HEADER`，非 root 使用 `PAGE_FORMAT_IMAGE`。
- patch 2 为 record heap `[66,heapTop)`，含 infimum/supremum、有效 pointer、garbage record 字节。
- patch 3 为 Page Directory `[dirStart,pageSize-8)`；free-space 空洞不进入 redo。
- `subjectId`：header 携 level；heap/directory 携当前 pointer count，供诊断，不参与恢复算法。
- `RecordPage` 只增加有界只读结构快照 API，不暴露 `PageGuard`、frame 或写原语。
- root shrink 到 level 0 时只收集 `[56,66)` 的 root level/index identity；叶记录区不误标为 node pointer。
- root split/grow 与 internal split 在页面完成 format + 全部 pointer 写入后捕获一次最终快照。
- parent 单 pointer 插入/摘除在该原子动作完成后捕获；redistribute 的 parent 替换在 remove+insert 后只捕获一次。
- merge internal 的 survivor 写完、parent 摘 victim 后分别捕获，victim 随 FSP free intent 回收，不为其记最终结构快照。

## 校验与失败边界

- `BTreePageDeltaRecord` 按 kind 校验固定 header offset/length，node area 禁止越过 INDEX body 起点。
- recovery 仍校验 page bounds，并新增 kind/offset 语义校验；非法 root/node delta 视为 redo 损坏。
- MTR collector 仅在 pageId、区间和字节内容均匹配时过滤物理 delta；不同的中间态写仍保留并按序重放。
- snapshot 在 B+Tree 已持 X latch 的 MTR 内读取，不增加 latch、lock、IO 或等待。
- operation redo budget 的 actual settlement 继续兜底；若 profile 低估，保持既有 COMMITTING fail-stop。

## 非目标

- 不把 leaf 用户记录、聚簇行或二级索引 entry 改成逻辑 redo。
- 不新增 PAGE_MAX_TRX_ID、PAGE_BTR_SEG_LEAF/TOP、B-link/OLC 或 DD root metadata。
- 不重写 RecordPage 编码、PageDirectory 算法、split/merge 决策或 redo 文件格式版本。
- 不做跨 MTR 结构压缩、whole-page image 或 recovery 逻辑搜索。

## 验收

- codec round-trip 与 recovery replay 覆盖 node header/heap/directory、root level/header 和非法布局。
- root leaf split、internal root grow、non-root internal split、redistribute/merge/root shrink 产生最终结构 delta。
- 每个结构 delta 覆盖的同值 `PAGE_BYTES` 不再进入持久 batch，未覆盖/中间态不同写仍保留。
- crash replay 后 internal pointer 顺序、child page、root level 与 index id 与提交后页面一致。
- 更新 current map/backlog；生产代码无 monitor/裸运行时异常；固定 Gradle/JDK 全量回归通过。
