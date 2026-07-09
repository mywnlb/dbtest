# 0.19h B+Tree Structure Page Delta Redo v1

## 目标

本 slice 为 B+Tree 结构页字段补第一版逻辑 redo，先覆盖 split/merge/root shrink 中最稳定的 sibling link。

- 覆盖 INDEX 页 FIL prev/next sibling 字段。
- 恢复期只 patch 页内 after-image，不重新执行 split、merge 或 root shrink。
- 同一 MTR 内被 sibling delta 精确覆盖的结构字节不再持久化为 `PAGE_BYTES`。

## 关键决策

- 新增 `BTreePageDeltaRecord(PageId, indexId, kind, subjectId, offset, afterImage)`。
- 新增 `BTreePageDeltaKind.SIBLING_LINKS`，并预留 page-format、node-pointer、root-header 分类。
- `BTreeRedoDeltas.writeSiblingLinks` 统一替代直接 `IndexPageHandle.writeSiblingLinks` 的生产写点。
- helper 在 `BTREE_STRUCTURE_BYTES` 分类下写真实页，再追加 `BTreePageDeltaRecord`。
- 新页初始化中被最终 sibling delta 覆盖的 generic header bytes 也允许精确过滤；`PAGE_INIT` + delta 已足够恢复最终状态。
- `PageRedoApplyHandler` 将 B+Tree delta 作为 page-local patch 处理。
- 本 slice 只接 `SplitCapableBTreeIndexService` 的 sibling link 写点。

## 非目标

- 不迁移 record row bytes、node pointer 数组、root level/header 全部格式 redo。
- 不实现 B-link right-link、OLC 版本重启或 PAGE_MAX_TRX_ID。
- 不改变 B+Tree split/merge/latch coupling 算法。
- 不让 redo replay 重跑 B+Tree 结构决策。

## 验收测试

- `BTreePageDeltaRecord` codec round-trip 与 `byteLength` 精确匹配。
- 无 `PAGE_BYTES` 的 sibling delta 可重放 FIL prev/next 并盖 batch end LSN。
- MTR 提交会过滤被 B+Tree delta 覆盖的结构 bytes。
- root split 生产路径会写 sibling-link logical delta，且 leaf 链仍双向正确。
- 既有多层 insert/scan/delete/merge/root shrink 用例不倒退。

## current map 更新要求

- B+Tree 当前状态补充 sibling-link redo v1 已接。
- Redo collect/replay 行加入 `BTREE_PAGE_DELTA`。
- Known gaps 中把“btree 专用 redo”改为“row/node/root 更细结构 redo 未迁移”。
