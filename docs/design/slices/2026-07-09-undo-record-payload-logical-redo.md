# 0.19g Undo Record Payload Logical Redo

## 目标

本 slice 接在 0.19e/0.19f undo metadata redo 之后，把完整 undo record 槽从普通物理字节迁到可命名的逻辑 redo。

- 覆盖 undo 页内 record 槽 `len u16 + payload`。
- undo page header / undo log header 仍由 `UndoMetadataDeltaRecord` 负责。
- 恢复期只 patch 槽 after-image，不重新执行 undo append 状态机。
- MTR 提交视图过滤被 payload delta 精确覆盖的 `UNDO_PAGE_BYTES`。

## 关键决策

- 新增 `UndoRecordPayloadRecord(PageId, TransactionId, UndoNo, recordOffset, slotImage)`。
- `slotImage` 必须包含 2 字节长度前缀，且前缀等于 payload 长度。
- `UndoPage.appendRecord` 一次性写完整槽镜像，避免 len 前缀与 payload 由两条物理 redo 分裂。
- `UndoRedoDeltas.writeRecordPayload` 负责真实页写入 + logical redo 追加。
- `MtrRedoCollector` 只删除被 `UndoRecordPayloadRecord` 精确覆盖的 `UNDO_PAGE_BYTES`。
- `PageRedoApplyHandler` 将 payload record 作为普通页内 after-image patch 处理，并在 batch 末尾盖 pageLSN。
- redo codec 追加新 tag，不重排既有 tag。

## 非目标

- 不实现 extern undo payload；超单页旧 image 仍抛 `UndoPageOverflowException`。
- 不改变 undo record codec、rollback、MVCC 版本链语义。
- 不把 undo append / slot / transaction state 逻辑放进 redo replay。
- 不实现多 rseg、多 undo tablespace 或并发 multi-writer。

## 验收测试

- `UndoRecordPayloadRecord` codec round-trip 与 `byteLength` 精确匹配。
- 无 `PAGE_BYTES` 的 payload redo 可单独重放槽 after-image 并盖 batch end LSN。
- MTR 提交会过滤被 payload record 覆盖的 undo payload 物理 bytes。
- `UndoLogSegment.append` 生产路径实际写出 payload logical redo。
- 既有 `UndoPageTest` / `UndoLogSegmentTest` 读写行为不倒退。

## current map 更新要求

- Redo collect/replay 行加入 `UNDO_RECORD_PAYLOAD`。
- Undo 底层类型行说明 append payload 已有逻辑 redo。
- Known gaps 中移除“完整 undo record payload redo 缺失”，保留 extern payload、多 rseg/DD 等真实缺口。
