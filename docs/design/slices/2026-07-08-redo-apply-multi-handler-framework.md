# 0.19a Redo Apply 多 Handler 框架

## 目标

本 slice 只落 redo replay 的多 handler 分发框架，不新增持久 redo tag。

- 现有持久格式仍只有 `PAGE_INIT` / `PAGE_BYTES`。
- `RedoApplyDispatcher` 从单 page handler 改为 handler registry。
- `PageRedoApplyHandler` 继续保持批末统一 stamp batch end LSN 的物理恢复语义。

## 关键决策

- 新增 `RedoApplyHandler`，负责声明 `supports(record)`、`affectedPages(record)` 和 `openBatch(range, context)`。
- 新增 `RedoApplyBatchHandler`，作为单个 batch 内的有状态 handler session。
- dispatcher 按 redo 文件中的原始 record 顺序逐条分发，不能按 handler 分组后重排。
- 一个 record 必须恰好匹配一个 handler；零匹配或多匹配都是配置错误。
- FORCE_SKIP 过滤基于 `affectedPages`，发生在打开 handler session 和触碰 `PageStore` 之前。

## 非目标

- 不修改 `RedoRecordType` tag。
- 不修改 `RedoBatchFrameCodec`。
- 不新增 `SPACE_HEADER_UPDATE`、`XDES_UPDATE`、`BTREE_PAGE_SPLIT` 等持久逻辑 redo。
- 不改变 MTR collector 的 `MtrRedoCategory` 语义；分类仍不进入 redo 文件。

## 验收测试

- 两个测试 handler 分别支持 `PAGE_INIT` / `PAGE_BYTES` 时，dispatcher 按原始 record 顺序调用。
- 多个 handler 同时支持同一 record 时抛 `DatabaseValidationException`。
- 没有 handler 支持 record 时抛 `DatabaseValidationException`。
- skip predicate 命中 affected page 时，该 record 不进入 handler，summary 统计正确。
- 现有物理恢复回归继续通过：batch end LSN stamping、FORCE_SKIP、PAGE_INIT extend-on-demand、越界 PAGE_BYTES 损坏检测。

## current map 更新要求

- Redo recovery 小节标明 `RedoApplyDispatcher` 已是多 handler registry。
- 生产默认仍只注册 `PageRedoApplyHandler`，因此持久逻辑 redo 类型仍未实现。
- Known gaps 中删除“dispatcher 单 handler”缺口，保留“仅两种持久 redo record 类型”。

## 后续

- 0.19b 优先考虑 FSP/page allocation 相关逻辑 redo。
- B+Tree split/merge 专用 redo 暂缓，避免恢复阶段重新执行逻辑 split 决策。
