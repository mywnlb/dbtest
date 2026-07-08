# 0.15 Extent Allocation Direction Slice

## 目标

- 为 Disk Manager/FSP 增加 `UP` / `DOWN` / `NO_DIRECTION` 的 extent 分配输入。
- 让 leaf segment 在明确顺序增长时一次最多挂入 2-4 个 extent。
- 保持普通 `DiskSpaceManager.allocatePage(mtr, ref)` 行为兼容。
- 只在 storage 内闭环，不接 DD、session、executor 或 DDL。

## 设计依据

- `innodb-disk-manager-design.md` §7.2/§7.3：fragment 页先行，segment 达 32 页后走完整 extent；大 segment 且顺序写入明显时一次最多 4 个 extent。
- `innodb-disk-manager-design.md` §16.3：B+Tree split 可向 Disk Manager 传 `hint` 和 `UP`。
- 当前实现：`ExtentAllocationPolicy` 只接收 `ownedExtentCount`，`FreeExtentService.acquireFreeExtent` 只取 `FSP_FREE` 链头。

## 关键决策

- 对外新增轻量 `PageAllocationHint`，默认 `none()` 等价现有无方向分配。
- FSP 内部使用独立 `ExtentAllocationRequest`，避免 `storage.fsp` 依赖 `storage.api`。
- `NO_DIRECTION`、UNDO、INDEX_NON_LEAF、SYSTEM 首版仍一次只取 1 个 extent。
- `INDEX_LEAF` 只有收到 `UP` 或 `DOWN` hint 时才启用 2-4 extent 批量挂段。
- `FreeExtentService` 按 hint extent 选最近方向候选；无候选时回退链头，不改变无方向稳定行为。
- 不使用 record 层 `PAGE_DIRECTION` 字段作为依据；当前该字段仍是页内简化统计。

## 非目标

- 不实现右分裂优化或 PAGE_DIRECTION 真实统计。
- 不实现独立 XDES 管理页、segment header 扩展或 `reserveFactor` 动态策略。
- 不实现 btree 专用 redo 或逻辑 MLOG。
- 不接 DROP/DISCARD lifecycle、DD discovery、SQL/session/executor。

## 验收测试

- `ExtentAllocationPolicyTest` 覆盖方向、segment purpose、owned extent、pagesNeeded clamp。
- `FreeExtentServiceTest` 覆盖 `UP` / `DOWN` 最近候选、`NO_DIRECTION` 链头、无候选回退。
- `SegmentPageAllocatorTest` 覆盖 fragment 路径不变、leaf 顺序 hint 批量挂 extent、no-hint 仍单 extent。
- Disk/BTree 集成测试覆盖旧 API 兼容和顺序 leaf split 传入方向 hint。
- 全量 Gradle `test` 通过。

## Current Map 更新要求

- 更新 Disk Manager Current Flow / Data Chains 中 allocate page 的方向 hint 链路。
- 更新 `storage.fsp.extent` 与 `storage.fsp.segment` package status。
- 从 Known Gaps 中移除或改写 0.15 缺口。
- `storage-backlog.md` 将 0.15 标为完成，并保留未做项边界。
