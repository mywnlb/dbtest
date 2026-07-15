# Slice: 1.6 — 独立 INSERT / UPDATE Undo Log v2

## 目标

- 同一事务最多拥有一条 INSERT undo log 和一条 UPDATE undo log，各自占独立 rseg slot 与 FSP segment。
- UPDATE_ROW 与 DELETE_MARK 共用 UPDATE log；TEMPORARY kind 本片继续 fail-closed。
- 保持 DML 的 `planX -> redo admission -> appendPlanned -> clustered write` 原子 MTR 协议。
- full、partial、recovery rollback 与 commit/purge 都能正确处理双 log。

## 关键决策

- 普通 UNDO 页格式升级为 v2；first page 和 chain page 都持久化 `UndoLogKind`。
- v1/未知版本不做在线迁移，open/recovery 直接报不兼容，避免把旧混合段误解释为独立 log。
- `UndoContext` 保存事务全局 `lastUndoNo`，并用两个 `UndoLogBinding` 保存 slot、首页、局部 head 与 append snapshot。
- `undoNo` 在事务内全局唯一且严格递增；每个 kind 的局部物理高水位只要求递增，允许另一 kind 造成间隙。
- `UndoRecord.prevRollPointer` 指向同 kind 的局部回滚前驱。
- 记录版本链仍由旧隐藏列 `oldHiddenColumns.dbRollPtr` 表达，不与事务回滚局部链混用。
- `UndoWritePlan` 冻结目标 kind、是否首建 log、期望全局高水位、局部 head 与持久 append snapshot。
- INSERT log 只接受 INSERT_ROW；UPDATE log 只接受 UPDATE_ROW/DELETE_MARK；append、direct read、遍历都守门。
- mixed commit 在一个 MTR 内 drop/clear INSERT、标记 UPDATE COMMITTED，并只写一条 terminal transaction delta。
- 只有 committed UPDATE log 进入 history/purge；committed INSERT evidence 在恢复时视为损坏。
- full/recovery rollback 预检双链后按 head.undoNo 归并逆序消费，双 EMPTY 后批量终结。
- partial rollback 同样归并执行 inverse，并在一个按 PageId 排序的 marker MTR 中推进所有变化的局部 head。
- slot batch finalization all-or-none 获取 lease；物理前失败恢复 ACTIVE，物理边界后失败保留 FINALIZING fence。
- recovery 按 creator transaction id 聚合证据；同 kind 重复、ACTIVE/COMMITTED 冲突、超过两条均 fail-closed。

## 非目标

- 不实现 cached INSERT/UPDATE undo segment reuse，终结后仍 drop FSP segment。
- 不实现多 rollback segment、多 undo tablespace 或扩展 RollPointer 编码。
- 不实现 temporary undo、DDL undo、XA/PREPARED 决议或在线 v1->v2 迁移。
- 不接 SQL/session/executor/DD，也不扩展到二级索引 rollback/purge。
- 不改变 external undo payload descriptor v1 与 UNDO_PAYLOAD 页格式。

## 验收测试

- v2 first/chain page kind 往返；v1/未知版本 fail-closed；record type/kind 错配拒绝。
- I-U-I 与 U-I-U 序列验证全局 undoNo 连续、两个局部 predecessor 正确且 slot/segment 独立。
- `slotCapacity=1` 的 mixed 第二类写在物理创建前安全失败，不污染既有 binding。
- INSERT-only、UPDATE-only、mixed commit 验证 slot/history/terminal delta 与原子批次。
- statement/savepoint rollback 验证双 head 快照、归并顺序、单 marker batch 与失败不提前发布。
- live/recovery 双 ACTIVE log rollback 验证数据逆操作、双 slot 清理与重启幂等。
- committed INSERT、重复 kind、ACTIVE+COMMITTED 证据验证 recovery fail-closed。
- purge 只接受 committed UPDATE log，拒绝 INSERT/TEMPORARY/ACTIVE。
- 固定 JDK/Gradle 全量测试不得低于切片前 1249 tests。

## Current map 更新

- 更新 DML/undo write、commit、full/partial/recovery rollback、finalization 与 purge 的生产调用链。
- 更新普通 UNDO page v2、双 binding、全局 undoNo/局部链和 fail-closed 边界。
- 从 Known Gaps 删除独立 INSERT/UPDATE log，保留 cached reuse、多 rseg/tablespace 与上层接线。
