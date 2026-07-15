# Persistent Rollback-Segment History List v1

## Goal

- 在单 undo tablespace / 单 rollback segment 约束内，把 committed UPDATE undo history 从内存队列升级为持久双向链。
- commit、purge、crash recovery 共享 page3 base 与 UPDATE undo first-page link，内存 `HistoryList` 只作运行时投影。
- 保存 `lastTransactionNo` 高水位，history 全部 purge 后重启也不复用旧提交号。

## Key Decisions

- rollback segment header 升为 v3，增加 head、tail、length、lastTransactionNo；active/cache arrays 从 offset 98 开始。
- 普通 UNDO 页升为 v3，在 first-page log header 增加 history prev/next，record area 从 120 后移到 136。
- v1/v2 page3 与普通 UNDO 页不做在线迁移，open/recovery fail-closed；旧教学数据需要重建。
- history 节点只允许 COMMITTED UPDATE first page；INSERT、TEMPORARY、ACTIVE、CACHED 均不得链接。
- 链顺序以真实 commit append 顺序为准，不按 `TransactionNo` 重排；page3 高水位保存已挂入的最大提交号。
- page3 base/slot owner 与 undo first-page link 在同一 MTR 修改，并用独立 metadata delta 覆盖 redo replay。
- mixed commit 同批处理 INSERT cache/drop、UPDATE COMMITTED+link、page3 base 与唯一 terminal transaction delta。
- purge 先完成 B+Tree 清理，再同批 unlink head、cache/drop segment、更新 page3 owner/base；持久提交后才发布内存状态。

## Concurrency and Failure Boundary

- `HistoryList` 用短 `ReentrantLock`、`Condition` 与独立可配置 timeout 串行化 append/unlink transition。
- transition lease 只冻结 head/tail/count；任何磁盘 IO、page latch、FSP 或 B+Tree 工作都在 history lock 外执行。
- 物理修改前取消会释放门并唤醒；越过物理边界未完成则保留 fail-stop flag，后续 writer 只能有界超时。
- commit append lease 在任何 finalization IO 前取得；purge removal lease 在 B+Tree tasks 成功后、undo finalization 前取得。
- purge 最终资格要求 creator 已离开 active table，并检查提交号 low water 与所有 live ReadView 可见性，封闭 prepared-commit 窗口。

## Recovery and Truncate

- recovery 按 page3 head/next 物理顺序逐节点短 MTR 读取，不按提交号排序。
- 校验 exact length、head/tail、prev/next、cycle、slot 一一映射、COMMITTED UPDATE 状态、creator/commitNo 唯一性。
- orphan COMMITTED、linked ACTIVE、遗漏节点、重复 identity 或 `lastTransactionNo` 回退均在开放流量前 fail-closed。
- counter 基线取 transaction sidecar/redo nextNo 与 `lastTransactionNo + 1` 的最大值，并拒绝溢出。
- 真正写 truncate marker 与 TRUNCATING recovery 都要求 history length 为零；初始大小的稳定重复调用仍可无副作用 no-op。

## Non-goals

- 不实现多 rollback segment、多 undo tablespace 选择、扩展 `RollPointer` 或 rollback-segment directory。
- 不实现持久 free undo segment list、purge 多 worker、二级索引 purge 或 purge→truncate 自动调度。
- 不实现 XA/PREPARED 决议、DD discovery、DDL undo 或旧 v1/v2 文件在线迁移。
- 不保证物理链按 TransactionNo 单调；较新的 head 可以安全阻塞其后的旧提交节点，影响吞吐但不影响可见性。

## Acceptance

- page3/UNDO v3 format、offset、redo stable code、append/unlink CAS 与旧版本拒绝均有断言测试。
- UPDATE-only/mixed commit、purge cache/drop、并发 commit/purge 与 commit 后内存发布前 crash 均保持 disk/runtime 一致。
- recovery 覆盖物理顺序、环/断链/orphan/linked-active/重复 identity/high-water 损坏和 all-purged counter。
- truncate 在非空 history 时写 marker 前拒绝；timeout、中断、物理后失败 fence 有并发测试。
- 固定 JDK/Gradle 全量测试通过，测试数不低于切片前 1276；current map、backlog 与相关厚设计同步。
