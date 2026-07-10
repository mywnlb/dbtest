# Slice: Atomic undo segment finalization v1

- 日期：2026-07-10
- 关联设计：`innodb-undo-log-purge-design.md` §7.4/§7.6/§7.7/§14.3–14.5/§15.4。
- 前置：persistent rseg page3、undo segment drop、1.4c full/recovery per-record rollback progress。
- 定位：一次闭合 INSERT commit、live/recovery rollback、committed purge 的 segment/slot 终态。

## 1. 范围

做：
- 新增共享 `UndoSegmentFinalizer`，四条终态路径必须经同一协议回收 undo segment。
- page3 slot 非空时 segment 必须存在；slot 为空时 segment drop 必须属于同一 redo batch。
- finalization 前用短读 MTR 校验内存/page3 owner、creator、state、logical head、commitNo 并取得 handle。
- final MTR 固定执行 FSP drop，再 CAS 清 page3 slot，最后追加可选 trx-state diagnostic redo。
- commit 成功后才释放 expected 内存 slot；purge 再精确摘除 expected history head。
- 纯 INSERT commit 直接 drop+clear，不再依赖进程内 insert-reclaim 队列。
- 增加只在 finalization commit 后触发的包内 fault injector。

不做：
- 不新增 redo type、page format、单次 finalization fsync 或持久 history linked-list。
- 不实现 transaction recovery table、PREPARED/RECOVERED_ACTIVE、多 rseg、多 undo tablespace 或 DD。
- 不修复全局 MTR COMMITTING 语义；finalization 进入物理写后的失败本地 fail-stop。
- 不改变 partial/savepoint rollback、MVCC update-version chain 或 purge boundary。

## 2. 关键决策

1. `claimSlot` 只接受磁盘空槽；`clearSlot(expectedFirstPage)` 必须 CAS 精确 owner。
2. 内存 `release(slot, expectedFirstPage)` 使用同样的 owner 校验，防止清掉已复用 slot。
3. 短读阶段按 page3→undo first page 获取 S latch；写阶段按 FSP page0/page2→page3 获取 X latch。
4. INSERT finalization batch durable 即提交权威；未 durable 时恢复仍看见 ACTIVE slot 并回滚。
5. rollback 只有 logical head=EMPTY 才可 finalization；purge 只有 COMMITTED header 与 history identity 匹配才可回收。
6. final MTR 开始后的异常或 commit 后内存发布不一致抛 fatal `UndoFinalizationException`，不得同进程重试。
7. `TRX_STATE_DELTA` 继续保留诊断与未来 trx-table 输入；本片不据此恢复 transaction counters。

## 3. 验收测试

- slot CAS：重复 claim、stale clear、错误 expected page、重复 finalization 均 fail-closed。
- batch：FSP free、FSP metadata、RSEG_SLOT clear 与可选 trx-state delta 位于同一 MTR batch。
- INSERT：commit 后立即回收；旧 ReadView 仍按 insert bit 判不可见；无 reclaim queue。
- rollback：EMPTY 前拒绝；commit 后 crash 重启不恢复 slot、不重放 inverse。
- recovery：ACTIVE 多记录事务首启 rollback+finalize，二次启动不再发现该 slot。
- purge：task 中断保留 COMMITTED slot；finalization 后 crash 不重建 history；失败 worker 进入 FAILED。
- 多页/多 extent undo 验证小 Buffer Pool、latch order 与 inode/page 复用。
- 全量 Gradle tests 不少于基线 211 suites / 1074 tests，静态规则与 diff check 通过。

## 4. 文档与复核

- 只更新 current map、storage backlog 和受影响注释，不改全局目标图或历史 slice。
- 删除 insert-reclaim 生产类型/队列后，不得把它留成 Reserved / Unwired。
- 完成后按调用链、锁序、redo、recovery、测试/文档五个维度各复核一遍并记录证据。
