# Slice: Undo slot finalization concurrency hardening

- 日期：2026-07-10。
- 关联设计：`innodb-undo-log-purge-design.md` §5.1/§5.4/§9/§15.3–15.4；
  `innodb-disk-manager-design.md` §9/§18；`mysql-lock-observability-deadlock-design.md` §5.2/§10.3。
- 前置：atomic undo segment finalization、page3 owner CAS、persistent full/recovery rollback progress。
- 定位：关闭重复终结 TOCTOU 与持久 claim 冲突导致的内存 slot 泄漏，不改变持久格式。

## 1. 范围

做：
- 把运行期 slot 生命周期显式化为 `FREE -> RESERVED -> ACTIVE -> FINALIZING -> FREE`。
- `reserveClaim` 在短锁内占住空槽、计入 active 并返回 claim lease；未绑定 first page 前关闭可安全退回 `FREE`。
- claim lease 先以短 S latch 预检 page3 槽为空并释放 latch，再创建 FSP/undo segment、绑定 owner 并持久 claim。
- `RollbackSegmentSlotManager.beginFinalization` 在短锁内校验 expected owner，并返回非阻塞 RAII lease。
- finalization lease 存续期间 slot 仍计入 active，禁止 claim 复用且不让 active-count 诊断提前归零。
- 重复或 stale finalizer 必须在 page latch、FSP drop 与 page3 clear 之前失败。
- 预检阶段失败时 lease 恢复 `FINALIZING -> ACTIVE`；开始物理写后失败则保留 `FINALIZING` 并 fail-stop。
- final MTR 保持 FSP page0/page2 -> page3 owner CAS clear -> commit 的既有锁序和 redo batch。
- commit 后 crash hook 仍位于内存发布前；正常路径最后由 lease 完成 `FINALIZING -> FREE`。
- page3 预检的“磁盘槽已占用”改用专门的 ownership conflict 异常，并在任何 segment 分配前取消 reservation。
- 持久 claim 继续在写前复核 owner；一旦 reservation 已绑定 segment，后续失败不得把 slot 静默恢复为可复用。
- bind 后的持久 claim/context 发布失败必须抛 fatal `UndoClaimPublicationException`，明确要求 crash recovery。

不做：
- 不先持 page3 latch 再反向获取 FSP page0/page2 latch。
- 不新增 inode generation、磁盘 slot 状态码、redo record、page format 或 recovery handler。
- 不为普通 MTR 增加 content undo，也不把 slot mutex 持有到 Buffer Pool、IO 或 MTR commit。
- 不改变 rollback/purge/commit 的业务可见性、history boundary、TRX_STATE_DELTA 或 durability policy。
- 不处理跨进程并发；crash 后仍以 redo 恢复出的 page3 与 FSP 元数据为权威。

## 2. 关键决策

1. slot 短锁只执行状态/owner CAS，不跨模块调用；物理等待不进入事务 Wait-For Graph。
2. `RESERVED/FINALIZING` 都是运行期 fence，不进入持久格式；重启只从 page3 occupied slots 重建 `ACTIVE`。
3. lease 在 `dropUndoSegment` 前标记 physical mutation started；此后任何异常不得把 slot 恢复为可复用。
4. 可补偿 ownership conflict 只允许由 segment 创建前的 page3 预检产生；绑定 owner 后不做内存补偿。
5. bind 后发布失败保留 ACTIVE fence 并 fail-stop；finalizer 继续集中四条终态路径。

## 3. 验收测试

- 并发 double-finalize：阻塞首个 drop 时，第二个调用在进入 allocator 前失败，drop 只执行一次。
- stale/reuse：终结完成并复用 slot 后，旧 owner finalizer 不得触碰新 segment/inode。
- lease rollback：owner/state/undo-header 预检失败恢复 ACTIVE，slot 仍可由原事务终结。
- fail-stop：physical mutation started 后注入失败，slot 保持 FINALIZING 且不能重新 claim。
- claim conflict：page3 occupied、内存 slot 空时在 segment 创建前失败，reservation/active count 不泄漏且空间用量不变。
- 回归：四条 finalization、rollback crash progress、purge/recovery、small Buffer Pool 与全量 Gradle tests 通过。

## 4. 文档更新

- 只更新 `current-implementation-map.md` 中 slot/finalization 状态、异常与 review log。
- `storage-backlog.md` 继续推荐正式 trx recovery table v1，不改变长期优先级。
- 不修改全局目标架构图，不生成持久 implementation plan 或 `docs/superpowers` 文档。
