# Per-operation Redo Budget Slice

## 目标

- 删除 `StorageEngine` 对所有 MTR 统一使用 `redoCapacity/8` 的固定预算。
- 每个生产写 MTR 在获取 page latch、buffer fix、tablespace/FSP lease 前申请操作级 redo 上界。
- 只读 MTR 明确申请零预算；commit 用真实持久 records 精确结算并验证估算没有低估。
- 保持逻辑 LSN、LogBlock v1、WAL、checkpoint 和 MTR dirty 发布顺序不变。

## 关键决策

- `RedoAppendBudget` 是不可变值：purpose、logicalUpperBound、physicalBlockUpperBound。
- logical 上界以 `Σ RedoRecord.byteLength()` 为单位，继续匹配 checkpoint-age policy 的 LSN 单位。
- positive budget 的 physical 上界为 `ceil((12+20+logical)/472)*512`，零预算为 0，不预编码伪 batch。
- `RedoBudgetBuilder` 用 checked arithmetic 组合 page bytes、page init、logical delta 和重复次数。
- `MiniTransactionManager.beginReadOnly()` 固定零预算；`begin(budget)` 在任何页/FSP 资源前调用 throttle。
- capacity-aware 生产 manager 禁止无参 `begin()`；默认/no-op 测试 manager 保留无界测试预算，避免污染页原语测试。
- reservation 在 MTR memo 中兜底正常/早期回滚释放；append 成功后把账本所有权转给真实 current LSN。
- commit 冻结一次 persisted records，先算真实 logical/physical bytes 并验证不超过预算，再 append、盖 pageLSN、发布 dirty、markClosed。
- 预算低估是 `DatabaseFatalException`：MTR 保持 fail-stop，不能 rollback 后发布无 redo 的 dirty page。

## 估算职责

- redo 层只提供编码尺寸公式和值对象，不依赖 B+Tree、Undo、FSP 或 DML 类型。
- B+Tree estimator 按 index height、page size、split/merge/root 最坏触页与现有 split page quota 给上界。
- Undo estimator 按 slot image、undo grow pages、metadata delta、segment handle/page count 和终态 delta 给上界。
- DML estimator 组合 undo 与 clustered B+Tree 预算；update/delete 使用 current-read 已物化的旧记录尺寸。
- rollback/purge 在只读 MTR 物化 undo/task 后、写 MTR 开始前计算 inverse/marker/finalization 预算。
- 可扩展既有只读 plan snapshot（如 segment fragment/extent count），禁止写 MTR 开始后再补做容量等待。
- boot、truncate、事务状态等固定布局写点使用专用小预算；所有纯读调用改 `beginReadOnly()`。

## 并发与失败边界

- throttle 等待只发生在 begin admission；不得在 page latch、row lock wait、FSP lease 或 commit 中等待 flush。
- DML begin 可能已持已授予 row/gap lock；flush/checkpoint 不反向获取事务锁，且 admission 受 timeout 约束。
- 多线程 outstanding budget 仍由原子账本汇总，实际 append 后及时释放，避免 fixed budget 长期双计数。
- estimator 溢出、单批 physical 上界超过 ring fileBytes、实际值超过上界都在 append 前 fail-closed。
- 低估发生于 COMMITTING 时不释放 touched guard/reservation；调用方必须终止实例，而非继续处理请求。
- 本切片不把 logical LSN 与 physical occupancy 混为同一单位。

## 非目标

- 不实现 SN、ring 总物理占用/文件尾碎片的并发 bin-packing admission 或动态 resize。
- 不改变一个 MTR 一个 sealed LogBlock chain、batch 不跨 ring 文件的格式决策。
- 不重写 MTR content undo，也不把 budget estimator 放进 redo repository。
- 不为测试伪造生产预算；严格预算由 engine integration 和各领域 estimator 测试覆盖。

## 验收

- 尺寸公式覆盖 0/单块/跨块/溢出，且与真实 codec 输出逐值一致。
- throttle 覆盖并发预留、append ownership transfer、异常/rollback 释放和单文件上限。
- 所有生产 `MiniTransactionManager.begin()` 调用归零：只读或显式 budget 二选一。
- DML normal/split、undo grow/finalization、rollback/purge、boot/truncate 分支断言 actual ≤ budget。
- 小 redo capacity 下读 MTR不触发 flush；不同操作按自身预算触发 ASYNC/SYNC/HARD。
- 更新 current map/backlog 受影响小节并通过全量 Gradle 回归。
