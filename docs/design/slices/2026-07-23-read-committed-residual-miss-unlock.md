# Read Committed Residual-Miss Unlock Slice

日期：2026-07-23
状态：designed / unwired

## 目标

- RC locking SELECT、range UPDATE/DELETE 在确认候选不满足最终 residual 后立即释放其记录锁。
- 只释放本候选精确句柄，不影响匹配行、其它批次或同事务既有锁。
- 等待、重定位、页资源释放和异常 fail-closed 语义保持不变。

## API 决策

- 保留现有 `lockPoint/lockRange` 签名与“锁留到事务终态”行为。
- 新增 RC-only scoped candidate API，返回记录快照与不透明的精确锁句柄。
- 候选对象不是 `AutoCloseable`；遗忘分类时锁仍由事务持有，避免异常误解锁。
- `releaseResidualMiss()` 仅在创建时已验证 READ_COMMITTED 的候选上合法且幂等。
- LockManager 继续登记普通 `TRANSACTION` retention，不复用 savepoint/statement-duration 分类。
- relocation 失败仍由 current-read 内部释放本轮句柄并消费原绝对 deadline。

## Gateway 分类

- clustered access：access record handle 同时就是 clustered row handle。
- secondary access：候选 bundle 包含 access record handle 与随后取得的 clustered record handle。
- cluster miss、重复 identity 或最终 residual=false 均属于 residual miss。
- RC 对 miss 按“clustered 后 access”逆序精确释放；matched bundle 不调用 release。
- residual 求值、类型转换或 corruption 异常时不能证明 miss，全部候选锁保留到事务终态。
- LOB hydration 发生在 matched rows 完整收集后，不参与 residual-miss 解锁证明。

## Secondary Equality

- `SecondaryCurrentReadService` 的 current logical-key 复核失败时释放 clustered point candidate。
- normalized logical-prefix S/X 是谓词协调锁，不是单行 residual candidate，本切片仍保留到事务终态。
- NULL equality、RU、RR 与 SERIALIZABLE 不进入 scoped candidate API。

## DML 边界

- range UPDATE/DELETE 只释放 miss；miss identity 仍留 seen 集，匹配 X 锁覆盖后续 point mutation。
- statement rollback 不批量释放普通匹配锁，保持现有 savepoint retention 语义。
- 提前释放时不持 page latch、RecordCursor、buffer fix、row guard 或 MTR。
- release 唤醒 waiter 后不重新读取已判 miss 行；扫描 continuation 仍来自 access physical key。

## 非目标

- 不提前释放匹配行、RR next-key/gap、terminal gap 或 secondary logical-prefix 锁。
- 不实现 global gap 精确化、semi-consistent UPDATE 旧版本返回或 SKIP_LOCKED。
- 不改变一致性读 ReadView 生命周期和普通 point locking read。

## 验收

- clustered/secondary access 的 FOR SHARE 与 FOR UPDATE residual miss 均唤醒等待事务。
- matched 行在 commit/rollback 前仍阻塞；RR/SERIALIZABLE miss 也继续阻塞。
- secondary stale、cluster miss、重复 identity 精确释放且不释放同事务旧锁。
- residual 异常、relocation 重试、容量异常不发生未经证明的提前释放。
- range DML 只修改匹配 identities，Halloween/partial 防线不退化。

## Current Map

- 实现前仅把本 slice 标为 `planned / unwired`，不修改 current production flow 实线。
- 完成后更新 range current-read 与 secondary current-read 的句柄消费链。
