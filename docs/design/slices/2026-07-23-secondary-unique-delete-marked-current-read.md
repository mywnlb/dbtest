# Secondary Unique Delete-Marked Current-Read Slice

日期：2026-07-23

## 目标

- 唯一二级 key 在删除已提交、purge 尚未执行时可以被其它主键复用。
- 删除回滚恢复 live entry 时，等待者仍必须得到 duplicate。
- PREPARED 删除在二阶段终态前不得提前释放唯一 key。
- 判断必须保持 current-read，不读取调用事务的旧 ReadView 或候选 undo。

## 关键决策

- 非 NULL logical unique key 先取得归一化事务级 X 锁。
- 删除和改出旧 key 的事务持有相同资源直到 commit/rollback。
- 等待结束后重扫 including-deleted 当前 logical prefix。
- 任意主键的 live 候选都构成 duplicate。
- 其它主键的 delete-marked 历史不构成 duplicate。
- 同一完整 physical identity 的 marked entry 只供 UPDATE revive。
- INSERT 使用新聚簇主键 suffix 时按 ABSENT 发布新 entry。
- NULL logical key 延续允许多行的既有行为。
- 扫描容量固定为 1024，并多取一个候选检测 overflow。
- 已发现 live 时直接 duplicate；否则 overflow 抛领域容量异常。
- 禁止将截断结果当成无冲突证明。

## Purge 竞态

- UPDATE 的初次 unique check 发生在取得目标 row guard 之前。
- 初查同 identity marked 后，purge 可能物理删除该 entry。
- 取得 row guard 后执行 exact including-deleted 重读。
- 重读结果 ABSENT/DELETE_MARKED 才能冻结进 undo mutation。
- 重读遇到 live 必须在聚簇首写前报告 duplicate。
- row guard 内不得再次申请事务锁或扫描完整 logical prefix。

## 非目标

- 不改变聚簇主键物理 unique 的 delete-marked 保守语义。
- 不实现 ReadView/undo 驱动的唯一约束判断。
- 不改变 RC residual-miss 解锁或 global gap reference。
- 不改变二级 entry、undo tail、redo 或磁盘格式。
- 不引入后台 purge 加速或自动容量调参。

## 验收测试

- DELETE commit 后等待 INSERT 在 purge 前成功。
- DELETE rollback 后等待 INSERT 得到 duplicate。
- PREPARED commit/rollback 均只在二阶段后唤醒并重扫。
- 同事务 A→B 后可让新主键复用 A。
- 多个 marked 历史后方的 live 冲突不会被遗漏。
- 候选超过配置化测试容量时 fail-closed。
- 初查 marked 与 row guard 之间真实 purge 后 UPDATE/rollback 收敛。

## Current Map

- 更新 B+Tree `Secondary logical key DML check` 为真实 current-read 链。
- 将剩余正确性缺口收敛为 clustered marked primary-key reuse。
