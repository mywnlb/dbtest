# Clustered Delete-Marked Key Reuse Slice

日期：2026-07-23；状态：designed / unwired

## 目标

- DELETE 已提交、purge 尚未删除聚簇记录时，同一主键可以被新 INSERT 复用。
- DELETE 回滚恢复 live 行时必须 duplicate；PREPARED 在二阶段终态前继续阻塞复用。
- 复用不能截断旧 ReadView 所需的聚簇版本链。

## Current-Read 与锁

- 新增 clustered insert state check：`ABSENT`、`LIVE`、`DELETE_MARKED`。
- `ABSENT` 取得 insert-intention；`LIVE` 取得 REC_S；`DELETE_MARKED` 取得 REC_X。
- 锁等待后重定位，物理位置或 delete 状态变化时按同一绝对 deadline 重试。
- `LIVE` 报 duplicate；`DELETE_MARKED` 进入复用候选，不读取 ReadView/undo。
- 所有事务锁等待先于 row guard/MTR；guard 内 exact 重读，marked 被 purge 后改走普通 INSERT。

## 聚簇发布

- 复用不是 INSERT_ROW undo，也不是先 purge 再 insert。
- 新增 `REUSE_DELETE_MARKED`，稳定 code=4，归属 UPDATE undo log。
- 该 undo 保存 marked 旧全行、旧隐藏列，并隐含 predecessor `deleted=true`。
- B+Tree prepare 必须校验同 key、当前 marked、旧隐藏列 CAS 和目标页容量。
- publish 用新事务 id/roll pointer 原子替换记录并清除 delete mark。
- 页内增长不足沿用 UPDATE 的 fail-closed，不在本切片实现跨页搬迁。

## Secondary 与 LOB

- reuse 路径检查并发布新行的每个 secondary entry，不把旧 marked entry 当 live 冲突。
- 新增 `PUBLISH_ENTRY` mutation，保存发布前 `ABSENT` 或 `DELETE_MARKED`。
- rollback 对 ABSENT 物理删除，对 DELETE_MARKED 重新标记；旧行其它 marked entry 不变。
- 新 external LOB 只记录 rollback-new ownership；旧链仍由先前 DELETE_MARK undo 持有 purge 权限。
- 初查 marked 后若聚簇已被 purge，最终模式改为普通 INSERT，并冻结匹配该模式的 secondary inverse。

## MVCC、Rollback 与 Purge

- MVCC 与 secondary purge safety traversal 消费 reuse undo 时构造 marked predecessor，再按旧隐藏列继续版本链。
- rollback 顺序为新 secondary inverse、恢复 marked 聚簇旧 image、释放新 LOB、推进 undo head。
- committed reuse undo 进入 history；purge 只推进其 undo head，不删除当前聚簇/secondary。
- 先前 DELETE_MARK purge 以 owner/roll-pointer 发现聚簇 stale，并用 secondary safety checker RETAIN/REMOVE。
- recovery rollback 与 live rollback 共用同一 inverse，未知 code 继续 fail-closed。

## 非目标

- 不支持主键 UPDATE、跨页 clustered relocation 或同步强制 purge。
- 不改变唯一二级 current-read，也不把 reusable marked 记录交给 Change Buffer。

## 验收

- commit/rollback/PREPARED commit/PREPARED rollback 四种等待结果。
- pre-delete、between-delete-reuse、post-reuse 三类 ReadView 可见性。
- reuse 自身 statement/full/recovery rollback 与 crash 重启。
- 相同/不同 secondary key、LOB 新旧链、row-guard 前并发 purge。
- earlier DELETE purge 对 reused identity 的 stale/RETAIN 收敛。

## Current Map

- 实现前只在 Known Gap 标记本 slice 为 `planned / unwired`。
- 完成后更新 clustered unique、undo/MVCC/rollback/purge 的真实生产链。
