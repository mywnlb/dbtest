# Atomic DROP Secondary Index v1

## Goal

- 同时支持 `DROP INDEX idx ON t` 与 `ALTER TABLE t DROP INDEX idx`。
- 两种语法归一为同一个 DD command，并在 DDL 前隐式提交用户事务。
- 只允许删除普通或唯一二级索引；聚簇主键和不存在的索引明确失败。
- 使用 DDL log、page3 descriptor、DD/SDI 与 FSP segment 回收形成 crash-safe 闭环。

## Decisions

- v1 不支持 `IF EXISTS`、多 action ALTER、online DROP、binlog participant 或后台异步 GC。
- DDL 全程持 schema IX、table X；等待 history/pin 时不持 page latch、MTR 或文件锁。
- 先提交“不含目标索引”的 ACTIVE DD，再同步回收 leaf/non-leaf segment。
- DD commit 是不可回退提交点：旧 DD 回滚 descriptor，新 DD 只能前滚物理回收。
- `DdlLogOperation.DROP_INDEX` 使用新稳定码，不重排既有 operation code。
- DDL log 继续保存 tableId + secondary indexId；跨 phase identity 必须完全相同。
- page3 的 96-byte index footer 升为 v2，新增 BUILD/DROP action；v1 按 BUILD 兼容读取。
- DROP descriptor 保存 ddl/version/table/index/root/两个 segment 的完整物理身份。
- segment drop 与 descriptor clear 在同一个动态预算 MTR 中提交，再满足 WAL、flush、force。
- recovery 只解释 committed DD、DDL marker 和 descriptor，不重新解析 SQL 或运行 B+Tree 删除算法。
- `PREPARED + 新 DD` 表达 DD commit 后 phase transition 前崩溃，恢复必须前滚。

## Non-goals

- 不删除聚簇主键，不实现 `DROP PRIMARY KEY`。
- 不实现 foreign key/constraint dependency、不可见索引或 optimizer plan invalidation。
- 不改变 `TableStorageBinding`、catalog aggregate、SDI payload 或 B+Tree page 格式。
- 不在本片新增 background index garbage collector。

## Acceptance Tests

- Parser/Binder 将两种语法归一，并拒绝 `IF EXISTS`、缺失 `ON` 和多 ALTER action。
- Repository 只接受 exact remove-one-secondary aggregate，拒绝重排或 binding 漂移。
- DDL log stable code/phase/secondary identity 跨重启一致。
- footer v1 BUILD 兼容、v2 BUILD/DROP round-trip、CRC/action/reserved bytes 损坏 fail-closed。
- 正常 DROP 保留其它索引顺序、rowFormat/LOB/space/path，并物理回收目标两个 segment。
- history/pin/MDL timeout 在 DD commit 前不删除索引或物理页。
- PREPARED、descriptor、SDI、DD commit、physical drop、ENGINE_DONE 各 crash point 重启幂等。
- Session 两种 SQL 均隐式提交并在重启后保持目标索引不可见、其余 DML 可用。

## Current Map Update

- 更新 SQL DDL、DD aggregate replacement、DDL log、page3 footer、FSP drop 与 recovery 实线。
- 从 DD/DDL 和 recovery 缺口中移除 DROP INDEX，保留 online DDL、其它 ALTER 与 catalog rebuild。
- `storage-backlog.md` 标记 DROP INDEX 完成，并删除过时的 doublewrite 下一步推荐。
