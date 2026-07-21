# Object-level Force Recovery v1 Slice

## 目标

- 把管理员显式 SpaceId 在用户表空间 discovery 前映射到 committed DD 对象并持久隔离。
- 普通启动继续服务健康表，同时稳定报告不可用对象。
- 隔离对象允许 raw DISCARD/DROP，并只能从本实例签名的 clean backup 恢复。
- redo/doublewrite/undo/purge 全程不访问被排除空间。

## 关键决策

- DD 新增 `RECOVERY_UNAVAILABLE` 与 `RECOVERY_DISCARDED`，普通 lookup、SDI 和 discovery 均隐藏。
- FORCE 集合必须一对一命中稳定对象；系统/undo、未知、共享 SpaceId/路径及未决 DDL 相交均拒绝。
- 同次 FORCE 的 ACTIVE 目标在一个 dictionary transaction 内切换，不允许部分隔离。
- storage 排除策略取管理员集合与 DD 长期隔离集合并集。
- `RECOVERY_EXPORT_READ_ONLY` 禁止 SQL 写、事务控制、DDL、XA、MTR、checkpoint 和后台 worker。
- 普通启动含隔离对象时为 `DEGRADED`；访问模式在一次 open 生命周期内保持稳定快照。
- live rollback/purge 遇不可用目标失败；recovery 只在完整解码并验证 predecessor 后跳过链首。
- raw DISCARD/DROP 不读取 page0，要求 SpaceId 独占 lease、无打开句柄和 resident frame。
- recovery DDL 固定 physical-first：`PREPARED -> ENGINE_DONE -> DD -> DICTIONARY_COMMITTED -> COMMITTED`。
- DDL log 稳定 operation code 8/9/10 分别表示隔离 DISCARD、DROP、可信 replacement IMPORT。
- trusted backup 仅允许 ACTIVE 源，在 table X、history/pin drain、SpaceId X lease 下复制。
- 数据副本 page0 改为 DISCARDED并重盖 checksum；HMAC manifest 最后原子发布。
- 实例 identity 保存 UUID、256-bit HMAC key 与 CRC；仅 backup/import 懒加载，普通启动不依赖。
- import 只读取固定 recovery-incoming pair，并校验 HMAC、文件 hash、定义 hash、identity 与 page0。
- archive 与 incoming 保留给管理员；DROP 只删除对象当前拥有的 canonical/discarded 文件。
- 所有 canonical/transfer/raw 路径逐级拒绝符号链接，不能经父目录逃逸实例根。

## 非目标

- 不提供 SQL 语法、权限系统、跨实例信任或任意主机路径导入。
- 不修复损坏表空间，不从坏 page0/SDI 猜对象归属。
- 不支持系统表空间、undo 表空间或共享表空间对象级隔离。
- 不改变全实例 catalog-loss recovery、XA 决议、普通 DDL 或在线 DDL row-log 设计。
- 不动态改变同一次 open 的 `DatabaseAccessMode`；终态在下次启动重新计算。

## 验收测试

- FORCE 隔离跨重启持久化，健康路径可正常启动，目标表访问返回恢复不可用异常。
- 导出只读模式同时由 Session、gateway、MTR/checkpoint 和 worker gate 拒绝写入。
- recovery undo/purge 跳过不可用链首并计数，且不触碰目标 B+Tree/page。
- raw DISCARD、可信 backup/import、恢复 ACTIVE 与再次普通启动的数据读取端到端通过。
- identity CRC、manifest HMAC、字段篡改、文件 hash/定义/page0 identity 错配均 fail-closed。
- 重复物理路径、共享/未知 SpaceId、系统/undo SpaceId 与未决 DDL 相交在 DD 写入前失败。
- DDL op 8/9/10 的持久 phase 在重启恢复中幂等收敛，不越过缺失物理证据。
- 固定 JDK 25.0.2 与 Gradle 9.5.1 全量测试数不得低于既有 1659。

## Current Map 更新要求

- 更新 Recovery、Undo/Transaction、DD/DDL、Session/Engine 的真实生产链。
- 新类型均有生产调用，不进入 Reserved / Unwired 表。
- backlog 将对象级 force recovery 标为完成，并保留 online DDL/临时 undo 等真实缺口。
