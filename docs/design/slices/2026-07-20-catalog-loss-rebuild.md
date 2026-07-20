# Catalog Loss Recovery v1

## 目标

- 一次性完成 backlog 2.9 剩余的 SDI candidate 扫描、schema/directory manifest、冲突隔离、catalog rebuild 和全页 checksum scrub。
- 普通 `DatabaseEngine.open()` 保持 fail-closed；恢复只能走显式离线 API。
- 完成后由普通启动再次执行 storage/DD recovery，不能让工具直接发布 SQL 流量。

## 关键决策

- 新增独立 `mysql.dd.manifest`，持久化 control reservation、catalog mutation intent 和 clean dictionary snapshot。
- catalog mutation intent 必须早于 catalog append，避免 CREATE SCHEMA 等无文件变化被旧 manifest 静默遗漏。
- clean snapshot 保存 schema 默认字符语义、ACTIVE/DISCARDED 完整表定义、ACTIVE 路径和 SDI digest。
- catalog 丢失且没有 clean manifest 时拒绝重建；旧健康实例在首次正常恢复完成后补建 manifest。
- catalog 新增首批 baseline 格式，保留对象原 version；既有 mutation 与 DDL log stable code 不变。
- scrubber 只解释物理页和 opaque SDI，不 import DD；DD 层完成 manifest/SDI 聚合比较。
- NOFOLLOW 同时约束属性探测和 channel 打开；扫描结束复核 regular type、size、mtime 与 fileKey。
- 每个已初始化页严格校验 checksum/trailer/spaceId/pageNo/type；XDES state/owner/list/bitmap
  先通过语义校验，FSP/XDES 证明 free 的全零页才可接受。
- 非零索引 DDL footer、临时 table state、未决 mutation intent、missing expected 或损坏 expected 均阻止重建。
- inspect 完整枚举和逐页扫描后签发 token；quarantine/rebuild 都必须在实例锁下重验。
- quarantine 只接受显式 conflict IDs，只移动 extra/duplicate 或损坏 catalog/control，永不移动唯一 expected。
- 隔离目标固定在 `catalog-recovery/quarantine/<scanId>`；只允许无覆盖同盘 `ATOMIC_MOVE`。
- rebuild 先发布安全 control，再按 clean digest 固定临时 catalog 名写/重开验证 baseline，最后 atomic
  move 发布 `mysql.ibd`；control 推进使 token 变化时仍可复用同一已验证 temp。
- 已存在的零长度 manifest 按损坏证据严格打开并保留，只有明确 missing 才允许新建 journal。

## 非目标

- 不从没有 manifest 的散落 SDI 猜测 schema。
- 不自动修复 torn page、legacy initialized-page zero checksum、未决索引 DDL 或丢失的表空间。
- 不恢复历史 catalog mutation/DDL log；只重建最新稳定快照和安全 identity 高水位。
- 不自动启动重建、不覆盖原始证据、不提供 force/bypass。
- 不保证 Java/Windows 缺失的可移植目录 fsync。

## 验收

- manifest torn/CRC/SHA/intent resolution、baseline round-trip 与旧 catalog 兼容测试通过。
- 全页 checksum、zero/XDES state/owner/list/bitmap、SDI/footer、路径/symlink/扫描变化、
  duplicate/missing/extra 冲突矩阵通过。
- stale token、并发实例锁、atomic move 失败、control 推进后的 temp 幂等复用和部分隔离重试均保留证据。
- catalog missing/empty/corrupt 经显式流程重建后，普通引擎可完成 recovery 并读取 schema/table。
- catalog-loss 普通 open 仍 fail-closed；健康旧实例首次启动生成 clean manifest。
- 更新 current map/backlog；按源码复核五遍并运行固定 JDK/Gradle 全量测试。
- 无新增 Reserved / Unwired 生产类型；所有新增实线必须回到组合根和源码调用点核对。
