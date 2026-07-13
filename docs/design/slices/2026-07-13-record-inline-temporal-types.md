# 0.21e1 Record Inline Temporal Types

## 目标

- 在现有 Record 类型系统中补齐 `TIME`、`TIMESTAMP`、`YEAR` 三种定长内联标量。
- 保持 `DATE`、`DATETIME` 的既有编码字节与自然排序不变。
- 让 Record、Undo 与 B+Tree 经共享 `TypeCodecRegistry` 自动获得新类型能力。

## 设计依据

- `innodb-record-design.md` §8.2：第一阶段时间类型包含 DATE/TIME/DATETIME/TIMESTAMP/YEAR。
- `innodb-record-design.md` §8.3：定长类型的编码字节必须可直接按自然序比较。
- `current-implementation-map.md`：生产编码、解码和索引比较均汇入共享类型 registry。

## 关键决策

- `TIME` 使用 8B 带符号毫秒时长；翻转符号位后大端编码，unsigned byte 次序即时间次序。
- `TIMESTAMP` 使用 8B UTC epoch millis，与 DATETIME 物理宽度相同但保留独立 TypeId/TemporalKind。
- `YEAR` 使用 2B unsigned 大端教学编码，物理范围为 0..65535。
- DATE 保持 4B signed epochDay；DATETIME 保持 8B signed epoch millis，不改变历史记录字节。
- `TemporalValue.normalized` 继续作为统一 long 载体，不增加 SQL 日期时间对象依赖。
- 三种类型均为 FIXED，`ColumnType.length/scale/unsigned` 保持 0/0/false。

## 与 MySQL/InnoDB 的差异

- 本片不限制 MySQL TIME 的 838:59:59 范围；SQL 层以后负责语法与业务范围校验。
- YEAR 未采用 MySQL YEAR 的显示宽度和 1901..2155/0000 规则，只表达无符号 2B 存储值。
- TIMESTAMP 的 session 时区转换不进入 Record；Record 只接受已归一化 UTC epoch millis。
- 不追求 MySQL/InnoDB 二进制格式兼容，只保持本项目已有保序编码规则。

## 非目标

- 不实现 `BIT(n)`；它需要独立的位宽和 canonical unused-bit 约束，留给 0.21e2。
- 不实现 TEXT/BLOB/JSON、overflow page chain、ENUM/SET 或 Unicode weight。
- 不接 SQL parser、binder、DD 类型解析、session 时区或 executor 类型转换。
- 不改变 record header、页布局、redo/undo payload framing 或 schema 持久化格式。

## 验收测试

- `ColumnTypeTest` 覆盖三个 factory、TypeId 与 FIXED 属性。
- `TemporalCodecTest` 覆盖三类往返、边界、保序、kind mismatch 与 YEAR 越界。
- `TypeCodecRegistryTest` 覆盖 registry 路由及 8B/8B/2B 固定宽度。
- `RecordCodecTest` 覆盖三类通过真实 schema 的整记录往返。
- `UndoRecordCodecTest` 与 `SearchKeyComparatorTest` 覆盖共享 registry 的 undo/B+Tree 生产协作链。
- 编译期穷尽 switch 校验覆盖 B+Tree 最坏键预算；全量 Gradle test 通过。

## Current Map / Backlog 更新

- `record.schema` TypeId 数量从 13 更新为 16，列出新增时间类型。
- `record.type` 标明五种时间类型的保序编码已接共享 registry。
- Record 缺口移除 TIME/TIMESTAMP/YEAR，下一步指向 0.21e2 `BIT(n)`。
