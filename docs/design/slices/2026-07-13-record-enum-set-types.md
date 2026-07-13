# 0.21f Record ENUM / SET Types

## 目标

- 增加 schema-owned `ENUM` 与 `SET` 字典及定长物理编码。
- 让 Record、Undo 与 B+Tree 通过共享 registry 使用 ordinal/bitmap 排序。
- 保持 SQL 字符串解析和隐式转换在 Record 模块之外。

## 设计依据

- `innodb-record-design.md` §8.2：ENUM/SET 字典保存在 schema，记录保存 ordinal/bitmap。
- `ColumnType` 是 schema 内不可变类型描述符，适合持有不可变 symbols 快照。
- 定长字段必须可直接比较 encoded bytes，避免索引比较构造字符串集合。

## 关键决策

- `ColumnType.symbols` 保存声明顺序，非 ENUM/SET 类型必须为空。
- symbols 必须非空、非 blank、无重复；ENUM 上限 65535，SET 上限 64。
- ENUM ordinal 从 1 开始；1..255 使用 1B，256..65535 使用 2B unsigned big-endian。
- SET 使用最多 8B unsigned bitmap；symbol ordinal 1 对应 bit 0，宽度为 `ceil(symbolCount/8)`。
- `EnumValue` 只承载 ordinal，`SetValue` 只承载 bitmap；Record 不做名称解析。
- encoded bytes 按 unsigned lexicographic 比较，等价于 ordinal/bitmap 数值序。

## 与 MySQL/InnoDB 的差异

- 不支持 ENUM 的内部 0/空串错误值；ordinal 必须为 1..N。
- 不支持重复 SET member、逗号转义、SQL mode 或字符串到集合的转换。
- 字典变更属于未来 DD/online DDL schema-version 迁移，不在本片原地改写。

## 非目标

- 不接 parser/binder/DD，不接受 StringValue 直接写 ENUM/SET。
- 不实现 ENUM/SET 的 collation、prefix index 或 locale 名称比较。
- 不修改 record header、page layout、redo/undo framing 或恢复协议。
- 不实现 TEXT/BLOB/JSON、Unicode weight 或 overflow chain。

## 验收测试

- `ColumnTypeTest` 覆盖不可变字典、重复/blank/数量边界与非枚举 symbols 拒绝。
- `EnumeratedCodecTest` 覆盖 ENUM 1B/2B、SET width/bitmap 边界、往返与保序。
- `TypeCodecRegistryTest` 覆盖 ENUM/SET codec 路由。
- `RecordCodecTest` 覆盖真实 fixed-area schema 往返。
- `SearchKeyComparatorTest` 覆盖 ENUM/SET ASC/DESC 与 prefix 拒绝。
- 固定 JDK/Gradle 全量测试通过。

## Current Map / Backlog 更新

- `record.schema` 记录 symbols 是 schema 权威快照，类型数增加 2。
- `record.type` 标记 ordinal/bitmap codec 已生产接线。
- Record 缺口移除 ENUM/SET，剩余 JSON/LOB 与 Unicode weight。
