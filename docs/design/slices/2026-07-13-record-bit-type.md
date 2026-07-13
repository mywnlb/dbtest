# 0.21e2 Record BIT(n) Type

## 目标

- 在 Record 类型系统中增加定长 `BIT(n)`，支持 1..64 位。
- 保持编码字节可直接用于 leaf/node-pointer 的稳定索引比较。
- 通过共享 registry 自动覆盖 Record、Undo 与 B+Tree 调用链。

## 设计依据

- 既有 0.21 type slice：BIT 使用 `ceil(n/8)` bytes，unused low bits 必须清零。
- `innodb-record-design.md` §8.3：固定长度字段应能直接比较 encoded bytes。
- `current-implementation-map.md`：Record/Undo/B+Tree 均使用同一 `TypeCodecRegistry`。

## 关键决策

- `ColumnType.length` 对 BIT 表示 bit width，不是 byte length。
- 物理宽度为 `ceil(bitWidth/8)`，bit string 从首字节最高位开始连续排列。
- 最后字节未使用的低位必须为 0，拒绝同一逻辑位串的非 canonical 编码。
- `ColumnValue.BitValue` 持有防御性复制的 byte array，不复用整数 signed/unsigned 语义。
- 比较使用 unsigned byte lexicographic，等价于固定宽度 bit string 的自然序。
- BIT 是 FIXED；不允许 `unsigned=true`、scale、charset/collation 或 prefixBytes 改变其语义。

## 与 MySQL/InnoDB 的差异

- 本片只实现 1..64 位，不实现更大 bit string 或 SQL bit literal parser。
- Record API 使用 canonical byte array，不在本层提供 Java long 的隐式有符号转换。
- 不追求 MySQL/InnoDB 行格式二进制兼容，只保证本项目页内排序稳定。

## 非目标

- 不实现 BOOLEAN TypeId；BOOLEAN 继续由 SQL/DD 映射为 TINYINT(1)。
- 不实现 BIT 与整数/字符串之间的隐式 cast。
- 不修改 record header、变长目录、redo/undo framing 或页恢复协议。
- 不实现 TEXT/BLOB/JSON/ENUM/SET、Unicode weight 或 overflow chain。

## 验收测试

- `ColumnTypeTest` 覆盖 1/8/9/64 位 factory、固定宽度与 0/65 拒绝。
- `BitCodecTest` 覆盖宽度、往返、防御性复制、保序与 unused low-bit 拒绝。
- `TypeCodecRegistryTest` 覆盖 BIT 路由和固定宽度。
- `RecordCodecTest` 覆盖 BIT 通过真实 schema 的整记录往返。
- `SearchKeyComparatorTest` 覆盖 BIT ASC/DESC 保序和 prefixBytes 拒绝。
- 固定 JDK/Gradle 全量 `test --rerun-tasks` 通过。

## Current Map / Backlog 更新

- `record.schema` TypeId 数量增加 1，注明 `length=bit width`。
- `record.type` 增加 canonical fixed bitstring codec 状态。
- Record 缺口移除 BIT，下一项转向剩余第二阶段类型与 Unicode/LOB。
