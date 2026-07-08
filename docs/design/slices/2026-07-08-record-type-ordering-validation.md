# 0.21a Record Type / Ordering / Validation Slice

## 目标

- 补齐 `storage.record` 的第一批 0.21 能力：内联类型扩展、字符排序策略入口、页内结构校验。
- 让 Record 层更贴近 `innodb-record-design.md` §8/§11/§15/§16，但仍保持当前 B+Tree/Undo 生产链路稳定。
- 明确当前源码已具备 ASC/DESC/NULL/prefix 比较语义，避免后续实现重复改同一比较路径。
- 只在 `storage.record` 与必要的 `storage.btree.SearchKeyComparator` 边界内闭环。

## 设计依据

- `innodb-record-design.md` §8.2：第一阶段类型应覆盖整数、定点、浮点、字符/二进制、DATE/TIME/DATETIME/TIMESTAMP/YEAR。
- `innodb-record-design.md` §8.3：字符类型比较必须经 `CollationStrategy`，不能散落在 codec 或 B+Tree。
- `innodb-record-design.md` §11：RecordComparator / SearchKeyComparator 必须保持 leaf 记录与 node pointer 同序。
- `innodb-record-design.md` §15/§16：页内结构损坏必须抛领域异常，并覆盖 record header、PageDirectory、next_record 链测试。

## 关键决策

- 新增 `TIME`、`TIMESTAMP`、`YEAR`、`BIT` 作为本片内联类型；`BOOLEAN` 仍作为 SQL/DD 层 `TINYINT(1)` 语义别名，不新增物理 TypeId。
- `TIME` 用带符号毫秒 duration 保序编码；`TIMESTAMP` 用 epoch millis 保序编码；`YEAR` 用无符号 2B 年份教学编码。
- `BIT(n)` 使用定长 `ceil(n/8)` byte slice 保存 canonical bits；末字节未使用低位必须清零，比较按 unsigned byte lexicographic。
- `TypeCodecRegistry` 增加 `collationFor(CharsetId, CollationId)`，现有 binary collation 保持默认；本片新增 `CollationId.UTF8_GENERAL_CI` 教学 collation。
- 现有 `RecordComparator` / `SearchKeyComparator` 的 ASC/DESC/NULL/prefix 逻辑只补测试与文档校准，不做语义重写。
- 新增页内校验 helper，检查系统记录、record length 边界、next_record 链、directory slot 目标、`n_owned` 与环检测。

## 非目标

- 不实现 `TEXT` / `BLOB` / overflow page chain；该内容另列 0.21b，并需要 Disk reservation / BLOB page 分配协作。
- 不实现 `ENUM` / `SET` / `JSON` 物理格式。
- 不实现非 UTF8 charset、真实 MySQL collation weight table 或字符级 prefix 截断。
- 不改变当前 record header 8 字节教学布局，也不追求 InnoDB 二进制兼容。
- 不接 SQL parser、DD 类型解析、session/executor 或二级索引 DML。

## 验收测试

- `ColumnTypeTest` 覆盖新 TypeId factory、非法 length/scale/bit width/year 边界。
- `TemporalCodecTest` 覆盖 DATE/DATETIME 旧行为不变，以及 TIME/TIMESTAMP/YEAR 编解码和排序。
- 新增 BIT codec 测试，覆盖 canonical unused bits、长度边界、编码后比较。
- 字符 collation 测试覆盖 binary 不变、case-insensitive UTF8 策略、registry 缺失 collation 拒绝。
- `RecordComparatorTest` 与 `SearchKeyComparatorTest` 覆盖新类型排序一致性，并确认 ASC/DESC/NULL/prefix 不倒退。
- 页内校验测试覆盖坏 record length、next_record 越界/成环、directory slot 指向非法 offset、`n_owned` 异常。
- 固定 JDK/Gradle 全量 `test` 通过。

## Current Map / Backlog 更新要求

- 更新 `storage-backlog.md`：把 0.21 拆成 0.21a 已落与 0.21b overflow/BLOB 后续，不再把 ASC/DESC/NULL 排序列为未实现。
- 更新 `current-implementation-map.md` 的 Record Layer：`record.schema` TypeId 数量、`record.type` codec/collation 状态、Record 缺口表。
- 若新增校验 helper 暂无生产调用，必须进入 `Reserved / Unwired Production Types` 并写明下一步接入点。

## 5 遍复核清单

- 第 1 遍：逐项对照 `storage-backlog.md` 0.21 与 `innodb-record-design.md` §8/§11/§15/§16。
- 第 2 遍：核对当前源码，确认 ASC/DESC/NULL/prefix 已有语义不被重复实现或回退。
- 第 3 遍：核对依赖方向，Record 不 import `fil`、`fsp`、SQL/session 或事务可见性模块。
- 第 4 遍：核对 slice 规模，overflow/BLOB、JSON、ENUM/SET 已明确移出本片。
- 第 5 遍：核对验收测试与 current map/backlog 更新项没有空泛表述或未决决策。
