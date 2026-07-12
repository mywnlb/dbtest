# 0.21c Record Charset / Collation / Ordering Slice

## 目标

- 补齐字符编码与 collation 的稳定注册，让 CHAR/VARCHAR 不再硬编码 UTF-8 + binary 比较。
- 保持 `RecordComparator` 与 `SearchKeyComparator` 的 NULL、ASC/DESC、prefix 语义严格同序。
- 默认 `UTF8+BINARY` 的记录字节和索引顺序不变，不升级 page、undo 或 redo 格式。

## 设计依据与当前事实

- `innodb-record-design.md` §8.3/§11/§14/§15 要求字符比较经 `CollationStrategy`，registry 初始化后只读，缺失 pair 时禁止比较。
- 当前两个 key 比较器已支持复合 part、短 key、NULL、ASC/DESC 与 byte-prefix；本片只收敛重复逻辑，不重新定义它们。
- `TypeCodecRegistry` 由 `StorageEngine` 单点创建并注入 Record/B+Tree/Undo；无参构造必须保持兼容。

## 稳定标识与注册

- `CharsetId` 保留 `UTF8` 并新增 `LATIN1`；每项使用显式 stable id，禁止把 enum ordinal 当持久标识。
- `CollationId` 保留 `BINARY`，新增 `UTF8_ASCII_CI`、`LATIN1_ASCII_CI`，同样使用显式 stable id。
- 新增不可变 `CharacterTypeRegistry`，注册 charset 编解码器及 `(CharsetId, CollationId)` 到 `CollationStrategy` 的精确映射。
- 合法 pair 固定为 UTF8/LATIN1 各自的 BINARY 与 ASCII_CI；跨 charset 的 CI pair 必须拒绝。
- `TypeCodecRegistry` 无参构造装配默认只读 registry，并暴露 `collationFor`；不提供运行时修改注册表的入口。
- 未注册或 charset 不匹配的 pair 抛领域异常，禁止静默回退到 binary 或 Java 默认 charset。

## 编码与 collation 语义

- UTF8 使用严格 UTF-8，LATIN1 使用 ISO-8859-1；不可映射输入和损坏字节均 fail-closed，不使用 replacement character。
- `FixedBytesCodec` / `VarBytesCodec` 的字符模式经 charset strategy 编解码；BINARY/VARBINARY 继续保存原始字节并按 unsigned byte 排序。
- `ColumnType.charType/varchar` 增加 charset/collation 重载，既有两参 factory 继续生成 UTF8+BINARY。
- `BinaryCollation` 保持逐字节 unsigned lexicographic，确保现有默认 schema 的磁盘与排序兼容。
- 两个 `*_ASCII_CI` 是确定性的教学 collation：仅折叠 ASCII `A-Z`，其余编码字节原样比较，不冒充 MySQL Unicode weight table。
- prefix 仍先按原始编码截 `prefixBytes` 再走 collation；ASCII folding 是逐字节函数，因此截断多字节 UTF-8 仍有确定结果。

## 统一 key-part 排序

- 新增 Record 层 `EncodedKeyPartComparator`，唯一负责 nullable 两侧、byte-prefix、类型/collation 比较与 ASC/DESC 反转。
- `RecordComparator` 保留 infimum/supremum、record slice 与短 key 处理；非 NULL part 委托统一比较器。
- `SearchKeyComparator` 保留 key 编码与短 key 处理；每个 part 委托同一比较器，禁止 B+Tree 自建字符排序。
- 比较结果只承诺负/零/正；DESC 反转规范化后的符号，避免依赖策略返回值 magnitude。

## 非目标

- 不实现完整 Unicode case/accent 权重、locale tailoring、normalization、weight cache 或持久 weight string。
- 不改变 byte-prefix 为字符前缀，不实现存储层前缀截断、回表确认或二级索引 DML。
- 不实现 TEXT/BLOB/overflow、ENUM/SET/JSON、TIME/TIMESTAMP/YEAR/BIT；这些在排序基础稳定后另切片推进。
- 不做 schema-aware 页内 key 顺序 validator；现有物理结构 validator 继续不接 schema/keyDef。

## 验收测试与文档

- stable id 测试覆盖唯一性、反查和未知 id；字符 registry 覆盖合法 pair、错配 pair 与只读默认装配。
- 字符 codec 测试覆盖 UTF8/LATIN1 round-trip、不可映射/损坏字节、CHAR padding、binary 兼容和 ASCII-CI 等价/次序。
- 两个 key 比较器用共享矩阵覆盖 BINARY/ASCII-CI × NULL/非 NULL × ASC/DESC × full/prefix，并断言符号一致。
- 全量测试通过；更新 current map 的 registry/codec/比较链与 Record 缺口，并把 backlog 下一项改为 0.21d schema-aware key 顺序校验。

## 5 遍复核清单

- 第 1 遍：对照 Record 设计 §8/§11/§14/§15，确认 registry、缺失 pair 和字符比较边界完整。
- 第 2 遍：核对两个现有比较器，确认 NULL、ASC/DESC、prefix、短 key 与哨兵不被重复或改义。
- 第 3 遍：核对兼容性，默认 UTF8+BINARY 的 payload、undo/redo/page 格式和构造 API 均不变。
- 第 4 遍：核对依赖与高扇出，Record 不反向依赖 B+Tree/SQL/DD，生产 registry 仍由 StorageEngine 单点装配。
- 第 5 遍：核对范围与测试，Unicode weight、LOB/新类型、schema-aware validator 均有明确后续边界。
