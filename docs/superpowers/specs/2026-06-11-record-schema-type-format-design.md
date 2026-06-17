# Spec：记录层 R1+R2（schema + type + format）— innodb-record-design.md §3-§8

- 日期：2026-06-11
- 关联设计：`C:\coding\java\self\dbtest\dbtest\docs\design\innodb-record-design.md`（§3 架构、§4 包、§5 领域模型、§6 物理↔逻辑、§7 页内结构、§8 类型系统、§15 异常、§16 测试、§17 顺序）
- 上游依赖：domain（值对象）、common.exception。**不依赖** buf/fil/fsp（本片纯值对象 + byte 缓冲编解码）。
- 状态：brainstorming 评审通过——第一片 = R1+R2（schema+type+format）；类型集已锁定；binary collation；无隐藏列/页/RecordRef；MVCC/redo 暂停。

## 1. 背景与范围

记录层第一片落地 §17 step1-3：`record.schema`（表/列/索引描述 + 类型描述符）、`record.type`（类型编码/解码/比较 + 值对象）、`record.format`（物理记录头 + NULL bitmap + 变长目录 + 编解码）。本片在 **byte 缓冲**上工作，不碰页、不碰 IO，可完全单测，是 R3（页内 heap/PageDirectory）与 R4（cursor/insert）的地基。

**做**：schema 值对象；type 类型系统（10 类）+ codec/comparator/registry/ColumnValue；format 记录头/NULL bitmap/变长目录/RecordEncoder/RecordDecoder。

**不做**（注释标注，后续片）：
- 页内结构（infimum/supremum、next_record 真实值、PageDirectory、heap、GarbageList）→ R3；本片 `RecordHeader.nextRecordOffset` 仅是字段（编解码），值由页层维护。
- `RecordCursor`、`RecordPageAccessor`、insert/lookup → R4；`RecordRef`（页定位）→ R3。
- 隐藏列（DB_TRX_ID/DB_ROLL_PTR/DB_ROW_ID）+ MVCC、redo/undo payload、update/delete/purge/reorganize → 后续（依赖 trx/redo，已暂停）。
- 类型：DECIMAL/FLOAT/DOUBLE/CHAR/VARCHAR/BINARY/VARBINARY/DATE/DATETIME/INT 家族**以外**的（TIME/TIMESTAMP/YEAR/BOOLEAN/ENUM/SET/JSON/TEXT/BLOB）。
- collation：仅 binary（UTF-8 字节序）；ci/weight collation 延后（`CollationStrategy` 接口预留）。

## 2. 包与依赖（依 §4，无环）

`record.schema`（依赖 domain） ← `record.type`（依赖 schema） ← `record.format`（依赖 type）。异常各置所属包，继承 `DatabaseRuntimeException`。

## 3. record.schema

- `TypeId`（枚举）：`TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR, VARCHAR, BINARY, VARBINARY, DATE, DATETIME`。
- `StorageKind`（枚举）：`FIXED`、`VARIABLE`。
- `CharsetId`（枚举：`UTF8`）、`CollationId`（枚举：`BINARY`）——首版各一个值，留扩展。
- `ColumnType`（不可变，§8.1）：`typeId`、`nullable`、`length`（CHAR/VARCHAR/BINARY/VARBINARY = 最大**字节**长度；DECIMAL = 精度 p；其余忽略=0）、`scale`（DECIMAL 用，其余 0）、`unsigned`（整数用）、`charset`、`collation`、`storageKind`。
  - 静态工厂：`tinyint/smallint/int/bigint(boolean unsigned, boolean nullable)`、`floatType/doubleType(nullable)`、`decimal(int p, int s, boolean nullable)`、`charType(int nBytes, nullable)`、`varchar(int nBytes, nullable)`、`binary(int n, nullable)`、`varbinary(int n, nullable)`、`date(nullable)`、`datetime(nullable)`。
  - 构造校验：length/scale/p 合法（DECIMAL: 1≤p≤38、0≤s≤p；CHAR/VARCHAR/BINARY/VARBINARY: n≥1）；storageKind 与 typeId 一致（仅 VARCHAR/VARBINARY = VARIABLE）。
- `ColumnId`（record：int ordinal 包装）、`ColumnDef`（`ColumnId id, String name, ColumnType type, int ordinal`）。
- `TableSchema`（`long schemaVersion, List<ColumnDef> columns`，按 ordinal 0..n-1 有序、不可变副本）；提供 `column(ColumnId)`、`columnCount()`、`nullableColumns()`、`variableColumns()`。
- `KeyOrder`（枚举 `ASC, DESC`）、`KeyPartDef`（`ColumnId columnId, KeyOrder order, int prefixBytes`(0=全长)）、`IndexKeyDef`（`long indexId, List<KeyPartDef> parts`）。

## 4. record.type

### 4.1 ColumnValue（sealed 风格 + 子类型）
`NullValue`（单例）、`IntValue(long)`、`DoubleValue(double)`、`DecimalValue(BigDecimal)`、`StringValue(String)`、`BinaryValue(byte[] 防御性拷贝)`、`TemporalValue(TemporalKind kind, long normalized)`；`TemporalKind`（`DATE, DATETIME`）。
- 不知道自身在哪页；不可变（BinaryValue 拷入拷出）。

### 4.2 字段读写抽象（不依赖 buf）
- `FieldSlice`：只读视图 `(byte[] backing, int offset, int length)`，提供 `byteAt/readInt/readLong/copyBytes/compareTo(其它 FieldSlice，无符号字节序)`。
- `FieldWriter`：向 `(byte[] backing, int offset)` 顺序写 `putByte/putInt/putLong/putBytes`，记录已写长度。

### 4.3 TypeCodec（Strategy）
`int encodedLength(ColumnValue, ColumnType)`、`void encode(ColumnValue, ColumnType, FieldWriter)`、`ColumnValue decode(FieldSlice, ColumnType)`、`int compare(FieldSlice, FieldSlice, ColumnType)`（返回 <0/0/>0）、`void validate(ColumnValue, ColumnType)`。NULL 不由 codec 处理（由 format 的 NullBitmap）。
- `CollationStrategy`：`int compare(byte[] a, int ao, int al, byte[] b, int bo, int bl)`；`BinaryCollation` = 无符号字节字典序。

### 4.4 具体 codec 与**保序编码**（编码后字节按无符号字典序 = 类型自然序，可直接比字节）
- `IntegerCodec(width∈{1,2,4,8}, unsigned)`：signed → 翻最高位符号位后大端 width 字节；unsigned → 原样大端。decode 反变换（signed 还原符号位 + 符号扩展）。`compare` = 无符号字节序。
- `FloatingCodec(width∈{4,8})`：`bits=doubleToLongBits/floatToIntBits(normalize(v))`，`normalize`: `v==0.0 → +0.0`（-0.0 归一），NaN 用规范 NaN；保序变换 `ordered = bits ^ ((bits>>(W*8-1)) | MIN)`（正数翻符号位、负数翻全部位），大端存。decode 反变换 → double（FLOAT 经 `(float)` 窄化）。排序：`-Inf<负<±0<正<+Inf<NaN`。
- `DecimalCodec(p, s)`：`W = ceil((p*log2(10)+1)/8)`；encode：`bd.setScale(s)`（需要舍入 → 抛 `ColumnValueOutOfRangeException`），`unscaled=bd.unscaledValue()`（`|unscaled|<10^p` 否则越界），两补大端左补到 W（正补 0x00 负补 0xFF），翻最高位符号位。decode：翻回符号位 → `new BigInteger(bytes)` → `new BigDecimal(unscaled, s)`。`compare` = 无符号字节序（同列同 scale 故等价数值序）。
- `FixedBytesCodec(nBytes, padByte, asString)`：CHAR(padByte=0x20,asString=true)/BINARY(padByte=0x00,asString=false)。encode：取字节（StringValue→UTF-8 / BinaryValue→原始），>n 抛越界，补 padByte 至 n。decode：读 n 字节；asString → 去尾部 0x20 后 `new String(UTF-8)`（CHAR 语义）；否则 → BinaryValue（保留全 n 字节）。`encodedLength=n`。`compare` = binary collation。
- `VarBytesCodec(maxBytes, asString)`：VARCHAR/VARBINARY。encode：字节 >maxBytes 抛越界，写实际字节；`encodedLength=实际字节数`（长度由 format 变长目录记录，字段内不存长度）。decode：按 slice 长度读 → StringValue/BinaryValue。`compare` = binary collation。
- `TemporalCodec(kind)`：DATE → normalized=epochDay，4 字节 signed 保序；DATETIME → normalized=epochMilli，8 字节 signed 保序。`compare` = 保序字节序（线性）。`ColumnValue` = `TemporalValue(kind, normalized)`。

### 4.5 TypeCodecRegistry（只读）
`codecFor(ColumnType)`：按 typeId（+ width/p/s/n 参数）返回（可缓存）codec；`comparatorFor(ColumnType)` 复用 codec.compare；`validate(ColumnValue, ColumnType)` 委托 codec。未知 typeId → `UnsupportedColumnTypeException`。

## 5. record.format

### 5.1 RecordType（枚举）
`CONVENTIONAL(0), NODE_POINTER(1), INFIMUM(2), SUPREMUM(3)`（落盘 2 bit）。

### 5.2 RecordHeader + 布局（简化定长头，前向布局，非 InnoDB 二进制兼容）
字段：`deletedFlag, minRecFlag, recordType, heapNo, nOwned, nextRecordOffset, recordLength`。
`RecordHeaderLayout`（HEADER_SIZE=8）：`FLAGS(0,1)`（bit0 deleted、bit1 minRec、bit2-3 recordType code）、`HEAP_NO(1,2)`、`N_OWNED(3,1)`、`NEXT_RECORD_OFFSET(4,2)`、`RECORD_LENGTH(6,2)`。`writeTo(FieldWriter@0)/readFrom(FieldSlice@0)`。

### 5.3 NullBitmap
`record.format.NullBitmap`：位数 = schema 中 nullable 列数；`byteLength = ceil(nullableCount/8)`；按 nullable 列在 schema 中的顺序编号；`set(i)/get(i)`、`encode→bytes`、`decode(bytes)`。1=NULL。

### 5.4 VarLenDirectory
非 NULL 的变长列（VARCHAR/VARBINARY）按列序各存 2 字节长度（≤65535，超 page 用 overflow 延后）。`encode/decode`。

### 5.5 LogicalRecord
`(long schemaVersion, List<ColumnValue> columnValues(按 ordinal 有序), boolean deleted, RecordType recordType)`。

### 5.6 物理布局（byte[]，前向）
`[RecordHeader 8][NullBitmap ceil(nn/8)][VarLenDirectory 2*活跃变长列数][FixedArea][VariableArea]`，其中：
- NULL 列：bitmap 置位，不占 fixed/var/dir 空间（InnoDB 风格）。
- 非 NULL FIXED 列：按列序写入 FixedArea（codec.encodedLength 定长）。
- 非 NULL VARIABLE 列：长度入 VarLenDirectory（列序），字节入 VariableArea。

### 5.7 RecordEncoder / RecordDecoder（§6）
- `byte[] RecordEncoder.encode(LogicalRecord, TableSchema)`：① 校验 schemaVersion 与列数；② 逐列 `registry.validate`；③ 规划：NULL bitmap、各非 NULL 变长列长度、各区长度、recordLength；④ 写 header（heapNo=0/nOwned=0/nextRecordOffset=0/recordLength/flags by recordType+deleted）、null bitmap、varlen dir、fixed area、var area。
- `LogicalRecord RecordDecoder.decode(byte[], TableSchema)`：读 header → null bitmap → 逐列：NULL→NullValue；FIXED→FieldOffsetResolver 切 FixedArea 按 encodedLength；VARIABLE→读 dir 长度切 VariableArea；codec.decode → ColumnValue。schemaVersion 不匹配 → `SchemaVersionMismatchException`。
- `FieldOffsetResolver`：按 schema + null bitmap + varlen dir 计算每列在 byte[] 中的 slice（fixed 累加 encodedLength；variable 累加 dir 长度）。

## 6. 异常（各置所属包，extends DatabaseRuntimeException）

- type：`UnsupportedColumnTypeException`、`ColumnValueOutOfRangeException`、`InvalidColumnValueException`（值与类型不符，如把 StringValue 给整数列）。
- format：`RecordFormatException`、`RecordTooLargeException`（编码超 page inline 上限——本片以一个常量上限近似，无 overflow）、`SchemaVersionMismatchException`。
- 参数 null/非法用 `DatabaseValidationException`（common）。

## 7. 并发

本片全是不可变值对象与无状态 codec（registry 只读），线程安全，无 latch/锁。format 在调用方提供的 byte[] 上工作，缓冲所有权归调用方。

## 8. 测试（纯单测，无 IO）

- schema：ColumnType 工厂/校验（DECIMAL p/s 边界、storageKind 一致）、TableSchema 有序/nullable/variable 视图、IndexKeyDef。
- type：
  - IntegerCodec：各宽度 signed/unsigned 往返 + 边界（MIN/MAX/0/-1）；**保序**——编码字节无符号比较 = 数值序（含跨正负）。
  - FloatingCodec：FLOAT/DOUBLE 往返；排序 `-Inf<负<-0.0=+0.0<正<+Inf<NaN`；-0.0 与 +0.0 编码相等；FLOAT 窄化。
  - DecimalCodec：往返（正/负/0、不同 p,s）、负<正字节序、scale>s 拒绝、有效数字>p 拒绝、列宽 W。
  - FixedBytesCodec：CHAR 空格补齐 + 去尾空格、BINARY 0 补齐保留、超长拒绝、字节序比较。
  - VarBytesCodec：空串/最大长度/超长拒绝、字节序比较。
  - TemporalCodec：DATE/DATETIME 往返 + 排序（含 epoch 前负值）。
  - registry：codecFor 各类型、未知类型拒绝。
- format：RecordHeader 往返（各 flag/heapNo/nOwned/nextRecordOffset/recordLength）；NullBitmap（全 NULL/无 NULL/混合、字节长度）；VarLenDirectory；**整条 encode→decode 往返**（多 schema：纯定长、含变长、含 NULL、含 DECIMAL/时间/浮点混合）；schemaVersion 不匹配拒绝；列值类型不符拒绝。

## 9. 简化点（注释标注）

- 保序编码（整数/浮点/decimal/时间翻符号位）使 key 可直接比字节；浮点 NaN 排最大、-0.0=+0.0。
- collation 仅 binary（UTF-8 字节序）；CHAR/VARCHAR length 以**字节**计；CHAR 解码去尾空格。
- 记录头定长前向布局，非 InnoDB 二进制兼容；nextRecordOffset/heapNo/nOwned 本片只编解码、值由页层（R3）维护。
- 无隐藏列、无 RecordRef、无页、无 overflow（超长以常量上限近似抛 RecordTooLargeException）。
- 时间仅 DATE/DATETIME；DECIMAL p≤38。

## 10. 后续衔接

- R3 page：用 §5.3 `FilePageHeader(INDEX)` 框定 INDEX 页，把 RecordEncoder 产出的字节放入 heap，维护 infimum/supremum、next_record 链、PageDirectory、FreeSpace/GarbageList；引入 `RecordRef`。
- R4 cursor + insert + lookup：`RecordCursor` 字段级读、`RecordPageAccessor.findInsertPosition/findEqual/insert`（directory 二分 + key 有序插入），用 `IndexKeyDef` + codec.compare。
- 后续：隐藏列+MVCC、update/delete/purge、redo/undo payload、reorganize、更多类型/collation。

## 11. 计划分片与当前代码对齐

本 spec 拆成三份 implementation plan，便于按纯值对象、类型系统、记录格式三条独立验证线推进：

- `docs/superpowers/plans/2026-06-11-record-schema.md`：第 1/3 批，覆盖 §3 `record.schema`。当前已有 `src/main/java/cn/zhangyis/db/storage/record/schema` 与对应 schema 测试。
- `docs/superpowers/plans/2026-06-11-record-type.md`：第 2/3 批，覆盖 §4 `record.type`。当前已有 `src/main/java/cn/zhangyis/db/storage/record/type` 与对应 type 测试。
- `docs/superpowers/plans/2026-06-11-record-format.md`：第 3/3 批，覆盖 §5 `record.format`。当前已有 `src/main/java/cn/zhangyis/db/storage/record/format`；后续应优先补齐 `src/test/java/cn/zhangyis/db/storage/record/format` 下的记录头、NULL bitmap、变长目录、整条记录往返和异常测试。

三批的依赖方向固定为：

```text
record.schema -> record.type -> record.format
```

`record.format` 仍然只在 byte array 上工作，不接触 `storage.buf`、`storage.fil`、`storage.fsp` 或 `storage.mtr`。R3 开始引入页内 heap/PageDirectory 时，才能把本批 `RecordEncoder` 产出的物理记录字节放入 INDEX page。
