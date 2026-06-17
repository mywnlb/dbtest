# 记录层 format（R1+R2 第 3/3 批）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 `cn.zhangyis.db.storage.record.format`：物理记录头、NULL bitmap、变长字段目录、逻辑记录、RecordEncoder/RecordDecoder 与格式异常。

**Architecture:** `record.format` 位于 `record.type` 之上，只在 byte array 上编解码物理记录，不接触页、Buffer Pool、MTR 或 Disk Manager。布局采用简化前向格式 `[RecordHeader][NullBitmap][VarLenDirectory][FixedArea][VariableArea]`，与 InnoDB 思想一致但不做二进制兼容。

**Tech Stack:** Java 25、JUnit Jupiter、`record.schema`、`record.type`。

**Spec:** `docs/superpowers/specs/2026-06-11-record-schema-type-format-design.md`（§5）

**现状说明:** 当前源码已存在 `src/main/java/cn/zhangyis/db/storage/record/format`，但测试目录中尚无 `record/format` 测试。本计划的首要价值是补齐 format 测试计划，并把现有实现的职责边界写清楚。

**TDD 说明:** 下文“运行确认失败”的 Expected 是从空实现或删除对应实现后的重放语义；在当前分支直接运行时，源码已经存在，失败点更可能体现为缺测试或行为断言未覆盖。

---

## 文件结构

**Create / Verify:**
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordType.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordHeader.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordHeaderLayout.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/U16.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/NullBitmap.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/VarLenDirectory.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/LogicalRecord.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordEncoder.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordDecoder.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordFormatException.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordTooLargeException.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/SchemaVersionMismatchException.java`

**Tests to create:**
- `src/test/java/cn/zhangyis/db/storage/record/format/RecordHeaderTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/format/NullBitmapTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/format/VarLenDirectoryTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/format/RecordCodecRoundTripTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/format/RecordFormatValidationTest.java`

---

## Task 1: RecordType + RecordHeader + U16

**Files:**
- Create / Verify: `RecordType.java`, `RecordHeader.java`, `RecordHeaderLayout.java`, `U16.java`
- Test: `RecordHeaderTest.java`

- [ ] **Step 1: 写失败测试**

覆盖记录类型 code、flags、heapNo、nOwned、nextRecordOffset、recordLength 的往返，以及 u16/u8 越界拒绝。

```java
@Test
void recordHeaderRoundTripsAllFields() {
    byte[] buf = new byte[RecordHeaderLayout.SIZE];
    RecordHeader header = new RecordHeader(true, true, RecordType.NODE_POINTER, 42, 7, 128, 256);
    header.writeTo(buf, 0);

    RecordHeader decoded = RecordHeader.readFrom(buf, 0);

    assertTrue(decoded.deletedFlag());
    assertTrue(decoded.minRecFlag());
    assertEquals(RecordType.NODE_POINTER, decoded.recordType());
    assertEquals(42, decoded.heapNo());
    assertEquals(7, decoded.nOwned());
    assertEquals(128, decoded.nextRecordOffset());
    assertEquals(256, decoded.recordLength());
}
```

- [ ] **Step 2: 运行确认失败**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.format.RecordHeaderTest"
```

Expected: 缺少测试或实现不满足 header 编解码断言。

- [ ] **Step 3: 实现记录头**

实现规则：
- `RecordType` code 落盘 2 bit，顺序固定：`CONVENTIONAL=0`、`NODE_POINTER=1`、`INFIMUM=2`、`SUPREMUM=3`。
- `RecordHeaderLayout.SIZE = 8`。
- flags bit：bit0 delete-mark、bit1 min-rec、bit2-3 record type code。
- `heapNo`、`nextRecordOffset`、`recordLength` 使用 2 字节无符号大端。
- `nOwned` 使用 1 字节无符号。
- 越界使用 `DatabaseValidationException`；未知 record type code 使用 `RecordFormatException`。

- [ ] **Step 4: 运行确认通过**

Expected: `RecordHeaderTest` 通过。

---

## Task 2: NullBitmap

**Files:**
- Create / Verify: `NullBitmap.java`
- Test: `NullBitmapTest.java`

- [ ] **Step 1: 写失败测试**

覆盖 `byteLength`、全 0、混合 NULL、跨字节 NULL 位、越界拒绝。

```java
@Test
void nullBitmapUsesNullableColumnOrder() {
    NullBitmap bm = new NullBitmap(10);
    bm.set(0);
    bm.set(8);
    byte[] buf = new byte[bm.byteLength()];
    bm.writeTo(buf, 0);

    NullBitmap decoded = NullBitmap.readFrom(buf, 0, 10);

    assertTrue(decoded.get(0));
    assertFalse(decoded.get(1));
    assertTrue(decoded.get(8));
    assertEquals(2, decoded.byteLength());
}
```

- [ ] **Step 2: 运行确认失败**

Expected: 缺少测试或 bitmap 行为不满足断言。

- [ ] **Step 3: 实现 NullBitmap**

实现规则：
- 位数等于 schema 中 nullable 列数量，不等于总列数。
- 按 nullable 列出现顺序编号，第 i 个 nullable 列使用 bit `i % 8`。
- `1` 表示 NULL，`0` 表示非 NULL。
- `byteLength(count) = (count + 7) / 8`。
- `writeTo` 和 `readFrom` 只拷贝 bitmap 字节，不解释 schema。

- [ ] **Step 4: 运行确认通过**

Expected: `NullBitmapTest` 通过。

---

## Task 3: VarLenDirectory

**Files:**
- Create / Verify: `VarLenDirectory.java`
- Test: `VarLenDirectoryTest.java`

- [ ] **Step 1: 写失败测试**

覆盖 0 个变长列、多个长度往返、长度 65535 边界、负数和超过 u16 拒绝。

```java
@Test
void directoryRoundTripsLengths() {
    VarLenDirectory dir = new VarLenDirectory(new int[] {0, 5, 65535});
    byte[] buf = new byte[dir.byteLength()];
    dir.writeTo(buf, 0);

    VarLenDirectory decoded = VarLenDirectory.readFrom(buf, 0, 3);

    assertEquals(3, decoded.count());
    assertEquals(0, decoded.length(0));
    assertEquals(5, decoded.length(1));
    assertEquals(65535, decoded.length(2));
}
```

- [ ] **Step 2: 运行确认失败**

Expected: 缺少测试或目录编解码断言失败。

- [ ] **Step 3: 实现 VarLenDirectory**

实现规则：
- 每个非 NULL 变长列对应一个 2 字节无符号长度。
- 目录中不存列 ID，列含义由 schema 的列序和 NULL bitmap 推导。
- NULL 的变长列不进入目录，也不占 variable area。
- 构造函数防御性拷贝长度数组。

- [ ] **Step 4: 运行确认通过**

Expected: `VarLenDirectoryTest` 通过。

---

## Task 4: LogicalRecord + RecordEncoder

**Files:**
- Create / Verify: `LogicalRecord.java`, `RecordEncoder.java`
- Test: `RecordCodecRoundTripTest.java`, `RecordFormatValidationTest.java`

- [ ] **Step 1: 写失败测试**

至少覆盖三种 schema：
- 纯定长：BIGINT + INT + DATE。
- 混合 NULL：nullable VARCHAR 为 NULL，不进入 varlen dir 和 variable area。
- 混合类型：DECIMAL + DOUBLE + VARCHAR + VARBINARY + DATETIME。

```java
@Test
void encodeDecodeMixedRecordRoundTrips() {
    TableSchema schema = new TableSchema(11L, List.of(
            col(0, "id", ColumnType.bigint(false, false)),
            col(1, "name", ColumnType.varchar(20, true)),
            col(2, "amount", ColumnType.decimal(10, 2, false)),
            col(3, "created_at", ColumnType.datetime(false))));
    LogicalRecord record = new LogicalRecord(11L, List.of(
            new ColumnValue.IntValue(7),
            new ColumnValue.StringValue("alice"),
            new ColumnValue.DecimalValue(new BigDecimal("12.34")),
            new ColumnValue.TemporalValue(TemporalKind.DATETIME, 1700000000000L)),
            false,
            RecordType.CONVENTIONAL);

    TypeCodecRegistry registry = new TypeCodecRegistry();
    byte[] encoded = new RecordEncoder(registry).encode(record, schema);
    LogicalRecord decoded = new RecordDecoder(registry).decode(encoded, schema);

    assertEquals(record.columnValues(), decoded.columnValues());
    assertFalse(decoded.deleted());
    assertEquals(RecordType.CONVENTIONAL, decoded.recordType());
}
```

- [ ] **Step 2: 运行确认失败**

Expected: 编译失败或 encode/decode 往返失败。

- [ ] **Step 3: 实现 LogicalRecord 与 RecordEncoder**

实现规则：
- `LogicalRecord` 保存 `schemaVersion`、按 ordinal 排列的 `columnValues`、`deleted`、`recordType`。
- `RecordEncoder.encode` 先校验 schemaVersion 和列数量。
- NULL 给非 nullable 列时抛 `RecordFormatException`。
- 非 NULL 列委托 `TypeCodecRegistry.validate`。
- 规划阶段计算 nullableCount、NullBitmap、非 NULL 变长列长度、fixed area 长度、variable area 长度。
- 物理布局固定为 `[RecordHeader][NullBitmap][VarLenDirectory][FixedArea][VariableArea]`。
- `RecordHeader.heapNo/nOwned/nextRecordOffset` 在本批统一写 0，后续 page 层维护真实值。
- `recordLength` 超过 `0xFFFF` 时抛 `RecordTooLargeException`。

- [ ] **Step 4: 运行确认通过**

Expected: encoder 相关测试通过。

---

## Task 5: RecordDecoder

**Files:**
- Create / Verify: `RecordDecoder.java`
- Test: `RecordCodecRoundTripTest.java`, `RecordFormatValidationTest.java`

- [ ] **Step 1: 写失败测试**

覆盖：
- `RecordHeader.recordLength != byte[].length` 抛 `RecordFormatException`。
- NULL bitmap 解码后 NULL 列返回 `ColumnValue.NullValue.INSTANCE`。
- 定长列按 codec fixed width 从 fixed area 解码。
- 变长列按 VarLenDirectory 从 variable area 解码。

- [ ] **Step 2: 运行确认失败**

Expected: decoder 行为不满足断言。

- [ ] **Step 3: 实现 RecordDecoder**

实现规则：
- 先读取 header，并校验 `header.recordLength() == buf.length`。
- 读取 NullBitmap 后，按 schema 列序生成 `isNull[]`。
- 扫描 schema 计算 active var count 与 fixed area 长度。
- 读取 VarLenDirectory。
- 对每列按 storageKind 和 NULL 状态切出 `FieldSlice`。
- 委托 codec decode。
- 解码结果使用传入 schema 的 `schemaVersion`；当前物理记录字节不单独存 schemaVersion。

- [ ] **Step 4: 运行确认通过**

Expected: decoder 相关测试通过。

---

## Task 6: format 批全量验证

- [ ] **Step 1: 运行 format 测试**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.format.*"
```

Expected: `record.format` 全部测试通过。

- [ ] **Step 2: 运行 record 包回归**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.*"
```

Expected: schema/type/format 测试全部通过。

- [ ] **Step 3: 运行全量测试**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test
```

Expected: `BUILD SUCCESSFUL`。

---

## Self-Review

**Spec 覆盖:** 覆盖 spec §5 的 `RecordType`、`RecordHeader`、`NullBitmap`、`VarLenDirectory`、`LogicalRecord`、`RecordEncoder`、`RecordDecoder` 和 format 异常。

**简化点固定:** 记录头是 8 字节前向布局，不兼容 InnoDB 二进制格式；`heapNo`、`nOwned`、`nextRecordOffset` 本批只编解码；无隐藏列、无 page heap、无 PageDirectory、无 overflow。

**与前两批一致性:** format 只依赖 `record.schema` 和 `record.type`；NULL 由 NullBitmap 管，codec 不处理 NULL；变长列长度由 VarLenDirectory 管，字段内不存长度。
