# 记录层 type（R1+R2 第 2/3 批）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 `cn.zhangyis.db.storage.record.type`：逻辑列值、字段切片/写入器、排序规则、类型 codec、codec registry 与类型异常。

**Architecture:** `record.type` 是 `record.schema` 之上的纯类型系统。所有 codec 都是无状态 Strategy，NULL 由 `record.format.NullBitmap` 管，不进入 codec；整数、浮点、DECIMAL、时间采用保序编码，使同类型 encoded bytes 的无符号字节序等价于自然序。

**Tech Stack:** Java 25、JUnit Jupiter、`BigDecimal`、`BigInteger`、UTF-8 字节编码。

**Spec:** `docs/superpowers/specs/2026-06-11-record-schema-type-format-design.md`（§4）

**现状说明:** 当前源码已存在 `src/main/java/cn/zhangyis/db/storage/record/type` 与对应 type 测试。本计划用于补齐原先过粗的开发文档，使后续复盘、重放或修正实现时有明确步骤。

**TDD 说明:** 下文“运行确认失败”的 Expected 是从空实现或删除对应实现后的重放语义；在当前分支直接运行时，相关测试可能已经通过。

---

## 文件结构

**Create / Verify:**
- `src/main/java/cn/zhangyis/db/storage/record/type/ColumnValue.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/TemporalKind.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/FieldSlice.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/FieldWriter.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/CollationStrategy.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/BinaryCollation.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/TypeCodec.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/IntegerCodec.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/FloatingCodec.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/DecimalCodec.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/FixedBytesCodec.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/VarBytesCodec.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/TemporalCodec.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/TypeCodecRegistry.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/InvalidColumnValueException.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/ColumnValueOutOfRangeException.java`
- `src/main/java/cn/zhangyis/db/storage/record/type/UnsupportedColumnTypeException.java`

**Tests:**
- `src/test/java/cn/zhangyis/db/storage/record/type/FieldSliceTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/type/IntegerCodecTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/type/FloatingCodecTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/type/DecimalCodecTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/type/BytesCodecTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/type/TemporalCodecTest.java`
- `src/test/java/cn/zhangyis/db/storage/record/type/TypeCodecRegistryTest.java`

---

## Task 1: 基础值对象、字段视图、排序规则和异常

**Files:**
- Create / Verify: `ColumnValue.java`, `TemporalKind.java`, `FieldSlice.java`, `FieldWriter.java`, `CollationStrategy.java`, `BinaryCollation.java`, `TypeCodec.java`
- Create / Verify: `InvalidColumnValueException.java`, `ColumnValueOutOfRangeException.java`, `UnsupportedColumnTypeException.java`
- Test: `FieldSliceTest.java`

- [ ] **Step 1: 写失败测试**

覆盖 `FieldSlice.compareUnsigned` 的高位字节比较、公共前缀长度比较和 `byteAt` 越界。

```java
@Test
void compareUnsignedTreatsHighBitAsLarge() {
    FieldSlice low = new FieldSlice(new byte[] {0x7F}, 0, 1);
    FieldSlice high = new FieldSlice(new byte[] {(byte) 0x80}, 0, 1);
    assertTrue(FieldSlice.compareUnsigned(low, high) < 0);
    assertTrue(FieldSlice.compareUnsigned(high, low) > 0);
    assertEquals(0, FieldSlice.compareUnsigned(low, low));
}
```

- [ ] **Step 2: 运行确认失败**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.type.FieldSliceTest"
```

Expected: 编译失败，缺少 `record.type` 基础类。

- [ ] **Step 3: 实现基础类型**

实现约束：
- `ColumnValue` 使用 sealed interface，允许 `NullValue`、`IntValue`、`DoubleValue`、`DecimalValue`、`StringValue`、`BinaryValue`、`TemporalValue`。
- `BinaryValue` 构造和访问都做防御性拷贝，避免调用方修改内部 byte array。
- `FieldSlice` 不复制 backing，只做只读切片和无符号比较。
- `FieldWriter` 从指定 offset 顺序写字节，并暴露 `written()`。
- 三个异常继承 `DatabaseRuntimeException`，保留 message 和 cause 构造。
- 参数 null 或下标非法使用 `DatabaseValidationException`。

核心接口签名：

```java
public sealed interface ColumnValue permits ... { }
public record FieldSlice(byte[] backing, int offset, int length) { }
public final class FieldWriter { public void putByte(int b); public void putBytes(byte[] src); }
public interface TypeCodec {
    int encodedLength(ColumnValue value, ColumnType type);
    int fixedWidth(ColumnType type);
    void encode(ColumnValue value, ColumnType type, FieldWriter writer);
    ColumnValue decode(FieldSlice slice, ColumnType type);
    int compare(FieldSlice left, FieldSlice right, ColumnType type);
    void validate(ColumnValue value, ColumnType type);
}
```

- [ ] **Step 4: 运行确认通过**

Expected: `FieldSliceTest` 通过。

---

## Task 2: IntegerCodec

**Files:**
- Create / Verify: `IntegerCodec.java`
- Test: `IntegerCodecTest.java`

- [ ] **Step 1: 写失败测试**

覆盖 signed/unsigned、1/2/4/8 字节、边界值、越界和值类型错误。

```java
@Test
void signedIntRoundTripAndOrder() {
    IntegerCodec c = new IntegerCodec(4, false);
    for (long v : new long[] {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
        assertEquals(v, decode(c, ColumnType.intType(false, false), encode(c, ColumnType.intType(false, false), v)));
    }
    assertTrue(compare(encode(c, INT_S, Integer.MIN_VALUE), encode(c, INT_S, -1)) < 0);
    assertTrue(compare(encode(c, INT_S, -1), encode(c, INT_S, 0)) < 0);
}
```

- [ ] **Step 2: 运行确认失败**

Expected: 编译失败，缺少 `IntegerCodec` 或行为不满足断言。

- [ ] **Step 3: 实现编码逻辑**

实现规则：
- width 只能是 `1,2,4,8`。
- signed 编码：`stored = value ^ signBit`，再按大端写 width 字节。
- unsigned 编码：原始值大端写入；width < 8 时拒绝负数和超过无符号上界的值。
- signed width < 8 时执行范围校验；decode 时符号扩展。
- `compare` 直接调用 `FieldSlice.compareUnsigned`。

- [ ] **Step 4: 运行确认通过**

Run type 单测；Expected: `IntegerCodecTest` 通过。

---

## Task 3: FloatingCodec

**Files:**
- Create / Verify: `FloatingCodec.java`
- Test: `FloatingCodecTest.java`

- [ ] **Step 1: 写失败测试**

覆盖 FLOAT/DOUBLE 往返、`-0.0` 与 `+0.0` 编码相等、排序 `-Inf < 负数 < 0 < 正数 < +Inf < NaN`。

- [ ] **Step 2: 运行确认失败**

Expected: 编译失败或浮点排序断言失败。

- [ ] **Step 3: 实现编码逻辑**

实现规则：
- width 只能是 `4` 或 `8`。
- 编码前 `value == 0.0` 归一为 `+0.0`。
- FLOAT 使用 `Float.floatToIntBits((float) normalized)`，DOUBLE 使用 `Double.doubleToLongBits(normalized)`。
- 保序变换：正数翻最高位，负数翻全部位，然后大端存储。
- decode 做反变换。
- NaN 采用 JDK 规范 NaN bits，排在最大。

- [ ] **Step 4: 运行确认通过**

Expected: `FloatingCodecTest` 通过。

---

## Task 4: DecimalCodec

**Files:**
- Create / Verify: `DecimalCodec.java`
- Test: `DecimalCodecTest.java`

- [ ] **Step 1: 写失败测试**

覆盖正数、负数、零、列宽、保序、scale 超限、precision 超限、错误值类型。

- [ ] **Step 2: 运行确认失败**

Expected: 编译失败或 DECIMAL 校验/排序断言失败。

- [ ] **Step 3: 实现编码逻辑**

实现规则：
- 列宽 `W = ceil((precision * log2(10) + 1) / 8)`。
- `BigDecimal.setScale(scale)` 不允许舍入；需要舍入时抛 `ColumnValueOutOfRangeException`。
- `unscaled.abs() < 10^precision`，否则抛越界。
- two's complement 大端左补到 W 字节，负数补 `0xFF`，非负补 `0x00`。
- 翻最高位符号位，使无符号字节序等价数值序。
- decode 翻回符号位后构造 `new BigDecimal(unscaled, scale)`。

- [ ] **Step 4: 运行确认通过**

Expected: `DecimalCodecTest` 通过。

---

## Task 5: FixedBytesCodec + VarBytesCodec

**Files:**
- Create / Verify: `FixedBytesCodec.java`, `VarBytesCodec.java`, `BinaryCollation.java`
- Test: `BytesCodecTest.java`

- [ ] **Step 1: 写失败测试**

覆盖 CHAR 空格补齐和去尾空格、BINARY 0 补齐并保留、VARCHAR 空串/普通值、VARBINARY 最大长度和超长拒绝、字节序比较。

- [ ] **Step 2: 运行确认失败**

Expected: 编译失败或补齐/比较断言失败。

- [ ] **Step 3: 实现编码逻辑**

实现规则：
- `FixedBytesCodec(nBytes, padByte, asString)` 用于 CHAR/BINARY。
- CHAR 使用 UTF-8 字节，补 `0x20`，decode 去尾部空格。
- BINARY 使用原始 byte，补 `0x00`，decode 保留全部 n 字节。
- `VarBytesCodec(maxBytes, asString)` 用于 VARCHAR/VARBINARY；字段内不存长度，长度由 `record.format.VarLenDirectory` 管。
- 字符和二进制比较都经 `BinaryCollation`，不使用 `String.compareTo`。

- [ ] **Step 4: 运行确认通过**

Expected: `BytesCodecTest` 通过。

---

## Task 6: TemporalCodec

**Files:**
- Create / Verify: `TemporalCodec.java`, `TemporalKind.java`
- Test: `TemporalCodecTest.java`

- [ ] **Step 1: 写失败测试**

覆盖 DATE epochDay、DATETIME epochMilli、epoch 前负值排序、DATE 超 int 范围拒绝、TemporalKind 不匹配拒绝。

- [ ] **Step 2: 运行确认失败**

Expected: 编译失败或时间排序断言失败。

- [ ] **Step 3: 实现编码逻辑**

实现规则：
- DATE 固定 4 字节，DATETIME 固定 8 字节。
- normalized long 翻最高位符号位后大端写入。
- DATE 校验 normalized 落在 `Integer.MIN_VALUE..Integer.MAX_VALUE`。
- `TemporalValue.kind` 必须与 codec kind 一致。

- [ ] **Step 4: 运行确认通过**

Expected: `TemporalCodecTest` 通过。

---

## Task 7: TypeCodecRegistry

**Files:**
- Create / Verify: `TypeCodecRegistry.java`
- Test: `TypeCodecRegistryTest.java`

- [ ] **Step 1: 写失败测试**

覆盖所有 `TypeId` 分派：
- TINYINT/SMALLINT/INT/BIGINT → `IntegerCodec` 且宽度正确。
- FLOAT/DOUBLE → `FloatingCodec` 且宽度正确。
- DECIMAL → `DecimalCodec` 且列宽按 precision/scale。
- CHAR/BINARY → `FixedBytesCodec`。
- VARCHAR/VARBINARY → `VarBytesCodec`。
- DATE/DATETIME → `TemporalCodec`。

- [ ] **Step 2: 运行确认失败**

Expected: 编译失败或 registry 分派断言失败。

- [ ] **Step 3: 实现 registry**

实现规则：
- `codecFor(ColumnType)` 根据 `type.typeId()` 创建 codec。
- `compare(left, right, type)` 委托 codec。
- `validate(value, type)` 委托 codec。
- registry 本身无状态，线程安全；当前实现可按需 new codec，后续再做 Flyweight 缓存。

- [ ] **Step 4: 运行确认通过**

Expected: `TypeCodecRegistryTest` 通过。

---

## Task 8: type 批全量验证

- [ ] **Step 1: 运行 type 包测试**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.type.*"
```

Expected: 所有 type 测试通过。

- [ ] **Step 2: 运行 schema + type 回归**

Run:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.record.*"
```

Expected: schema/type 相关测试通过；若 format 测试尚未创建，只要求已有测试通过。

---

## Self-Review

**Spec 覆盖:** 覆盖 spec §4 的 `ColumnValue`、`FieldSlice`、`FieldWriter`、`TypeCodec`、`CollationStrategy`、六类 codec、registry 与三类异常。

**关键不变量:** NULL 不由 codec 编码；所有可参与索引比较的类型都采用可直接字节比较的保序编码；字符串只做 binary collation；CHAR/VARCHAR 长度按 UTF-8 字节数计算。

**与 schema 批一致性:** 依赖 `ColumnType.typeId()`、`length()`、`scale()`、`unsigned()`、`storageKind()`，与 `2026-06-11-record-schema.md` 中定义的工厂名一致。
