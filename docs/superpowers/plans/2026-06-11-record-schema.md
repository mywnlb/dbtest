# 记录层 schema（R1+R2 第 1/3 批）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 `cn.zhangyis.db.storage.record.schema`：`TypeId`/`StorageKind`/`CharsetId`/`CollationId`/`ColumnType` + `ColumnId`/`ColumnDef`/`TableSchema` + `KeyOrder`/`KeyPartDef`/`IndexKeyDef`，全为不可变值对象。

**Architecture:** 纯值对象 + 构造校验，无 IO、无并发。type/format 后续两批依赖之。

**Tech Stack:** Java 25、JUnit Jupiter。

**Spec:** `docs/superpowers/specs/2026-06-11-record-schema-type-format-design.md`（§3）

**现状说明:** 当前源码已存在 `src/main/java/cn/zhangyis/db/storage/record/schema` 与对应 schema 测试。本计划保留为第 1/3 批的可重放实现步骤；后续第 2/3 批见 `2026-06-11-record-type.md`，第 3/3 批见 `2026-06-11-record-format.md`。

**TDD 说明:** 下文“运行确认失败”的 Expected 是从空实现或删除对应实现后的重放语义；在当前分支直接运行时，相关测试可能已经通过。

**通用约定：** 包 `cn.zhangyis.db.storage.record.schema`；中文 Javadoc；禁裸异常（非法参数用 `DatabaseValidationException`）；**不提交**；单类测试 `... --tests "<FQCN>"`，全量 `clean test`。

---

## Task 1: 枚举 + ColumnType

**Files:** Create `TypeId.java`、`StorageKind.java`、`CharsetId.java`、`CollationId.java`、`ColumnType.java`；Test `ColumnTypeTest.java`（均在 `src/main|test/java/cn/zhangyis/db/storage/record/schema/`）

- [ ] **Step 1: 写失败测试 `ColumnTypeTest.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ColumnType 工厂与校验：storageKind 一致、DECIMAL/CHAR 参数边界。 */
class ColumnTypeTest {

    @Test
    void integerFactories() {
        ColumnType t = ColumnType.intType(false, true);
        assertEquals(TypeId.INT, t.typeId());
        assertTrue(t.nullable());
        assertFalse(t.unsigned());
        assertEquals(StorageKind.FIXED, t.storageKind());
    }

    @Test
    void varcharIsVariableCharIsFixed() {
        assertEquals(StorageKind.VARIABLE, ColumnType.varchar(20, false).storageKind());
        assertEquals(StorageKind.FIXED, ColumnType.charType(10, false).storageKind());
        assertEquals(StorageKind.VARIABLE, ColumnType.varbinary(8, true).storageKind());
        assertEquals(StorageKind.FIXED, ColumnType.binary(8, true).storageKind());
    }

    @Test
    void decimalValidatesPrecisionScale() {
        ColumnType d = ColumnType.decimal(10, 2, false);
        assertEquals(10, d.length());
        assertEquals(2, d.scale());
        assertThrows(DatabaseValidationException.class, () -> ColumnType.decimal(0, 0, false));
        assertThrows(DatabaseValidationException.class, () -> ColumnType.decimal(39, 0, false));
        assertThrows(DatabaseValidationException.class, () -> ColumnType.decimal(5, 6, false));
    }

    @Test
    void charLengthMustBePositive() {
        assertThrows(DatabaseValidationException.class, () -> ColumnType.charType(0, false));
        assertThrows(DatabaseValidationException.class, () -> ColumnType.varchar(-1, false));
    }

    @Test
    void temporalAndFloating() {
        assertEquals(TypeId.DATE, ColumnType.date(true).typeId());
        assertEquals(TypeId.DATETIME, ColumnType.datetime(false).typeId());
        assertEquals(TypeId.FLOAT, ColumnType.floatType(false).typeId());
        assertEquals(TypeId.DOUBLE, ColumnType.doubleType(true).typeId());
    }
}
```

- [ ] **Step 2: 运行确认失败**（编译失败）。

- [ ] **Step 3: 写 `TypeId.java`**

```java
package cn.zhangyis.db.storage.record.schema;

/** 第一片支持的列类型（innodb-record-design §8.2 子集）。 */
public enum TypeId {
    TINYINT, SMALLINT, INT, BIGINT,
    FLOAT, DOUBLE, DECIMAL,
    CHAR, VARCHAR, BINARY, VARBINARY,
    DATE, DATETIME
}
```

- [ ] **Step 4: 写 `StorageKind.java`**

```java
package cn.zhangyis.db.storage.record.schema;

/** 列存储形态：定长或变长（变长进入记录变长目录）。 */
public enum StorageKind {
    FIXED, VARIABLE
}
```

- [ ] **Step 5: 写 `CharsetId.java` 与 `CollationId.java`**

```java
package cn.zhangyis.db.storage.record.schema;

/** 字符集标识。首版仅 UTF8，留扩展。 */
public enum CharsetId {
    UTF8
}
```

```java
package cn.zhangyis.db.storage.record.schema;

/** 排序规则标识。首版仅 BINARY（按编码字节序），留扩展给 ci/weight collation。 */
public enum CollationId {
    BINARY
}
```

- [ ] **Step 6: 写 `ColumnType.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 列类型描述符（innodb-record-design §8.1，不可变）。只描述类型属性，不读写页。
 * length 语义：CHAR/VARCHAR/BINARY/VARBINARY = 最大字节长度；DECIMAL = 精度 p；其余 = 0。
 * scale 仅 DECIMAL 用；unsigned 仅整数用；charset/collation 仅字符类型有效（其余携带默认值，被忽略）。
 *
 * @param typeId      类型。
 * @param nullable    是否允许 NULL。
 * @param length      见上。
 * @param scale       DECIMAL 小数位。
 * @param unsigned    整数是否无符号。
 * @param charset     字符集。
 * @param collation   排序规则。
 * @param storageKind 定长/变长。
 */
public record ColumnType(TypeId typeId, boolean nullable, int length, int scale, boolean unsigned,
                         CharsetId charset, CollationId collation, StorageKind storageKind) {

    /** DECIMAL 精度上限（教学简化）。 */
    public static final int MAX_DECIMAL_PRECISION = 38;

    public ColumnType {
        if (typeId == null || charset == null || collation == null || storageKind == null) {
            throw new DatabaseValidationException("column type enum fields must not be null");
        }
        switch (typeId) {
            case DECIMAL -> {
                if (length < 1 || length > MAX_DECIMAL_PRECISION) {
                    throw new DatabaseValidationException("decimal precision out of range: " + length);
                }
                if (scale < 0 || scale > length) {
                    throw new DatabaseValidationException("decimal scale out of range: " + scale);
                }
            }
            case CHAR, VARCHAR, BINARY, VARBINARY -> {
                if (length < 1) {
                    throw new DatabaseValidationException("char/binary length must be positive: " + length);
                }
                if (scale != 0) {
                    throw new DatabaseValidationException("scale only valid for DECIMAL");
                }
            }
            default -> {
                if (scale != 0) {
                    throw new DatabaseValidationException("scale only valid for DECIMAL");
                }
            }
        }
        StorageKind expected = (typeId == TypeId.VARCHAR || typeId == TypeId.VARBINARY)
                ? StorageKind.VARIABLE : StorageKind.FIXED;
        if (storageKind != expected) {
            throw new DatabaseValidationException("storageKind mismatch for " + typeId + ": " + storageKind);
        }
    }

    private static ColumnType fixed(TypeId id, int length, int scale, boolean unsigned, boolean nullable) {
        return new ColumnType(id, nullable, length, scale, unsigned, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.FIXED);
    }

    public static ColumnType tinyint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.TINYINT, 0, 0, unsigned, nullable);
    }

    public static ColumnType smallint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.SMALLINT, 0, 0, unsigned, nullable);
    }

    public static ColumnType intType(boolean unsigned, boolean nullable) {
        return fixed(TypeId.INT, 0, 0, unsigned, nullable);
    }

    public static ColumnType bigint(boolean unsigned, boolean nullable) {
        return fixed(TypeId.BIGINT, 0, 0, unsigned, nullable);
    }

    public static ColumnType floatType(boolean nullable) {
        return fixed(TypeId.FLOAT, 0, 0, false, nullable);
    }

    public static ColumnType doubleType(boolean nullable) {
        return fixed(TypeId.DOUBLE, 0, 0, false, nullable);
    }

    public static ColumnType decimal(int precision, int scale, boolean nullable) {
        return fixed(TypeId.DECIMAL, precision, scale, false, nullable);
    }

    public static ColumnType charType(int nBytes, boolean nullable) {
        return fixed(TypeId.CHAR, nBytes, 0, false, nullable);
    }

    public static ColumnType binary(int nBytes, boolean nullable) {
        return fixed(TypeId.BINARY, nBytes, 0, false, nullable);
    }

    public static ColumnType date(boolean nullable) {
        return fixed(TypeId.DATE, 0, 0, false, nullable);
    }

    public static ColumnType datetime(boolean nullable) {
        return fixed(TypeId.DATETIME, 0, 0, false, nullable);
    }

    public static ColumnType varchar(int nBytes, boolean nullable) {
        return new ColumnType(TypeId.VARCHAR, nullable, nBytes, 0, false, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.VARIABLE);
    }

    public static ColumnType varbinary(int nBytes, boolean nullable) {
        return new ColumnType(TypeId.VARBINARY, nullable, nBytes, 0, false, CharsetId.UTF8, CollationId.BINARY,
                StorageKind.VARIABLE);
    }
}
```

- [ ] **Step 7: 运行确认通过**，不提交。

---

## Task 2: ColumnId + ColumnDef + TableSchema

**Files:** Create `ColumnId.java`、`ColumnDef.java`、`TableSchema.java`；Test `TableSchemaTest.java`

- [ ] **Step 1: 写失败测试 `TableSchemaTest.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** TableSchema 有序列、ordinal 连续校验、列查找。 */
class TableSchemaTest {

    private ColumnDef col(int ord, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ord), name, type, ord);
    }

    @Test
    void buildsAndLooksUpColumns() {
        TableSchema s = new TableSchema(1L, List.of(
                col(0, "id", ColumnType.bigint(true, false)),
                col(1, "name", ColumnType.varchar(64, true))));
        assertEquals(2, s.columnCount());
        assertEquals("name", s.column(1).name());
        assertEquals(TypeId.BIGINT, s.column(0).type().typeId());
    }

    @Test
    void rejectsNonContiguousOrdinals() {
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, List.of(
                col(0, "a", ColumnType.intType(false, false)),
                col(2, "b", ColumnType.intType(false, false)))));
    }

    @Test
    void rejectsEmptyOrNull() {
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, List.of()));
        assertThrows(DatabaseValidationException.class, () -> new TableSchema(1L, null));
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 `ColumnId.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 列标识（表内序号包装）。
 *
 * @param value 列序号，非负。
 */
public record ColumnId(int value) {

    public ColumnId {
        if (value < 0) {
            throw new DatabaseValidationException("column id must be non-negative: " + value);
        }
    }
}
```

- [ ] **Step 4: 写 `ColumnDef.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 列定义。
 *
 * @param id      列标识。
 * @param name    列名（非空白）。
 * @param type    列类型。
 * @param ordinal 表内有序位置（0..n-1）。
 */
public record ColumnDef(ColumnId id, String name, ColumnType type, int ordinal) {

    public ColumnDef {
        if (id == null || type == null) {
            throw new DatabaseValidationException("column def id/type must not be null");
        }
        if (name == null || name.isBlank()) {
            throw new DatabaseValidationException("column name must not be blank");
        }
        if (ordinal < 0) {
            throw new DatabaseValidationException("column ordinal must be non-negative: " + ordinal);
        }
    }
}
```

- [ ] **Step 5: 写 `TableSchema.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 表结构（不可变）。列按 ordinal 0..n-1 连续有序；记录编解码以此为权威列序与类型来源。
 *
 * @param schemaVersion schema 版本，编解码时校验一致。
 * @param columns       有序列定义（防御性不可变副本）。
 */
public record TableSchema(long schemaVersion, List<ColumnDef> columns) {

    public TableSchema {
        if (columns == null || columns.isEmpty()) {
            throw new DatabaseValidationException("table schema must have at least one column");
        }
        columns = List.copyOf(columns);
        for (int i = 0; i < columns.size(); i++) {
            ColumnDef c = columns.get(i);
            if (c.ordinal() != i || c.id().value() != i) {
                throw new DatabaseValidationException(
                        "column ordinal/id must equal position " + i + ": " + c.name());
            }
        }
    }

    /** 列数。 */
    public int columnCount() {
        return columns.size();
    }

    /** 按 ordinal 取列；越界抛校验异常。 */
    public ColumnDef column(int ordinal) {
        if (ordinal < 0 || ordinal >= columns.size()) {
            throw new DatabaseValidationException("column ordinal out of range: " + ordinal);
        }
        return columns.get(ordinal);
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 3: KeyOrder + KeyPartDef + IndexKeyDef

**Files:** Create `KeyOrder.java`、`KeyPartDef.java`、`IndexKeyDef.java`；Test `IndexKeyDefTest.java`

- [ ] **Step 1: 写失败测试 `IndexKeyDefTest.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** IndexKeyDef 复合 key part、ASC/DESC、prefix。 */
class IndexKeyDefTest {

    @Test
    void compositeKey() {
        IndexKeyDef k = new IndexKeyDef(7L, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.DESC, 4)));
        assertEquals(2, k.parts().size());
        assertEquals(KeyOrder.DESC, k.parts().get(1).order());
        assertEquals(4, k.parts().get(1).prefixBytes());
    }

    @Test
    void rejectsEmptyParts() {
        assertThrows(DatabaseValidationException.class, () -> new IndexKeyDef(1L, List.of()));
    }

    @Test
    void rejectsNegativePrefix() {
        assertThrows(DatabaseValidationException.class,
                () -> new KeyPartDef(new ColumnId(0), KeyOrder.ASC, -1));
    }
}
```

- [ ] **Step 2: 运行确认失败。**

- [ ] **Step 3: 写 `KeyOrder.java`**

```java
package cn.zhangyis.db.storage.record.schema;

/** 索引 key part 排序方向。 */
public enum KeyOrder {
    ASC, DESC
}
```

- [ ] **Step 4: 写 `KeyPartDef.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 复合索引中的一个 key part。
 *
 * @param columnId    参与列。
 * @param order       ASC/DESC。
 * @param prefixBytes 前缀字节数；0 表示整列。
 */
public record KeyPartDef(ColumnId columnId, KeyOrder order, int prefixBytes) {

    public KeyPartDef {
        if (columnId == null || order == null) {
            throw new DatabaseValidationException("key part columnId/order must not be null");
        }
        if (prefixBytes < 0) {
            throw new DatabaseValidationException("prefix bytes must be non-negative: " + prefixBytes);
        }
    }
}
```

- [ ] **Step 5: 写 `IndexKeyDef.java`**

```java
package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 索引 key 定义：有序 key part 组合。
 *
 * @param indexId 索引标识。
 * @param parts   有序 key part（非空，防御性不可变副本）。
 */
public record IndexKeyDef(long indexId, List<KeyPartDef> parts) {

    public IndexKeyDef {
        if (parts == null || parts.isEmpty()) {
            throw new DatabaseValidationException("index key must have at least one part");
        }
        parts = List.copyOf(parts);
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 4: 全量回归

- [ ] **Step 1: `clean test`** 期望 BUILD SUCCESSFUL。（GitNexus 索引等 format 批结束后统一刷新。）

---

## Self-Review（已执行）

**1. Spec 覆盖：** §3 schema 全部类型→Task1-3。**2. Placeholder：** 无。**3. 一致性：** `ColumnType` 工厂名（intType/charType/varchar/...）、`TableSchema.{columnCount,column}`、`ColumnDef.{id,name,type,ordinal}`、`KeyPartDef`/`IndexKeyDef` 字段与测试一致；后续 type/format 批将引用这些名字（codec 按 `ColumnType.typeId/length/scale/unsigned/storageKind` 分派）。
