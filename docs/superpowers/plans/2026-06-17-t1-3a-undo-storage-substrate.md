# T1.3a 物理 Undo 存储基座 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: 用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 按任务逐个执行。步骤用 checkbox（`- [ ]`）跟踪。

**Goal:** 建一个 redo 保护的物理 undo 日志：把一条 INSERT undo record 追加到真实 undo 页、得到非 NULL `RollPointer`、再由该指针读回；**完全不接事务/btree/rollback**。

**Architecture:** 自底向上：domain 值对象 `UndoNo` → `PageType.UNDO` → undo 枚举/记录/异常 → `UndoRecordCodec`（每列自带 `[nullFlag][len][bytes]` framing，复用 `TypeCodecRegistry`）→ `UndoPage`/`UndoPageAccess`（MTR-owned `PageGuard`，自动收 `PAGE_INIT`/`PAGE_BYTES` redo）→ `UndoLog`（append→RollPointer、readRecord）。undo 页写经现有 D3/D4 物理 redo 幂等覆盖，不新增 redo 类型。

**Tech Stack:** Java 25、JUnit Jupiter、Lombok、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**Spec:** `docs/superpowers/specs/2026-06-17-t1-3a-undo-storage-substrate-design.md`

**项目约束（每个任务都适用）：**
- TDD：先写失败测试 → 跑红 → 最小实现 → 跑绿。
- **无 `synchronized`/`wait`/`notify`**；本片无并发新增。
- 生产代码不抛裸 `IllegalArgumentException`/`RuntimeException`，用 `DatabaseValidationException`/`DatabaseRuntimeException` 层次。
- 中文 Javadoc/字段注释，解释数据库语义、undo 物理布局、redo 边界、简化点。
- **项目 no-commit**：每个任务以「跑绿相关测试」收口，不执行 `git commit`。
- 改高扇出符号 `PageType`（Task 2）前后跑全量回归确认非穷举 switch 不破。

**测试命令（PowerShell）：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "<pattern>" --console=plain
```

---

## 文件结构

**新建（生产）：**
- `src/main/java/cn/zhangyis/db/domain/UndoNo.java` — 事务内 undo 序号值对象
- `src/main/java/cn/zhangyis/db/storage/undo/UndoLogKind.java` — undo log 种类枚举
- `src/main/java/cn/zhangyis/db/storage/undo/UndoRecordType.java` — undo record 类型枚举（带稳定 code）
- `src/main/java/cn/zhangyis/db/storage/undo/UndoPageOverflowException.java`
- `src/main/java/cn/zhangyis/db/storage/undo/UndoLogFormatException.java`
- `src/main/java/cn/zhangyis/db/storage/undo/UndoRecord.java` — INSERT undo 命令对象
- `src/main/java/cn/zhangyis/db/storage/undo/UndoRecordCodec.java` — UndoRecord ↔ byte[]
- `src/main/java/cn/zhangyis/db/storage/undo/UndoPageLayout.java` — undo page header 偏移常量
- `src/main/java/cn/zhangyis/db/storage/undo/UndoPage.java` — PageGuard 上的 undo 页视图
- `src/main/java/cn/zhangyis/db/storage/undo/UndoPageAccess.java` — MTR 生产入口
- `src/main/java/cn/zhangyis/db/storage/undo/UndoLog.java` — append/readRecord 基座 Facade

**修改：**
- `src/main/java/cn/zhangyis/db/storage/page/PageType.java` — 追加 `UNDO(6)`
- `src/main/java/cn/zhangyis/db/storage/undo/package-info.java` — 更新包职责中文说明

**新建（测试）：**
- `src/test/java/cn/zhangyis/db/domain/UndoNoTest.java`
- `src/test/java/cn/zhangyis/db/storage/undo/UndoRecordCodecTest.java`
- `src/test/java/cn/zhangyis/db/storage/undo/UndoPageTest.java`
- `src/test/java/cn/zhangyis/db/storage/undo/UndoLogStoreTest.java`

**修改（测试）：**
- `src/test/java/cn/zhangyis/db/storage/page/PageTypeTest.java` — 加 `UNDO(6)` 断言

---

## Task 1: UndoNo 值对象

**Files:**
- Create: `src/main/java/cn/zhangyis/db/domain/UndoNo.java`
- Test: `src/test/java/cn/zhangyis/db/domain/UndoNoTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UndoNoTest {
    @Test void noneIsZeroAndDetected() {
        assertEquals(0L, UndoNo.NONE.value());
        assertTrue(UndoNo.NONE.isNone());
        assertFalse(UndoNo.of(1).isNone());
    }
    @Test void rejectsNegative() {
        assertThrows(DatabaseValidationException.class, () -> UndoNo.of(-1));
    }
    @Test void valueRoundTrips() {
        assertEquals(42L, UndoNo.of(42).value());
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.domain.UndoNoTest" --console=plain`
Expected: 编译失败（`UndoNo` 不存在）。

- [ ] **Step 3: 实现 UndoNo**

```java
package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务内 undo record 序号（对齐 InnoDB undo no）。单调非负，用于后续 savepoint 边界（T1.3c+）与 undo 页
 * header 的 lastUndoNo 落盘值。0 为 {@code NONE} 哨兵，表示「无 undo」；真实 undo record 的 undoNo 必须 &gt; 0
 * （由 UndoRecord 构造器与 UndoPage.appendRecord 前置校验），从而 page header lastUndoNo=0 可无歧义表「空页」。
 *
 * @param value 非负序号；0 表示无 undo。
 */
public record UndoNo(long value) {
    /** 无 undo 哨兵。 */
    public static final UndoNo NONE = new UndoNo(0);

    public UndoNo {
        if (value < 0) {
            throw new DatabaseValidationException("undo no must be non-negative: " + value);
        }
    }
    public static UndoNo of(long value) { return new UndoNo(value); }
    /** 是否为「无 undo」哨兵。 */
    public boolean isNone() { return value == 0; }
}
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.domain.UndoNoTest" --console=plain`
Expected: PASS。项目 no-commit，跑绿即完成。

---

## Task 2: PageType.UNDO(6)

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/page/PageType.java`
- Test: `src/test/java/cn/zhangyis/db/storage/page/PageTypeTest.java`（修改）

- [ ] **Step 1: 改测试加 UNDO 断言（先红）**

在 `PageTypeTest.codesAreStable()` 末尾追加一行：

```java
        assertEquals(6, PageType.UNDO.code());
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.page.PageTypeTest" --console=plain`
Expected: 编译失败（`PageType.UNDO` 不存在）。

- [ ] **Step 3: 追加 UNDO(6) 常量**

在 `PageType` 枚举 `INDEX(5)` 后追加（注意 `INDEX(5);` 末尾分号要移到 `UNDO(6);`）：

```java
    /** B+Tree 索引页。 */
    INDEX(5),
    /** Undo 日志页：承载 undo page header + undo record（T1.3a 起）。 */
    UNDO(6);
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.page.PageTypeTest" --console=plain`
Expected: PASS（`codesAreStable` 含 UNDO=6、`fromCodeRoundTrips` 覆盖 UNDO）。

- [ ] **Step 5: 排查 PageType 非穷举 switch（高扇出守护）**

用 Grep 搜索对 `PageType` 的 switch 表达式（switch 表达式要求穷举，新增常量可能编译失败）：
Grep pattern: `switch.*[Pp]ageType` 与 `case INDEX` （glob `*.java`）。
若发现 `switch` 表达式（`= switch`）覆盖 PageType 且无 `default`，需补 `case UNDO` 分支或确认其为语句型 switch（语句型无 default 不要求穷举，可不改）。

- [ ] **Step 6: 全量编译回归**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain`
Expected: BUILD SUCCESSFUL，无因新增枚举常量导致的编译错误；测试数不倒退。

---

## Task 3: UndoLogKind + UndoRecordType 枚举

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoLogKind.java`, `UndoRecordType.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoRecordCodecTest.java`（本任务先只放枚举 pin 断言，Task 5 补 codec 往返）

- [ ] **Step 1: 写失败测试（枚举落盘值钉死）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UndoRecordCodecTest {
    @Test void undoLogKindOrdinalsStable() {
        assertEquals(0, UndoLogKind.INSERT.ordinal());
        assertEquals(1, UndoLogKind.UPDATE.ordinal());
        assertEquals(2, UndoLogKind.TEMPORARY.ordinal());
    }
    @Test void undoRecordTypeCodesStable() {
        assertEquals(1, UndoRecordType.INSERT_ROW.code());
        assertEquals(2, UndoRecordType.UPDATE_ROW.code());
        assertEquals(3, UndoRecordType.DELETE_MARK.code());
        assertEquals(UndoRecordType.INSERT_ROW, UndoRecordType.fromCode(1));
        assertThrows(DatabaseValidationException.class, () -> UndoRecordType.fromCode(99));
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoRecordCodecTest" --console=plain`
Expected: 编译失败（枚举不存在）。

- [ ] **Step 3: 实现 UndoLogKind**

```java
package cn.zhangyis.db.storage.undo;

/**
 * undo log 种类（对齐 InnoDB insert/update/temporary undo）。ordinal 落盘到 undo page header UNDO_KIND 字段，
 * 顺序不可改（{@code UndoRecordCodecTest} 钉死）。本片 header 恒写 {@code INSERT}；UPDATE/TEMPORARY 留 T1.3b/d。
 */
public enum UndoLogKind {
    /** insert undo：只服务事务回滚，提交后即可释放，不进 history list。 */
    INSERT,
    /** update undo：服务 rollback 与 MVCC 旧版本，需 purge 边界推进后释放（T1.3d）。 */
    UPDATE,
    /** 临时对象 undo（临时表模块接入后启用）。 */
    TEMPORARY
}
```

- [ ] **Step 4: 实现 UndoRecordType**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * undo record 类型。{@code code} 作为 undo record 首字节落盘（稳定，{@code UndoRecordCodecTest} 钉死）；
 * code 从 1 起（0 留作「非法/零页」可检测）。本片 codec 仅实现 {@code INSERT_ROW}，其余类型 payload 留 T1.3d。
 */
public enum UndoRecordType {
    /** 插入未提交行的撤销：rollback 时按 cluster key 物理删除该插入。 */
    INSERT_ROW(1),
    /** 更新前镜像（T1.3d）。 */
    UPDATE_ROW(2),
    /** delete-mark 前镜像（T1.3d）。 */
    DELETE_MARK(3);

    private final int code;

    UndoRecordType(int code) {
        this.code = code;
    }

    /** 落盘 code（undo record 首字节）。 */
    public int code() {
        return code;
    }

    /** 由落盘 code 还原；未知 code 视为 undo record 类型损坏。 */
    public static UndoRecordType fromCode(int code) {
        for (UndoRecordType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new DatabaseValidationException("unknown undo record type code: " + code);
    }
}
```

- [ ] **Step 5: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoRecordCodecTest" --console=plain`
Expected: PASS。

---

## Task 4: 异常 + UndoRecord

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoPageOverflowException.java`, `UndoLogFormatException.java`, `UndoRecord.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoRecordCodecTest.java`（追加 UndoRecord 校验）

- [ ] **Step 1: 在 UndoRecordCodecTest 追加失败测试**

在 `UndoRecordCodecTest` 类体内追加（import 见下）：

```java
    // 追加 import：
    // import cn.zhangyis.db.domain.RollPointer;
    // import cn.zhangyis.db.domain.TransactionId;
    // import cn.zhangyis.db.domain.UndoNo;
    // import cn.zhangyis.db.storage.record.type.ColumnValue;
    // import java.util.List;

    @Test void undoRecordRejectsNoneUndoNo() {
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.INSERT_ROW, UndoNo.NONE, TransactionId.of(7), 1L, 9L,
                List.of(new ColumnValue.IntValue(1)), RollPointer.NULL));
    }
    @Test void undoRecordRejectsNonInsertType() {
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.UPDATE_ROW, UndoNo.of(1), TransactionId.of(7), 1L, 9L,
                List.of(new ColumnValue.IntValue(1)), RollPointer.NULL));
    }
    @Test void undoRecordRejectsEmptyKey() {
        assertThrows(DatabaseValidationException.class, () -> new UndoRecord(
                UndoRecordType.INSERT_ROW, UndoNo.of(1), TransactionId.of(7), 1L, 9L,
                List.of(), RollPointer.NULL));
    }
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoRecordCodecTest" --console=plain`
Expected: 编译失败（`UndoRecord` 不存在）。

- [ ] **Step 3: 实现两个异常**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * undo record 放不下当前 undo 页（写页前判定，不留半改页）。调用方可分配新 undo 页重试（多页链 T1.3b 接入）。
 */
public class UndoPageOverflowException extends DatabaseRuntimeException {
    public UndoPageOverflowException(String message) { super(message); }
    public UndoPageOverflowException(String message, Throwable cause) { super(message, cause); }
}
```

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * undo 日志物理格式损坏：undo record 解码越界/字段不符、recordAt offset 出 record area、RollPointer 与页不符、
 * 打开的页信封类型非 UNDO。属高风险数据一致性问题，不能静默跳过（设计 §10）。
 */
public class UndoLogFormatException extends DatabaseRuntimeException {
    public UndoLogFormatException(String message) { super(message); }
    public UndoLogFormatException(String message, Throwable cause) { super(message, cause); }
}
```

- [ ] **Step 4: 实现 UndoRecord**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * INSERT undo record（命令对象，设计 §5.5/§6.5）。保存按 cluster key 物理删除未提交插入所需信息；
 * T1.3c rollback 用 {@code indexId}+{@code clusterKey} 反查聚簇记录，并以 {@code transactionId} 校验
 * 「找到的是本事务的未提交插入」（insert undo 无旧版本，故不存 inserted hidden columns，见 spec §9）。
 *
 * <p>本片不存 UPDATE/DELETE 的 old image；{@code type} 仅允许 {@code INSERT_ROW}。{@code undoNo} 必须 &gt; 0
 * （非 {@code NONE}），与 undo 页 header lastUndoNo=0 表「空页」语义不冲突。
 *
 * @param type            undo 类型（本片仅 INSERT_ROW）。
 * @param undoNo          事务内序号（&gt; 0）。
 * @param transactionId   写入该 undo 的事务 id。
 * @param tableId         表 id（rollback 定位用）。
 * @param indexId         聚簇索引 id（rollback 定位用）。
 * @param clusterKey      主键列值，顺序对应 IndexKeyDef.parts()；可含 {@link ColumnValue.NullValue}。
 * @param prevRollPointer 事务反向 undo 链前驱（本片仅字段往返）。
 */
public record UndoRecord(UndoRecordType type, UndoNo undoNo, TransactionId transactionId,
                         long tableId, long indexId, List<ColumnValue> clusterKey,
                         RollPointer prevRollPointer) {

    public UndoRecord {
        if (type == null || undoNo == null || transactionId == null
                || clusterKey == null || prevRollPointer == null) {
            throw new DatabaseValidationException("undo record fields must not be null");
        }
        if (type != UndoRecordType.INSERT_ROW) {
            throw new DatabaseValidationException("T1.3a only supports INSERT_ROW undo record: " + type);
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo record undoNo must be > 0 (not NONE)");
        }
        if (clusterKey.isEmpty()) {
            throw new DatabaseValidationException("undo record clusterKey must not be empty");
        }
        clusterKey = List.copyOf(clusterKey);
    }
}
```

- [ ] **Step 5: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoRecordCodecTest" --console=plain`
Expected: PASS（枚举 pin + UndoRecord 三个校验）。

---

## Task 5: UndoRecordCodec

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoRecordCodec.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoRecordCodecTest.java`（追加往返）

- [ ] **Step 1: 在 UndoRecordCodecTest 追加往返/拒绝失败测试**

追加 import、helper 字段/方法与测试：

```java
    // 追加 import：
    // import cn.zhangyis.db.domain.PageNo;
    // import cn.zhangyis.db.storage.record.schema.ColumnDef;
    // import cn.zhangyis.db.storage.record.schema.ColumnId;
    // import cn.zhangyis.db.storage.record.schema.ColumnType;
    // import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
    // import cn.zhangyis.db.storage.record.schema.KeyOrder;
    // import cn.zhangyis.db.storage.record.schema.KeyPartDef;
    // import cn.zhangyis.db.storage.record.schema.TableSchema;
    // import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
    // import java.util.Arrays;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema twoColSchema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1)), true);
    }
    // 复合 key：(id, name)，覆盖 int + varchar。
    private static IndexKeyDef twoColKey() {
        return new IndexKeyDef(9L, List.of(
                new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0),
                new KeyPartDef(new ColumnId(1), KeyOrder.ASC, 0)));
    }
    private UndoRecord rec(List<ColumnValue> key, RollPointer prev) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, UndoNo.of(5), TransactionId.of(0x1122334455L),
                7L, 9L, key, prev);
    }

    @Test void roundTripsTwoColKeyNullRollPtr() {
        UndoRecord r = rec(List.of(new ColumnValue.IntValue(42),
                new ColumnValue.StringValue("alice")), RollPointer.NULL);
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(r, twoColKey(), twoColSchema());
        UndoRecord back = codec.decode(buf, 0, twoColKey(), twoColSchema());
        assertEquals(r, back);
    }
    @Test void roundTripsNonNullPrevAndNullKeyColumn() {
        RollPointer prev = new RollPointer(true, PageNo.of(0x01020304L), 0xABCD);
        UndoRecord r = rec(List.of(new ColumnValue.IntValue(1),
                ColumnValue.NullValue.INSTANCE), prev);
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        UndoRecord back = codec.decode(codec.encode(r, twoColKey(), twoColSchema()), 0,
                twoColKey(), twoColSchema());
        assertEquals(r, back);
        assertTrue(back.prevRollPointer().insert());
        assertEquals(ColumnValue.NullValue.INSTANCE, back.clusterKey().get(1));
    }
    @Test void decodeRejectsTruncated() {
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(rec(List.of(new ColumnValue.IntValue(1),
                new ColumnValue.StringValue("x")), RollPointer.NULL), twoColKey(), twoColSchema());
        byte[] cut = Arrays.copyOf(buf, buf.length - 1);
        assertThrows(UndoLogFormatException.class, () -> codec.decode(cut, 0, twoColKey(), twoColSchema()));
    }
    @Test void decodeRejectsKeyColCountMismatch() {
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(rec(List.of(new ColumnValue.IntValue(1),
                new ColumnValue.StringValue("x")), RollPointer.NULL), twoColKey(), twoColSchema());
        buf[40] = 1; // keyColCount 字节：type(1)+undoNo(8)+trx(8)+table(8)+index(8)+rollPtr(7)
        assertThrows(UndoLogFormatException.class, () -> codec.decode(buf, 0, twoColKey(), twoColSchema()));
    }
    @Test void decodeRejectsNonInsertTypeOnDisk() {
        UndoRecordCodec codec = new UndoRecordCodec(registry);
        byte[] buf = codec.encode(rec(List.of(new ColumnValue.IntValue(1),
                new ColumnValue.StringValue("x")), RollPointer.NULL), twoColKey(), twoColSchema());
        buf[0] = (byte) UndoRecordType.UPDATE_ROW.code();
        assertThrows(UndoLogFormatException.class, () -> codec.decode(buf, 0, twoColKey(), twoColSchema()));
    }
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoRecordCodecTest" --console=plain`
Expected: 编译失败（`UndoRecordCodec` 不存在）。

- [ ] **Step 3: 实现 UndoRecordCodec**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * INSERT undo record 编解码（设计 §5.5/§6.5）。布局（big-endian）：
 * <pre>[type u8][undoNo u64][transactionId u64][tableId u64][indexId u64][prevRollPointer 7B][keyColCount u8]
 * 后跟每个 key 列：[nullFlag u8]（非 null 再 [len u16][bytes]）</pre>
 *
 * <p><b>为什么自带 framing</b>：undo record 无 record 页的 NullBitmap/变长目录，而 {@link TypeCodec} 约定 NULL 不由
 * codec 处理、变长 codec 不自带长度。故本 codec 为每列写 nullFlag + 显式长度。<b>为什么需要 TableSchema</b>：
 * {@link IndexKeyDef} 只含 ColumnId，不含 ColumnType；类型由 {@code schema.column(columnId).type()} 解析
 * （columnId==ordinal==列序，由 TableSchema 不变量保证）。
 */
public final class UndoRecordCodec {

    private final TypeCodecRegistry registry;

    public UndoRecordCodec(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("undo record codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 编码一条 INSERT undo record。数据流：写定长前缀（type/undoNo/事务/表/索引/prev 指针）→ 写 keyColCount →
     * 逐列按 schema 类型自带 framing 编码。非 INSERT_ROW 或 key 列数与 keyDef 不符抛 {@link DatabaseValidationException}。
     */
    public byte[] encode(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (rec == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo encode args must not be null");
        }
        List<KeyPartDef> parts = keyDef.parts();
        if (rec.clusterKey().size() != parts.size()) {
            throw new DatabaseValidationException("clusterKey size " + rec.clusterKey().size()
                    + " != key parts " + parts.size());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(rec.type().code());
        writeU64(out, rec.undoNo().value());
        writeU64(out, rec.transactionId().value());
        writeU64(out, rec.tableId());
        writeU64(out, rec.indexId());
        out.writeBytes(rec.prevRollPointer().encode());
        out.write(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            ColumnValue v = rec.clusterKey().get(i);
            if (v == ColumnValue.NullValue.INSTANCE) {
                out.write(1);
                continue;
            }
            out.write(0);
            ColumnType ct = schema.column(parts.get(i).columnId().value()).type();
            TypeCodec codec = registry.codecFor(ct);
            int len = codec.encodedLength(v, ct);
            byte[] colBuf = new byte[len];
            codec.encode(v, ct, new FieldWriter(colBuf, 0));
            writeU16(out, len);
            out.writeBytes(colBuf);
        }
        return out.toByteArray();
    }

    /**
     * 解码一条 INSERT undo record（从 {@code off} 起）。任何越界/字段不符/落盘类型非 INSERT_ROW 抛
     * {@link UndoLogFormatException}（物理损坏，不静默跳过）。
     */
    public UndoRecord decode(byte[] buf, int off, IndexKeyDef keyDef, TableSchema schema) {
        if (buf == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo decode args must not be null");
        }
        int[] c = {off};
        int typeCode = readU8(buf, c);
        if (typeCode != UndoRecordType.INSERT_ROW.code()) {
            throw new UndoLogFormatException("undo record type not INSERT_ROW on disk: " + typeCode);
        }
        long undoNo = readU64(buf, c);
        long txn = readU64(buf, c);
        long tableId = readU64(buf, c);
        long indexId = readU64(buf, c);
        if (c[0] + RollPointer.BYTES > buf.length) {
            throw new UndoLogFormatException("undo record truncated (roll pointer)");
        }
        RollPointer prev = RollPointer.decode(buf, c[0]);
        c[0] += RollPointer.BYTES;
        int keyColCount = readU8(buf, c);
        List<KeyPartDef> parts = keyDef.parts();
        if (keyColCount != parts.size()) {
            throw new UndoLogFormatException("keyColCount " + keyColCount + " != key parts " + parts.size());
        }
        List<ColumnValue> key = new ArrayList<>(keyColCount);
        for (int i = 0; i < keyColCount; i++) {
            int nullFlag = readU8(buf, c);
            if (nullFlag == 1) {
                key.add(ColumnValue.NullValue.INSTANCE);
                continue;
            }
            int len = readU16(buf, c);
            if (c[0] + len > buf.length) {
                throw new UndoLogFormatException("undo record truncated (key col " + i + ")");
            }
            ColumnType ct = schema.column(parts.get(i).columnId().value()).type();
            key.add(registry.codecFor(ct).decode(new FieldSlice(buf, c[0], len), ct));
            c[0] += len;
        }
        return new UndoRecord(UndoRecordType.INSERT_ROW, UndoNo.of(undoNo), TransactionId.of(txn),
                tableId, indexId, key, prev);
    }

    private static void writeU16(ByteArrayOutputStream out, int v) {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void writeU64(ByteArrayOutputStream out, long v) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) ((v >>> shift) & 0xFF));
        }
    }

    private static int readU8(byte[] b, int[] c) {
        if (c[0] + 1 > b.length) {
            throw new UndoLogFormatException("undo record truncated (u8)");
        }
        return b[c[0]++] & 0xFF;
    }

    private static int readU16(byte[] b, int[] c) {
        if (c[0] + 2 > b.length) {
            throw new UndoLogFormatException("undo record truncated (u16)");
        }
        int v = ((b[c[0]] & 0xFF) << 8) | (b[c[0] + 1] & 0xFF);
        c[0] += 2;
        return v;
    }

    private static long readU64(byte[] b, int[] c) {
        if (c[0] + 8 > b.length) {
            throw new UndoLogFormatException("undo record truncated (u64)");
        }
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[c[0] + i] & 0xFFL);
        }
        c[0] += 8;
        return v;
    }
}
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoRecordCodecTest" --console=plain`
Expected: PASS（往返含 int+varchar、null key 列、非 NULL prev、截断拒绝、keyColCount 不符拒绝、落盘非 INSERT 类型拒绝、枚举 pin、UndoRecord 校验）。

---

## Task 6: UndoPageLayout + UndoPage + UndoPageAccess

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoPageLayout.java`, `UndoPage.java`, `UndoPageAccess.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoPageTest.java`

- [ ] **Step 1: 写失败测试（onPool harness：format/append/recordAt/溢出/越界/类型守门）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.IndexPageAccess;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** T1.3a UndoPage：format 初值、append 推进 header、recordAt 取回、溢出/越界、openUndoPage 页类型守门。 */
class UndoPageTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir Path dir;

    @Test void formatThenAppendAdvancesHeaderAndReadsBack() {
        onUndoPage((page) -> {
            assertEquals(60, page.freeOffset());
            assertEquals(0, page.recordCount());
            assertEquals(0L, page.lastUndoNo().value());
            assertEquals(UndoLogKind.INSERT, page.undoKind());

            byte[] a = {1, 2, 3};
            int offA = page.appendRecord(a, UndoNo.of(1));
            byte[] b = {9, 9};
            int offB = page.appendRecord(b, UndoNo.of(2));

            assertEquals(60, offA);
            assertEquals(60 + 2 + 3, offB);
            assertEquals(2, page.recordCount());
            assertEquals(2L, page.lastUndoNo().value());
            assertArrayEquals(a, page.recordAt(offA));
            assertArrayEquals(b, page.recordAt(offB));
        });
    }

    @Test void appendRejectsNoneUndoNo() {
        onUndoPage((page) ->
                assertThrows(DatabaseValidationException.class,
                        () -> page.appendRecord(new byte[]{1}, UndoNo.NONE)));
    }

    @Test void appendOverflowThrows() {
        onUndoPage((page) ->
                assertThrows(UndoPageOverflowException.class,
                        () -> page.appendRecord(new byte[PS.bytes()], UndoNo.of(1))));
    }

    @Test void recordAtRejectsOutOfArea() {
        onUndoPage((page) -> {
            page.appendRecord(new byte[]{1, 2}, UndoNo.of(1));
            assertThrows(UndoLogFormatException.class, () -> page.recordAt(10_000));
        });
    }

    @Test void openUndoPageRejectsAllocatedType() {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(m, seg);
            mgr.commit(m);

            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(r, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(r);
        });
    }

    @Test void openUndoPageRejectsIndexType() {
        onPool((mgr, disk, undoAccess, pool) -> {
            IndexPageAccess idx = new IndexPageAccess(pool, PS);
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.INDEX_LEAF);
            PageId pid = disk.allocatePage(m, seg);
            idx.createIndexPage(m, pid, 1L, 0); // 建成 INDEX 页
            mgr.commit(m);

            MiniTransaction r = mgr.begin();
            assertThrows(UndoLogFormatException.class,
                    () -> undoAccess.openUndoPage(r, pid, PageLatchMode.SHARED));
            mgr.rollbackUncommitted(r);
        });
    }

    // ---- harness ----

    private interface PageBody { void run(UndoPage page); }
    private interface PoolBody { void run(MiniTransactionManager mgr, DiskSpaceManager disk,
                                          UndoPageAccess undoAccess, BufferPool pool); }

    private void onUndoPage(PageBody body) {
        onPool((mgr, disk, undoAccess, pool) -> {
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
            PageId pid = disk.allocatePage(m, seg);
            UndoPage page = undoAccess.createUndoPage(m, pid, UndoLogKind.INSERT, TransactionId.of(7));
            body.run(page);
            mgr.commit(m);
        });
    }

    private void onPool(PoolBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoPageAccess undoAccess = new UndoPageAccess(pool, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, disk, undoAccess, pool);
        }
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoPageTest" --console=plain`
Expected: 编译失败（`UndoPage`/`UndoPageAccess`/`UndoPageLayout` 不存在）。

- [ ] **Step 3: 实现 UndoPageLayout**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * undo page header 字段偏移（紧接 FIL 信封 body 起始 = {@link PageEnvelopeLayout#FIL_PAGE_HEADER_BYTES}=38）。
 * 本片合并 undo page header 与 undo log header（单页单 log 假设）；T1.3b 引入 segment 页链时再拆分。
 * record area 起 {@link #RECORD_AREA_START}=60；undo record 槽 = [len u16][payload]。
 */
final class UndoPageLayout {

    private UndoPageLayout() {
    }

    /** undo log 种类 ordinal（u8）。 */
    static final int UNDO_KIND = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES; // 38
    /** undo log 状态占位（u8，本片恒 ACTIVE=0）。 */
    static final int STATE = UNDO_KIND + 1;            // 39
    /** 所属事务 id（u64）。 */
    static final int TRANSACTION_ID = STATE + 1;       // 40
    /** 下一条 record 追加位置（u16）。 */
    static final int FREE_OFFSET = TRANSACTION_ID + 8; // 48
    /** 已追加 record 数（u16）。 */
    static final int RECORD_COUNT = FREE_OFFSET + 2;   // 50
    /** 最近 record 的 undoNo（u64，0=空页）。 */
    static final int LAST_UNDO_NO = RECORD_COUNT + 2;  // 52
    /** record area 起点（= 60）。 */
    static final int RECORD_AREA_START = LAST_UNDO_NO + 8; // 60

    /** state 占位常量：ACTIVE。 */
    static final int STATE_ACTIVE = 0;
}
```

- [ ] **Step 4: 实现 UndoPage**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * undo 页视图（PageGuard 之上，仿 RecordPage）。所有状态在页字节里；写经 MTR-owned X latch 的 guard，
 * 自动收 PAGE_BYTES redo、commit 盖 pageLSN（与 INDEX 页同机制）。u8/u16 由 readBytes/writeBytes 组合
 * （PageGuard 只提供 int/long/bytes 原语）。
 *
 * <p>简化（单页单 log）：undo page header 与 undo log header 合并；多页链的 nextPageNo 复用 FIL next 链
 * （本片不写），T1.3b 接多页时拆分。
 */
public final class UndoPage {

    private final PageGuard guard;
    private final PageSize pageSize;

    UndoPage(PageGuard guard, PageSize pageSize) {
        if (guard == null || pageSize == null) {
            throw new DatabaseValidationException("undo page guard/pageSize must not be null");
        }
        this.guard = guard;
        this.pageSize = pageSize;
    }

    /** 初始化 undo page header（要求 X）：kind/state=ACTIVE/txnId、freeOffset=60、recordCount=0、lastUndoNo=0。 */
    void format(UndoLogKind kind, TransactionId txnId) {
        if (kind == null || txnId == null) {
            throw new DatabaseValidationException("undo page format kind/txnId must not be null");
        }
        setU8(UndoPageLayout.UNDO_KIND, kind.ordinal());
        setU8(UndoPageLayout.STATE, UndoPageLayout.STATE_ACTIVE);
        guard.writeLong(UndoPageLayout.TRANSACTION_ID, txnId.value());
        setU16(UndoPageLayout.FREE_OFFSET, UndoPageLayout.RECORD_AREA_START);
        setU16(UndoPageLayout.RECORD_COUNT, 0);
        guard.writeLong(UndoPageLayout.LAST_UNDO_NO, 0L);
    }

    /**
     * 在 freeOffset 追加一条 record（要求 X）。数据流：校验 undoNo&gt;0 → 溢出判定（写页前）→ 写 [len][payload]
     * → 推进 freeOffset/recordCount/lastUndoNo → 返回槽起点 offset（供 RollPointer.offset）。
     *
     * @throws DatabaseValidationException undoNo 为 NONE。
     * @throws UndoPageOverflowException 放不下当前页。
     */
    int appendRecord(byte[] payload, UndoNo undoNo) {
        if (payload == null || undoNo == null) {
            throw new DatabaseValidationException("undo append payload/undoNo must not be null");
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo append undoNo must be > 0 (not NONE)");
        }
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        int need = 2 + payload.length;
        int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
        if (free + need > limit) {
            throw new UndoPageOverflowException("undo record (" + need + "B) does not fit at free="
                    + free + " limit=" + limit);
        }
        setU16(free, payload.length);
        guard.writeBytes(free + 2, payload);
        setU16(UndoPageLayout.FREE_OFFSET, free + need);
        setU16(UndoPageLayout.RECORD_COUNT, getU16(UndoPageLayout.RECORD_COUNT) + 1);
        guard.writeLong(UndoPageLayout.LAST_UNDO_NO, undoNo.value());
        return free;
    }

    /** 读 offset 处 record 的 payload（S/X 均可）。offset/len 出 record area 或越 freeOffset 抛 {@link UndoLogFormatException}。 */
    byte[] recordAt(int offset) {
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        if (offset < UndoPageLayout.RECORD_AREA_START || offset + 2 > free) {
            throw new UndoLogFormatException("undo record offset out of area: " + offset + " free=" + free);
        }
        int len = getU16(offset);
        if (offset + 2 + len > free) {
            throw new UndoLogFormatException("undo record length out of area: off=" + offset + " len=" + len);
        }
        return guard.readBytes(offset + 2, len);
    }

    /** 本页 id。 */
    PageId pageId() { return guard.pageId(); }

    /** undo log 种类。 */
    UndoLogKind undoKind() {
        int idx = getU8(UndoPageLayout.UNDO_KIND);
        UndoLogKind[] all = UndoLogKind.values();
        if (idx < 0 || idx >= all.length) {
            throw new UndoLogFormatException("undo kind ordinal out of range: " + idx);
        }
        return all[idx];
    }

    /** undo log 状态占位值。 */
    int state() { return getU8(UndoPageLayout.STATE); }

    /** 所属事务 id。 */
    TransactionId transactionId() { return TransactionId.of(guard.readLong(UndoPageLayout.TRANSACTION_ID)); }

    /** 下一条追加位置。 */
    int freeOffset() { return getU16(UndoPageLayout.FREE_OFFSET); }

    /** 已追加 record 数。 */
    int recordCount() { return getU16(UndoPageLayout.RECORD_COUNT); }

    /** 最近 record 的 undoNo。 */
    UndoNo lastUndoNo() { return UndoNo.of(guard.readLong(UndoPageLayout.LAST_UNDO_NO)); }

    private int getU8(int off) {
        return guard.readBytes(off, 1)[0] & 0xFF;
    }

    private void setU8(int off, int v) {
        guard.writeBytes(off, new byte[]{(byte) v});
    }

    private int getU16(int off) {
        byte[] b = guard.readBytes(off, 2);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    private void setU16(int off, int v) {
        guard.writeBytes(off, new byte[]{(byte) (v >>> 8), (byte) v});
    }
}
```

- [ ] **Step 5: 实现 UndoPageAccess**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/**
 * undo 页的 MTR 生产入口（仿 {@link cn.zhangyis.db.storage.api.IndexPageAccess}）。建/开 undo 页绑定到 MTR-owned
 * guard，使 PAGE_INIT/PAGE_BYTES 自动产 redo、commit 盖 pageLSN。返回的 UndoPage 由 mtr memo 持 guard，勿自行 close。
 */
public final class UndoPageAccess {

    private final BufferPool pool;
    private final PageSize pageSize;

    public UndoPageAccess(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("undo page access pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    /**
     * 建并格式化一个 UNDO 页（要求在 mtr 内）：newPage(X,UNDO) → 写信封(UNDO) → format(kind,txnId)。
     * 产 PAGE_INIT(UNDO) + 信封/header PAGE_BYTES；commit 盖 pageLSN。校验全部前置于写页前。
     *
     * <p><b>破坏性入口</b>（走 newPage 会清零重初始化），仅用于新分配/有意重初始化的 undo 页。测试路径
     * 先 {@code DiskSpaceManager.allocatePage}（newPage ALLOCATED）再本方法（newPage UNDO），同 MTR 内同页
     * 两条 PAGE_INIT，由 D4a 重初始化 + redo 顺序覆盖最终态为 UNDO（与 INDEX 路径同款，见 Task 7 测试）。
     */
    public UndoPage createUndoPage(MiniTransaction mtr, PageId pageId, UndoLogKind kind, TransactionId txnId) {
        if (mtr == null || pageId == null || kind == null || txnId == null) {
            throw new DatabaseValidationException("createUndoPage args must not be null");
        }
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.UNDO);
        PageEnvelope.writeHeader(g, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.UNDO));
        UndoPage page = new UndoPage(g, pageSize);
        page.format(kind, txnId);
        return page;
    }

    /**
     * 打开已存在 undo 页（X 追加 / S 读回）。读信封校验页类型为 UNDO，否则抛 {@link UndoLogFormatException}
     * （防止把 ALLOCATED/INDEX 页按 undo header 误解释）。
     */
    public UndoPage openUndoPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        if (mtr == null || pageId == null || mode == null) {
            throw new DatabaseValidationException("openUndoPage args must not be null");
        }
        PageGuard g = mtr.getPage(pool, pageId, mode);
        FilePageHeader h = PageEnvelope.readHeader(g);
        if (h.pageType() != PageType.UNDO) {
            throw new UndoLogFormatException("page " + pageId + " is not an UNDO page: " + h.pageType());
        }
        return new UndoPage(g, pageSize);
    }
}
```

- [ ] **Step 6: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoPageTest" --console=plain`
Expected: PASS（format/header、append/recordAt、undoNo=NONE、溢出、越界、ALLOCATED/INDEX 页类型守门均通过）。

---

## Task 7: UndoLog + 端到端 / 双 newPage / 持久化

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/undo/UndoLog.java`
- Test: `src/test/java/cn/zhangyis/db/storage/undo/UndoLogStoreTest.java`

- [ ] **Step 1: 写失败测试（onPool e2e）**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** T1.3a UndoLog 端到端：append→RollPointer→readRecord、prevRollPointer 链、双 newPage redo 顺序、持久化重读。 */
class UndoLogStoreTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId UNDO_SPACE = SpaceId.of(77);

    @TempDir Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();
    private final UndoLog undoLog = new UndoLog(registry);

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(64, true), 1)), true);
    }
    private static IndexKeyDef keyDef() {
        return new IndexKeyDef(9L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }
    private UndoRecord rec(long undoNo, long id, RollPointer prev) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, UndoNo.of(undoNo), TransactionId.of(7),
                1L, 9L, List.of(new ColumnValue.IntValue(id)), prev);
    }

    @Test void appendReturnsNonNullPointerAndReadsBack() {
        onPool((mgr, disk, access) -> {
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            UndoRecord r = rec(1, 100, RollPointer.NULL);
            RollPointer rp = undoLog.append(page, r, keyDef(), schema());
            assertFalse(rp.isNull());
            assertTrue(rp.insert());
            assertEquals(page.pageId().pageNo(), rp.pageNo());
            assertEquals(r, undoLog.readRecord(page, rp, keyDef(), schema()));
            mgr.commit(m);
        });
    }

    @Test void prevRollPointerChainsTwoRecords() {
        onPool((mgr, disk, access) -> {
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            RollPointer rp1 = undoLog.append(page, rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer rp2 = undoLog.append(page, rec(2, 101, rp1), keyDef(), schema());
            UndoRecord back2 = undoLog.readRecord(page, rp2, keyDef(), schema());
            assertEquals(rp1, back2.prevRollPointer());
            assertEquals(rec(1, 100, RollPointer.NULL),
                    undoLog.readRecord(page, back2.prevRollPointer(), keyDef(), schema()));
            mgr.commit(m);
        });
    }

    @Test void readRecordRejectsPointerFromOtherPage() {
        onPool((mgr, disk, access) -> {
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            RollPointer rp = undoLog.append(page, rec(1, 100, RollPointer.NULL), keyDef(), schema());
            RollPointer wrongPage = new RollPointer(true, PageNo.of(rp.pageNo().value() + 1), rp.offset());
            assertThrows(UndoLogFormatException.class,
                    () -> undoLog.readRecord(page, wrongPage, keyDef(), schema()));
            mgr.commit(m);
        });
    }

    @Test void doubleNewPageEndsAsUndoAndSurvivesReload() {
        onPool((mgr, disk, access) -> {
            // 同一 MTR：allocatePage(ALLOCATED) 后 createUndoPage(UNDO)，append 一条，commit。
            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            PageId pid = page.pageId();
            RollPointer rp = undoLog.append(page, rec(1, 100, RollPointer.NULL), keyDef(), schema());
            mgr.commit(m);

            // 新 MTR 重读：页类型仍为 UNDO（两条 PAGE_INIT 的 redo 顺序最终态），record 完好。
            MiniTransaction r = mgr.begin();
            UndoPage reopened = access.openUndoPage(r, pid, PageLatchMode.SHARED);
            assertEquals(UndoLogKind.INSERT, reopened.undoKind());
            assertEquals(1, reopened.recordCount());
            assertEquals(rec(1, 100, RollPointer.NULL),
                    undoLog.readRecord(reopened, rp, keyDef(), schema()));
            mgr.rollbackUncommitted(r);
        });
    }

    @Test void appendSurvivesStoreReopen() {
        Path path = dir.resolve("undo-reopen.ibu");
        UndoRecord expected = rec(1, 100, RollPointer.NULL);
        PageId pid;
        RollPointer rp;

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoPageAccess access = new UndoPageAccess(pool, PS);

            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, path, PageNo.of(64));
            mgr.commit(boot);

            MiniTransaction m = mgr.begin();
            UndoPage page = freshUndoPage(m, disk, access);
            pid = page.pageId();
            rp = undoLog.append(page, expected, keyDef(), schema());
            mgr.commit(m);
        }

        try (PageStore store = new FileChannelPageStore();
             BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(UNDO_SPACE, path, PS);
            MiniTransactionManager mgr = new MiniTransactionManager();
            UndoPageAccess access = new UndoPageAccess(pool, PS);

            MiniTransaction r = mgr.begin();
            UndoPage reopened = access.openUndoPage(r, pid, PageLatchMode.SHARED);
            assertEquals(UndoLogKind.INSERT, reopened.undoKind());
            assertEquals(expected, undoLog.readRecord(reopened, rp, keyDef(), schema()));
            mgr.rollbackUncommitted(r);
        }
    }

    // ---- harness ----

    private interface PoolBody { void run(MiniTransactionManager mgr, DiskSpaceManager disk, UndoPageAccess access); }

    private UndoPage freshUndoPage(MiniTransaction m, DiskSpaceManager disk, UndoPageAccess access) {
        var seg = disk.createSegment(m, UNDO_SPACE, SegmentPurpose.UNDO);
        PageId pid = disk.allocatePage(m, seg);
        return access.createUndoPage(m, pid, UndoLogKind.INSERT, TransactionId.of(7));
    }

    private void onPool(PoolBody body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            UndoPageAccess access = new UndoPageAccess(pool, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, UNDO_SPACE, dir.resolve("undo.ibu"), PageNo.of(64));
            mgr.commit(boot);
            body.run(mgr, disk, access);
        }
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogStoreTest" --console=plain`
Expected: 编译失败（`UndoLog` 不存在）。

- [ ] **Step 3: 实现 UndoLog**

```java
package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * undo 日志基座 Facade：把 {@link UndoRecordCodec} 与 {@link UndoPage} 串起，append 返回真实 {@link RollPointer}、
 * readRecord 用指针读回。**单 undo space 假设**：RollPointer 只编 pageNo+offset（space 由唯一 undo 表空间隐含）；
 * 多 rseg/多 undo 表空间编码留 T1.3d+。本片不接事务/rollback——只是物理 undo 日志的读写底座。
 */
public final class UndoLog {

    private final UndoRecordCodec codec;

    public UndoLog(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("undo log registry must not be null");
        }
        this.codec = new UndoRecordCodec(registry);
    }

    /**
     * 追加一条 undo record：codec 编码 → UndoPage.appendRecord 得槽起点 offset → 组装 insert RollPointer。
     * 返回的 RollPointer 指向该 record 槽起点（与 {@link #readRecord} 约定一致）。
     */
    public RollPointer append(UndoPage page, UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (page == null) {
            throw new DatabaseValidationException("undo append page must not be null");
        }
        byte[] payload = codec.encode(rec, keyDef, schema);
        int offset = page.appendRecord(payload, rec.undoNo());
        return new RollPointer(true, page.pageId().pageNo(), offset);
    }

    /**
     * 按 RollPointer 读回 undo record。校验指针非 NULL 且页号匹配（值对象用 {@code equals}，不可用 {@code ==}）；
     * 不符抛 {@link UndoLogFormatException}。
     */
    public UndoRecord readRecord(UndoPage page, RollPointer rp, IndexKeyDef keyDef, TableSchema schema) {
        if (page == null || rp == null) {
            throw new DatabaseValidationException("undo readRecord page/rp must not be null");
        }
        if (rp.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        if (!rp.pageNo().equals(page.pageId().pageNo())) {
            throw new UndoLogFormatException("roll pointer page " + rp.pageNo()
                    + " != undo page " + page.pageId().pageNo());
        }
        byte[] payload = page.recordAt(rp.offset());
        return codec.decode(payload, 0, keyDef, schema);
    }
}
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.undo.UndoLogStoreTest" --console=plain`
Expected: PASS（e2e append→read、prev 链、跨页指针拒绝、双 newPage redo 顺序、新 pool/store 重开后持久化读回）。

---

## Task 8: package-info + 全量回归

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/undo/package-info.java`

- [ ] **Step 1: 更新 undo 包职责说明**

```java
/**
 * Undo 日志物理存储基座（T1.3a）：undo page 格式、INSERT undo record 编解码、RollPointer 寻址。
 *
 * <p>本片提供 {@link cn.zhangyis.db.storage.undo.UndoPage}/{@link cn.zhangyis.db.storage.undo.UndoPageAccess}/
 * {@link cn.zhangyis.db.storage.undo.UndoLog}：把一条 {@link cn.zhangyis.db.storage.undo.UndoRecord} 追加到真实
 * undo 页并由 {@link cn.zhangyis.db.domain.RollPointer} 读回；undo 页写经 MTR-owned guard，复用 D3/D4 物理 redo。
 *
 * <p>非目标（后续片）：rollback、UndoContext、rollback segment slot、history list、MVCC 旧版本、purge、
 * 恢复期 rollback、undo truncation、多页链、多 rseg/多 undo 表空间。
 */
package cn.zhangyis.db.storage.undo;
```

- [ ] **Step 2: 全量回归**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain`
Expected: BUILD SUCCESSFUL，failures=0 errors=0；测试数较基线只增不减（新增 `UndoNoTest`、`UndoRecordCodecTest`、`UndoPageTest`、`UndoLogStoreTest` 与 `PageTypeTest` 增量）。**关键回归**：record R1-R5 / B1/B2/B3 / redo / recovery 全绿（非聚簇路径与 PageType 既有 code 不变，ALLOCATED/INDEX 页不能被误开成 UNDO，store 重开后 undo record 仍可由 RollPointer 读回）。

- [ ] **Step 3: 收口**

项目 no-commit：不执行 `git commit`。在 `MEMORY.md` / `storage-build-sequence.md` 追加「T1.3a undo 存储基座完成」要点（实际测试数、新增类清单：`UndoNo`、`UndoLogKind`、`UndoRecordType`、`UndoRecord`、`UndoRecordCodec`、`UndoPageLayout`、`UndoPage`、`UndoPageAccess`、`UndoLog`、`UndoPageOverflowException`、`UndoLogFormatException`、`PageType.UNDO`）。

---

## 自检（写计划后对照 spec）

1. **spec 覆盖**：`UndoNo`(Task1)、`PageType.UNDO`(Task2)、枚举(Task3)、异常+`UndoRecord`(Task4)、`UndoRecordCodec`(Task5，含 keyColCount/type 损坏拒绝)、`UndoPageLayout`/`UndoPage`/`UndoPageAccess`(Task6，含 ALLOCATED/INDEX 页类型守门)、`UndoLog`+e2e/双newPage/新 store 重开持久化(Task7)、package-info+回归(Task8)。spec §3-§9 各节均有对应任务。
2. **placeholder 扫描**：未发现待补占位词或“按实际签名调整”类执行期再决策语句；测试 helper 的 `ColumnType.intType(false,false)`、`ColumnType.varchar(64,true)` 已按现有 `ColumnType` 工厂方法确认。
3. **类型/签名一致**：`UndoRecordCodec.encode` 入参为 `UndoRecord, IndexKeyDef, TableSchema`，`UndoRecordCodec.decode` 入参为 `byte[], int, IndexKeyDef, TableSchema`；`UndoLog.append/readRecord` 均接收 `UndoPage, RollPointer/UndoRecord, IndexKeyDef, TableSchema`；`UndoPage.appendRecord(byte[],UndoNo)→int`、`recordAt(int)→byte[]`、`UndoPageAccess.createUndoPage/openUndoPage`、`UndoRecordType.code()/fromCode`、header 偏移（38/39/40/48/50/52/60）跨任务一致。
4. **风险点**：PageType 高扇出由 Task2 Step5 grep 非穷举 switch + Step6 全量编译守护；双 newPage redo 顺序由 Task7 `doubleNewPageEndsAsUndoAndSurvivesReload` 钉死；真正落盘重读由 `appendSurvivesStoreReopen` 钉死；非聚簇/既有 code 不变由 Task8 全量回归守护。
5. **codec 可落地**：每列 `[nullFlag][len][bytes]` 自带 framing（不依赖 NullBitmap/变长目录），类型经 `schema.column(part.columnId().value()).type()` 解析；与 spec §4.4 修订一致。
