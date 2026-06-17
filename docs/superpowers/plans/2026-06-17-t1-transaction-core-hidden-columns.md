# T1.1+T1.2 事务核心 + 聚簇记录隐藏列 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: 用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 按任务逐个执行。步骤用 checkbox（`- [ ]`）跟踪。

**Goal:** 建立纯内存事务生命周期（id/no/状态机/活跃表），并让聚簇 conventional 记录携带 `DB_TRX_ID`/`DB_ROLL_PTR` 隐藏列、由聚簇 insert 用 `TransactionId` 盖戳。

**Architecture:** 自底向上：domain 值对象 → storage.trx 核心 → record 隐藏区编解码 → B+Tree 聚簇盖戳。承重项是 record 格式改动：非聚簇 schema 字节逐位不变（兼容副构造器 `clustered=false`），隐藏区仅贴在 clustered 记录尾部 15B。ReadView/undo 不在本片。

**Tech Stack:** Java 25、JUnit Jupiter、Lombok、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**Spec:** `docs/superpowers/specs/2026-06-17-t1-transaction-core-hidden-columns-design.md`

**项目约束（每个任务都适用）：**
- TDD：先写失败测试 → 跑红 → 最小实现 → 跑绿。
- **无 `synchronized`/`wait`/`notify`**；并发用 `ReentrantLock` + `try/finally`。
- 生产代码不抛裸 `IllegalArgumentException`/`RuntimeException`，用 `DatabaseValidationException`/`DatabaseRuntimeException` 层次。
- 中文 Javadoc/字段注释，解释数据库语义与不变量。
- **项目 no-commit**：每个任务以「跑绿相关测试」收口，不执行 `git commit`。
- 改 CRITICAL/HIGH 符号前跑 `gitnexus_impact`（本片改 `TableSchema`/`LogicalRecord`/`RecordEncoder`/`RecordFieldResolver`/`RecordCursor`/`BTreeIndex` 属高扇出，任务 5-10 前必跑）。
- 收口任务跑 `npx gitnexus analyze`。

**测试命令（PowerShell）：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "<pattern>" --console=plain
```

---

## 文件结构

**新建：**
- `src/main/java/cn/zhangyis/db/domain/TransactionId.java`
- `src/main/java/cn/zhangyis/db/domain/TransactionNo.java`
- `src/main/java/cn/zhangyis/db/domain/RollPointer.java`
- `src/main/java/cn/zhangyis/db/storage/trx/IsolationLevel.java`
- `src/main/java/cn/zhangyis/db/storage/trx/TransactionState.java`
- `src/main/java/cn/zhangyis/db/storage/trx/TransactionOptions.java`
- `src/main/java/cn/zhangyis/db/storage/trx/TransactionStateException.java`
- `src/main/java/cn/zhangyis/db/storage/trx/Transaction.java`
- `src/main/java/cn/zhangyis/db/storage/trx/ActiveTransactionTable.java`
- `src/main/java/cn/zhangyis/db/storage/trx/TransactionSystem.java`
- `src/main/java/cn/zhangyis/db/storage/trx/TransactionManager.java`
- `src/main/java/cn/zhangyis/db/storage/trx/package-info.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/HiddenColumns.java`
- `src/main/java/cn/zhangyis/db/storage/record/format/HiddenColumnLayout.java`
- 测试镜像包下的对应 `*Test.java`

**修改：**
- `src/main/java/cn/zhangyis/db/storage/record/schema/TableSchema.java`（加 `clustered` + 兼容副构造器）
- `src/main/java/cn/zhangyis/db/storage/record/format/LogicalRecord.java`（加 `hiddenColumns`）
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordEncoder.java`（clustered 写隐藏区）
- `src/main/java/cn/zhangyis/db/storage/record/format/RecordFieldResolver.java`（解析隐藏区 + tail check + materialize 带隐藏列）
- `src/main/java/cn/zhangyis/db/storage/record/page/RecordCursor.java`（`dbTrxId()`/`dbRollPtr()`）
- `src/main/java/cn/zhangyis/db/storage/btree/BTreeIndex.java`（派生 `clustered()`）
- `src/main/java/cn/zhangyis/db/storage/btree/SplitCapableBTreeIndexService.java`（聚簇 insert 入口）

---

## Task 1: domain 值对象 TransactionId / TransactionNo

**Files:**
- Create: `src/main/java/cn/zhangyis/db/domain/TransactionId.java`, `TransactionNo.java`
- Test: `src/test/java/cn/zhangyis/db/domain/TransactionIdTest.java`, `TransactionNoTest.java`

- [ ] **Step 1: 写失败测试 TransactionIdTest**
```java
package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionIdTest {
    @Test void noneIsZeroAndDetected() {
        assertEquals(0L, TransactionId.NONE.value());
        assertTrue(TransactionId.NONE.isNone());
        assertFalse(TransactionId.of(1).isNone());
    }
    @Test void rejectsNegative() {
        assertThrows(DatabaseValidationException.class, () -> TransactionId.of(-1));
    }
    @Test void valueRoundTrips() {
        assertEquals(42L, TransactionId.of(42).value());
    }
}
```

- [ ] **Step 2: 跑红**
Run: `... test --tests "cn.zhangyis.db.domain.TransactionIdTest"`
Expected: 编译失败（TransactionId 不存在）。

- [ ] **Step 3: 实现 TransactionId**
```java
package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务 id（对应聚簇记录隐藏列 DB_TRX_ID）。单调非负；0 为 NONE 哨兵，表示只读/尚未分配写者。
 *
 * @param value 非负事务序号；0 表示无写者。
 */
public record TransactionId(long value) {
    /** 无写者哨兵（只读或未分配写 id）。 */
    public static final TransactionId NONE = new TransactionId(0);

    public TransactionId {
        if (value < 0) {
            throw new DatabaseValidationException("transaction id must be non-negative: " + value);
        }
    }
    public static TransactionId of(long value) { return new TransactionId(value); }
    /** 是否为「无写者」哨兵。 */
    public boolean isNone() { return value == 0; }
}
```

- [ ] **Step 4: 写并实现 TransactionNoTest + TransactionNo（同形）**
```java
// TransactionNoTest：noneIsZero、rejectsNegative、valueRoundTrips（同 TransactionId 结构）
```
```java
package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务提交序号 DB_TRX_NO。commit 时给读写事务分配，单调；用于后续 purge 边界。0 为 NONE（未提交/只读事务）。
 *
 * @param value 非负提交序号；0 表示未分配。
 */
public record TransactionNo(long value) {
    /** 未分配提交序号哨兵。 */
    public static final TransactionNo NONE = new TransactionNo(0);
    public TransactionNo {
        if (value < 0) {
            throw new DatabaseValidationException("transaction no must be non-negative: " + value);
        }
    }
    public static TransactionNo of(long value) { return new TransactionNo(value); }
    public boolean isNone() { return value == 0; }
}
```

- [ ] **Step 5: 跑绿**
Run: `... test --tests "cn.zhangyis.db.domain.TransactionIdTest" --tests "cn.zhangyis.db.domain.TransactionNoTest"`
Expected: PASS。项目 no-commit，跑绿即完成。

---

## Task 2: RollPointer + 7 字节 codec

**Files:**
- Create: `src/main/java/cn/zhangyis/db/domain/RollPointer.java`
- Test: `src/test/java/cn/zhangyis/db/domain/RollPointerTest.java`

布局（big-endian，7B=56bit，与 InnoDB 二进制不兼容）：byte0 bit7=insert flag、bit6..0=reserved(必须 0)；byte1..4=pageNo(u32)；byte5..6=offset(u16)。space 由 undo 表空间隐含（单 undo 空间假设）。NULL=全零。

- [ ] **Step 1: 写失败测试**
```java
package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RollPointerTest {
    @Test void nullIsAllZeroAndDetected() {
        byte[] b = RollPointer.NULL.encode();
        assertArrayEquals(new byte[7], b);
        assertTrue(RollPointer.NULL.isNull());
        assertTrue(RollPointer.decode(new byte[7], 0).isNull());
    }
    @Test void roundTripsInsertPageOffset() {
        RollPointer p = new RollPointer(true, PageNo.of(0x01020304L), 0xABCD);
        RollPointer back = RollPointer.decode(p.encode(), 0);
        assertEquals(p, back);
        assertTrue(back.insert());
        assertEquals(0x01020304L, back.pageNo().value());
        assertEquals(0xABCD, back.offset());
    }
    @Test void rejectsOffsetOutOfU16() {
        assertThrows(DatabaseValidationException.class,
            () -> new RollPointer(false, PageNo.of(1), 0x10000));
    }
    @Test void rejectsPageNoOutOfU32() {
        assertThrows(DatabaseValidationException.class,
            () -> new RollPointer(false, PageNo.of(0x1_0000_0000L), 0));
    }
    @Test void decodeRejectsReservedBitsSet() {
        byte[] bad = new byte[7];
        bad[0] = 0x40; // reserved bit set
        assertThrows(DatabaseValidationException.class, () -> RollPointer.decode(bad, 0));
    }
    @Test void decodeRejectsShortBuffer() {
        assertThrows(DatabaseValidationException.class, () -> RollPointer.decode(new byte[6], 0));
    }
}
```
（注意：把方法名 `rejectsOffsetOutОfU16` 里的西里尔字母改成 ASCII `rejectsOffsetOutOfU16`。）

- [ ] **Step 2: 跑红**
Run: `... test --tests "cn.zhangyis.db.domain.RollPointerTest"`
Expected: 编译失败（RollPointer 不存在）。

- [ ] **Step 3: 实现 RollPointer**
```java
package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * DB_ROLL_PTR：指向 undo record 的位置。本片只用 NULL（无 undo）。
 *
 * <p>7 字节定长编码（big-endian，与 InnoDB 二进制不兼容）：byte0 bit7=insert flag、bit6..0 reserved(0)；
 * byte1..4=pageNo(u32)；byte5..6=offset(u16)。space id 不存，由 undo 表空间隐含（单 undo 空间假设；
 * 多 undo 表空间需改为 InnoDB 风格 rseg-id 编码，留 T1.3）。NULL=全零（undo page 0 保留作头页，无歧义）。
 *
 * @param insert 是否 insert undo（true=insert，false=update/delete）。
 * @param pageNo undo page 号，u32 范围。
 * @param offset undo record 页内偏移，u16 范围。
 */
public record RollPointer(boolean insert, PageNo pageNo, int offset) {
    private static final long U32_MAX = 0xFFFFFFFFL;
    private static final int U16_MAX = 0xFFFF;
    private static final int INSERT_BIT = 0x80;
    private static final int RESERVED_MASK = 0x7F;
    /** 编码字节宽度。 */
    public static final int BYTES = 7;
    /** 全零空指针哨兵。 */
    public static final RollPointer NULL = new RollPointer(false, PageNo.of(0), 0);

    public RollPointer {
        if (pageNo == null) {
            throw new DatabaseValidationException("roll pointer pageNo must not be null");
        }
        if (pageNo.value() < 0 || pageNo.value() > U32_MAX) {
            throw new DatabaseValidationException("roll pointer pageNo out of u32: " + pageNo.value());
        }
        if (offset < 0 || offset > U16_MAX) {
            throw new DatabaseValidationException("roll pointer offset out of u16: " + offset);
        }
    }

    /** 是否空指针（无 undo）。 */
    public boolean isNull() { return !insert && pageNo.value() == 0 && offset == 0; }

    /** 编码为 7 字节。 */
    public byte[] encode() {
        byte[] b = new byte[BYTES];
        b[0] = (byte) (insert ? INSERT_BIT : 0);
        long p = pageNo.value();
        b[1] = (byte) (p >>> 24); b[2] = (byte) (p >>> 16); b[3] = (byte) (p >>> 8); b[4] = (byte) p;
        b[5] = (byte) (offset >>> 8); b[6] = (byte) offset;
        return b;
    }

    /** 从 off 处解码 7 字节；reserved 位非 0 视为损坏。 */
    public static RollPointer decode(byte[] buf, int off) {
        if (buf == null || off < 0 || off + BYTES > buf.length) {
            throw new DatabaseValidationException("roll pointer buffer too short");
        }
        int flags = buf[off] & 0xFF;
        if ((flags & RESERVED_MASK) != 0) {
            throw new DatabaseValidationException("roll pointer reserved bits set: " + flags);
        }
        boolean insert = (flags & INSERT_BIT) != 0;
        long p = ((long) (buf[off + 1] & 0xFF) << 24) | ((buf[off + 2] & 0xFF) << 16)
               | ((buf[off + 3] & 0xFF) << 8) | (buf[off + 4] & 0xFF);
        int offset = ((buf[off + 5] & 0xFF) << 8) | (buf[off + 6] & 0xFF);
        return new RollPointer(insert, PageNo.of(p), offset);
    }
}
```

- [ ] **Step 4: 跑绿**
Run: `... test --tests "cn.zhangyis.db.domain.RollPointerTest"`
Expected: PASS。

---

## Task 3: trx 枚举 + 选项 + 异常

**Files:**
- Create: `IsolationLevel.java`, `TransactionState.java`, `TransactionOptions.java`, `TransactionStateException.java`, `package-info.java`（均在 `src/main/java/cn/zhangyis/db/storage/trx/`）
- Test: `src/test/java/cn/zhangyis/db/storage/trx/TransactionStateTest.java`, `TransactionOptionsTest.java`

- [ ] **Step 1: 写失败测试 TransactionStateTest**
```java
package cn.zhangyis.db.storage.trx;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionStateTest {
    @Test void legalCommitPath() {
        assertTrue(TransactionState.ACTIVE.canTransitionTo(TransactionState.COMMITTING));
        assertTrue(TransactionState.COMMITTING.canTransitionTo(TransactionState.COMMITTED));
    }
    @Test void legalRollbackPath() {
        assertTrue(TransactionState.ACTIVE.canTransitionTo(TransactionState.ROLLING_BACK));
        assertTrue(TransactionState.ROLLING_BACK.canTransitionTo(TransactionState.ROLLED_BACK));
    }
    @Test void illegalTransitionsRejected() {
        assertFalse(TransactionState.COMMITTED.canTransitionTo(TransactionState.ACTIVE));
        assertFalse(TransactionState.ACTIVE.canTransitionTo(TransactionState.COMMITTED));
        assertFalse(TransactionState.ROLLED_BACK.canTransitionTo(TransactionState.COMMITTING));
    }
}
```

- [ ] **Step 2: 跑红 → 实现枚举与异常**
```java
package cn.zhangyis.db.storage.trx;

/** 事务隔离级别。本片仅作记录字段，不驱动行为（无 ReadView）；语义随可见性片接入。 */
public enum IsolationLevel { READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE }
```
```java
package cn.zhangyis.db.storage.trx;

/**
 * 数据库事务状态机（本片仅 5 个流转态；PREPARED/RECOVERED_ACTIVE 待恢复片再加）。
 * 合法转换：ACTIVE→COMMITTING→COMMITTED、ACTIVE→ROLLING_BACK→ROLLED_BACK。
 */
public enum TransactionState {
    ACTIVE, COMMITTING, COMMITTED, ROLLING_BACK, ROLLED_BACK;

    /** 本状态是否允许转到 target；非法转换由 TransactionManager 据此抛 TransactionStateException。 */
    public boolean canTransitionTo(TransactionState target) {
        return switch (this) {
            case ACTIVE -> target == COMMITTING || target == ROLLING_BACK;
            case COMMITTING -> target == COMMITTED;
            case ROLLING_BACK -> target == ROLLED_BACK;
            case COMMITTED, ROLLED_BACK -> false;
        };
    }
}
```
```java
package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 事务状态/生命周期非法操作（非法状态转换、对只读事务分配写 id、对已结束事务再操作）。 */
public class TransactionStateException extends DatabaseRuntimeException {
    public TransactionStateException(String message) { super(message); }
    public TransactionStateException(String message, Throwable cause) { super(message, cause); }
}
```
```java
package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务启动选项。
 * @param isolationLevel 隔离级别（本片仅记录）。
 * @param readOnly 只读事务不分配写 id、不进活跃表、commit 不分配 transactionNo。
 * @param autoCommit 自动提交标志（本片仅记录）。
 */
public record TransactionOptions(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    public TransactionOptions {
        if (isolationLevel == null) {
            throw new DatabaseValidationException("isolation level must not be null");
        }
    }
    /** 默认：REPEATABLE_READ、读写、autocommit。 */
    public static TransactionOptions defaults() {
        return new TransactionOptions(IsolationLevel.REPEATABLE_READ, false, true);
    }
}
```
`TransactionOptionsTest`：断言 `defaults()` 三字段值、null isolation 抛 `DatabaseValidationException`。`package-info.java` 写包职责中文说明。

- [ ] **Step 3: 跑绿**
Run: `... test --tests "cn.zhangyis.db.storage.trx.TransactionStateTest" --tests "cn.zhangyis.db.storage.trx.TransactionOptionsTest"`
Expected: PASS。

---

## Task 4: Transaction + ActiveTransactionTable + TransactionSystem + TransactionManager

**Files:**
- Create: `Transaction.java`, `ActiveTransactionTable.java`, `TransactionSystem.java`, `TransactionManager.java`
- Test: `src/test/java/cn/zhangyis/db/storage/trx/TransactionManagerTest.java`, `TransactionSystemTest.java`

- [ ] **Step 1: 写失败测试 TransactionManagerTest**
```java
package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionManagerTest {
    @Test void beginCommitAssignsNoAndRemovesFromActive() {
        TransactionManager mgr = new TransactionManager(new TransactionSystem());
        Transaction t = mgr.begin(TransactionOptions.defaults());
        assertEquals(TransactionState.ACTIVE, t.state());
        assertTrue(t.transactionId().isNone(), "rw txn id lazy");
        TransactionId id = mgr.assignWriteId(t);
        assertFalse(id.isNone());
        assertEquals(id, mgr.assignWriteId(t), "assignWriteId idempotent");
        assertTrue(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
        mgr.commit(t);
        assertEquals(TransactionState.COMMITTED, t.state());
        assertFalse(t.transactionNo().isNone(), "rw commit assigns no");
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
    }
    @Test void readOnlyCommitAssignsNoTransactionNo() {
        TransactionManager mgr = new TransactionManager(new TransactionSystem());
        Transaction t = mgr.begin(new TransactionOptions(IsolationLevel.REPEATABLE_READ, true, true));
        assertThrows(TransactionStateException.class, () -> mgr.assignWriteId(t));
        mgr.commit(t);
        assertEquals(TransactionState.COMMITTED, t.state());
        assertTrue(t.transactionNo().isNone(), "read-only txn gets no commit no");
    }
    @Test void rollbackRemovesFromActive() {
        TransactionManager mgr = new TransactionManager(new TransactionSystem());
        Transaction t = mgr.begin(TransactionOptions.defaults());
        TransactionId id = mgr.assignWriteId(t);
        mgr.rollback(t);
        assertEquals(TransactionState.ROLLED_BACK, t.state());
        assertFalse(mgr.system().snapshotActiveReadWriteIds().contains(id.value()));
    }
    @Test void doubleCommitRejected() {
        TransactionManager mgr = new TransactionManager(new TransactionSystem());
        Transaction t = mgr.begin(TransactionOptions.defaults());
        mgr.commit(t);
        assertThrows(TransactionStateException.class, () -> mgr.commit(t));
    }
}
```

- [ ] **Step 2: 跑红 → 实现 Transaction**
```java
package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

/**
 * 数据库事务聚合（单 owner 线程使用）。状态/id/no 仅由 TransactionManager 经状态机修改，不暴露公共 setter。
 * 不持有 BufferFrame/PageGuard；本片不含 readView（ReadView 推迟到可见性片）。
 */
public final class Transaction {
    private final IsolationLevel isolationLevel;
    private final boolean readOnly;
    private final boolean autoCommit;
    private final long startTimeMillis;
    private TransactionId transactionId = TransactionId.NONE;
    private TransactionNo transactionNo = TransactionNo.NONE;
    private TransactionState state = TransactionState.ACTIVE;

    Transaction(TransactionOptions options, long startTimeMillis) {
        this.isolationLevel = options.isolationLevel();
        this.readOnly = options.readOnly();
        this.autoCommit = options.autoCommit();
        this.startTimeMillis = startTimeMillis;
    }

    public IsolationLevel isolationLevel() { return isolationLevel; }
    public boolean readOnly() { return readOnly; }
    public boolean autoCommit() { return autoCommit; }
    public long startTimeMillis() { return startTimeMillis; }
    public TransactionId transactionId() { return transactionId; }
    public TransactionNo transactionNo() { return transactionNo; }
    public TransactionState state() { return state; }

    // 包内可见：仅 TransactionManager 调用
    void setTransactionId(TransactionId id) { this.transactionId = id; }
    void setTransactionNo(TransactionNo no) { this.transactionNo = no; }
    /** 经状态机校验后推进状态；非法转换抛 TransactionStateException。 */
    void transitionTo(TransactionState target) {
        if (!state.canTransitionTo(target)) {
            throw new TransactionStateException("illegal transition " + state + " -> " + target);
        }
        this.state = target;
    }
}
```

- [ ] **Step 3: 实现 ActiveTransactionTable（包内，TransactionSystem 锁内独占调用）**
```java
package cn.zhangyis.db.storage.trx;

import java.util.Set;
import java.util.TreeSet;

/**
 * 活跃读写事务 id 注册表。**非线程安全**：仅由 TransactionSystem 在其 ReentrantLock 内调用，无第二 owner。
 * 有序集合便于后续 ReadView 取 min（本片无 ReadView 消费）。
 */
final class ActiveTransactionTable {
    private final TreeSet<Long> activeReadWriteIds = new TreeSet<>();
    void register(long txnId) { activeReadWriteIds.add(txnId); }
    void remove(long txnId) { activeReadWriteIds.remove(txnId); }
    /** 不可变拷贝快照。 */
    Set<Long> snapshot() { return Set.copyOf(activeReadWriteIds); }
}
```

- [ ] **Step 4: 实现 TransactionSystem**
```java
package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务全局协调器。ReentrantLock 短锁保护 nextTransactionId/nextTransactionNo 与私有 ActiveTransactionTable。
 * 持锁期间不访问 Buffer Pool、不持 page latch、不等待（trx-mvcc §17）。
 */
public final class TransactionSystem {
    private final ReentrantLock lock = new ReentrantLock();
    private long nextTransactionId = 1;
    private long nextTransactionNo = 1;
    private final ActiveTransactionTable active = new ActiveTransactionTable();

    /** 分配单调事务 id 并登记为活跃读写事务。 */
    TransactionId allocateWriteId() {
        lock.lock();
        try {
            long id = nextTransactionId++;
            active.register(id);
            return TransactionId.of(id);
        } finally { lock.unlock(); }
    }
    /** 分配单调提交序号。 */
    TransactionNo allocateTransactionNo() {
        lock.lock();
        try { return TransactionNo.of(nextTransactionNo++); }
        finally { lock.unlock(); }
    }
    /** 从活跃表移出（commit/rollback 读写事务）。 */
    void removeActive(long txnId) {
        lock.lock();
        try { active.remove(txnId); }
        finally { lock.unlock(); }
    }
    /** 活跃读写事务 id 不可变快照。 */
    public Set<Long> snapshotActiveReadWriteIds() {
        lock.lock();
        try { return active.snapshot(); }
        finally { lock.unlock(); }
    }
}
```

- [ ] **Step 5: 实现 TransactionManager**
```java
package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 事务门面。begin/commit/rollback 纯内存生命周期；读写事务惰性分配写 id、commit 分配 transactionNo。
 * 本片不提供 current()（不引入 ThreadLocal 隐藏全局态）；调用方显式持有并传递 Transaction。
 * 本片无 undo：commit/rollback 不撤销/不刷已写记录。
 */
public final class TransactionManager {
    private final TransactionSystem system;
    public TransactionManager(TransactionSystem system) {
        if (system == null) throw new DatabaseValidationException("transaction system must not be null");
        this.system = system;
    }
    public TransactionSystem system() { return system; }

    public Transaction begin(TransactionOptions options) {
        if (options == null) throw new DatabaseValidationException("transaction options must not be null");
        return new Transaction(options, System.currentTimeMillis());
    }

    /** 首次写入分配写 id（幂等）；只读事务调用直接拒绝。 */
    public TransactionId assignWriteId(Transaction txn) {
        requireActive(txn);
        if (txn.readOnly()) {
            throw new TransactionStateException("read-only transaction cannot assign write id");
        }
        if (!txn.transactionId().isNone()) return txn.transactionId();
        TransactionId id = system.allocateWriteId();
        txn.setTransactionId(id);
        return id;
    }

    public void commit(Transaction txn) {
        requireActive(txn);
        txn.transitionTo(TransactionState.COMMITTING);
        if (!txn.transactionId().isNone()) { // 读写事务
            txn.setTransactionNo(system.allocateTransactionNo());
            system.removeActive(txn.transactionId().value());
        }
        txn.transitionTo(TransactionState.COMMITTED);
    }

    public void rollback(Transaction txn) {
        requireActive(txn);
        txn.transitionTo(TransactionState.ROLLING_BACK);
        if (!txn.transactionId().isNone()) {
            system.removeActive(txn.transactionId().value());
        }
        txn.transitionTo(TransactionState.ROLLED_BACK);
    }

    private static void requireActive(Transaction txn) {
        if (txn == null) throw new DatabaseValidationException("transaction must not be null");
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("transaction not ACTIVE: " + txn.state());
        }
    }
}
```
`TransactionSystemTest`：id/no 单调递增、`snapshotActiveReadWriteIds` 拷贝隔离（拿快照后再 allocate 不影响旧快照）。

- [ ] **Step 6: 跑绿**
Run: `... test --tests "cn.zhangyis.db.storage.trx.*"`
Expected: PASS。

---

## Task 5: HiddenColumns + HiddenColumnLayout

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/record/format/HiddenColumns.java`, `HiddenColumnLayout.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/format/HiddenColumnLayoutTest.java`

- [ ] **Step 1: 写失败测试**
```java
package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HiddenColumnLayoutTest {
    @Test void encodeDecodeRoundTrip() {
        byte[] buf = new byte[HiddenColumnLayout.HIDDEN_BYTES + 5];
        TransactionId trx = TransactionId.of(0x1122334455L);
        RollPointer rp = new RollPointer(true, PageNo.of(7), 9);
        HiddenColumnLayout.encode(buf, 5, trx, rp);
        assertEquals(trx, HiddenColumnLayout.decodeTrxId(buf, 5));
        assertEquals(rp, HiddenColumnLayout.decodeRollPtr(buf, 5));
    }
    @Test void fifteenBytesWide() { assertEquals(15, HiddenColumnLayout.HIDDEN_BYTES); }
}
```

- [ ] **Step 2: 跑红 → 实现 HiddenColumns + HiddenColumnLayout**
```java
package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 聚簇 conventional 记录的隐藏列值对象（DB_TRX_ID + DB_ROLL_PTR）。非聚簇记录无此对象（null）。
 */
public record HiddenColumns(TransactionId dbTrxId, RollPointer dbRollPtr) {
    public HiddenColumns {
        if (dbTrxId == null || dbRollPtr == null) {
            throw new DatabaseValidationException("hidden columns dbTrxId/dbRollPtr must not be null");
        }
    }
}
```
```java
package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 聚簇记录尾部隐藏区编解码（15B）：DB_TRX_ID(8B 无符号 big-endian) + DB_ROLL_PTR(7B，见 RollPointer)。
 * 仅聚簇 leaf CONVENTIONAL 记录写此区；node-pointer/非聚簇记录无隐藏区。
 */
public final class HiddenColumnLayout {
    /** 隐藏区字节宽度。 */
    public static final int HIDDEN_BYTES = 15;
    private static final int DB_TRX_ID_OFFSET = 0;
    private static final int DB_ROLL_PTR_OFFSET = 8;
    private HiddenColumnLayout() {}

    /** 在 off 处写 8B trxId + 7B rollPtr。 */
    public static void encode(byte[] buf, int off, TransactionId trxId, RollPointer rollPtr) {
        if (buf == null || off < 0 || off + HIDDEN_BYTES > buf.length) {
            throw new DatabaseValidationException("hidden area buffer too short");
        }
        long v = trxId.value();
        for (int i = 0; i < 8; i++) {
            buf[off + DB_TRX_ID_OFFSET + i] = (byte) (v >>> (56 - 8 * i));
        }
        byte[] rp = rollPtr.encode();
        System.arraycopy(rp, 0, buf, off + DB_ROLL_PTR_OFFSET, RollPointer.BYTES);
    }
    public static TransactionId decodeTrxId(byte[] buf, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[off + DB_TRX_ID_OFFSET + i] & 0xFFL);
        }
        return TransactionId.of(v);
    }
    public static RollPointer decodeRollPtr(byte[] buf, int off) {
        return RollPointer.decode(buf, off + DB_ROLL_PTR_OFFSET);
    }
}
```

- [ ] **Step 3: 跑绿**
Run: `... test --tests "cn.zhangyis.db.storage.record.format.HiddenColumnLayoutTest"`
Expected: PASS。

---

## Task 6: TableSchema.clustered + LogicalRecord.hiddenColumns

**先跑 impact：** `gitnexus_impact({target:"TableSchema", direction:"upstream", repo:"dbtest"})` 与 `target:"LogicalRecord"`，向用户报告 blast radius（预期 HIGH：大量 record/btree 测试与编码点）。

**Files:**
- Modify: `record/schema/TableSchema.java`, `record/format/LogicalRecord.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/schema/TableSchemaTest.java`（新增/补充），`record/format/LogicalRecordTest.java`

- [ ] **Step 1: 写失败测试**
```java
// TableSchemaTest
@Test void compatConstructorDefaultsNonClustered() {
    TableSchema s = new TableSchema(1L, cols());      // 旧两参构造器
    assertFalse(s.clustered());
}
@Test void clusteredConstructorSetsFlag() {
    TableSchema s = new TableSchema(2L, cols(), true);
    assertTrue(s.clustered());
}
```
```java
// LogicalRecordTest
@Test void hiddenColumnsOptional() {
    LogicalRecord r = new LogicalRecord(1L, vals(), false, RecordType.CONVENTIONAL); // 旧四参
    assertNull(r.hiddenColumns());
}
@Test void hiddenColumnsCarried() {
    HiddenColumns h = new HiddenColumns(TransactionId.of(5), RollPointer.NULL);
    LogicalRecord r = new LogicalRecord(1L, vals(), false, RecordType.CONVENTIONAL, h);
    assertEquals(h, r.hiddenColumns());
}
```

- [ ] **Step 2: 跑红 → 改 TableSchema**
在 `TableSchema` record 增加 `clustered` 为 canonical 组件，并加兼容副构造器：
```java
public record TableSchema(long schemaVersion, List<ColumnDef> columns, boolean clustered) {
    public TableSchema {
        // ...原有校验不变...
    }
    /** 兼容副构造器：默认非聚簇，保旧调用点（B1/B2/B3、node-pointer schema）源码不破。 */
    public TableSchema(long schemaVersion, List<ColumnDef> columns) {
        this(schemaVersion, columns, false);
    }
    // ...columnCount()/column() 不变...
}
```

- [ ] **Step 3: 改 LogicalRecord**
```java
public record LogicalRecord(long schemaVersion, List<ColumnValue> columnValues, boolean deleted,
                            RecordType recordType, HiddenColumns hiddenColumns) {
    public LogicalRecord {
        if (columnValues == null) throw new DatabaseValidationException("column values must not be null");
        if (recordType == null) throw new DatabaseValidationException("record type must not be null");
        // 只校验自身形状：hiddenColumns 在场则其内部已非空（由 HiddenColumns 构造器保证）。
        // clustered ⇔ hiddenColumns 在场的一致性由 RecordEncoder/RecordDecoder（持 schema）校验。
        columnValues = List.copyOf(columnValues);
    }
    /** 兼容副构造器：无隐藏列（非聚簇）。 */
    public LogicalRecord(long schemaVersion, List<ColumnValue> columnValues, boolean deleted, RecordType recordType) {
        this(schemaVersion, columnValues, deleted, recordType, null);
    }
}
```

- [ ] **Step 4: 跑绿（含全量 record 回归确认旧调用点编译通过）**
Run: `... test --tests "cn.zhangyis.db.storage.record.*"`
Expected: PASS（兼容副构造器保证旧测试不破）。

---

## Task 7: RecordEncoder 写隐藏区 + 一致性校验

**Files:**
- Modify: `record/format/RecordEncoder.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/format/RecordEncoderHiddenTest.java`

- [ ] **Step 1: 写失败测试**
```java
@Test void clusteredAppendsFifteenBytes() {
    TableSchema clustered = schemaWith(/*...*/ true);
    LogicalRecord rec = recWithHidden(clustered, TransactionId.of(9), RollPointer.NULL);
    byte[] plain = new RecordEncoder(reg).encode(recNoHidden(nonClustered), nonClustered);
    byte[] withHidden = new RecordEncoder(reg).encode(rec, clustered);
    assertEquals(plain.length + 15, withHidden.length);
}
@Test void clusteredSchemaButNoHiddenColumnsRejected() {
    TableSchema clustered = schemaWith(true);
    LogicalRecord noHidden = recNoHidden(clustered);
    assertThrows(DatabaseValidationException.class, () -> new RecordEncoder(reg).encode(noHidden, clustered));
}
@Test void nonClusteredWithHiddenColumnsRejected() {
    TableSchema nonClustered = schemaWith(false);
    LogicalRecord withHidden = recWithHidden(nonClustered, TransactionId.of(1), RollPointer.NULL);
    assertThrows(DatabaseValidationException.class, () -> new RecordEncoder(reg).encode(withHidden, nonClustered));
}
```

- [ ] **Step 2: 跑红 → 改 encode()**
在 `encode()`：
1. 计算 `recordLength` 后，若 `schema.clustered()` 则 `recordLength += HiddenColumnLayout.HIDDEN_BYTES`。
2. 一致性校验（在 MAX 检查附近）：`schema.clustered()` 必须与 `record.hiddenColumns() != null` 相等，否则抛 `DatabaseValidationException("clustered/hiddenColumns mismatch")`。
3. 写完变长区后，若 clustered：`HiddenColumnLayout.encode(buf, recordLength - HiddenColumnLayout.HIDDEN_BYTES, hc.dbTrxId(), hc.dbRollPtr())`。
代码片段：
```java
// 一致性校验（紧接 column count 校验之后）
if (schema.clustered() != (record.hiddenColumns() != null)) {
    throw new DatabaseValidationException("clustered=" + schema.clustered()
        + " but hiddenColumns " + (record.hiddenColumns() == null ? "absent" : "present"));
}
// recordLength 计算后
if (schema.clustered()) {
    recordLength += HiddenColumnLayout.HIDDEN_BYTES;
}
// ... RecordHeader 用更新后的 recordLength 写 ...
// 写完 var 区后：
if (schema.clustered()) {
    HiddenColumns hc = record.hiddenColumns();
    HiddenColumnLayout.encode(buf, recordLength - HiddenColumnLayout.HIDDEN_BYTES, hc.dbTrxId(), hc.dbRollPtr());
}
```
注意：`RecordHeader(... recordLength)` 必须用含 +15 的 `recordLength`，使 resolver 的 `header.recordLength()==buffer.length` 成立。

- [ ] **Step 3: 跑绿**
Run: `... test --tests "cn.zhangyis.db.storage.record.format.RecordEncoderHiddenTest"`
Expected: PASS。

---

## Task 8: RecordFieldResolver 解析隐藏区 + tail check + materialize 带隐藏列；RecordCursor 访问器

**Files:**
- Modify: `record/format/RecordFieldResolver.java`（含内部 `Resolved`）, `record/page/RecordCursor.java`
- Test: `src/test/java/cn/zhangyis/db/storage/record/format/RecordEncoderHiddenTest.java`（补 round-trip），`record/page/RecordCursorHiddenTest.java`

- [ ] **Step 1: 写失败测试（往返 + tail check + cursor）**
```java
// resolver 往返：clustered encode → resolve → materialize().hiddenColumns() 等值
@Test void clusteredRoundTripCarriesHidden() {
    HiddenColumns h = new HiddenColumns(TransactionId.of(0x7F), new RollPointer(false, PageNo.of(3), 4));
    byte[] buf = new RecordEncoder(reg).encode(recWithHidden(clustered, h), clustered);
    LogicalRecord back = new RecordDecoder(reg).decode(buf, clustered);
    assertEquals(h, back.hiddenColumns());
    assertEquals(userCols, back.columnValues()); // 隐藏列不混入 columnValues
}
@Test void decoderRejectsClusteredWrongTailLength() {
    byte[] buf = new RecordEncoder(reg).encode(recWithHidden(clustered, h), clustered);
    byte[] truncated = Arrays.copyOf(buf, buf.length - 1); // 少 1B
    // 修正 header recordLength 使长度校验通过、但尾部不足 15
    // 期望 RecordFormatException（尾部校验）
    assertThrows(RecordFormatException.class, () -> new RecordDecoder(reg).decode(truncated, clustered));
}
// cursor：RecordCursorHiddenTest 经真实 RecordPage 写一条 clustered 记录，cursor.dbTrxId()/dbRollPtr() 读回
```

- [ ] **Step 2: 跑红 → 改 RecordFieldResolver.resolve()**
在算出 `fixedOff`/`varOff` 与各 slice 后，计算用户字段末尾并做 tail check + 解析隐藏区：
```java
int userEnd = off + fixedAreaLen + varAreaLen; // off=dir 之后；varAreaLen 为活跃变长总长（需累加 dir.length）
HiddenColumns hidden = null;
if (schema.clustered()) {
    if (header.recordLength() != userEnd + HiddenColumnLayout.HIDDEN_BYTES) {
        throw new RecordFormatException("clustered record tail mismatch: len=" + header.recordLength()
            + " userEnd=" + userEnd);
    }
    int hOff = header.recordLength() - HiddenColumnLayout.HIDDEN_BYTES;
    hidden = new HiddenColumns(HiddenColumnLayout.decodeTrxId(recordBytes, hOff),
                               HiddenColumnLayout.decodeRollPtr(recordBytes, hOff));
} else {
    if (header.recordLength() != userEnd) {
        throw new RecordFormatException("non-clustered record has trailing bytes: len="
            + header.recordLength() + " userEnd=" + userEnd);
    }
}
return new Resolved(schema, header, isNull, slices, registry, hidden);
```
（需在循环里累计 `varAreaLen`：解析时把 `dir.length(dirIdx)` 累加，或解析后由 `header.recordLength()` 反推；实现选其一，确保 userEnd 精确。）
`Resolved` 增加 `HiddenColumns hidden` 字段与 `hiddenColumns()` 访问器；`materialize()` 改为：
```java
return new LogicalRecord(schema.schemaVersion(), values, header.deletedFlag(), header.recordType(), hidden);
```

- [ ] **Step 3: 改 RecordCursor**
```java
/** 聚簇记录的 DB_TRX_ID；非聚簇记录调用应通过 schema.clustered() 预判（否则 hidden 为 null）。 */
public TransactionId dbTrxId() {
    HiddenColumns h = resolved().hiddenColumns();
    if (h == null) throw new RecordFormatException("record has no hidden columns (non-clustered)");
    return h.dbTrxId();
}
public RollPointer dbRollPtr() {
    HiddenColumns h = resolved().hiddenColumns();
    if (h == null) throw new RecordFormatException("record has no hidden columns (non-clustered)");
    return h.dbRollPtr();
}
```
（`materialize()` 已自动经 `resolved().materialize()` 带上 hidden。）

- [ ] **Step 4: 跑绿 + 全量 record 回归**
Run: `... test --tests "cn.zhangyis.db.storage.record.*"`
Expected: PASS。**关键回归：非聚簇记录字节/解析逐位不变**（已有 R1-R5 测试守护）。

---

## Task 9: BTreeIndex.clustered() 派生 + node-pointer schema 非 clustered

**先跑 impact：** `gitnexus_impact({target:"BTreeIndex", direction:"upstream", repo:"dbtest"})`。

**Files:**
- Modify: `btree/BTreeIndex.java`
- 核对：`SplitCapableBTreeIndexService` 中 `pointerSchema` 的构造（确认 `clustered=false`，即用 `TableSchema` 两参兼容构造器；若它显式三参则改为 false）。
- Test: `src/test/java/cn/zhangyis/db/storage/btree/BTreeIndexClusteredTest.java`

- [ ] **Step 1: 写失败测试**
```java
@Test void clusteredDerivesFromSchema() {
    BTreeIndex idx = new BTreeIndex(1, root, 0, keyDef, clusteredSchema(), true);
    assertTrue(idx.clustered());
    BTreeIndex idx2 = new BTreeIndex(1, root, 0, keyDef, nonClusteredSchema(), true);
    assertFalse(idx2.clustered());
}
```

- [ ] **Step 2: 跑红 → 加派生方法（不改 canonical constructor）**
```java
/** 是否聚簇索引：从 schema 派生，clustered 单一权威态在 TableSchema，避免双重状态。 */
public boolean clustered() { return schema.clustered(); }
```

- [ ] **Step 3: 核对 node-pointer schema 为非 clustered**
打开 `SplitCapableBTreeIndexService`，定位 `pointerSchema` 构造处（搜索 `pointerSchema`）。确认它用 `new TableSchema(version, columns)` 两参（clustered=false）。若用三参显式 true，改为 false。补断言测试：root split 后根页 node-pointer 记录 `materialize().hiddenColumns()==null`。

- [ ] **Step 4: 跑绿**
Run: `... test --tests "cn.zhangyis.db.storage.btree.*"`
Expected: PASS。

---

## Task 10: SplitCapableBTreeIndexService 聚簇 insert 盖戳 + split 保留不变量

**先跑 impact：** `gitnexus_impact({target:"SplitCapableBTreeIndexService", direction:"upstream", repo:"dbtest"})`。

**Files:**
- Modify: `btree/SplitCapableBTreeIndexService.java`
- Test: `src/test/java/cn/zhangyis/db/storage/btree/ClusteredInsertTest.java`

- [ ] **Step 1: 写失败测试（端到端 + split 保留）**
```java
// 用现有 B3 测试的 onPool 风格 harness 建一个 clustered index + clustered schema
@Test void clusteredInsertStampsTrxIdAndNullRollPtr() {
    // begin txn -> assignWriteId -> insertClustered -> lookup -> cursor.dbTrxId()==id, dbRollPtr().isNull()
}
@Test void rejectsNoneTrxId() {
    assertThrows(DatabaseValidationException.class,
        () -> service.insertClustered(mtr, clusteredIndex, rec, TransactionId.NONE));
}
@Test void rejectsNonClusteredIndex() {
    assertThrows(DatabaseValidationException.class,
        () -> service.insertClustered(mtr, nonClusteredIndex, rec, TransactionId.of(1)));
}
@Test void splitPreservesHiddenColumnsAcrossLeaves() {
    // 插入足够多 clustered 记录触发 root split（参考 B3 split 测试的数据量），
    // 然后对落在两个子 leaf 上的若干 key 各 lookup，断言 dbTrxId 仍为插入时的事务 id。
}
```

- [ ] **Step 2: 跑红 → 加 insertClustered**
```java
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.record.format.HiddenColumns;

/**
 * 聚簇 insert：用调用方事务 id 盖戳 DB_TRX_ID、DB_ROLL_PTR=NULL（本片无 undo），再走通用 insert。
 * split 时 materializeLeafRecords→insertAll 重编码，因 index.schema().clustered() 为真且记录带 hiddenColumns，
 * encoder 自动带住隐藏列，故 split 不丢 DB_TRX_ID（隐藏列保留不变量）。
 *
 * <p>事务 id 来源：调用方须先 TransactionManager.assignWriteId(txn)，此处只验非 NONE，不依赖 TransactionManager。
 */
public BTreeInsertResult insertClustered(MiniTransaction mtr, BTreeIndex index,
                                         LogicalRecord record, TransactionId transactionId) {
    if (record == null || transactionId == null) {
        throw new DatabaseValidationException("clustered insert record/transactionId must not be null");
    }
    if (!index.clustered()) {
        throw new DatabaseValidationException("insertClustered requires a clustered index: " + index.indexId());
    }
    if (transactionId.isNone()) {
        throw new DatabaseValidationException("clustered insert requires a non-NONE transaction id");
    }
    LogicalRecord stamped = new LogicalRecord(record.schemaVersion(), record.columnValues(),
        record.deleted(), record.recordType(),
        new HiddenColumns(transactionId, RollPointer.NULL));
    return insert(mtr, index, stamped);
}
```

- [ ] **Step 3: 跑绿**
Run: `... test --tests "cn.zhangyis.db.storage.btree.ClusteredInsertTest"`
Expected: PASS（尤其 splitPreservesHiddenColumnsAcrossLeaves，验证 split 保留不变量）。

---

## Task 11: 全量回归 + GitNexus 收口

- [ ] **Step 1: 全量测试**
Run: `... test --console=plain`
Expected: BUILD SUCCESSFUL，failures=0 errors=0；测试数较 374 基线只增不减（用实际输出记录，不在文档写死数字）。

- [ ] **Step 2: 非聚簇字节不变核验**
确认 B1/B2/B3/record R1-R5/redo/recovery 测试全部通过（非聚簇 schema 走兼容副构造器 clustered=false，编码字节逐位不变）。若任何 record 字节断言失败，说明 encoder/resolver 误把隐藏区作用到了非聚簇路径，回到 Task 7/8 修。

- [ ] **Step 3: 刷新 GitNexus**
Run: `npx gitnexus analyze`
Expected: 索引刷新成功（node/edge 数更新）。若 stale 告警则先 analyze 再继续。

- [ ] **Step 4: 收口**
项目 no-commit：不执行 `git commit`。在 `MEMORY.md` / `storage-build-sequence.md` 追加「T1.1+T1.2 完成」要点（实际测试数、新增类清单）。

---

## 自检（写计划后对照 spec）

1. **spec 覆盖**：domain 值对象(Task1-2)、trx 枚举/选项/异常(Task3)、trx 核心(Task4)、HiddenColumnLayout(Task5)、TableSchema/LogicalRecord(Task6)、Encoder(Task7)、Resolver/Cursor(Task8)、BTreeIndex 派生+node-pointer 非 clustered(Task9)、聚簇 insert+split 保留(Task10)、回归+gitnexus(Task11)。spec 各节均有对应任务。
2. **placeholder 扫描**：无 TBD/TODO；测试体中标注的 harness 复用（onPool 风格）指向现有 B3 测试模式，非占位。
3. **类型一致**：`TransactionId/TransactionNo/RollPointer/HiddenColumns/HiddenColumnLayout.HIDDEN_BYTES(15)/RollPointer.BYTES(7)` 跨任务签名一致；`insertClustered(mtr,index,record,transactionId)` 与 spec §6 一致；`TableSchema` 三参 canonical + 两参兼容、`LogicalRecord` 五参 canonical + 四参兼容贯穿一致。
4. **风险点**：Task7/8 的非聚簇字节不变由 Task11 Step2 显式守护；split 保留由 Task10 splitPreservesHiddenColumnsAcrossLeaves 守护。
