# TablespaceRegistry 接入 DiskSpaceManager + 空间管理准入 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: 用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 按任务逐个执行。步骤用 checkbox（`- [ ]`）跟踪。

**Goal:** 让 `DiskSpaceManager` 持有 `TablespaceRegistry`：建/开表空间注册权威 metadata、空间管理 API 经 `require()` 状态准入、运行时生命周期标记、recovery `requireForRecovery` 准入；`type` 持久化进 page-0 `spaceFlags`，reopen 经 page-0 raw loader 从磁盘重建。

**Architecture:** 自底向上：`TablespaceType.code()/fromCode()` → `TablespaceTypeFlags`(type↔flags int) → `TablespaceRegistry.markInactive` → `PageStore.pathOf` → fsp `SpaceHeaderRawCodec`(raw page0 解析) → api `PageZeroTablespaceMetadataLoader` → `DiskSpaceManager`(注册 + 空间管理 API 准入门 + 生命周期标记 + recovery 开)。门设在 DiskSpaceManager 空间管理 API 层，PageStore 保持 state-free；per-page 数据 IO 本片不拦（空间管理准入，非全 IO 准入）。

**Tech Stack:** Java 25、JUnit Jupiter、Lombok、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**Spec:** `docs/superpowers/specs/2026-06-18-tablespace-registry-diskspacemanager-admission-design.md`

**项目约束（每个任务都适用）：**
- TDD：先写失败测试 → 跑红 → 最小实现 → 跑绿。
- 无 `synchronized`/`wait`/`notify`；registry 并发沿用既有 `ConcurrentHashMap` 桶级原子，本片不新增锁。
- 生产代码不抛裸 `IllegalArgumentException`/`RuntimeException`，用 `DatabaseValidationException` 与 fil 既有 `TablespaceCorruptedException`/`TablespaceUnavailableException`/`TablespaceNotFoundException`/`TablespaceNotOpenException`。
- 中文 Javadoc/字段注释，解释准入语义、type↔flags 编码、loader raw 读边界、依赖方向、简化点（state 不持久化 / 非全 IO 门 / size 非实时）。
- **依赖方向**：fsp 保持 fil-无关（page0 只存 int flags）；`SpaceHeaderRawCodec` 在 fsp（可用 package-private `SpaceHeaderLayout`）；loader 在 api（依赖 fil+fsp）。
- **向后兼容**：保留 `new DiskSpaceManager(pool,store,ps)` 与 `createTablespace(mtr,spaceId,path,initialSize)`；新增注入/typed 重载。30 处既有调用零改动。
- **项目 no-commit**：每个任务以「跑绿相关测试」收口，不执行 `git commit`。

**测试命令（PowerShell）：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "<pattern>" --console=plain
```

---

## 文件结构

**新建（生产）：**
- `src/main/java/cn/zhangyis/db/storage/fil/TablespaceTypeFlags.java` — type↔spaceFlags int 编解码（bits 装配）
- `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderPhysical.java` — page0 物理字段值对象
- `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRawCodec.java` — raw ByteBuffer → SpaceHeaderPhysical（public，封装偏移）
- `src/main/java/cn/zhangyis/db/storage/api/PageZeroTablespaceMetadataLoader.java` — raw 读 page0 → TablespaceMetadata

**修改（生产）：**
- `src/main/java/cn/zhangyis/db/storage/fil/TablespaceType.java` — 加 `code()`/`fromCode()`
- `src/main/java/cn/zhangyis/db/storage/fil/TablespaceRegistry.java` — 加 `markInactive(SpaceId)`
- `src/main/java/cn/zhangyis/db/storage/fil/CachingTablespaceRegistry.java` — 实现 `markInactive`
- `src/main/java/cn/zhangyis/db/storage/fil/PageStore.java` — 加 `Path pathOf(SpaceId)`
- `src/main/java/cn/zhangyis/db/storage/fil/FileChannelPageStore.java` — 实现 `pathOf`
- `src/main/java/cn/zhangyis/db/storage/fil/DataFileHandle.java` — 加 `path()` 访问器
- `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java` — registry 字段 + 注册 + 准入门 + 生命周期标记 + recovery 开

**新建（测试）：**
- `src/test/java/cn/zhangyis/db/storage/fil/TablespaceTypeTest.java`
- `src/test/java/cn/zhangyis/db/storage/fil/TablespaceTypeFlagsTest.java`
- `src/test/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRawCodecTest.java`
- `src/test/java/cn/zhangyis/db/storage/api/PageZeroTablespaceMetadataLoaderTest.java`
- `src/test/java/cn/zhangyis/db/storage/api/DiskSpaceManagerTablespaceAdmissionTest.java`

**修改（测试）：**
- `src/test/java/cn/zhangyis/db/storage/fil/CachingTablespaceRegistryTest.java` — 加 `markInactive` 增量
- `src/test/java/cn/zhangyis/db/storage/fil/FileChannelPageStoreTest.java` — 加 `pathOf` 增量

---

## Task 1: TablespaceType.code()/fromCode()

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/TablespaceType.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/TablespaceTypeTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** TablespaceType 稳定落盘 code 钉死（page-0 spaceFlags 依赖）+ fromCode 往返 + 未知 code 拒绝。 */
class TablespaceTypeTest {

    @Test void codesAreStable() {
        assertEquals(0, TablespaceType.SYSTEM.code());
        assertEquals(1, TablespaceType.FILE_PER_TABLE.code());
        assertEquals(2, TablespaceType.GENERAL.code());
        assertEquals(3, TablespaceType.UNDO.code());
        assertEquals(4, TablespaceType.TEMPORARY.code());
    }

    @Test void fromCodeRoundTrips() {
        for (TablespaceType t : TablespaceType.values()) {
            assertEquals(t, TablespaceType.fromCode(t.code()));
        }
    }

    @Test void fromCodeRejectsUnknown() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceType.fromCode(7));
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.TablespaceTypeTest" --console=plain`
Expected: 编译失败（`code()`/`fromCode()` 不存在）。

- [ ] **Step 3: 给 TablespaceType 加 code()/fromCode()**

整体替换 `TablespaceType.java`（保留各常量原 Javadoc，加稳定 code）：

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间类型。用于区分 system、独立表空间、general tablespace、undo 和 temporary 的加载来源与扩展策略。
 * {@code code} 是稳定落盘值（编入 page-0 spaceFlags 低 3 位，见 {@link TablespaceTypeFlags}），不随 enum 重排漂移
 * （对齐 UndoRecordType/PageType 风格）。
 */
public enum TablespaceType {
    /** 系统表空间。承载 InnoDB 系统级元数据和部分内部结构，通常在实例启动时由固定配置加载。 */
    SYSTEM(0),
    /** 独立表空间。对应 file-per-table 的 .ibd 文件，是普通用户表最常见的数据文件形态。 */
    FILE_PER_TABLE(1),
    /** 通用表空间。一个 tablespace 可承载多个表或索引，元数据需要结合数据字典判断对象归属。 */
    GENERAL(2),
    /** Undo 表空间。承载 undo segment/page，扩展策略和普通用户表空间不同，恢复和 purge 会重点访问。 */
    UNDO(3),
    /** 临时表空间。用于临时对象和内部临时结构，通常允许简化 redo 持久化语义，实例重启后可重建。 */
    TEMPORARY(4);

    private final int code;

    TablespaceType(int code) {
        this.code = code;
    }

    /** 稳定落盘 code（编入 spaceFlags）。 */
    public int code() {
        return code;
    }

    /** 由落盘 code 还原；未知 code 视为表空间类型损坏。 */
    public static TablespaceType fromCode(int code) {
        for (TablespaceType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new DatabaseValidationException("unknown tablespace type code: " + code);
    }
}
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.TablespaceTypeTest" --console=plain`
Expected: PASS。

---

## Task 2: TablespaceTypeFlags

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fil/TablespaceTypeFlags.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/TablespaceTypeFlagsTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** type ↔ spaceFlags int：低 3 位 code 往返、高保留位被忽略（future-flags 可扩展）、未知 type code 拒绝。 */
class TablespaceTypeFlagsTest {

    @Test void roundTripsAllTypes() {
        for (TablespaceType t : TablespaceType.values()) {
            assertEquals(t, TablespaceTypeFlags.decode(TablespaceTypeFlags.encode(t)));
        }
    }

    @Test void highReservedBitsDoNotAffectType() {
        // 高位（future flags）置位不应改变解出的 type，否则将来无法扩展 flags
        int withHighBits = TablespaceTypeFlags.encode(TablespaceType.UNDO) | 0x100;
        assertEquals(TablespaceType.UNDO, TablespaceTypeFlags.decode(withHighBits));
    }

    @Test void decodeRejectsUnknownTypeCode() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceTypeFlags.decode(5)); // 低 3 位=5 非法
    }

    @Test void encodeRejectsNull() {
        assertThrows(DatabaseValidationException.class, () -> TablespaceTypeFlags.encode(null));
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.TablespaceTypeFlagsTest" --console=plain`
Expected: 编译失败（`TablespaceTypeFlags` 不存在）。

- [ ] **Step 3: 实现 TablespaceTypeFlags**

```java
package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间 {@link TablespaceType} 与 page-0 {@code spaceFlags} int 的编解码。本类只负责 bits 装配，
 * code 权威在 {@link TablespaceType#code()}。
 *
 * <p>布局：bits 0..2 = {@code type.code()}；bits 3..31 = reserved/future flags（压缩、加密等）。
 * {@link #decode} 只取低 3 位映射 type、**忽略高位**——否则将来扩展 flags 会被误拒。
 */
public final class TablespaceTypeFlags {

    /** type code 占位（低 3 位）。 */
    private static final int TYPE_MASK = 0x7;

    private TablespaceTypeFlags() {
    }

    /** 把 type 编入 spaceFlags 低 3 位（高位 0，留给 future flags）。 */
    public static int encode(TablespaceType type) {
        if (type == null) {
            throw new DatabaseValidationException("tablespace type must not be null");
        }
        return type.code() & TYPE_MASK;
    }

    /** 从 spaceFlags 低 3 位解 type；高位保留位忽略；未知 type code 由 {@link TablespaceType#fromCode} 抛。 */
    public static TablespaceType decode(int spaceFlags) {
        return TablespaceType.fromCode(spaceFlags & TYPE_MASK);
    }
}
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.TablespaceTypeFlagsTest" --console=plain`
Expected: PASS。

---

## Task 3: TablespaceRegistry.markInactive

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/TablespaceRegistry.java`, `src/main/java/cn/zhangyis/db/storage/fil/CachingTablespaceRegistry.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/CachingTablespaceRegistryTest.java`（增量）

- [ ] **Step 1: 在 CachingTablespaceRegistryTest 追加失败测试**

在 `CachingTablespaceRegistryTest` 类体内追加（复用既有 `metadata(state)` helper + `SpaceId.of(10)`）：

```java
    @Test
    void shouldMarkInactiveBlockingOrdinaryRequireButAllowingRecovery() {
        CachingTablespaceRegistry registry = new CachingTablespaceRegistry(spaceId -> Optional.of(metadata(TablespaceState.NORMAL)));
        registry.require(SpaceId.of(10)); // 先载入 NORMAL

        TablespaceHandle inactive = registry.markInactive(SpaceId.of(10));

        assertEquals(TablespaceState.INACTIVE, inactive.tablespace().state());
        assertThrows(TablespaceUnavailableException.class, () -> registry.require(SpaceId.of(10)));
        assertEquals(TablespaceState.INACTIVE, registry.requireForRecovery(SpaceId.of(10)).tablespace().state());
        assertEquals(TablespaceState.INACTIVE, registry.find(SpaceId.of(10)).orElseThrow().tablespace().state());
    }
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.CachingTablespaceRegistryTest" --console=plain`
Expected: 编译失败（`markInactive` 不存在）。

- [ ] **Step 3: 在 TablespaceRegistry 接口加 markInactive**

在 `TablespaceRegistry` 接口的 `markCorrupted` 与 `markDiscarded` 之间加：

```java
    /**
     * 标记表空间为 INACTIVE（如 undo 待截断），阻断普通 IO 路径但允许 recovery 路径访问。
     * 与 markCorrupted/markDiscarded 一致，状态读改写在桶级原子内完成。
     *
     * @param spaceId 表空间编号。
     * @return INACTIVE 状态的运行时句柄。
     */
    TablespaceHandle markInactive(SpaceId spaceId);
```

- [ ] **Step 4: 在 CachingTablespaceRegistry 实现 markInactive**

在 `CachingTablespaceRegistry` 的 `markCorrupted` 后加（与 markDiscarded 同款 compute+transitTo）：

```java
    /**
     * 标记表空间 INACTIVE：桶级原子内「读当前快照（缺失则按权威来源加载）→ transitTo(INACTIVE) → 发布」。
     * NORMAL/ACTIVE→INACTIVE 由 {@link TablespaceState} 允许；INACTIVE 后普通 require 抛
     * {@link TablespaceUnavailableException}，requireForRecovery 仍可访问。
     *
     * @param spaceId 表空间编号。
     * @return INACTIVE 状态句柄。
     */
    @Override
    public TablespaceHandle markInactive(SpaceId spaceId) {
        validateSpaceId(spaceId);
        TablespaceHandle inactive = handles.compute(spaceId, (id, existing) -> {
            Tablespace current = (existing != null ? existing : loadHandle(id)).tablespace();
            return new TablespaceHandle(current.transitTo(TablespaceState.INACTIVE));
        });
        // INACTIVE 是显式生命周期转换（如 undo 待截断），记录便于追踪；非高频路径。
        log.info("tablespace {} marked inactive", spaceId.value());
        return inactive;
    }
```

- [ ] **Step 5: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.CachingTablespaceRegistryTest" --console=plain`
Expected: PASS（既有用例 + markInactive 增量）。

---

## Task 4: PageStore.pathOf + DataFileHandle.path()

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/fil/PageStore.java`, `src/main/java/cn/zhangyis/db/storage/fil/FileChannelPageStore.java`, `src/main/java/cn/zhangyis/db/storage/fil/DataFileHandle.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fil/FileChannelPageStoreTest.java`（增量）

- [ ] **Step 1: 在 FileChannelPageStoreTest 追加失败测试**

在 `FileChannelPageStoreTest` 追加（用方法级 `@TempDir Path dir` 参数避免与既有字段冲突；如下 import 已存在则忽略重复：`import cn.zhangyis.db.domain.PageNo; import cn.zhangyis.db.domain.PageSize; import cn.zhangyis.db.domain.SpaceId; import java.nio.file.Path; import org.junit.jupiter.api.io.TempDir; import static org.junit.jupiter.api.Assertions.assertEquals; import static org.junit.jupiter.api.Assertions.assertThrows;`）：

```java
    @Test
    void pathOfReturnsOpenFilePath(@TempDir Path dir) {
        Path p = dir.resolve("pathof.ibd");
        try (FileChannelPageStore store = new FileChannelPageStore()) {
            store.create(SpaceId.of(31), p, PageSize.ofBytes(16 * 1024), PageNo.of(8));
            assertEquals(p, store.pathOf(SpaceId.of(31)));
        }
    }

    @Test
    void pathOfRejectsUnopenedSpace() {
        try (FileChannelPageStore store = new FileChannelPageStore()) {
            assertThrows(TablespaceNotOpenException.class, () -> store.pathOf(SpaceId.of(99)));
        }
    }
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.FileChannelPageStoreTest" --console=plain`
Expected: 编译失败（`pathOf` 不存在）。

- [ ] **Step 3: PageStore 接口加 pathOf**

在 `PageStore` 接口加（建议放在 `currentSizeInPages` 之后，import `java.nio.file.Path` 已有）：

```java
    /**
     * 返回已登记表空间的数据文件路径。供上层（如 metadata loader）在不持有 path catalog 时取得 path；
     * 未登记抛 {@link TablespaceNotOpenException}。
     *
     * @param spaceId 表空间编号。
     * @return 已打开数据文件路径。
     */
    Path pathOf(SpaceId spaceId);
```

- [ ] **Step 4: DataFileHandle 加 path() 访问器**

在 `DataFileHandle` 加包内访问器（紧邻 `currentSizeInPages()`）：

```java
    /** 数据文件路径（诊断 / 上层 metadata 重建用；IO 仍走已打开 channel）。 */
    Path path() {
        return path;
    }
```

- [ ] **Step 5: FileChannelPageStore 实现 pathOf**

在 `FileChannelPageStore` 加（复用既有私有 `require(spaceId)`，未登记即抛 `TablespaceNotOpenException`）：

```java
    @Override
    public Path pathOf(SpaceId spaceId) {
        validateSpaceId(spaceId);
        return require(spaceId).path();
    }
```

- [ ] **Step 6: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fil.FileChannelPageStoreTest" --console=plain`
Expected: PASS。

---

## Task 5: SpaceHeaderPhysical + SpaceHeaderRawCodec（fsp）

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderPhysical.java`, `src/main/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRawCodec.java`
- Test: `src/test/java/cn/zhangyis/db/storage/fsp/SpaceHeaderRawCodecTest.java`

> codec 放 fsp 才能用 package-private `SpaceHeaderLayout` 偏移；只解 loader 需要的物理字段，不解 3 个 FLST base（FlstBase 解码依赖 PageGuard，raw ByteBuffer 路径不复刻）。big-endian 与 PageGuard 写一致。

- [ ] **Step 1: 写失败测试（经 headerRepo 写 page0 + commit/flush → raw 读字节 → readPhysical 字段一致）**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** SpaceHeaderRawCodec：经 headerRepo 写 page0 + 刷盘后 raw 读字节，readPhysical 解出的物理字段与写入一致。 */
class SpaceHeaderRawCodecTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(42);

    @TempDir Path dir;

    @Test void readPhysicalParsesWrittenHeader() {
        Path path = dir.resolve("hdr.ibd");
        // session1：写 page0 header + commit；pool close 刷盘
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 64)) {
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager mgr = new MiniTransactionManager();
            SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
            MiniTransaction m = mgr.begin();
            headerRepo.initialize(m, new SpaceHeaderSnapshot(SPACE, PS, 0xABC, PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 7L));
            mgr.commit(m);
        }
        // session2：raw 读 page0 字节 → readPhysical
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            ByteBuffer buf = ByteBuffer.allocate(PS.bytes());
            store.readPage(PageId.of(SPACE, PageNo.of(0)), buf);
            SpaceHeaderPhysical phys = SpaceHeaderRawCodec.readPhysical(buf);
            assertEquals(SPACE, phys.spaceId());
            assertEquals(PS, phys.pageSize());
            assertEquals(0xABC, phys.spaceFlags());
            assertEquals(64L, phys.currentSizeInPages().value());
            assertEquals(0L, phys.freeLimitPageNo().value());
            assertEquals(7L, phys.spaceVersion());
        }
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fsp.SpaceHeaderRawCodecTest" --console=plain`
Expected: 编译失败（`SpaceHeaderPhysical`/`SpaceHeaderRawCodec` 不存在）。

- [ ] **Step 3: 实现 SpaceHeaderPhysical**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

/**
 * page-0 物理字段子集（不含 FLST base/inode/版本号以外的链信息），供上层 metadata loader 从 raw ByteBuffer 重建
 * 表空间权威物理元数据。{@code spaceFlags} 为原始 int（type 等由 fil 层解码，fsp 不解释）。
 *
 * @param spaceId            表空间编号。
 * @param pageSize           页大小。
 * @param spaceFlags         原始标志位（含 type 低 3 位，fsp 不解释）。
 * @param currentSizeInPages 当前文件大小页数。
 * @param freeLimitPageNo    FSP 可分配页上界。
 * @param spaceVersion       表空间元数据版本。
 */
public record SpaceHeaderPhysical(SpaceId spaceId, PageSize pageSize, int spaceFlags,
                                  PageNo currentSizeInPages, PageNo freeLimitPageNo, long spaceVersion) {

    public SpaceHeaderPhysical {
        if (spaceId == null || pageSize == null || currentSizeInPages == null || freeLimitPageNo == null) {
            throw new DatabaseValidationException("space header physical fields must not be null");
        }
    }
}
```

- [ ] **Step 4: 实现 SpaceHeaderRawCodec**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.ByteBuffer;

/**
 * 从 raw page-0 ByteBuffer 解出 {@link SpaceHeaderPhysical}（封装 {@link SpaceHeaderLayout} 偏移，供 api 层 loader
 * 复用而不直接依赖 package-private 偏移常量）。只解物理字段，不解 FLST base（其解码依赖 PageGuard）。
 *
 * <p>big-endian 绝对位置读，与 {@code PageGuard.writeInt/writeLong}（{@code SpaceHeaderRepository.initialize}）写入一致。
 */
public final class SpaceHeaderRawCodec {

    private SpaceHeaderRawCodec() {
    }

    /**
     * 解析 page-0 物理字段。{@code page} 须含完整一页字节（capacity ≥ SPACE_VERSION+8）；用绝对位置读，不依赖 position。
     *
     * @param page page-0 字节缓冲。
     * @return 物理字段快照。
     */
    public static SpaceHeaderPhysical readPhysical(ByteBuffer page) {
        if (page == null) {
            throw new DatabaseValidationException("space header page buffer must not be null");
        }
        if (page.capacity() < SpaceHeaderLayout.SPACE_VERSION + 8) {
            throw new DatabaseValidationException("space header page buffer too small: " + page.capacity());
        }
        int spaceIdVal = page.getInt(SpaceHeaderLayout.SPACE_ID);
        int pageSizeBytes = page.getInt(SpaceHeaderLayout.PAGE_SIZE_BYTES);
        int spaceFlags = page.getInt(SpaceHeaderLayout.SPACE_FLAGS);
        long currentSize = page.getLong(SpaceHeaderLayout.CURRENT_SIZE);
        long freeLimit = page.getLong(SpaceHeaderLayout.FREE_LIMIT);
        long spaceVersion = page.getLong(SpaceHeaderLayout.SPACE_VERSION);
        return new SpaceHeaderPhysical(SpaceId.of(spaceIdVal), PageSize.ofBytes(pageSizeBytes), spaceFlags,
                PageNo.of(currentSize), PageNo.of(freeLimit), spaceVersion);
    }
}
```

- [ ] **Step 5: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.fsp.SpaceHeaderRawCodecTest" --console=plain`
Expected: PASS。

---

## Task 6: PageZeroTablespaceMetadataLoader（api）

**Files:**
- Create: `src/main/java/cn/zhangyis/db/storage/api/PageZeroTablespaceMetadataLoader.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/PageZeroTablespaceMetadataLoaderTest.java`

- [ ] **Step 1: 写失败测试（建表空间+刷盘后 loader 从磁盘 page0 重建 metadata；含 type 复原与未开拒绝）**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fil.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.TablespaceState;
import cn.zhangyis.db.storage.fil.TablespaceType;
import cn.zhangyis.db.storage.fil.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fsp.FlstBase;
import cn.zhangyis.db.storage.fsp.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** PageZeroTablespaceMetadataLoader：刷盘后 raw 读 page0 重建 metadata（type 经 flags 复原、state=NORMAL、path）；未开 → empty。
 *  session1 直接经 SpaceHeaderRepository 写 page0（不依赖 Task 7 的 DiskSpaceManager 改动）。 */
class PageZeroTablespaceMetadataLoaderTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(60);

    @TempDir Path dir;

    @Test void rebuildsMetadataFromDiskPageZero() {
        Path path = dir.resolve("loader.ibu");
        // session1：直接经 SpaceHeaderRepository 写 page0（type=UNDO 编入 spaceFlags）+ commit；pool close 刷盘。
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.create(SPACE, path, PS, PageNo.of(64));
            MiniTransactionManager mgr = new MiniTransactionManager();
            SpaceHeaderRepository headerRepo = new SpaceHeaderRepository(pool);
            MiniTransaction m = mgr.begin();
            headerRepo.initialize(m, new SpaceHeaderSnapshot(SPACE, PS,
                    TablespaceTypeFlags.encode(TablespaceType.UNDO), PageNo.of(64), PageNo.of(0), 1L,
                    FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY, PageNo.of(2), 0L, 80046, 1L));
            mgr.commit(m);
        }
        // session2：loader raw 读 page0 重建
        try (PageStore store = new FileChannelPageStore()) {
            store.open(SPACE, path, PS);
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            TablespaceMetadata md = loader.load(SPACE).orElseThrow();
            assertEquals(SPACE, md.spaceId());
            assertEquals(TablespaceType.UNDO, md.type());        // type 经 spaceFlags 复原
            assertEquals(TablespaceState.NORMAL, md.state());    // 干净可读 → NORMAL
            assertEquals(64L, md.currentSizeInPages().value());
            assertEquals(path, md.dataFiles().get(0).path());    // path 来自 PageStore.pathOf
        }
    }

    @Test void returnsEmptyForUnopenedSpace() {
        try (PageStore store = new FileChannelPageStore()) {
            PageZeroTablespaceMetadataLoader loader = new PageZeroTablespaceMetadataLoader(store, PS);
            assertEquals(Optional.empty(), loader.load(SpaceId.of(777)));
        }
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.PageZeroTablespaceMetadataLoaderTest" --console=plain`
Expected: 编译失败（`PageZeroTablespaceMetadataLoader` 不存在）。本任务只依赖 Task 1-5（session1 用 `SpaceHeaderRepository` 直接写 page0），不依赖 Task 7 的 DiskSpaceManager 改动。

- [ ] **Step 3: 实现 PageZeroTablespaceMetadataLoader**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fil.SpaceFlags;
import cn.zhangyis.db.storage.fil.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.TablespaceMetadataLoader;
import cn.zhangyis.db.storage.fil.TablespaceNotOpenException;
import cn.zhangyis.db.storage.fil.TablespaceState;
import cn.zhangyis.db.storage.fil.TablespaceType;
import cn.zhangyis.db.storage.fil.TablespaceTypeFlags;
import cn.zhangyis.db.storage.fsp.SpaceHeaderPhysical;
import cn.zhangyis.db.storage.fsp.SpaceHeaderRawCodec;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * 从已开表空间的 page-0 FSP_HDR raw 字节重建权威 {@link TablespaceMetadata}（{@link TablespaceMetadataLoader} 实现）。
 * 数据流：{@code pageStore.pathOf}（取 path）→ {@code pageStore.readPage(page0)}（raw 物理读，不开 MTR/不经 BufferPool，
 * 故可被 registry.require 在外层 MTR 内安全懒调用）→ {@link SpaceHeaderRawCodec} 解物理字段 →
 * {@link TablespaceTypeFlags} 解 type、state 取 {@link TablespaceState#NORMAL}（干净可读 FSP_HDR）。
 *
 * <p>健壮性：page0 无 FilePageHeader 信封可校验，故以「解出 spaceId 自洽」替代 PageType 校验；文件未开 → empty
 * （registry 转 TablespaceNotFoundException）。state 不从磁盘读（本片不持久化 state），lifecycle 标记仅运行时。
 */
public final class PageZeroTablespaceMetadataLoader implements TablespaceMetadataLoader {

    private final PageStore pageStore;
    private final PageSize pageSize;

    public PageZeroTablespaceMetadataLoader(PageStore pageStore, PageSize pageSize) {
        if (pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("page zero loader pageStore/pageSize must not be null");
        }
        this.pageStore = pageStore;
        this.pageSize = pageSize;
    }

    @Override
    public Optional<TablespaceMetadata> load(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
        Path path;
        try {
            path = pageStore.pathOf(spaceId);
        } catch (TablespaceNotOpenException notOpen) {
            return Optional.empty(); // 文件未开：registry 会转 TablespaceNotFoundException
        }
        ByteBuffer page = ByteBuffer.allocate(pageSize.bytes());
        pageStore.readPage(PageId.of(spaceId, PageNo.of(0)), page);
        SpaceHeaderPhysical phys = SpaceHeaderRawCodec.readPhysical(page);
        if (!phys.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("page0 space id mismatch: expected " + spaceId.value()
                    + " got " + phys.spaceId().value());
        }
        TablespaceType type = TablespaceTypeFlags.decode(phys.spaceFlags());
        DataFileDescriptor dataFile = DataFileDescriptor.single(path, PageNo.of(0), phys.currentSizeInPages());
        TablespaceMetadata metadata = new TablespaceMetadata(spaceId, "space-" + spaceId.value(), type,
                phys.pageSize(), TablespaceState.NORMAL, List.of(dataFile), new SpaceFlags(phys.spaceFlags()),
                phys.currentSizeInPages(), phys.freeLimitPageNo(), phys.spaceVersion());
        return Optional.of(metadata);
    }
}
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.PageZeroTablespaceMetadataLoaderTest" --console=plain`
Expected: PASS（type=UNDO 经 spaceFlags 复原、state=NORMAL、path 来自 pathOf；未开 → empty）。

---

## Task 7: DiskSpaceManager 注册 + recovery 开 + state 查询

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/DiskSpaceManagerTablespaceAdmissionTest.java`（新建，本任务放注册/recovery 用例；Task 8 续加准入用例）

- [ ] **Step 1: 写失败测试（注册 NORMAL + typed + recovery 重开冒烟）**

```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import cn.zhangyis.db.storage.fil.TablespaceState;
import cn.zhangyis.db.storage.fil.TablespaceType;
import cn.zhangyis.db.storage.fsp.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** DiskSpaceManager 表空间准入：建表空间注册 NORMAL、空间管理 API 准入门、生命周期标记、recovery 重开。 */
class DiskSpaceManagerTablespaceAdmissionTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir Path dir;

    @Test void createTablespaceRegistersNormalAndAllowsSpaceManagement() {
        onPool((mgr, disk, store) -> {
            SpaceId s = SpaceId.of(50);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, s, dir.resolve("a.ibu"), PageNo.of(64), TablespaceType.UNDO);
            mgr.commit(boot);
            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(s));
            // NORMAL 下空间管理 API 通过（无回归）
            MiniTransaction m = mgr.begin();
            var seg = disk.createSegment(m, s, SegmentPurpose.UNDO);
            disk.allocatePage(m, seg);
            mgr.commit(m);
        });
    }

    @Test void defaultCreateTablespaceUsesGeneralType() {
        onPool((mgr, disk, store) -> {
            SpaceId s = SpaceId.of(51);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, s, dir.resolve("b.ibu"), PageNo.of(64)); // 4 参，type=GENERAL
            mgr.commit(boot);
            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(s));
        });
    }

    @Test void openTablespaceForRecoveryReopensNormalFromDisk() {
        Path path = dir.resolve("rec.ibu");
        SpaceId s = SpaceId.of(52);
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, s, path, PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);
        } // pool close 刷 page0 到盘
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            disk.openTablespaceForRecovery(s, path); // pageStore.open + requireForRecovery（loader 读盘 page0）
            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(s));
        }
    }

    // ---- harness ----

    private interface Body { void run(MiniTransactionManager mgr, DiskSpaceManager disk, PageStore store); }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        try (PageStore ignored = store; BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            body.run(mgr, disk, store);
        }
    }
}
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTablespaceAdmissionTest" --console=plain`
Expected: 编译失败（`createTablespace(...,type)`/`tablespaceState`/`openTablespaceForRecovery` 不存在）。

- [ ] **Step 3: DiskSpaceManager 加 registry 字段 + 注册 + recovery 开 + state 查询**

3a. 加 import（在现有 import 区）：
```java
import cn.zhangyis.db.storage.fil.CachingTablespaceRegistry;
import cn.zhangyis.db.storage.fil.DataFileDescriptor;
import cn.zhangyis.db.storage.fil.SpaceFlags;
import cn.zhangyis.db.storage.fil.TablespaceMetadata;
import cn.zhangyis.db.storage.fil.TablespaceNotFoundException;
import cn.zhangyis.db.storage.fil.TablespaceRegistry;
import cn.zhangyis.db.storage.fil.TablespaceState;
import cn.zhangyis.db.storage.fil.TablespaceType;
import cn.zhangyis.db.storage.fil.TablespaceTypeFlags;
import java.util.List;
```

3b. 加字段（与其它 final 字段并列）：
```java
    /** 表空间运行时注册表：建/开登记权威 metadata，空间管理 API 经 require 做状态准入。默认内建（page-0 loader 支撑 reopen 懒加载）。 */
    private final TablespaceRegistry registry;
```

3c. 把现有 3 参构造器整体替换为「3 参委托 + 4 参（注入 registry）+ 默认 registry 工厂」：
```java
    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize) {
        this(pool, pageStore, pageSize, defaultRegistry(pageStore, pageSize));
    }

    /**
     * 注入 registry 的构造器（测试/上层装配用）。registry 默认由 3 参构造器内建为 {@link CachingTablespaceRegistry}
     * + {@link PageZeroTablespaceMetadataLoader}（reopen/直接 store.open 后 require 懒加载从 page0 重建 metadata）。
     */
    public DiskSpaceManager(BufferPool pool, PageStore pageStore, PageSize pageSize, TablespaceRegistry registry) {
        if (pool == null || pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("DiskSpaceManager dependencies must not be null");
        }
        if (registry == null) {
            throw new DatabaseValidationException("tablespace registry must not be null");
        }
        this.pool = pool;
        this.pageStore = pageStore;
        this.pageSize = pageSize;
        this.registry = registry;
        this.headerRepo = new SpaceHeaderRepository(pool);
        this.xdes = new ExtentDescriptorRepository(pool, pageSize);
        this.inodeRepo = new SegmentInodeRepository(pool, pageSize);
        this.flst = new Flst(pool);
        this.freeExtents = new FreeExtentService(pool, pageSize, headerRepo, xdes, flst);
        this.segSpace = new SegmentSpaceService(pool, pageSize, headerRepo, inodeRepo, xdes, flst, freeExtents);
        this.allocator = new SegmentPageAllocator(pool, inodeRepo, flst, segSpace, new DefaultExtentAllocationPolicy());
    }

    private static TablespaceRegistry defaultRegistry(PageStore pageStore, PageSize pageSize) {
        if (pageStore == null || pageSize == null) {
            throw new DatabaseValidationException("DiskSpaceManager dependencies must not be null");
        }
        return new CachingTablespaceRegistry(new PageZeroTablespaceMetadataLoader(pageStore, pageSize));
    }
```

3d. 把现有 `createTablespace`（4 参）整体替换为「4 参委托 + 5 参实现 + metadata 助手」：
```java
    /** 建表空间（默认 type=GENERAL）。 */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages) {
        createTablespace(mtr, spaceId, path, initialSizePages, TablespaceType.GENERAL);
    }

    /**
     * 建表空间并注册权威 metadata（state=NORMAL）。物理建文件 → 写 page0 header（type 编入 spaceFlags 低 3 位）→
     * 保留系统 extent → {@code registry.replace}（此时 page0 未刷盘，直接登记内存权威，不走 page-0 loader）。
     */
    public void createTablespace(MiniTransaction mtr, SpaceId spaceId, Path path, PageNo initialSizePages,
                                 TablespaceType type) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        if (initialSizePages == null) {
            throw new DatabaseValidationException("initial size must not be null");
        }
        if (type == null) {
            throw new DatabaseValidationException("tablespace type must not be null");
        }
        pageStore.create(spaceId, path, pageSize, initialSizePages);
        SpaceHeaderSnapshot fresh = new SpaceHeaderSnapshot(spaceId, pageSize, TablespaceTypeFlags.encode(type),
                initialSizePages, PageNo.of(0), 1L,
                FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, SERVER_VERSION, 1L);
        headerRepo.initialize(mtr, fresh);
        xdes.reserveSystemExtent(mtr, spaceId);
        registry.replace(tablespaceMetadata(spaceId, path, type, initialSizePages));
    }

    /** 用建表参数构造权威 metadata 快照（state=NORMAL，type 同时入 spaceFlags 与 type 字段）。 */
    private TablespaceMetadata tablespaceMetadata(SpaceId spaceId, Path path, TablespaceType type, PageNo currentSize) {
        return new TablespaceMetadata(spaceId, "space-" + spaceId.value(), type, pageSize, TablespaceState.NORMAL,
                List.of(DataFileDescriptor.single(path, PageNo.of(0), currentSize)),
                new SpaceFlags(TablespaceTypeFlags.encode(type)), currentSize, PageNo.of(0), 1L);
    }
```

3e. 把现有 `openTablespace` 整体替换（加 registry.open + 半开清理），并新增 `openTablespaceForRecovery` 与 `tablespaceState`：
```java
    /** 打开已存在表空间：物理打开 → {@code registry.open}（page-0 loader 重建 metadata）。注册失败 close 防半开。 */
    public void openTablespace(SpaceId spaceId, Path path) {
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        pageStore.open(spaceId, path, pageSize);
        try {
            registry.open(spaceId);
        } catch (RuntimeException e) {
            pageStore.close(spaceId);
            throw e;
        }
    }

    /** recovery startup 打开：物理打开 → {@code registry.requireForRecovery}（不过状态门，允许 CORRUPTED）。失败 close。 */
    public void openTablespaceForRecovery(SpaceId spaceId, Path path) {
        requireSpace(spaceId);
        if (path == null) {
            throw new DatabaseValidationException("path must not be null");
        }
        pageStore.open(spaceId, path, pageSize);
        try {
            registry.requireForRecovery(spaceId);
        } catch (RuntimeException e) {
            pageStore.close(spaceId);
            throw e;
        }
    }

    /** 查询表空间当前运行时状态（诊断/测试）；未注册抛 {@link TablespaceNotFoundException}。 */
    public TablespaceState tablespaceState(SpaceId spaceId) {
        requireSpace(spaceId);
        return registry.find(spaceId)
                .map(handle -> handle.tablespace().state())
                .orElseThrow(() -> new TablespaceNotFoundException("tablespace not registered: " + spaceId.value()));
    }
```

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTablespaceAdmissionTest" --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTest" --console=plain`
Expected: PASS（注册/typed/recovery 重开；既有 `DiskSpaceManagerTest` 因 4 参重载零改动仍绿）。

---

## Task 8: 空间管理 API 准入门 + 生命周期标记

**Files:**
- Modify: `src/main/java/cn/zhangyis/db/storage/api/DiskSpaceManager.java`
- Test: `src/test/java/cn/zhangyis/db/storage/api/DiskSpaceManagerTablespaceAdmissionTest.java`（追加准入用例）

- [ ] **Step 1: 在 DiskSpaceManagerTablespaceAdmissionTest 追加失败测试（准入拦截 + reopen 懒加载）**

在 `DiskSpaceManagerTablespaceAdmissionTest` 追加（import 增 `cn.zhangyis.db.storage.fil.TablespaceCorruptedException`、`cn.zhangyis.db.storage.fil.TablespaceUnavailableException`、`cn.zhangyis.db.storage.fil.TablespaceNotFoundException`、`cn.zhangyis.db.storage.api.SegmentRef`）：

```java
    @Test void markCorruptedBlocksCreateSegment() {
        onPool((mgr, disk, store) -> {
            SpaceId s = SpaceId.of(60);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, s, dir.resolve("c.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);
            disk.markTablespaceCorrupted(s, "checksum mismatch");
            assertEquals(TablespaceState.CORRUPTED, disk.tablespaceState(s));
            MiniTransaction m = mgr.begin();
            assertThrows(TablespaceCorruptedException.class,
                    () -> disk.createSegment(m, s, SegmentPurpose.INDEX_LEAF));
            mgr.rollbackUncommitted(m);
        });
    }

    @Test void markInactiveBlocksAllocatePage() {
        onPool((mgr, disk, store) -> {
            SpaceId s = SpaceId.of(61);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, s, dir.resolve("i.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            SegmentRef seg = disk.createSegment(boot, s, SegmentPurpose.INDEX_LEAF);
            mgr.commit(boot);
            disk.markTablespaceInactive(s);
            MiniTransaction m = mgr.begin();
            assertThrows(TablespaceUnavailableException.class, () -> disk.allocatePage(m, seg));
            mgr.rollbackUncommitted(m);
        });
    }

    @Test void discardBlocksUsage() {
        onPool((mgr, disk, store) -> {
            SpaceId s = SpaceId.of(62);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, s, dir.resolve("d.ibu"), PageNo.of(64), TablespaceType.GENERAL);
            mgr.commit(boot);
            disk.discardTablespace(s);
            MiniTransaction m = mgr.begin();
            assertThrows(TablespaceNotFoundException.class, () -> disk.usage(m, s));
            mgr.rollbackUncommitted(m);
        });
    }

    @Test void reopenViaDirectStoreOpenLazyLoadsViaLoader() {
        // 模拟 undo/T1.3a reopen：直接 store.open（绕过 disk.openTablespace），首个空间管理 API 经 require 懒加载 page0
        Path path = dir.resolve("re.ibu");
        SpaceId s = SpaceId.of(63);
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction boot = mgr.begin();
            disk.createTablespace(boot, s, path, PageNo.of(64), TablespaceType.UNDO);
            mgr.commit(boot);
        } // 刷盘
        try (PageStore store = new FileChannelPageStore(); BufferPool pool = new LruBufferPool(store, PS, 128)) {
            store.open(s, path, PS); // 直接 store.open，registry 未登记
            MiniTransactionManager mgr = new MiniTransactionManager();
            DiskSpaceManager disk = new DiskSpaceManager(pool, store, PS);
            MiniTransaction m = mgr.begin();
            SegmentRef seg = disk.createSegment(m, s, SegmentPurpose.UNDO); // require 懒加载 loader → NORMAL → 通过
            disk.allocatePage(m, seg);
            mgr.commit(m);
            assertEquals(TablespaceState.NORMAL, disk.tablespaceState(s));
        }
    }
```

- [ ] **Step 2: 跑红**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTablespaceAdmissionTest" --console=plain`
Expected: FAIL（`markTablespaceCorrupted`/`markTablespaceInactive`/`discardTablespace` 不存在；且未加门时 createSegment/allocatePage/usage 不抛）。

- [ ] **Step 3: 给 5 个空间管理 API 加准入门 + 加 3 个生命周期标记**

3a. 在 `createSegment` 的 `purpose` 校验后加一行（其余不变）：
```java
        registry.require(spaceId); // 空间管理准入门：非 NORMAL/ACTIVE 抛 fil 状态异常
```

3b. 在 `allocatePage` 的 `requireRef(ref)` 后加：
```java
        registry.require(ref.spaceId());
```

3c. 在 `freePage` 的 `requireRef(ref)` 后加：
```java
        registry.require(ref.spaceId());
```

3d. 在 `dropSegment` 的 `requireRef(ref)` 后加：
```java
        registry.require(ref.spaceId());
```

3e. 在 `usage` 的 `requireSpace(spaceId)` 后加：
```java
        registry.require(spaceId);
```

3f. 新增 3 个运行时生命周期标记方法（放在 `closeTablespace` 附近）：
```java
    /** 标记表空间 INACTIVE（运行时）：后续空间管理 API require 抛 {@link TablespaceUnavailableException}。 */
    public void markTablespaceInactive(SpaceId spaceId) {
        requireSpace(spaceId);
        registry.markInactive(spaceId);
    }

    /** 标记表空间 CORRUPTED（运行时）：后续空间管理 API require 抛 {@link TablespaceCorruptedException}。 */
    public void markTablespaceCorrupted(SpaceId spaceId, String reason) {
        requireSpace(spaceId);
        registry.markCorrupted(spaceId, reason);
    }

    /** 标记表空间 DISCARDED（运行时，仅转 registry 状态，不关文件）：后续 require 抛 {@link TablespaceNotFoundException}。 */
    public void discardTablespace(SpaceId spaceId) {
        requireSpace(spaceId);
        registry.markDiscarded(spaceId);
    }
```
（import 增 `cn.zhangyis.db.storage.fil.TablespaceCorruptedException`、`cn.zhangyis.db.storage.fil.TablespaceUnavailableException`——若仅用于 Javadoc 的 `{@link}` 也需 import。）

- [ ] **Step 4: 跑绿**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTablespaceAdmissionTest" --tests "cn.zhangyis.db.storage.api.DiskSpaceManagerTest" --console=plain`
Expected: PASS（准入拦截 corrupted/inactive/discarded、reopen 懒加载、既有 DiskSpaceManagerTest 仍绿——所有空间都先 createTablespace→NORMAL，门透明通过）。

---

## Task 9: 全量回归 + current-implementation-map + 收口

**Files:**
- Modify: `docs/design/current-implementation-map.md`

- [ ] **Step 1: 全量回归**

Run: `$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain`
Expected: BUILD SUCCESSFUL，failures=0 errors=0；测试数较基线（468）只增不减（新增 `TablespaceTypeTest`、`TablespaceTypeFlagsTest`、`SpaceHeaderRawCodecTest`、`PageZeroTablespaceMetadataLoaderTest`、`DiskSpaceManagerTablespaceAdmissionTest` 与 `CachingTablespaceRegistryTest`/`FileChannelPageStoreTest` 增量）。**关键回归**：record R1-R5 / B+Tree / redo / flush / undo（T1.3a/b）全绿——所有既有调用先 `createTablespace`→NORMAL，准入门透明通过；undo/T1.3a 的直接 `store.open` reopen 路径经 `require` 懒加载 + page-0 loader 通过；`new DiskSpaceManager(pool,store,PS)` 与 4 参 `createTablespace` 因重载零改动。

> 用实际测试数核对：`Get-ChildItem build/test-results/test/*.xml` 聚合 `tests`/`failures`/`errors`（见 T1.3a/b 收口）。若某 `DiskSpaceManagerTest` 用例在未 `createTablespace` 的空间上调 createSegment/allocatePage 而新门拦截，给该用例补 `createTablespace`（属本片准入行为，非倒退）并记录。

- [ ] **Step 2: 更新 current-implementation-map.md（CLAUDE.md 要求）**

读 `docs/design/current-implementation-map.md`，按其文件内规则更新受影响小节：
- 把 `TablespaceRegistry`/`CachingTablespaceRegistry`/`Tablespace*`/`TablespaceMetadataLoader` 从 `Reserved / Unwired Production Types` 表移出（或标为 wired），因为本片已由 `DiskSpaceManager` 实线接线（createTablespace→`registry.replace`、空间管理 API→`registry.require`、open→`registry.open`、recovery→`requireForRecovery`、page-0 `PageZeroTablespaceMetadataLoader`）。
- 在 storage.api→fil 链路小节补实线：`DiskSpaceManager → TablespaceRegistry`、`DiskSpaceManager → PageZeroTablespaceMetadataLoader → PageStore.pathOf/readPage`。
- 标注仍 `planned`/`partial` 的部分：per-page 数据 IO 准入（IndexPageAccess/UndoPageAccess 不经门）、state 持久化、page-0 FSP_HDR 信封、undo 空间 type=UNDO（默认 GENERAL）。
- 按文件内 10 项检查清单复核，确保无误导性实线/占位式表达。

- [ ] **Step 3: 收口**

项目 no-commit：不执行 `git commit`。在 `MEMORY.md` / 适当 memory 文件追加「TablespaceRegistry 接入 DiskSpaceManager + 空间管理准入完成」要点（实际测试数、新增/改动清单、关键简化点：空间管理准入非全 IO 门 / state 运行时不持久化 / type 入 spaceFlags / loader raw 读 page0 / size 非实时；下一步候选：operation-level lease 全 IO 门、state 持久化、page-0 信封、undo 空间 type=UNDO）。

---

## 自检（写计划后对照 spec）

1. **spec 覆盖**：`TablespaceType.code/fromCode`(Task1)、`TablespaceTypeFlags`(Task2)、`markInactive`(Task3)、`PageStore.pathOf`(Task4)、`SpaceHeaderRawCodec`+`SpaceHeaderPhysical`(Task5)、`PageZeroTablespaceMetadataLoader`(Task6)、DiskSpaceManager 注册/recovery/state(Task7)、准入门+生命周期标记(Task8)、回归+map+收口(Task9)。spec §1-§9 各节均有任务；含 (a) type 入 spaceFlags、(b) loader raw 读 + pathOf + replace-on-create + size 非实时、(c) 空间管理准入非全 IO 门、(d) 向后兼容重载 + corrupted/inactive/discarded 测试、小坑1 SpaceHeaderRawCodec、小坑2 page0 信封以 spaceId sanity 替代。
2. **placeholder 扫描**：已移除 Task6 对 Task7 的占位依赖（改 SpaceHeaderRepository 自建 page0）；无 TODO/TBD；Task9 Step2 map 更新为按文件自有规则的过程性步骤（doc 内容由其规则治理）。
3. **类型/签名一致**：`TablespaceType.code()/fromCode(int)`；`TablespaceTypeFlags.encode(TablespaceType)→int`/`decode(int)→TablespaceType`；`TablespaceRegistry.markInactive(SpaceId)→TablespaceHandle`；`PageStore.pathOf(SpaceId)→Path`、`DataFileHandle.path()→Path`；`SpaceHeaderRawCodec.readPhysical(ByteBuffer)→SpaceHeaderPhysical`；`PageZeroTablespaceMetadataLoader(PageStore,PageSize)`+`load(SpaceId)→Optional<TablespaceMetadata>`；`DiskSpaceManager(pool,store,ps)`/`(pool,store,ps,registry)`、`createTablespace(...,4)`/`(...,5+type)`、`openTablespace`/`openTablespaceForRecovery`/`tablespaceState`/`markTablespaceInactive`/`markTablespaceCorrupted(reason)`/`discardTablespace` 跨任务一致。`SpaceHeaderSnapshot` 13 参顺序与既有 createTablespace 一致（仅 spaceFlags 0→encode(type)）。
4. **依赖方向**：fsp（SpaceHeaderRawCodec/SpaceHeaderPhysical）保持 fil-无关（只存/解 int flags）；loader 在 api 依赖 fil+fsp；DiskSpaceManager(api)→fil registry/metadata；TablespaceTypeFlags 在 fil。
5. **风险点**：DiskSpaceManager 高扇出由向后兼容重载（3 参/4 参）守护、Task7/8 跑 `DiskSpaceManagerTest` + Task9 全量回归确认；reopen 直接 store.open 路径由 `reopenViaDirectStoreOpenLazyLoadsViaLoader` + loader（pathOf + raw readPage 刷盘后 page0）钉死；准入透明性由「既有空间皆 createTablespace→NORMAL」保证；type 落盘往返由 `TablespaceTypeFlagsTest` + loader 测试 + reopen 测试钉死。
6. **测试可落地**：page0 刷盘依赖 pool.close（与 T1.3a/b reopen 测试同款，已验证）；SpaceHeaderRawCodec/loader 测试用 `SpaceHeaderRepository` 自建 page0，不循环依赖 DiskSpaceManager；准入测试用独立 onPool harness。

