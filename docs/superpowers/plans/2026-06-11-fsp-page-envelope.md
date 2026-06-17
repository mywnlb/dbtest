# 物理页 envelope（§5.3）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现物理页统一 envelope：`PageType` + `FilePageHeader`/`FilePageTrailer`（占满已预留的 `FIL_PAGE_DATA=38` 字节 + 页尾 8 字节）+ `PageChecksum`（CRC32 compute/stamp/verify）。

**Architecture:** 全部在 `cn.zhangyis.db.storage.fsp`，经 `PageGuard` 绝对读写。checksum 不接入 PageStore IO、不回填现有 fsp 页；pageLsn 恒 0（redo 暂停）。记录层 IndexPage 将是第一个消费者。

**Tech Stack:** Java 25、JUnit Jupiter（buf+fil+@TempDir）、`java.util.zip.CRC32`。

**Spec:** `docs/superpowers/specs/2026-06-11-fsp-page-envelope-design.md`

**通用约定：** `cn.zhangyis.db.storage.fsp`；中文 Javadoc；禁 synchronized；禁裸异常；**不提交**；no-redo；单类测试 `... --tests "<FQCN>"`，全量 `clean test`。

---

## Task 1: PageType

**Files:** Create `src/main/java/cn/zhangyis/db/storage/fsp/PageType.java`；Test `src/test/java/cn/zhangyis/db/storage/fsp/PageTypeTest.java`

- [ ] **Step 1: 写失败测试**

```java
package cn.zhangyis.db.storage.fsp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** PageType code 钉死（落盘依赖）+ fromCode 往返 + 未知 code 拒绝。 */
class PageTypeTest {

    @Test
    void codesAreStable() {
        assertEquals(0, PageType.ALLOCATED.code());
        assertEquals(1, PageType.FSP_HDR.code());
        assertEquals(2, PageType.IBUF_BITMAP.code());
        assertEquals(3, PageType.INODE.code());
        assertEquals(4, PageType.SDI.code());
        assertEquals(5, PageType.INDEX.code());
    }

    @Test
    void fromCodeRoundTrips() {
        for (PageType t : PageType.values()) {
            assertEquals(t, PageType.fromCode(t.code()));
        }
    }

    @Test
    void fromCodeRejectsUnknown() {
        assertThrows(FspMetadataException.class, () -> PageType.fromCode(99));
    }
}
```

- [ ] **Step 2: 运行确认失败**（编译失败）。

- [ ] **Step 3: 写 PageType**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * 物理页类型（设计 §5.3 FilePageHeader.pageType）。code 落盘（4 字节），取值不可改（PageTypeTest 钉死）。
 * ALLOCATED=0 使零初始化页天然解码为"未用"。
 */
public enum PageType {
    /** 已分配但未用（空闲数据页）；零初始化页解码为它。 */
    ALLOCATED(0),
    /** page 0：表空间头 + 首批 XDES。 */
    FSP_HDR(1),
    /** page 1：change buffer bitmap（保留）。 */
    IBUF_BITMAP(2),
    /** page 2：segment inode array。 */
    INODE(3),
    /** page 3：序列化数据字典（保留）。 */
    SDI(4),
    /** B+Tree 索引页。 */
    INDEX(5);

    private final int code;

    PageType(int code) {
        this.code = code;
    }

    /** 落盘 code。 */
    public int code() {
        return code;
    }

    /** 由落盘 code 还原；未知 code 视为页上类型损坏。 */
    public static PageType fromCode(int code) {
        for (PageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new FspMetadataException("unknown page type code on disk: " + code);
    }
}
```

- [ ] **Step 4: 运行确认通过**，不提交。

---

## Task 2: FilePageHeader + Trailer Layout + FilePageHeader

**Files:** Create `FilePageHeaderLayout.java`、`FilePageTrailerLayout.java`、`FilePageHeader.java`；Test `FilePageHeaderTest.java`（均在 fsp 包对应目录）

- [ ] **Step 1: 写失败测试 `FilePageHeaderTest.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** FilePageHeader 经真实 PageGuard 编解码往返（含邻居与 FIL_NULL）+ 构造校验。 */
class FilePageHeaderTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void roundTripWithNeighbors() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FilePageHeader h = new FilePageHeader(SPACE, 3, 2, 7, 0L, PageType.INDEX);
                h.writeTo(g);
                assertEquals(h, FilePageHeader.readFrom(g));
            }
        }
    }

    @Test
    void roundTripWithFilNullNeighbors() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                FilePageHeader h = new FilePageHeader(SPACE, 3,
                        FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.FSP_HDR);
                h.writeTo(g);
                FilePageHeader got = FilePageHeader.readFrom(g);
                assertEquals(h, got);
                assertEquals(FilePageHeader.FIL_NULL, got.prevPageNo());
                assertEquals(FilePageHeader.FIL_NULL, got.nextPageNo());
            }
        }
    }

    @Test
    void constructorRejectsNulls() {
        assertThrows(DatabaseValidationException.class, () -> new FilePageHeader(
                null, 1, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX));
        assertThrows(DatabaseValidationException.class, () -> new FilePageHeader(
                SPACE, 1, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, null));
    }
}
```

- [ ] **Step 2: 运行确认失败**（编译失败）。

- [ ] **Step 3: 写 `FilePageHeaderLayout.java`**

```java
package cn.zhangyis.db.storage.fsp;

/**
 * FilePageHeader 偏移（页首，全部在 0..FIL_PAGE_DATA-1）。设计 §5.3。
 */
final class FilePageHeaderLayout {

    private FilePageHeaderLayout() {
    }

    static final int CHECKSUM = 0;                     // int 4
    static final int SPACE_ID = CHECKSUM + 4;          // 4 int
    static final int PAGE_NO = SPACE_ID + 4;           // 8 int
    static final int PREV_PAGE_NO = PAGE_NO + 4;       // 12 int
    static final int NEXT_PAGE_NO = PREV_PAGE_NO + 4;  // 16 int
    static final int PAGE_LSN = NEXT_PAGE_NO + 4;      // 20 long
    static final int PAGE_TYPE = PAGE_LSN + 8;         // 28 int (ends 32)
    static final int SIZE = PageLayouts.FIL_PAGE_DATA; // 38（32..37 预留）
}
```

- [ ] **Step 4: 写 `FilePageTrailerLayout.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageSize;

/**
 * FilePageTrailer 偏移（页尾 8 字节）。设计 §5.3。
 */
final class FilePageTrailerLayout {

    private FilePageTrailerLayout() {
    }

    static final int CHECKSUM_TRAILER = 0; // int 4
    static final int LOW32_LSN = 4;        // int 4
    static final int SIZE = 8;

    /** trailer 在页内的起始偏移。 */
    static int offset(PageSize pageSize) {
        return pageSize.bytes() - SIZE;
    }
}
```

- [ ] **Step 5: 写 `FilePageHeader.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * 物理页统一头（设计 §5.3）。checksum 不入 record（派生值，由 {@link PageChecksum} 盖）。prev/next 无邻居用 FIL_NULL。
 * pageLsn 现阶段恒 0（redo 暂停）。
 *
 * @param spaceId    表空间。
 * @param pageNo     本页页号。
 * @param prevPageNo 前驱页号（leaf 兄弟链）；无则 FIL_NULL。
 * @param nextPageNo 后继页号；无则 FIL_NULL。
 * @param pageLsn    页 LSN（redo 接入前恒 0）。
 * @param pageType   页类型。
 */
public record FilePageHeader(SpaceId spaceId, long pageNo, long prevPageNo, long nextPageNo,
                             long pageLsn, PageType pageType) {

    /** 无邻居哨兵（InnoDB FIL_NULL = 0xFFFFFFFF）。 */
    public static final long FIL_NULL = 0xFFFFFFFFL;

    public FilePageHeader {
        if (spaceId == null || pageType == null) {
            throw new DatabaseValidationException("file page header spaceId/pageType must not be null");
        }
        if (pageNo < 0) {
            throw new DatabaseValidationException("page no must be non-negative: " + pageNo);
        }
        if (prevPageNo < 0 || nextPageNo < 0) {
            throw new DatabaseValidationException("prev/next page no must be non-negative or FIL_NULL");
        }
        if (pageLsn < 0) {
            throw new DatabaseValidationException("page lsn must be non-negative: " + pageLsn);
        }
    }

    /** 写头部字段（要求 X）；不写 checksum（由 PageChecksum.stamp 盖）。 */
    public void writeTo(PageGuard guard) {
        guard.writeInt(FilePageHeaderLayout.SPACE_ID, spaceId.value());
        guard.writeInt(FilePageHeaderLayout.PAGE_NO, (int) pageNo);
        guard.writeInt(FilePageHeaderLayout.PREV_PAGE_NO, (int) prevPageNo);
        guard.writeInt(FilePageHeaderLayout.NEXT_PAGE_NO, (int) nextPageNo);
        guard.writeLong(FilePageHeaderLayout.PAGE_LSN, pageLsn);
        guard.writeInt(FilePageHeaderLayout.PAGE_TYPE, pageType.code());
    }

    /** 读头部字段。prev/next 用 &0xFFFFFFFFL 还原（-1→FIL_NULL）。 */
    public static FilePageHeader readFrom(PageGuard guard) {
        return new FilePageHeader(
                SpaceId.of(guard.readInt(FilePageHeaderLayout.SPACE_ID)),
                guard.readInt(FilePageHeaderLayout.PAGE_NO) & 0xFFFFFFFFL,
                guard.readInt(FilePageHeaderLayout.PREV_PAGE_NO) & 0xFFFFFFFFL,
                guard.readInt(FilePageHeaderLayout.NEXT_PAGE_NO) & 0xFFFFFFFFL,
                guard.readLong(FilePageHeaderLayout.PAGE_LSN),
                PageType.fromCode(guard.readInt(FilePageHeaderLayout.PAGE_TYPE)));
    }
}
```

- [ ] **Step 6: 运行确认通过**，不提交。

---

## Task 3: PageChecksum

**Files:** Create `PageChecksum.java`；Test `PageChecksumTest.java`

- [ ] **Step 1: 写失败测试 `PageChecksumTest.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.LruBufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** PageChecksum stamp→verify、篡改失败、低 32 位 LSN、4K/16K 页尾偏移。 */
class PageChecksumTest {

    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private void withGuard(PageSize ps, BiConsumer<PageGuard, PageSize> body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s-" + ps.bytes() + ".ibd"), ps, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, ps, 4)) {
            try (PageGuard g = pool.getPage(PageId.of(SPACE, PageNo.of(3)), PageLatchMode.EXCLUSIVE)) {
                body.accept(g, ps);
            }
        }
    }

    @Test
    void stampThenVerifyPassesAndWritesLow32Lsn() {
        withGuard(PageSize.ofBytes(16 * 1024), (g, ps) -> {
            new FilePageHeader(SPACE, 3, FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX)
                    .writeTo(g);
            g.writeInt(100, 0xCAFEBABE);
            PageChecksum.stamp(g, ps);
            assertTrue(PageChecksum.verify(g, ps));
            int trailerOffset = ps.bytes() - FilePageTrailerLayout.SIZE;
            assertEquals(0, g.readInt(trailerOffset + FilePageTrailerLayout.LOW32_LSN));
        });
    }

    @Test
    void tamperFailsVerify() {
        withGuard(PageSize.ofBytes(16 * 1024), (g, ps) -> {
            PageChecksum.stamp(g, ps);
            assertTrue(PageChecksum.verify(g, ps));
            g.writeInt(200, 0x12345678);
            assertFalse(PageChecksum.verify(g, ps));
        });
    }

    @Test
    void worksAcross4kAnd16k() {
        for (int kb : new int[] {4, 16}) {
            withGuard(PageSize.ofBytes(kb * 1024), (g, ps) -> {
                PageChecksum.stamp(g, ps);
                assertTrue(PageChecksum.verify(g, ps));
            });
        }
    }
}
```

- [ ] **Step 2: 运行确认失败**（编译失败）。

- [ ] **Step 3: 写 `PageChecksum.java`**

```java
package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;

import java.util.zip.CRC32;

/**
 * 页校验和（设计 §5.3/§14，CRC32 简化实现）。计算范围 [4, pageSize-8)，排除 4 字节头 checksum 与 8 字节 trailer。
 * 简化点：单一 CRC32、不接入 PageStore IO、未抽 ChecksumStrategy 接口（待 flush 切片）。
 */
public final class PageChecksum {

    private PageChecksum() {
    }

    /** 计算页体 [4, pageSize-8) 的 CRC32（截断为 int）。 */
    public static int compute(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        byte[] body = guard.readBytes(4, pageSize.bytes() - 4 - FilePageTrailerLayout.SIZE);
        CRC32 crc = new CRC32();
        crc.update(body);
        return (int) crc.getValue();
    }

    /** 封页（要求 X）：算 checksum 并写 header.checksum、trailer.checksumTrailer，同步 trailer.low32Lsn = pageLsn 低 32 位。 */
    public static void stamp(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        long pageLsn = guard.readLong(FilePageHeaderLayout.PAGE_LSN);
        int checksum = compute(guard, pageSize);
        int trailerOffset = FilePageTrailerLayout.offset(pageSize);
        guard.writeInt(FilePageHeaderLayout.CHECKSUM, checksum);
        guard.writeInt(trailerOffset + FilePageTrailerLayout.CHECKSUM_TRAILER, checksum);
        guard.writeInt(trailerOffset + FilePageTrailerLayout.LOW32_LSN, (int) pageLsn);
    }

    /** 校验：重算 checksum，与 header.checksum 且 trailer.checksumTrailer 同时相等才返回 true。 */
    public static boolean verify(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        int checksum = compute(guard, pageSize);
        int header = guard.readInt(FilePageHeaderLayout.CHECKSUM);
        int trailerOffset = FilePageTrailerLayout.offset(pageSize);
        int trailer = guard.readInt(trailerOffset + FilePageTrailerLayout.CHECKSUM_TRAILER);
        return checksum == header && checksum == trailer;
    }

    private static void requireArgs(PageGuard guard, PageSize pageSize) {
        if (guard == null) {
            throw new DatabaseValidationException("page guard must not be null");
        }
        if (pageSize == null) {
            throw new DatabaseValidationException("page size must not be null");
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**，不提交。

---

## Task 4: 全量回归 + GitNexus

- [ ] **Step 1: `clean test`**，期望 BUILD SUCCESSFUL。
- [ ] **Step 2: `npx gitnexus analyze`**；失败记录并重试。
- [ ] **Step 3: 不提交。**

---

## Self-Review（已执行）

**1. Spec 覆盖：** §3.1 PageType→Task1；§3.2/3.3 Layout + §3.4 FilePageHeader→Task2；§3.5 PageChecksum→Task3；§6 测试→各 Task；Task4 回归+索引。
**2. Placeholder：** 无。
**3. 类型一致性：** `PageType.{code,fromCode}`；`FilePageHeaderLayout.{CHECKSUM,SPACE_ID,PAGE_NO,PREV_PAGE_NO,NEXT_PAGE_NO,PAGE_LSN,PAGE_TYPE,SIZE}`；`FilePageTrailerLayout.{CHECKSUM_TRAILER,LOW32_LSN,SIZE,offset}`；`FilePageHeader.{FIL_NULL,writeTo,readFrom,prevPageNo,nextPageNo}`；`PageChecksum.{compute,stamp,verify}`；复用 `PageGuard.{readInt,writeInt,readLong,writeLong,readBytes}`、`PageLayouts.FIL_PAGE_DATA`、`SpaceId.{of,value}`。
**4. 边界：** compute 范围 [4, pageSize-8)，长度 pageSize-12；trailer 偏移 pageSize-8；FIL_NULL 经 (int)截断/&0xFFFFFFFFL 还原；4K/16K 测试用不同文件名避免 create 冲突。
