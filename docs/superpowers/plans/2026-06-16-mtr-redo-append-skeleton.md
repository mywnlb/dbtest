# D3 — MTR Redo Append Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让页写入产生物理字节区间 redo（`PAGE_BYTES`/`PAGE_INIT`）并在 MTR commit 盖 pageLSN，建立 redo 最小闭环（不持久化、不回放）。

**Architecture:** 新增共享包 `storage.page`（页信封毕业，纯层 + PageGuard 访问器层）；`redo` 包做内存 append；`buf` 加一个默认 no-op 的 `PageWriteListener` 钩子（避免 buf 反向依赖 mtr/redo）；retrofit `MiniTransaction`：collector 捕获写、`newPage(type)` 产 PAGE_INIT、commit append + 盖 pageLSN。分层 `domain<buf<storage.page<redo<mtr<fsp/record`，无环。

**Tech Stack:** Java 25、JUnit Jupiter、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**关联 spec：** `docs/superpowers/specs/2026-06-16-mtr-redo-append-skeleton-design.md`

**项目规则（覆盖默认）：**
- **不提交 git**；用「全量 `clean test` 绿」作为每个 Task 的 checkpoint，**无 commit 步骤**。
- 禁 `synchronized`/`wait`/`notify`（用 `java.util.concurrent`）；禁裸 `IllegalArgumentException`/`RuntimeException`（用 `DatabaseValidationException`/`MtrStateException` 等）。
- 中文 Javadoc 解释语义/并发/简化点。
- **每批编辑前对「编辑目标」跑 `gitnexus_impact`，HIGH/CRITICAL 先告警**（CLAUDE.md 强制）；批末跑 `gitnexus_detect_changes`。

**Checkpoint 命令：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain
```
（D3a 信封迁移波及 fsp/record，须跑全量；D3b–D3d 可先跑相关包再收口全量。）

---

## File Structure

**新建包 `cn.zhangyis.db.storage.page`（信封毕业）：**
- `PageType.java`（从 fsp 迁入；`fromCode` 改抛 `DatabaseValidationException`）— 纯层。
- `PageEnvelopeLayout.java`（合并原 `FilePageHeaderLayout`+`FilePageTrailerLayout`+`PageLayouts` 常量）— 纯层。
- `FilePageHeader.java`（从 fsp 迁入的值 record；**移除 PageGuard 版 writeTo/readFrom**）— 纯层。
- `PageEnvelope.java`（新访问器：`writeHeader/readHeader/stampPageLsn/readPageLsn`）— 访问器层。
- `PageChecksum.java`（从 fsp 迁入；引用 PageEnvelopeLayout）— 访问器层。

**新建包 `cn.zhangyis.db.storage.redo`：**
- `LogRange.java`、`RedoRecord.java`（sealed）、`PageBytesRecord.java`、`PageInitRecord.java`、`RedoLogManager.java`。
- （`domain.Lsn` 已存在，复用。）

**修改：**
- `buf/PageWriteListener.java`（新建，接口）、`buf/PageGuard.java`（加 listener 字段 + attach + 写回调）。
- `mtr/MtrRedoCollector.java`（新建）、`mtr/MiniTransaction.java`、`mtr/MiniTransactionManager.java`、`mtr/MtrMemo.java`。
- `fsp/SpaceHeaderLayout.java`、`fsp/SegmentInodeLayout.java`（`PageLayouts.FIL_PAGE_DATA` → `PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES`）。
- `record/page/IndexPageLayout.java`（信封头/尾常量引用 PageEnvelopeLayout）。
- **删除** fsp：`PageType.java`、`FilePageHeader.java`、`FilePageHeaderLayout.java`、`FilePageTrailerLayout.java`、`PageChecksum.java`、`PageLayouts.java`。

**测试：**
- 迁移：`PageTypeTest`/`FilePageHeaderTest`/`PageChecksumTest` 从 `test/.../fsp` 移到 `test/.../storage/page`，改包名 + 把 `header.writeTo(g)`/`FilePageHeader.readFrom(g)` 改为 `PageEnvelope.writeHeader/readHeader`。
- 新建：`storage/page/PageEnvelopeTest`、`redo/LogRangeTest`、`redo/RedoRecordTest`、`redo/RedoLogManagerTest`、`mtr/MtrRedoAppendTest`。
- 修改：`mtr/MiniTransactionTest`（`new MiniTransaction(id)`→带 RedoLogManager；`newPage(...)`→带 PageType）。

---

## Task D3a：storage.page 信封毕业（迁移重构 + PageEnvelope）

> 这是一次原子重构：移动 6 个类、改少量引用、删旧文件，全量回归绿即成功。新增 `PageEnvelopeTest` 作为 stamp/read 的 TDD 工件。

**Impact（编辑前必跑，HIGH/CRITICAL 告警）：**
```
gitnexus_impact({target:"PageType", direction:"upstream", repo:"dbtest"})
gitnexus_impact({target:"FilePageHeader", direction:"upstream", repo:"dbtest"})
gitnexus_impact({target:"PageChecksum", direction:"upstream", repo:"dbtest"})
```
预期：仅 fsp 内 + 3 个 fsp envelope 测试 + record IndexPageLayout（注释）受影响；无跨 SQL/session。若超出预期范围，先报告。

- [ ] **Step 1: 建纯层 `storage.page/PageType.java`**

```java
package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 物理页类型（设计 §5.3 FilePageHeader.pageType）。code 落盘（4 字节），取值不可改（PageTypeTest 钉死）。
 * ALLOCATED=0 使零初始化页天然解码为"未用"。毕业到 storage.page：redo（PAGE_INIT）与 mtr（newPage 参数）需引用它，
 * 二者不能依赖 fsp，故下沉到此纯层（不 import PageGuard）。
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

    /** 由落盘 code 还原；未知 code 视为页上类型损坏（脱离 fsp 异常，用通用领域异常）。 */
    public static PageType fromCode(int code) {
        for (PageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new DatabaseValidationException("unknown page type code on disk: " + code);
    }
}
```

- [ ] **Step 2: 建纯层 `storage.page/PageEnvelopeLayout.java`**

```java
package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.domain.PageSize;

/**
 * 物理页信封布局（设计 §5.3）：页首 FilePageHeader + 页尾 FilePageTrailer 的偏移与尺寸。单一真相来源，
 * 取代原 fsp 的 FilePageHeaderLayout/FilePageTrailerLayout/PageLayouts，供 buf 之上各层共用（buf 不依赖本类）。
 */
public final class PageEnvelopeLayout {

    private PageEnvelopeLayout() {
    }

    // ---- 页首 FilePageHeader（[0, FIL_PAGE_HEADER_BYTES)）----
    /** CRC32 校验和（派生，PageChecksum 盖）。 */
    public static final int CHECKSUM = 0;          // int 4
    public static final int SPACE_ID = 4;          // int 4
    public static final int PAGE_NO = 8;           // int 4
    public static final int PREV_PAGE_NO = 12;     // int 4
    public static final int NEXT_PAGE_NO = 16;     // int 4
    /** 页 LSN（恢复幂等用的 header LSN）。 */
    public static final int PAGE_LSN = 20;         // long 8
    public static final int PAGE_TYPE = 28;        // int 4 (ends 32; 32..37 预留)
    /** 页首总字节数（= 旧 PageLayouts.FIL_PAGE_DATA）。 */
    public static final int FIL_PAGE_HEADER_BYTES = 38;

    // ---- 页尾 FilePageTrailer（[pageSize-8, pageSize)）----
    public static final int FIL_PAGE_TRAILER_BYTES = 8;
    public static final int TRAILER_CHECKSUM = 0;  // int 4
    public static final int TRAILER_LOW32_LSN = 4; // int 4

    /** trailer 在页内的起始偏移。 */
    public static int trailerOffset(PageSize pageSize) {
        return pageSize.bytes() - FIL_PAGE_TRAILER_BYTES;
    }
}
```

- [ ] **Step 3: 建纯层 `storage.page/FilePageHeader.java`（值 record，去掉 PageGuard I/O）**

```java
package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

/**
 * 物理页统一头（设计 §5.3，纯值对象，不 import PageGuard）。checksum 不入本对象（派生值由 {@link PageChecksum} 盖）；
 * PageGuard 读写下沉到访问器 {@link PageEnvelope}。prev/next 无邻居用 FIL_NULL。
 *
 * @param spaceId    表空间。
 * @param pageNo     本页页号。
 * @param prevPageNo 前驱页号（leaf 兄弟链）；无则 FIL_NULL。
 * @param nextPageNo 后继页号；无则 FIL_NULL。
 * @param pageLsn    页 LSN。
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
}
```

- [ ] **Step 4: 建访问器层 `storage.page/PageEnvelope.java`**

```java
package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * 页信封访问器（访问器层，import PageGuard）：在 PageGuard 上读写 FilePageHeader 字段，并提供 pageLSN 单字段盖戳/读取。
 *
 * <p>{@link #stampPageLsn} 只写 header PAGE_LSN（恢复幂等用），**不同步 trailer LSN、不重算 checksum**——
 * 那是 flush/checksum 切片（F1）的职责（见 {@link PageChecksum#stamp}）。
 */
public final class PageEnvelope {

    private PageEnvelope() {
    }

    /** 写信封头字段（要求 X）；不写 checksum（由 PageChecksum.stamp 盖）。 */
    public static void writeHeader(PageGuard guard, FilePageHeader h) {
        requireArgs(guard, h);
        guard.writeInt(PageEnvelopeLayout.SPACE_ID, h.spaceId().value());
        guard.writeInt(PageEnvelopeLayout.PAGE_NO, (int) h.pageNo());
        guard.writeInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) h.prevPageNo());
        guard.writeInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) h.nextPageNo());
        guard.writeLong(PageEnvelopeLayout.PAGE_LSN, h.pageLsn());
        guard.writeInt(PageEnvelopeLayout.PAGE_TYPE, h.pageType().code());
    }

    /** 读信封头字段。prev/next 用 &0xFFFFFFFFL 还原（-1→FIL_NULL）。 */
    public static FilePageHeader readHeader(PageGuard guard) {
        if (guard == null) {
            throw new DatabaseValidationException("page guard must not be null");
        }
        return new FilePageHeader(
                SpaceId.of(guard.readInt(PageEnvelopeLayout.SPACE_ID)),
                guard.readInt(PageEnvelopeLayout.PAGE_NO) & 0xFFFFFFFFL,
                guard.readInt(PageEnvelopeLayout.PREV_PAGE_NO) & 0xFFFFFFFFL,
                guard.readInt(PageEnvelopeLayout.NEXT_PAGE_NO) & 0xFFFFFFFFL,
                guard.readLong(PageEnvelopeLayout.PAGE_LSN),
                PageType.fromCode(guard.readInt(PageEnvelopeLayout.PAGE_TYPE)));
    }

    /** 仅盖 header pageLSN（要求 X）。MTR commit 用：分配 endLsn 后给 touched 页盖戳。 */
    public static void stampPageLsn(PageGuard guard, Lsn lsn) {
        if (guard == null || lsn == null) {
            throw new DatabaseValidationException("page guard / lsn must not be null");
        }
        guard.writeLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
    }

    /** 读 header pageLSN。 */
    public static Lsn readPageLsn(PageGuard guard) {
        if (guard == null) {
            throw new DatabaseValidationException("page guard must not be null");
        }
        return Lsn.of(guard.readLong(PageEnvelopeLayout.PAGE_LSN));
    }

    private static void requireArgs(PageGuard guard, FilePageHeader h) {
        if (guard == null || h == null) {
            throw new DatabaseValidationException("page guard / header must not be null");
        }
    }
}
```

- [ ] **Step 5: 建访问器层 `storage.page/PageChecksum.java`（迁移，引用 PageEnvelopeLayout）**

```java
package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;

import java.util.zip.CRC32;

/**
 * 页校验和（设计 §5.3/§14，CRC32 简化实现）。计算范围 [4, pageSize-8)，排除 4 字节头 checksum 与 8 字节 trailer。
 * 简化点：单一 CRC32、未抽 ChecksumStrategy（待 flush 切片）。
 */
public final class PageChecksum {

    private PageChecksum() {
    }

    /** 计算页体 [4, pageSize-8) 的 CRC32（截断为 int）。 */
    public static int compute(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        byte[] body = guard.readBytes(4, pageSize.bytes() - 4 - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES);
        CRC32 crc = new CRC32();
        crc.update(body);
        return (int) crc.getValue();
    }

    /** 封页（要求 X）：算 checksum 并写 header.checksum、trailer.checksumTrailer，同步 trailer.low32Lsn = pageLsn 低 32 位。 */
    public static void stamp(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        long pageLsn = guard.readLong(PageEnvelopeLayout.PAGE_LSN);
        int checksum = compute(guard, pageSize);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        guard.writeInt(PageEnvelopeLayout.CHECKSUM, checksum);
        guard.writeInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM, checksum);
        guard.writeInt(trailerOffset + PageEnvelopeLayout.TRAILER_LOW32_LSN, (int) pageLsn);
    }

    /** 校验：重算 checksum，与 header.checksum 且 trailer.checksumTrailer 同时相等才返回 true。 */
    public static boolean verify(PageGuard guard, PageSize pageSize) {
        requireArgs(guard, pageSize);
        int checksum = compute(guard, pageSize);
        int header = guard.readInt(PageEnvelopeLayout.CHECKSUM);
        int trailerOffset = PageEnvelopeLayout.trailerOffset(pageSize);
        int trailer = guard.readInt(trailerOffset + PageEnvelopeLayout.TRAILER_CHECKSUM);
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

- [ ] **Step 6: 删除旧 fsp 信封文件**

删除：
`src/main/java/cn/zhangyis/db/storage/fsp/PageType.java`、`FilePageHeader.java`、`FilePageHeaderLayout.java`、`FilePageTrailerLayout.java`、`PageChecksum.java`、`PageLayouts.java`。

- [ ] **Step 7: 更新 fsp 两个 layout 文件引用**

`fsp/SpaceHeaderLayout.java`：把 `static final int SPACE_ID = PageLayouts.FIL_PAGE_DATA;` 改为 `static final int SPACE_ID = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;`，并加 `import cn.zhangyis.db.storage.page.PageEnvelopeLayout;`。
`fsp/SegmentInodeLayout.java`：把 `static final int INODE_BASE = PageLayouts.FIL_PAGE_DATA;` 改为 `static final int INODE_BASE = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;`，并加同一 import。

- [ ] **Step 8: 更新 record/page/IndexPageLayout.java 引用共享常量**

把 IndexPageLayout 的本地复刻：
```java
    static final int FIL_PAGE_HEADER_BYTES = 38;
    static final int FIL_PAGE_TRAILER_BYTES = 8;
```
改为引用共享常量（删除复刻、消除钉死重复）：
```java
    static final int FIL_PAGE_HEADER_BYTES = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    static final int FIL_PAGE_TRAILER_BYTES = PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
```
并加 `import cn.zhangyis.db.storage.page.PageEnvelopeLayout;`。`IndexPageLayoutTest` 对 38/8 的断言保留（现在校验共享常量值），无需改。

- [ ] **Step 9: 迁移 3 个信封测试到 storage.page 测试包**

把 `src/test/java/cn/zhangyis/db/storage/fsp/{PageTypeTest,FilePageHeaderTest,PageChecksumTest}.java` 移到 `src/test/java/cn/zhangyis/db/storage/page/`：
- 包名改 `package cn.zhangyis.db.storage.page;`。
- `PageTypeTest`：`PageType`/异常引用本包；`fromCode` 非法码断言改 `DatabaseValidationException`（原 `FspMetadataException`）。
- `FilePageHeaderTest`：把 `header.writeTo(guard)` → `PageEnvelope.writeHeader(guard, header)`、`FilePageHeader.readFrom(guard)` → `PageEnvelope.readHeader(guard)`；其余断言不变。
- `PageChecksumTest`：`PageChecksum` 引用本包；如用到 `FilePageHeaderLayout`/`FilePageTrailerLayout` 常量改 `PageEnvelopeLayout`。

- [ ] **Step 10: 新建 `storage.page/PageEnvelopeTest`（stamp/read round-trip）**

`src/test/java/cn/zhangyis/db/storage/page/PageEnvelopeTest.java`：
```java
package cn.zhangyis.db.storage.page;

import cn.zhangyis.db.domain.Lsn;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** PageEnvelope：header 往返 + stampPageLsn/readPageLsn 单字段往返。 */
class PageEnvelopeTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void headerAndPageLsnRoundTrip() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            PageId pid = PageId.of(SPACE, PageNo.of(3));
            try (PageGuard g = pool.getPage(pid, PageLatchMode.EXCLUSIVE)) {
                FilePageHeader h = new FilePageHeader(SPACE, 3, FilePageHeader.FIL_NULL,
                        FilePageHeader.FIL_NULL, 0, PageType.INDEX);
                PageEnvelope.writeHeader(g, h);
                assertEquals(h, PageEnvelope.readHeader(g));

                PageEnvelope.stampPageLsn(g, Lsn.of(12345));
                assertEquals(Lsn.of(12345), PageEnvelope.readPageLsn(g));
            }
        }
    }
}
```

- [ ] **Step 11: 全量回归**

Run Checkpoint 命令。Expected: BUILD SUCCESSFUL（信封迁移无行为变化；fsp/record/page 全绿）。修任何遗漏 import 直到绿。

- [ ] **Step 12: detect_changes**

`gitnexus_detect_changes({repo:"dbtest", scope:"all"})`（仓库无 commit 时会报 HEAD 缺失——属预期，记录即可）。

---

## Task D3b：redo 包（LogRange + RedoRecord + RedoLogManager，内存 append）

> 纯新增，无 impact。`domain.Lsn` 已存在（`Lsn.of(long)`/`value()`）。

- [ ] **Step 1: 写失败测试 `redo/RedoLogManagerTest` + `redo/LogRangeTest` + `redo/RedoRecordTest`**

`src/test/java/cn/zhangyis/db/storage/redo/RedoRecordTest.java`：
```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RedoRecord：PageBytesRecord 防御性 copy（构造 clone + accessor clone）、byteLength 正值。 */
class RedoRecordTest {

    private static final PageId PID = PageId.of(SpaceId.of(1), PageNo.of(3));

    @Test
    void pageBytesRecordDefensivelyCopies() {
        byte[] src = {1, 2, 3};
        PageBytesRecord r = new PageBytesRecord(PID, 98, src);
        src[0] = 9; // 改外部数组不应影响记录
        assertArrayEquals(new byte[]{1, 2, 3}, r.bytes());
        assertNotSame(r.bytes(), r.bytes(), "accessor returns fresh clone each call");
        assertTrue(r.byteLength() > 3);
    }

    @Test
    void pageInitRecordHasPositiveLength() {
        PageInitRecord r = new PageInitRecord(PID, cn.zhangyis.db.storage.page.PageType.INDEX);
        assertTrue(r.byteLength() > 0);
    }
}
```

`src/test/java/cn/zhangyis/db/storage/redo/LogRangeTest.java`：
```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** LogRange：end>=start，endLsn 取 end。 */
class LogRangeTest {

    @Test
    void holdsRangeAndRejectsInverted() {
        LogRange r = new LogRange(Lsn.of(10), Lsn.of(27));
        assertEquals(Lsn.of(10), r.start());
        assertEquals(Lsn.of(27), r.end());
        assertThrows(DatabaseValidationException.class, () -> new LogRange(Lsn.of(27), Lsn.of(10)));
    }
}
```

`src/test/java/cn/zhangyis/db/storage/redo/RedoLogManagerTest.java`：
```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RedoLogManager：append 分配单调 LSN 区间、buffer 累积、空批退化、快照不可变。 */
class RedoLogManagerTest {

    private static final PageId PID = PageId.of(SpaceId.of(1), PageNo.of(3));

    @Test
    void appendAssignsContiguousMonotonicRanges() {
        RedoLogManager mgr = new RedoLogManager();
        PageBytesRecord a = new PageBytesRecord(PID, 0, new byte[]{1, 2, 3});
        PageInitRecord b = new PageInitRecord(PID, PageType.INDEX);

        LogRange r1 = mgr.append(List.of(a));
        LogRange r2 = mgr.append(List.of(b));

        assertEquals(0L, r1.start().value(), "first LSN at 0");
        assertEquals(r1.start().value() + a.byteLength(), r1.end().value());
        assertEquals(r1.end(), r2.start(), "ranges are contiguous");
        assertEquals(r1.end().value() + b.byteLength(), r2.end().value());
        assertEquals(r2.end(), mgr.currentLsn());
        assertEquals(2, mgr.bufferedRecords().size());
    }

    @Test
    void emptyBatchDoesNotAdvanceLsn() {
        RedoLogManager mgr = new RedoLogManager();
        LogRange r = mgr.append(List.of());
        assertEquals(r.start(), r.end(), "empty batch -> degenerate range");
        assertEquals(0, mgr.bufferedRecords().size());
    }

    @Test
    void bufferedRecordsIsImmutableSnapshot() {
        RedoLogManager mgr = new RedoLogManager();
        mgr.append(List.of(new PageInitRecord(PID, PageType.INDEX)));
        List<RedoRecord> snap = mgr.bufferedRecords();
        assertThrows(UnsupportedOperationException.class, () -> snap.add(new PageInitRecord(PID, PageType.INDEX)));
        assertTrue(snap.get(0) instanceof PageInitRecord);
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.redo.*" --console=plain`
Expected: 编译失败（redo 类不存在）。

- [ ] **Step 3: 实现 redo 类**

`src/main/java/cn/zhangyis/db/storage/redo/LogRange.java`：
```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * 一次 redo append 占据的 LSN 区间 {@code [start, end)}。end 为本批之后第一个空闲 LSN。
 *
 * @param start 区间起始 LSN。
 * @param end   区间结束（开区间）；空批时 end==start。
 */
public record LogRange(Lsn start, Lsn end) {

    public LogRange {
        if (start == null || end == null) {
            throw new DatabaseValidationException("log range start/end must not be null");
        }
        if (end.value() < start.value()) {
            throw new DatabaseValidationException("log range end < start: " + end.value() + " < " + start.value());
        }
    }
}
```

`src/main/java/cn/zhangyis/db/storage/redo/RedoRecord.java`：
```java
package cn.zhangyis.db.storage.redo;

/**
 * 物理 redo 记录（设计 §：物理字节区间 redo）。D3 仅两种：整页字节区间写 {@link PageBytesRecord}、页初始化 {@link PageInitRecord}。
 * 纯值，仅依赖 domain + storage.page 纯层 PageType，不依赖任何 repository/PageGuard（redo record 定义不依赖具体实现）。
 *
 * <p>D3 记录本身不带 LSN 字段；LSN 由 {@link RedoLogManager#append} 以 batch 区间分配，per-record LSN 元数据留 R1。
 */
public sealed interface RedoRecord permits PageBytesRecord, PageInitRecord {

    /** 估算落盘字节数，供 append 推进 LSN（D3 用一致的估算规则即可，真实编码格式留 R1）。 */
    int byteLength();
}
```

`src/main/java/cn/zhangyis/db/storage/redo/PageBytesRecord.java`：
```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * 物理字节区间写：把 {@code bytes} 覆盖到 {@code pageId} 的 {@code offset} 处。recovery（R1）按此重放即可幂等重建页内容。
 *
 * @param pageId 目标页。
 * @param offset 页内偏移（非负）。
 * @param bytes  写入字节（防御性 clone；accessor 也返回 clone，避免泄漏可变状态）。
 */
public record PageBytesRecord(PageId pageId, int offset, byte[] bytes) implements RedoRecord {

    /** 估算头开销：tag(1) + pageId(8) + offset(4) + len(4)。 */
    private static final int HEADER_BYTES = 17;

    public PageBytesRecord {
        if (pageId == null) {
            throw new DatabaseValidationException("page bytes record pageId must not be null");
        }
        if (offset < 0) {
            throw new DatabaseValidationException("page bytes record offset must be non-negative: " + offset);
        }
        if (bytes == null) {
            throw new DatabaseValidationException("page bytes record bytes must not be null");
        }
        bytes = bytes.clone();
    }

    /** 返回防御性副本，避免外部改动内部状态（Java record 数组 accessor 默认会泄漏内部数组）。 */
    @Override
    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public int byteLength() {
        return HEADER_BYTES + bytes.length;
    }
}
```

`src/main/java/cn/zhangyis/db/storage/redo/PageInitRecord.java`：
```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.page.PageType;

/**
 * 页初始化：{@code pageId} 在本 LSN 处以 {@code pageType} 创建。recovery（R1）据此初始化页骨架，随后的 PAGE_BYTES 填充内容。
 *
 * @param pageId   新页。
 * @param pageType 页类型。
 */
public record PageInitRecord(PageId pageId, PageType pageType) implements RedoRecord {

    /** 估算：tag(1) + pageId(8) + type(4)。 */
    private static final int LENGTH = 13;

    public PageInitRecord {
        if (pageId == null || pageType == null) {
            throw new DatabaseValidationException("page init record pageId/pageType must not be null");
        }
    }

    @Override
    public int byteLength() {
        return LENGTH;
    }
}
```

`src/main/java/cn/zhangyis/db/storage/redo/RedoLogManager.java`：
```java
package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redo 日志管理器（D3 仅内存子集）：同步分配 LSN 区间并把记录追加到内存 buffer。**无文件、无 fsync、无后台线程**——
 * writer/flusher/durability/recovery 留 R1。并发用显式 {@link ReentrantLock}（禁 synchronized）保护 LSN 分配与 buffer 追加。
 */
public final class RedoLogManager {

    /** 保护 nextLsn 与 buffer 的互斥锁。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 下一个空闲 LSN（long 计数，append 时推进）。 */
    private long nextLsn = 0;
    /** 内存 redo buffer（append 顺序）。 */
    private final List<RedoRecord> buffer = new ArrayList<>();

    /**
     * 追加一批 redo 记录，分配并返回其 LSN 区间 {@code [start, end)}（end = start + Σ byteLength）。空批返回退化区间 [cur,cur)。
     */
    public LogRange append(List<RedoRecord> records) {
        if (records == null) {
            throw new DatabaseValidationException("redo records must not be null");
        }
        lock.lock();
        try {
            long start = nextLsn;
            long end = start;
            for (RedoRecord r : records) {
                end += r.byteLength();
                buffer.add(r);
            }
            nextLsn = end;
            return new LogRange(Lsn.of(start), Lsn.of(end));
        } finally {
            lock.unlock();
        }
    }

    /** 当前下一个空闲 LSN。 */
    public Lsn currentLsn() {
        lock.lock();
        try {
            return Lsn.of(nextLsn);
        } finally {
            lock.unlock();
        }
    }

    /** 内存 buffer 的不可变快照（测试/诊断用）。 */
    public List<RedoRecord> bufferedRecords() {
        lock.lock();
        try {
            return List.copyOf(buffer);
        } finally {
            lock.unlock();
        }
    }
}
```

- [ ] **Step 4: 运行确认通过**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.redo.*" --console=plain`
Expected: PASS（RedoRecordTest/LogRangeTest/RedoLogManagerTest）。

- [ ] **Step 5: 全量回归** — Checkpoint 命令。Expected: BUILD SUCCESSFUL。

---

## Task D3c：buf PageWriteListener + PageGuard 写回调

**Impact（编辑前必跑，CRITICAL 告警）：**
```
gitnexus_impact({target:"PageGuard", direction:"upstream", repo:"dbtest"})
```
**已知 PageGuard 为 CRITICAL（~89 impacted / 45 direct）。本批改动为纯增量：新增字段/方法 + 写后回调，既有方法签名/语义不变，listener 默认 NO_OP，故行为兼容、全量回归把关。开工前向用户复述该 blast radius。**

- [ ] **Step 1: 新建接口 `buf/PageWriteListener.java`**

```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;

/**
 * 页写监听（依赖倒置接缝）：PageGuard 每次物理写后回调，把 (pageId, offset, 实际写入字节) 上报给挂载方。
 * 默认 {@link #NO_OP}。签名只用 domain + 原语，**不含 redo/mtr 类型**——buf 不反向依赖上层；
 * MTR 提供实现（{@code MtrRedoCollector}）把回调译成 redo record。
 */
public interface PageWriteListener {

    /** 空实现：非 MTR 路径（直接 pool.getPage）使用，零开销、零行为变化。 */
    PageWriteListener NO_OP = (pageId, offset, newBytes) -> { };

    /**
     * 一次物理写完成后回调。
     *
     * @param pageId   被写页。
     * @param offset   页内起始偏移。
     * @param newBytes 该次写入后该区间的实际字节（PageGuard 写后读回，调用方如需保留应自行 copy）。
     */
    void onWrite(PageId pageId, int offset, byte[] newBytes);
}
```

- [ ] **Step 2: 写失败测试 `buf/PageGuardWriteListenerTest`**

`src/test/java/cn/zhangyis/db/storage/buf/PageGuardWriteListenerTest.java`：
```java
package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.PageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** PageGuard.attachWriteListener：writeInt/writeBytes 回调上报实际字节；默认无 listener 不回调。 */
class PageGuardWriteListenerTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    private record Write(PageId pageId, int offset, byte[] bytes) { }

    @Test
    void reportsWritesAfterAttach() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        PageId pid = PageId.of(SPACE, PageNo.of(3));
        List<Write> seen = new ArrayList<>();
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(pid, PageLatchMode.EXCLUSIVE)) {
                g.attachWriteListener((p, off, b) -> seen.add(new Write(p, off, b)));
                g.writeInt(100, 0x01020304);
                g.writeBytes(200, new byte[]{9, 8, 7});
            }
        }
        assertEquals(2, seen.size());
        assertEquals(pid, seen.get(0).pageId());
        assertEquals(100, seen.get(0).offset());
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04}, seen.get(0).bytes());
        assertEquals(200, seen.get(1).offset());
        assertArrayEquals(new byte[]{9, 8, 7}, seen.get(1).bytes());
    }

    @Test
    void noListenerByDefaultMeansNoCallback() {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
        PageId pid = PageId.of(SPACE, PageNo.of(3));
        int[] count = {0};
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 4)) {
            try (PageGuard g = pool.getPage(pid, PageLatchMode.EXCLUSIVE)) {
                g.writeInt(100, 42); // 未 attach → NO_OP
            }
        }
        assertEquals(0, count[0]);
    }
}
```

- [ ] **Step 3: 运行确认失败**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.buf.PageGuardWriteListenerTest" --console=plain`
Expected: 编译失败（`attachWriteListener` 不存在）。

- [ ] **Step 4: 改 `buf/PageGuard.java`**

加 import：`import cn.zhangyis.db.domain.PageId;`（若未导入）。

在字段区（`private boolean wrote;` 附近）加：
```java
    /** 写监听；默认 NO_OP（非 MTR 路径零行为变化）。MTR 在 fix 后挂上 collector 以收集 redo。 */
    private PageWriteListener listener = PageWriteListener.NO_OP;
```

加方法（放在 markDirty 之后）：
```java
    /** 挂载写监听（mtr fix 后调用；null 视为 NO_OP）。仅属主线程调用。 */
    public void attachWriteListener(PageWriteListener listener) {
        ensureOpen();
        this.listener = (listener == null) ? PageWriteListener.NO_OP : listener;
    }

    /** 写后回调：把该区间实际字节读回上报（listener 为 NO_OP 时跳过，零开销）。 */
    private void notifyWrite(int offset, int length) {
        if (listener != PageWriteListener.NO_OP) {
            byte[] b = new byte[length];
            frame.buffer.get(offset, b, 0, length);
            listener.onWrite(frame.pageId, offset, b);
        }
    }
```

在 `writeInt` 末尾（`wrote = true;` 之后）加 `notifyWrite(offset, Integer.BYTES);`；
在 `writeLong` 末尾加 `notifyWrite(offset, Long.BYTES);`；
在 `writeBytes` 末尾加 `notifyWrite(offset, src.length);`。

- [ ] **Step 5: 运行确认通过**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.buf.*" --console=plain`
Expected: PASS（新测试 + 既有 buf 测试回归）。

- [ ] **Step 6: 全量回归 + detect_changes** — Checkpoint 命令 BUILD SUCCESSFUL；`gitnexus_detect_changes`。

---

## Task D3d：mtr retrofit（collector + newPage(type) + commit append/stamp + savepoint 不变量）

**Impact（编辑前必跑，HIGH 告警）：**
```
gitnexus_impact({target:"MiniTransaction", direction:"upstream", repo:"dbtest"})
gitnexus_impact({target:"MtrMemo", direction:"upstream", repo:"dbtest"})
```
**已知 MiniTransaction HIGH（~68/19）、MtrMemo MEDIUM。`newPage` 加 type 参仅影响 MTR 测试（经核实无生产调用）；`MiniTransactionManager` 保留无参构造（自建 RedoLogManager），故 fsp/DSM 测试不破。开工前复述。**

- [ ] **Step 1: 写失败测试 `mtr/MtrRedoAppendTest`**

`src/test/java/cn/zhangyis/db/storage/mtr/MtrRedoAppendTest.java`：
```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.domain.Lsn;
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
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MTR retrofit：写产 PAGE_BYTES、newPage 产 PAGE_INIT、commit 盖 pageLSN（不入 redo）、savepoint 拒绝释放 touched 页。 */
class MtrRedoAppendTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PID = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private interface Body { void run(BufferPool pool, MiniTransactionManager mgr); }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            body.run(pool, new MiniTransactionManager());
        }
    }

    @Test
    void writeProducesPageBytesAndStampsPageLsn() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            PageGuard g = mtr.getPage(pool, PID, PageLatchMode.EXCLUSIVE);
            g.writeBytes(100, new byte[]{1, 2, 3});
            mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertEquals(1, recs.size());
            assertTrue(recs.get(0) instanceof PageBytesRecord);
            PageBytesRecord pb = (PageBytesRecord) recs.get(0);
            assertEquals(100, pb.offset());
            // commit 后页已释放：重新取页验证 pageLSN==endLsn，且没有为 PAGE_LSN 偏移产生 PAGE_BYTES。
            Lsn endLsn = mgr.redoLogManager().currentLsn();
            try (PageGuard g2 = pool.getPage(PID, PageLatchMode.SHARED)) {
                assertEquals(endLsn, PageEnvelope.readPageLsn(g2));
            }
            assertFalse(recs.stream().anyMatch(r -> r instanceof PageBytesRecord pbr
                    && pbr.offset() == PageEnvelopeLayout.PAGE_LSN), "pageLSN stamp must not be in redo");
        });
    }

    @Test
    void newPageProducesPageInitAndStamps() {
        onPool((pool, mgr) -> {
            PageId fresh = PageId.of(SPACE, PageNo.of(5));
            MiniTransaction mtr = mgr.begin();
            mtr.newPage(pool, fresh, PageLatchMode.EXCLUSIVE, PageType.INDEX);
            mgr.commit(mtr);

            List<RedoRecord> recs = mgr.redoLogManager().bufferedRecords();
            assertTrue(recs.get(0) instanceof PageInitRecord, "newPage emits PAGE_INIT");
            assertEquals(PageType.INDEX, ((PageInitRecord) recs.get(0)).pageType());
            Lsn endLsn = mgr.redoLogManager().currentLsn();
            try (PageGuard g2 = pool.getPage(fresh, PageLatchMode.SHARED)) {
                assertEquals(endLsn, PageEnvelope.readPageLsn(g2), "PAGE_INIT page stamped even without extra write");
            }
        });
    }

    @Test
    void sharedOnlyPageProducesNoRedoNorStamp() {
        onPool((pool, mgr) -> {
            // 先用一个 MTR 写出 baseline pageLSN=endLsn1。
            MiniTransaction w = mgr.begin();
            w.getPage(pool, PID, PageLatchMode.EXCLUSIVE).writeBytes(50, new byte[]{7});
            mgr.commit(w);
            Lsn afterWrite = mgr.redoLogManager().currentLsn();
            int recsAfterWrite = mgr.redoLogManager().bufferedRecords().size();

            // 再用一个只读 MTR：S latch、不写。
            MiniTransaction r = mgr.begin();
            r.getPage(pool, PID, PageLatchMode.SHARED);
            mgr.commit(r);

            assertEquals(recsAfterWrite, mgr.redoLogManager().bufferedRecords().size(), "S-only adds no redo");
            try (PageGuard g2 = pool.getPage(PID, PageLatchMode.SHARED)) {
                assertEquals(afterWrite, PageEnvelope.readPageLsn(g2), "S-only does not restamp");
            }
        });
    }

    @Test
    void rollbackToSavepointRejectsTouchedPage() {
        onPool((pool, mgr) -> {
            MiniTransaction mtr = mgr.begin();
            MtrSavepoint sp = mtr.savepoint();
            PageGuard g = mtr.getPage(pool, PID, PageLatchMode.EXCLUSIVE);
            g.writeBytes(100, new byte[]{1}); // touched，且 guard 在保存点之上
            assertThrows(MtrStateException.class, () -> mtr.rollbackToSavepoint(sp));
            mgr.rollbackUncommitted(mtr); // 清理
        });
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.mtr.MtrRedoAppendTest" --console=plain`
Expected: 编译失败（`newPage(...,PageType)`、`redoLogManager()` 等不存在）。

- [ ] **Step 3: 新建 `mtr/MtrRedoCollector.java`**

```java
package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageWriteListener;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoRecord;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 本 MTR 的 redo 收集器：实现 buf 的 {@link PageWriteListener}，把页写译成 {@link PageBytesRecord} 累积；
 * {@code newPage} 经 {@link #recordInit} 产 {@link PageInitRecord}。维护 {@code touchedPages}（收到任一记录即标记该页），
 * commit 据此决定给哪些页盖 pageLSN（不读 PageGuard.wrote 私有状态）。{@code enabled} 开关：commit 盖戳前关闭，使 pageLSN 写不入 redo。
 * 单线程拥有（随 MiniTransaction），无需加锁。
 */
final class MtrRedoCollector implements PageWriteListener {

    private final List<RedoRecord> records = new ArrayList<>();
    private final Set<PageId> touchedPages = new LinkedHashSet<>();
    private boolean enabled = true;

    @Override
    public void onWrite(PageId pageId, int offset, byte[] newBytes) {
        if (!enabled) {
            return;
        }
        records.add(new PageBytesRecord(pageId, offset, newBytes));
        touchedPages.add(pageId);
    }

    /** 记录一次页初始化（newPage）。 */
    void recordInit(PageId pageId, PageType pageType) {
        if (!enabled) {
            return;
        }
        records.add(new PageInitRecord(pageId, pageType));
        touchedPages.add(pageId);
    }

    /** 关闭收集（commit 盖 pageLSN 前调用，排除 LSN 写进 redo）。 */
    void disable() {
        enabled = false;
    }

    List<RedoRecord> records() {
        return records;
    }

    Set<PageId> touchedPages() {
        return touchedPages;
    }
}
```

- [ ] **Step 4: 改 `mtr/MtrMemo.java` —— 加 guardFor + pageIdsAbove**

加 import：`import java.util.ArrayList;`、`import java.util.List;`。

在 `releaseAll()` 之后加：
```java
    /**
     * 返回指向 {@code pageId} 的最近一次 X latch 的 page guard；无则视为不变量破坏抛 {@link MtrStateException}。
     * 供 commit 给 touched 页盖 pageLSN（touched 页必有对应 X guard，见 MiniTransaction.rollbackToSavepoint 的保护）。
     */
    PageGuard guardFor(PageId pageId) {
        for (MemoEntry entry : stack) {
            if (entry.pageId() != null && entry.pageId().equals(pageId)
                    && entry.mode() == PageLatchMode.EXCLUSIVE) {
                return (PageGuard) entry.resource();
            }
        }
        throw new MtrStateException("no X page guard in memo for page " + pageId);
    }

    /** 返回深度 &gt; targetDepth（即将被 releaseTo 释放）的各槽位 pageId（非 page 资源跳过）。供 savepoint 回滚前检查 touched。 */
    List<PageId> pageIdsAbove(int targetDepth) {
        List<PageId> result = new ArrayList<>();
        int aboveCount = stack.size() - targetDepth;
        int i = 0;
        for (MemoEntry entry : stack) { // ArrayDeque 迭代 head→tail = 新→旧，头部即栈顶
            if (i++ >= aboveCount) {
                break;
            }
            if (entry.pageId() != null) {
                result.add(entry.pageId());
            }
        }
        return result;
    }
```

- [ ] **Step 5: 改 `mtr/MiniTransaction.java`**

加 import：
```java
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.domain.Lsn;
import java.util.List;
```

字段区加：
```java
    /** 本 MTR 的 redo 收集器（随 MTR 生命周期）。 */
    private final MtrRedoCollector collector = new MtrRedoCollector();
    /** redo 日志管理器（由 Manager 注入，commit 时 append）。 */
    private final RedoLogManager redoLogManager;
```

构造函数改为：
```java
    MiniTransaction(long id, RedoLogManager redoLogManager) {
        this.id = id;
        this.redoLogManager = redoLogManager;
    }
```

`getPage`/`newPage`/`fix` 改为（newPage 增 type 参，fix 增 type 参并挂 listener + 产 PAGE_INIT）：
```java
    public PageGuard getPage(BufferPool pool, PageId pageId, PageLatchMode mode) {
        return fix(pool, pageId, mode, true, null);
    }

    public PageGuard newPage(BufferPool pool, PageId pageId, PageLatchMode mode, PageType pageType) {
        if (pageType == null) {
            throw new DatabaseValidationException("newPage pageType must not be null");
        }
        return fix(pool, pageId, mode, false, pageType);
    }

    private PageGuard fix(BufferPool pool, PageId pageId, PageLatchMode mode, boolean existing, PageType pageType) {
        ensureActive();
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        if (mode == PageLatchMode.EXCLUSIVE
                && memo.holds(pageId, PageLatchMode.SHARED)
                && !memo.holds(pageId, PageLatchMode.EXCLUSIVE)) {
            throw new MtrStateException("S to X page latch upgrade is forbidden in one MTR: " + pageId);
        }
        PageGuard guard = existing ? pool.getPage(pageId, mode) : pool.newPage(pageId, mode);
        guard.attachWriteListener(collector);
        memo.pushPageGuard(guard, pageId, mode);
        if (!existing) {
            collector.recordInit(pageId, pageType);
        }
        return guard;
    }
```

`rollbackToSavepoint` 加 touched 页保护（在 `memo.releaseTo` 之前）：
```java
    public void rollbackToSavepoint(MtrSavepoint savepoint) {
        ensureActive();
        if (savepoint == null) {
            throw new DatabaseValidationException("savepoint must not be null");
        }
        if (savepoint.mtrId() != id) {
            throw new MtrStateException("savepoint does not belong to this mini transaction: "
                    + savepoint.mtrId() + " vs " + id);
        }
        // 不变量：已写过（touched）的页 guard 不允许被 savepoint 回滚释放，否则 commit 盖 pageLSN 时 guardFor 取不到它。
        for (PageId pid : memo.pageIdsAbove(savepoint.depth())) {
            if (collector.touchedPages().contains(pid)) {
                throw new MtrStateException("cannot release a written (touched) page in savepoint rollback: " + pid);
            }
        }
        memo.releaseTo(savepoint.depth());
    }
```

`commit` 改为 append + 盖 pageLSN：
```java
    void commit() {
        transitTo(MiniTransactionState.COMMITTING);
        LogRange range = redoLogManager.append(collector.records());
        Lsn endLsn = range.end();
        collector.disable(); // 其后 pageLSN 盖戳写不入 redo
        for (PageId pid : collector.touchedPages()) {
            PageEnvelope.stampPageLsn(memo.guardFor(pid), endLsn);
        }
        memo.releaseAll();
        transitTo(MiniTransactionState.COMMITTED);
    }
```
（`rollbackUncommitted` 不变：丢弃 collector.records、不 append、不盖戳。）

- [ ] **Step 6: 改 `mtr/MiniTransactionManager.java` —— 注入并暴露 RedoLogManager**

加 import：`import cn.zhangyis.db.storage.redo.RedoLogManager;`。

字段区加：
```java
    /** 全局 redo 日志管理器（D3 内存版）；注入每个 MTR，测试经 redoLogManager() 检视。 */
    private final RedoLogManager redoLogManager = new RedoLogManager();
```

`begin()` 内构造改为：
```java
        MiniTransaction mtr = new MiniTransaction(idSequence.incrementAndGet(), redoLogManager);
```

加 getter（类内任意位置）：
```java
    /** 本管理器的 redo 日志管理器（D3 内存版）。 */
    public RedoLogManager redoLogManager() {
        return redoLogManager;
    }
```

- [ ] **Step 7: 改 `mtr/MiniTransactionTest`（既有）适配新签名**

- 直接构造 `new MiniTransaction(id)` 处 → `new MiniTransaction(id, new RedoLogManager())`（加 import `cn.zhangyis.db.storage.redo.RedoLogManager`）。
- `newPage(pool, pageId, mode)` 调用处 → `newPage(pool, pageId, mode, PageType.INDEX)`（加 import `cn.zhangyis.db.storage.page.PageType`）。
- 其余断言不变。

- [ ] **Step 8: 运行确认通过**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.mtr.*" --console=plain`
Expected: PASS（MtrRedoAppendTest + MiniTransactionTest + MiniTransactionManagerTest 回归）。

- [ ] **Step 9: 全量回归** — Checkpoint 命令。Expected: BUILD SUCCESSFUL（fsp/DSM 测试用无参 Manager，redo 内存累积不影响其断言；pageLSN 写在偏移 20，落在信封内、不碰 fsp 元数据 [38, ...]）。

---

## Task D3e：收口

- [ ] **Step 1: 全量 clean test**

Run:
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain
```
Expected: BUILD SUCCESSFUL。

- [ ] **Step 2: 验证新测试计数**

读 `build/test-results/test/*.xml`，确认 `PageEnvelopeTest`、`RedoRecordTest`、`LogRangeTest`、`RedoLogManagerTest`、`PageGuardWriteListenerTest`、`MtrRedoAppendTest` 均 `failures=0 errors=0` 且 tests>0；迁移的 3 个信封测试在 `storage.page` 包下仍绿。

- [ ] **Step 3: GitNexus 刷新 + detect_changes**

`npx gitnexus analyze`；`gitnexus_detect_changes({repo:"dbtest", scope:"all"})`。失败（无 commit/HEAD）记录原因。

- [ ] **Step 4: 更新进度记忆**

更新 `C:\Users\李波\.claude\projects\C--coding-java-self-dbtest-dbtest\memory\storage-build-sequence.md`：标 D3 完成（redo 解除暂停，已做 MTR redo append 最小闭环 + storage.page 信封毕业）；列下一片 D4a（FSP/DSM 分配路径接 mtr.newPage + 信封初始化，触发 PAGE_INIT），需用户确认；R1/F1 仍后置。

---

## Self-Review（writing-plans 自检）

**Spec coverage：** §2 分层→D3a 建 storage.page；§4 信封毕业→D3a；§5 Lsn(已存在)/redo→D3b；§6 buf 钩子→D3c；§7 mtr retrofit + §7.5 savepoint 不变量→D3d；§9 衔接点（D4a allocatePage、D4b、R1）→D3e 记忆 + 各 task 注释；§10 测试→各 task 测试 + D3e 计数；§11 批次→D3a–D3e。impact（PageGuard/MiniTransaction/MtrMemo/信封）→各 task Impact 段。pageLSN 不入 redo / stamp 仅 header / readback 捕获 / PageBytesRecord clone → 均落到 D3c/D3d/D3b 代码与注释。

**Placeholder scan：** 无 TBD/TODO；每个 code step 给完整代码或精确 old→new 编辑；迁移类含完整新文件内容。Impact step 为 MCP 调用（非代码）。

**Type consistency：** `PageEnvelope.stampPageLsn(PageGuard, Lsn)`/`readPageLsn`/`writeHeader`/`readHeader`、`PageEnvelopeLayout.PAGE_LSN/FIL_PAGE_HEADER_BYTES/FIL_PAGE_TRAILER_BYTES/trailerOffset`、`RedoLogManager.append(List<RedoRecord>)->LogRange`/`currentLsn()`/`bufferedRecords()`、`PageBytesRecord(PageId,int,byte[])`+`bytes()`+`byteLength()`、`PageInitRecord(PageId,PageType)`、`PageWriteListener.onWrite(PageId,int,byte[])`+`NO_OP`、`PageGuard.attachWriteListener(PageWriteListener)`、`MiniTransaction.newPage(BufferPool,PageId,PageLatchMode,PageType)`、`MtrMemo.guardFor(PageId)`/`pageIdsAbove(int)`、`MiniTransactionManager.redoLogManager()` 在各 task 间一致。`domain.Lsn` 用现有 `Lsn.of`/`value()`，未改其 API。
