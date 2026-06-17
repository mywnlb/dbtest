# D4b — record.page 的 MTR 生产入口（IndexPageAccess）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 提供 `IndexPageAccess`（storage.api）薄 facade，把「建 RecordPage 的页」绑定到 MTR-owned guard，使 record 的格式化/写入自动产 redo；R3–R5 算子签名/实现不变。

**Architecture:** `createIndexPage` 走 `mtr.newPage(X,INDEX)`+`PageEnvelope.writeHeader`+`RecordPage.format`（产 PAGE_INIT(INDEX)+PAGE_BYTES）；`openIndexPage` 走 `mtr.getPage` 返回 RecordPage 供算子 CRUD/只读。参数校验全部前置（碰页前），RecordPage 不依赖 mtr。

**Tech Stack:** Java 25、JUnit Jupiter、固定 JDK `C:\Program Files\Java\jdk-25.0.2`、固定 Gradle `D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1`。

**关联 spec：** `docs/superpowers/specs/2026-06-16-record-mtr-access-design.md`

**项目规则（覆盖默认）：** 不提交 git；Task 末「全量 `clean test` 绿」作 checkpoint，无 commit 步骤；禁 synchronized/裸异常；中文 Javadoc；改既有符号前跑 `gitnexus_impact`（本片纯新增，无）。

**Checkpoint 命令：**
```
$env:JAVA_HOME="C:\Program Files\Java\jdk-25.0.2"; & "D:\worker\gradle71\gradle-9.5.1-bin\gradle-9.5.1\bin\gradle.bat" test --console=plain
```

---

## File Structure

**新建（production）：** `src/main/java/cn/zhangyis/db/storage/api/IndexPageAccess.java`（薄 facade，持 pool+pageSize）。
**新建（test）：** `src/test/java/cn/zhangyis/db/storage/api/IndexPageAccessTest.java`（4 用例，自包含）。
**不改** buf/mtr/record/storage.page 任何既有文件。

---

## Task D4b：IndexPageAccess + 测试

**Impact（编辑前跑，纯新增预期空）：**
```
gitnexus_impact({target:"IndexPageAccess", direction:"upstream", repo:"dbtest"})
```
预期：not found / 空（新类）。

- [ ] **Step 1: 写失败测试 `IndexPageAccessTest`**

`src/test/java/cn/zhangyis/db/storage/api/IndexPageAccessTest.java`：
```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
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
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** IndexPageAccess：createIndexPage 产 PAGE_INIT(INDEX)+格式 redo、盖 pageLSN；openIndexPage 供算子 CRUD/只读；参数前置校验。 */
class IndexPageAccessTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId P = PageId.of(SPACE, PageNo.of(3));

    @TempDir
    Path dir;

    private final TypeCodecRegistry registry = new TypeCodecRegistry();

    private static TableSchema schema() {
        return new TableSchema(1, List.of(
                new ColumnDef(new ColumnId(0), "id", ColumnType.intType(false, false), 0),
                new ColumnDef(new ColumnId(1), "name", ColumnType.varchar(20, true), 1)));
    }

    private static IndexKeyDef idKey() {
        return new IndexKeyDef(7L, List.of(new KeyPartDef(new ColumnId(0), KeyOrder.ASC, 0)));
    }

    private static SearchKey kId(long id) {
        return new SearchKey(List.of(new ColumnValue.IntValue(id)));
    }

    private static LogicalRecord row(long id, String name) {
        return new LogicalRecord(1, List.of(new ColumnValue.IntValue(id), new ColumnValue.StringValue(name)),
                false, RecordType.CONVENTIONAL);
    }

    private interface Body { void run(BufferPool pool, IndexPageAccess access, MiniTransactionManager mgr); }

    private void onPool(Body body) {
        PageStore store = new FileChannelPageStore();
        store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(8));
        try (PageStore s = store; BufferPool pool = new LruBufferPool(store, PS, 8)) {
            body.run(pool, new IndexPageAccess(pool, PS), new MiniTransactionManager());
        }
    }

    @Test
    void createIndexPageEmitsInitAndFormatRedo() {
        onPool((pool, access, mgr) -> {
            MiniTransaction m1 = mgr.begin();
            access.createIndexPage(m1, P, 7L, 0);
            mgr.commit(m1);

            List<?> recs = mgr.redoLogManager().bufferedRecords();
            assertTrue(recs.stream().anyMatch(r -> r instanceof PageInitRecord pir
                    && pir.pageId().equals(P) && pir.pageType() == PageType.INDEX), "PAGE_INIT(INDEX)");
            assertTrue(recs.stream().anyMatch(r -> r instanceof PageBytesRecord), "format PAGE_BYTES");
            Lsn endLsn = mgr.redoLogManager().currentLsn();

            // 读验证 MTR：断言后显式 commit 释放 guard。
            MiniTransaction m2 = mgr.begin();
            RecordPage rp2 = access.openIndexPage(m2, P, PageLatchMode.SHARED);
            assertEquals(7L, rp2.header().indexId());
            assertEquals(0, rp2.header().level());
            PageGuard g = m2.getPage(pool, P, PageLatchMode.SHARED); // 同 MTR 重入 S，读信封
            assertEquals(PageType.INDEX, PageEnvelope.readHeader(g).pageType());
            assertEquals(endLsn, PageEnvelope.readPageLsn(g));
            mgr.commit(m2);
        });
    }

    @Test
    void insertThroughMtrOwnedPageEmitsRedo() {
        onPool((pool, access, mgr) -> {
            TableSchema schema = schema();
            IndexKeyDef kd = idKey();
            // m1：仅 createIndexPage（format redo），记下 redo 条数基线。
            MiniTransaction m1 = mgr.begin();
            access.createIndexPage(m1, P, 7L, 0);
            mgr.commit(m1);
            int afterFormat = mgr.redoLogManager().bufferedRecords().size();

            // m2：openIndexPage(X) + insert → 经 MTR-owned guard 写记录，redo 应增长。
            MiniTransaction m2 = mgr.begin();
            RecordPage rp = access.openIndexPage(m2, P, PageLatchMode.EXCLUSIVE);
            new RecordPageInserter(registry).insert(rp, P, row(1, "a"), kd, schema);
            mgr.commit(m2);
            assertTrue(mgr.redoLogManager().bufferedRecords().size() > afterFormat,
                    "insert via MTR-owned page produced redo");

            // m3：重开查回（读验证 MTR 显式 commit）。
            MiniTransaction m3 = mgr.begin();
            RecordPage rp3 = access.openIndexPage(m3, P, PageLatchMode.SHARED);
            assertTrue(new RecordPageSearch(registry).findEqual(rp3, kId(1), kd, schema).isPresent(), "id=1 found");
            mgr.commit(m3);
        });
    }

    @Test
    void openSharedProducesNoRedo() {
        onPool((pool, access, mgr) -> {
            MiniTransaction m1 = mgr.begin();
            access.createIndexPage(m1, P, 7L, 0);
            mgr.commit(m1);
            int before = mgr.redoLogManager().bufferedRecords().size();
            Lsn pageLsnBefore;
            MiniTransaction mr = mgr.begin();
            PageGuard g0 = mr.getPage(pool, P, PageLatchMode.SHARED);
            pageLsnBefore = PageEnvelope.readPageLsn(g0);
            mgr.commit(mr);

            // 只读 MTR：openIndexPage(S) 读 header，不写。
            MiniTransaction m2 = mgr.begin();
            RecordPage rp2 = access.openIndexPage(m2, P, PageLatchMode.SHARED);
            rp2.header(); // 只读
            mgr.commit(m2);

            assertEquals(before, mgr.redoLogManager().bufferedRecords().size(), "S-only adds no redo");
            MiniTransaction m3 = mgr.begin();
            PageGuard g = m3.getPage(pool, P, PageLatchMode.SHARED);
            assertEquals(pageLsnBefore, PageEnvelope.readPageLsn(g), "S-only does not restamp pageLSN");
            mgr.commit(m3);
        });
    }

    @Test
    void createIndexPageValidatesArgsBeforeTouchingPage() {
        onPool((pool, access, mgr) -> {
            MiniTransaction m = mgr.begin();
            assertThrows(DatabaseValidationException.class, () -> access.createIndexPage(m, P, -1L, 0));
            assertThrows(DatabaseValidationException.class, () -> access.createIndexPage(m, P, 7L, -1));
            assertThrows(DatabaseValidationException.class, () -> access.createIndexPage(m, null, 7L, 0));
            assertThrows(DatabaseValidationException.class, () -> access.openIndexPage(m, P, null));
            // 非法入参在 newPage 前抛 → 页未被改、无 redo。
            assertTrue(mgr.redoLogManager().bufferedRecords().isEmpty(), "no redo produced on validation failure");
            mgr.rollbackUncommitted(m);
        });
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.api.IndexPageAccessTest" --console=plain`
Expected: 编译失败（`IndexPageAccess` 不存在）。

- [ ] **Step 3: 实现 `IndexPageAccess`**

`src/main/java/cn/zhangyis/db/storage/api/IndexPageAccess.java`：
```java
package cn.zhangyis.db.storage.api;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.page.RecordPage;

/**
 * INDEX 页的 MTR 生产入口（设计 §14，Facade）：把「建 RecordPage 的页」绑定到 MTR-owned guard，
 * 使 record 的格式化/写入经 D3 的 collector 自动产 redo（PAGE_INIT/PAGE_BYTES）、commit 盖 pageLSN。
 *
 * <p>R3–R5 算子（RecordPageInserter/Search/Updater/...）签名不变——调用方拿本类返回的 {@link RecordPage} 跑它们。
 * {@link RecordPage} 仍不依赖 mtr（保持 R3 解耦）。返回的 RecordPage 由 mtr memo 持 guard，**勿自行 close**，
 * 须在同一 MTR 内使用；MTR commit/rollback 释放 guard（commit 才盖 pageLSN）。无状态、线程安全。
 */
public final class IndexPageAccess {

    private final BufferPool pool;
    private final PageSize pageSize;

    public IndexPageAccess(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("index page access pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
    }

    /**
     * 建并格式化一个 INDEX 页（要求在 mtr 内）：newPage(X,INDEX) → 写信封(INDEX) → format(indexId,level)。
     * 产 PAGE_INIT(INDEX) + 信封/格式 PAGE_BYTES；commit 盖 pageLSN。
     *
     * <p><b>校验全部前置</b>：任何 newPage/写页之前先校验入参——否则 indexId/level 非法在 format 阶段才失败时，
     * 页已被 newPage 重初始化并收集 PAGE_INIT，而 MTR rollback 不做内容 undo（脏页）。
     *
     * <p><b>破坏性入口</b>：因走 newPage（D4a 对驻留页会清零重初始化），**只能用于新分配/有意重初始化的页**；
     * 对已有 INDEX 页的读写**必须走 {@link #openIndexPage}**，否则会清空在用页。
     */
    public RecordPage createIndexPage(MiniTransaction mtr, PageId pageId, long indexId, int level) {
        if (mtr == null || pageId == null) {
            throw new DatabaseValidationException("createIndexPage mtr/pageId must not be null");
        }
        if (indexId < 0) {
            throw new DatabaseValidationException("indexId must be non-negative: " + indexId);
        }
        if (level < 0) {
            throw new DatabaseValidationException("level must be non-negative: " + level);
        }
        PageGuard g = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.INDEX);
        PageEnvelope.writeHeader(g, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INDEX));
        RecordPage rp = new RecordPage(g, pageSize);
        rp.format(indexId, level);
        return rp;
    }

    /**
     * 取已存在页做 CRUD（X）或只读扫描（S，无写→无 redo）：getPage(mode) → 包成 {@link RecordPage}。
     * 入参先校验，再取页。
     */
    public RecordPage openIndexPage(MiniTransaction mtr, PageId pageId, PageLatchMode mode) {
        if (mtr == null || pageId == null || mode == null) {
            throw new DatabaseValidationException("openIndexPage mtr/pageId/mode must not be null");
        }
        PageGuard g = mtr.getPage(pool, pageId, mode);
        return new RecordPage(g, pageSize);
    }
}
```

- [ ] **Step 4: 运行确认通过**

Run: `... gradle.bat test --tests "cn.zhangyis.db.storage.api.IndexPageAccessTest" --console=plain`
Expected: PASS（4 用例）。

- [ ] **Step 5: 全量回归** — Checkpoint 命令。Expected: BUILD SUCCESSFUL（纯新增，无回归）。

- [ ] **Step 6: detect_changes** — `gitnexus_detect_changes({repo:"dbtest", scope:"all"})`（无 commit 报 HEAD 缺失属预期）。

---

## Task D4b-收口

- [ ] **Step 1: 验证测试计数** — 读 `build/test-results/test/*.xml`，确认 `IndexPageAccessTest` tests=4、failures=0、errors=0。

- [ ] **Step 2: GitNexus 刷新** — `npx gitnexus analyze`（失败记录原因）。

- [ ] **Step 3: 更新进度记忆** — `C:\Users\李波\.claude\projects\C--coding-java-self-dbtest-dbtest\memory\storage-build-sequence.md`：标 D4b 完成（IndexPageAccess：createIndexPage 产 PAGE_INIT(INDEX)+格式 redo、openIndexPage 供算子；record 写经 MTR 自动产 redo）；下一步 R1（writer/flusher/redo 文件/durability/recovery reader+apply）需用户确认；F1 后置。

---

## Self-Review（writing-plans 自检）

**Spec coverage：** §1/§3 facade（createIndexPage/openIndexPage）→ Step3 实现；§2 决策（newPage(INDEX)、校验前置、破坏性语义）→ Step3 代码+Javadoc；§4 redo 流 → 测试断言（PAGE_INIT/PAGE_BYTES/pageLSN/S-no-redo）；§8 四测试（含读 MTR 显式 commit、参数校验前置无 redo）→ Step1；§9 impact → Task Impact 段。

**Placeholder scan：** 无 TBD/TODO；IndexPageAccess 与测试均完整代码；命令含期望。

**Type consistency：** `IndexPageAccess(BufferPool,PageSize)`、`createIndexPage(MiniTransaction,PageId,long,int)`、`openIndexPage(MiniTransaction,PageId,PageLatchMode)`、`mtr.newPage(BufferPool,PageId,PageLatchMode,PageType)`(D3d)、`PageEnvelope.writeHeader/readHeader/readPageLsn`、`FilePageHeader(SpaceId,long,long,long,long,PageType)`+`FIL_NULL`、`RecordPage(PageGuard,PageSize)`+`format(long,int)`+`header().indexId()/level()`、`RecordPageInserter(reg).insert(rp,pageId,LogicalRecord,IndexKeyDef,TableSchema)`、`RecordPageSearch(reg).findEqual(rp,SearchKey,IndexKeyDef,TableSchema)`、`mgr.redoLogManager().bufferedRecords()/currentLsn()`、`PageInitRecord.pageId()/pageType()` 全一致。
