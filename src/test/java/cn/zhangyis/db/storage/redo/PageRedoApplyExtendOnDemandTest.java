package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorLayout;
import cn.zhangyis.db.storage.fsp.extent.ExtentState;
import cn.zhangyis.db.storage.fsp.extent.XdesPageCodec;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * extend-on-demand 加固（F1b）：
 * <ul>
 *   <li>PAGE_INIT 命中越界页时按需扩容并重建该页；</li>
 *   <li>首触是 PAGE_BYTES 且越界（无建页记录）判 redo 损坏，不静默造半成品页。</li>
 * </ul>
 */
class PageRedoApplyExtendOnDemandTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    /**
     * 验证 {@code pageInitBeyondEofIsRecreatedByExtendOnDemand} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void pageInitBeyondEofIsRecreatedByExtendOnDemand() {
        Path redoPath = dir.resolve("redo.log");
        PageId beyond = PageId.of(SPACE, PageNo.of(70));
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageInitRecord(beyond, PageType.ALLOCATED)));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4)); // 仅 0..3 物理存在
            List<RedoLogBatch> batches = repo.readBatches();
            RedoApplyDispatcher.pageDispatcher().applyAll(batches, new RedoApplyContext(store, PS));

            assertTrue(store.currentSizeInPages(SPACE).value() >= 71, "PAGE_INIT 应触发扩容容纳越界页");
            byte[] page = new byte[PS.bytes()];
            store.readPage(beyond, ByteBuffer.wrap(page));
            assertEquals(PageType.ALLOCATED.code(), ByteBuffer.wrap(page).getInt(PageEnvelopeLayout.PAGE_TYPE));
        }
    }

    /**
     * 验证 {@code firstTouchPageBytesBeyondEofIsRedoCorruption} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void firstTouchPageBytesBeyondEofIsRedoCorruption() {
        Path redoPath = dir.resolve("redo2.log");
        PageId beyond = PageId.of(SPACE, PageNo.of(70));
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(new PageBytesRecord(beyond, 100, new byte[]{1, 2, 3})));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            store.create(SPACE, dir.resolve("s2.ibd"), PS, PageNo.of(4));
            List<RedoLogBatch> batches = repo.readBatches();
            RedoApplyContext ctx = new RedoApplyContext(store, PS);
            assertThrows(RedoLogCorruptedException.class,
                    () -> RedoApplyDispatcher.pageDispatcher().applyAll(batches, ctx));
        }
    }

    /**
     * 重复管理页的 PAGE_INIT 必须与后续 XDES header/metadata delta 在同批恢复：文件尾丢失时先扩容，
     * 然后只做物理 after-image patch，不能重跑 freeLimit allocator。
     */
    @Test
    void standaloneXdesInitAndMetadataDeltaRecoverBeyondEof() {
        Path redoPath = dir.resolve("xdes-redo.log");
        PageId page5 = PageId.of(SPACE, PageNo.of(5));
        byte[] magic = ByteBuffer.allocate(Integer.BYTES).putInt(XdesPageCodec.MAGIC).array();
        byte[] state = ByteBuffer.allocate(Integer.BYTES).putInt(ExtentState.FREE_FRAG.ordinal()).array();
        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            RedoLogManager redo = RedoLogManager.durable(repo);
            redo.append(List.of(
                    new PageInitRecord(page5, PageType.XDES),
                    new PageBytesRecord(page5, XdesPageCodec.MAGIC_OFFSET, magic),
                    new FspMetadataDeltaRecord(page5, FspMetadataDeltaKind.XDES_FIELD,
                            237L, ExtentDescriptorLayout.STATE,
                            ExtentDescriptorLayout.ENTRIES_BASE + ExtentDescriptorLayout.STATE, state)));
            redo.flush();
        }
        try (PageStore store = new FileChannelPageStore();
             RedoLogFileRepository repo = RedoLogFileRepository.open(redoPath)) {
            store.create(SPACE, dir.resolve("xdes.ibd"), PS, PageNo.of(4));
            RedoApplyDispatcher.pageDispatcher().applyAll(repo.readBatches(), new RedoApplyContext(store, PS));

            assertTrue(store.currentSizeInPages(SPACE).value() >= 6L);
            byte[] image = new byte[PS.bytes()];
            store.readPage(page5, ByteBuffer.wrap(image));
            ByteBuffer page = ByteBuffer.wrap(image);
            assertEquals(PageType.XDES.code(), page.getInt(PageEnvelopeLayout.PAGE_TYPE));
            assertEquals(XdesPageCodec.MAGIC, page.getInt(XdesPageCodec.MAGIC_OFFSET));
            assertEquals(ExtentState.FREE_FRAG.ordinal(),
                    page.getInt(ExtentDescriptorLayout.ENTRIES_BASE + ExtentDescriptorLayout.STATE));
        }
    }
}
