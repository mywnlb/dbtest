package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 0.7 DETECT_ONLY doublewrite 测试：metadata slot 可枚举，但不能作为 full-copy 修复来源。
 */
class DoublewriteDetectOnlyTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    @Test
    void detectOnlyWritesMetadataWithoutFullPagePayload() {
        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            DetectOnlyDoublewriteStrategy strategy = new DetectOnlyDoublewriteStrategy(dw);
            FlushPageSnapshot snapshot = stamped(PAGE, 44);

            strategy.beforeDataFileWrite(snapshot);
            strategy.afterDataFileWrite(snapshot);

            assertEquals(DoublewriteMode.DETECT_ONLY, strategy.mode());
            assertEquals(Set.of(PAGE), Set.copyOf(dw.pageIds()));
            assertFalse(dw.latestCopy(PAGE).isPresent(), "detect-only slot must not expose a full page copy");
            List<DoublewriteSlotEntry> entries = dw.scanEntries();
            assertEquals(1, entries.size());
            assertEquals(DoublewriteSlotKind.DETECT_ONLY_METADATA, entries.getFirst().kind());
            assertFalse(entries.getFirst().hasFullCopy());
        }
    }

    @Test
    void detectOnlyRecoveryReportsTornPageButDoesNotRepair() {
        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            FlushPageSnapshot snapshot = stamped(PAGE, 44);
            DetectOnlyDoublewriteStrategy strategy = new DetectOnlyDoublewriteStrategy(dw);
            strategy.beforeDataFileWrite(snapshot);
            strategy.afterDataFileWrite(snapshot);

            byte[] broken = snapshot.pageImage().clone();
            broken[200] = 99;
            store.writePage(PAGE, ByteBuffer.wrap(broken));
            store.force(SPACE);

            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(dw, store, PS);
            DoublewriteRecoveryResult result = scanner.scanPageIfNeeded(PAGE);

            assertEquals(DoublewriteRecoveryOutcome.DETECTED_ONLY, result.outcome());
            byte[] afterRecovery = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(afterRecovery));
            assertEquals(99, afterRecovery[200], "detect-only recovery must not replace the data page");
        }
    }

    @Test
    void fullCopyWritesV2TypedSlotAndStillExposesRepairCopy() throws Exception {
        Path path = dir.resolve("dw-full-copy-v2.dat");
        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(path, PS)) {
            FlushPageSnapshot snapshot = stamped(PAGE, 55);

            dw.append(snapshot);
            dw.force();
            dw.releaseSlot(snapshot);

            ByteBuffer header = ByteBuffer.wrap(Files.readAllBytes(path));
            header.getInt(); // magic
            assertEquals(2, header.getInt(), "new full-copy slots must use v2 typed format");
            List<DoublewriteSlotEntry> entries = dw.scanEntries();
            assertEquals(1, entries.size());
            assertEquals(DoublewriteSlotKind.FULL_COPY, entries.getFirst().kind());
            assertTrue(entries.getFirst().hasFullCopy());
            assertTrue(dw.latestCopy(PAGE).isPresent());
        }
    }

    private static FlushPageSnapshot stamped(PageId pageId, long pageLsn) {
        byte[] image = new byte[PS.bytes()];
        ByteBuffer.wrap(image).putLong(PageEnvelopeLayout.PAGE_LSN, pageLsn);
        PageImageChecksum.stamp(image, PS);
        return new FlushPageSnapshot(pageId, Lsn.of(pageLsn), 1L, image);
    }
}
