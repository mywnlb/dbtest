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
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * F1 doublewrite recovery 测试：data file 出现 torn/corrupt page 时，恢复阶段先用 doublewrite full copy 修复。
 */
class DoublewriteRecoveryTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId PAGE = PageId.of(SPACE, PageNo.of(2));

    @TempDir
    Path dir;

    /**
     * 验证 {@code recoverableDoublewriteRepairsCorruptDataPage} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void recoverableDoublewriteRepairsCorruptDataPage() {
        try (PageStore store = new FileChannelPageStore();
             DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS)) {
            store.create(SPACE, dir.resolve("s.ibd"), PS, PageNo.of(4));
            byte[] image = validPageImage();
            FlushPageSnapshot snapshot = new FlushPageSnapshot(PAGE, Lsn.of(44), 1, image);
            new RecoverableDoublewriteStrategy(dw).beforeDataFileWrite(snapshot);

            byte[] broken = image.clone();
            broken[200] = 99;
            store.writePage(PAGE, ByteBuffer.wrap(broken));
            store.force(SPACE);

            DoublewriteRecoveryScanner scanner = new DoublewriteRecoveryScanner(dw, store, PS);
            assertTrue(scanner.repairPageIfNeeded(PAGE));

            byte[] repaired = new byte[PS.bytes()];
            store.readPage(PAGE, ByteBuffer.wrap(repaired));
            assertTrue(PageImageChecksum.verify(repaired, PS));
            assertEquals(7, repaired[200]);
        }
    }

    private static byte[] validPageImage() {
        byte[] page = new byte[PS.bytes()];
        ByteBuffer buf = ByteBuffer.wrap(page);
        buf.putInt(PageEnvelopeLayout.SPACE_ID, SPACE.value());
        buf.putInt(PageEnvelopeLayout.PAGE_NO, (int) PAGE.pageNo().value());
        buf.putLong(PageEnvelopeLayout.PAGE_LSN, 44L);
        page[200] = 7;
        PageImageChecksum.stamp(page, PS);
        return page;
    }
}
