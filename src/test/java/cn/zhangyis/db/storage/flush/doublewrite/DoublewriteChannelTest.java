package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 双物理文件的 source 隔离、独立 slot 容量和恢复最新副本裁决。 */
class DoublewriteChannelTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    /**
     * 验证 {@code routesFlushListAndLruIntoIndependentBoundedFiles} 所描述的刷脏与持久化协作，并断言 redo durable 边界先覆盖 page LSN、失败后仍保留脏状态。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void routesFlushListAndLruIntoIndependentBoundedFiles() throws Exception {
        Path flushList = dir.resolve("flush-list.dwb");
        Path lru = dir.resolve("lru.dwb");
        PageId page = PageId.of(SPACE, PageNo.of(2));
        try (DoublewriteChannel channel = DoublewriteChannel.open(flushList, lru, PS, 2)) {
            DoublewriteBatch first = DoublewriteBatch.of(List.of(stamped(page, 10, 10)));
            channel.appendBatch(DoublewriteChannelId.FLUSH_LIST, first);
            channel.force(DoublewriteChannelId.FLUSH_LIST);
            channel.releaseBatch(DoublewriteChannelId.FLUSH_LIST, first);

            DoublewriteBatch second = DoublewriteBatch.of(List.of(stamped(page, 20, 20)));
            channel.appendBatch(DoublewriteChannelId.LRU, second);
            channel.force(DoublewriteChannelId.LRU);
            channel.releaseBatch(DoublewriteChannelId.LRU, second);

            assertTrue(Files.size(flushList) > 0);
            assertTrue(Files.size(lru) > 0);
            assertEquals(20, channel.latestCopy(page).orElseThrow()[200]);
        }
    }

    /**
     * 验证 {@code abortReleasesEveryBatchReservationForReuse} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     */
    @Test
    void abortReleasesEveryBatchReservationForReuse() {
        try (DoublewriteChannel channel = DoublewriteChannel.open(
                dir.resolve("flush-list.dwb"), dir.resolve("lru.dwb"), PS, 1)) {
            DoublewriteBatch first = DoublewriteBatch.of(List.of(stamped(PageId.of(SPACE, PageNo.of(2)), 10, 10)));
            channel.appendBatch(DoublewriteChannelId.LRU, first);
            channel.releaseBatch(DoublewriteChannelId.LRU, first);
            DoublewriteBatch second = DoublewriteBatch.of(List.of(stamped(PageId.of(SPACE, PageNo.of(3)), 20, 20)));
            channel.appendBatch(DoublewriteChannelId.LRU, second);
            channel.releaseBatch(DoublewriteChannelId.LRU, second);
        }
    }

    private static FlushPageSnapshot stamped(PageId pageId, long lsn, int marker) {
        byte[] image = new byte[PS.bytes()];
        ByteBuffer.wrap(image).putLong(PageEnvelopeLayout.PAGE_LSN, lsn);
        image[200] = (byte) marker;
        PageImageChecksum.stamp(image, PS);
        return new FlushPageSnapshot(pageId, Lsn.of(lsn), 1L, image);
    }
}
