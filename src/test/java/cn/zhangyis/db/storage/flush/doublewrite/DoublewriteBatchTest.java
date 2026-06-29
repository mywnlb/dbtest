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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 0.5 doublewrite batch 测试：一批页必须在同一个 doublewrite 文件临界区中连续写入 slot，
 * data file force 后再整体释放 in-flight 标记，供后续 batch 复用。
 */
class DoublewriteBatchTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void appendsBatchIntoBoundedConsecutiveSlotsAndReusesThemAfterRelease() throws Exception {
        Path path = dir.resolve("dw-batch.dat");
        PageId p2 = PageId.of(SPACE, PageNo.of(2));
        PageId p3 = PageId.of(SPACE, PageNo.of(3));
        PageId p4 = PageId.of(SPACE, PageNo.of(4));
        PageId p5 = PageId.of(SPACE, PageNo.of(5));

        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(path, PS, 4)) {
            DoublewriteBatch first = DoublewriteBatch.of(List.of(stamped(p2, 10, 10), stamped(p3, 20, 20)));
            dw.appendBatch(first);
            dw.force();
            dw.releaseBatch(first);
            long firstBatchSize = Files.size(path);

            DoublewriteBatch second = DoublewriteBatch.of(List.of(stamped(p4, 30, 30), stamped(p5, 40, 40)));
            dw.appendBatch(second);
            dw.force();
            dw.releaseBatch(second);

            assertEquals(firstBatchSize * 2, Files.size(path),
                    "two two-page batches should occupy four consecutive slots before wrap");
            assertEquals(Set.of(p2, p3, p4, p5), Set.copyOf(dw.pageIds()));

            DoublewriteBatch third = DoublewriteBatch.of(List.of(stamped(p2, 50, 50), stamped(p3, 60, 60)));
            dw.appendBatch(third);
            dw.force();
            dw.releaseBatch(third);

            assertEquals(firstBatchSize * 2, Files.size(path),
                    "batch slot reuse must not grow the fixed doublewrite area after wrap");
            assertEquals(50, dw.latestCopy(p2).orElseThrow()[200]);
            assertEquals(60, dw.latestCopy(p3).orElseThrow()[200]);
            assertEquals(30, dw.latestCopy(p4).orElseThrow()[200]);
            assertEquals(40, dw.latestCopy(p5).orElseThrow()[200]);
        }
    }

    /** 构造带可区分 payload 的有效页镜像，便于断言 recovery 选择最新 batch slot。 */
    private static FlushPageSnapshot stamped(PageId pageId, long pageLsn, int marker) {
        byte[] image = new byte[PS.bytes()];
        ByteBuffer.wrap(image).putLong(PageEnvelopeLayout.PAGE_LSN, pageLsn);
        image[200] = (byte) marker;
        PageImageChecksum.stamp(image, PS);
        return new FlushPageSnapshot(pageId, Lsn.of(pageLsn), pageLsn, image);
    }
}
