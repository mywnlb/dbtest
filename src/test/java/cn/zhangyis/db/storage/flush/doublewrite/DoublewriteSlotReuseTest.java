package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.flush.FlushWriteException;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 0.5 doublewrite slot 复用测试：验证 full-copy 副本从 append-only 收敛为有界 slot 区域，
 * 同时恢复扫描仍能拿到同页最新有效副本。
 */
class DoublewriteSlotReuseTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);
    private static final SpaceId SPACE = SpaceId.of(1);

    @TempDir
    Path dir;

    @Test
    void reusesCompletedSlotsWithoutGrowingFileAndKeepsLatestCopy() throws Exception {
        Path path = dir.resolve("dw.dat");
        PageId p2 = PageId.of(SPACE, PageNo.of(2));
        PageId p5 = PageId.of(SPACE, PageNo.of(5));

        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(path, PS, 2)) {
            RecoverableDoublewriteStrategy strategy = new RecoverableDoublewriteStrategy(dw);
            writeCompleted(strategy, stamped(p2, 10, 10));
            writeCompleted(strategy, stamped(p5, 20, 20));
            long fullSize = Files.size(path);

            writeCompleted(strategy, stamped(p2, 30, 30));

            assertEquals(fullSize, Files.size(path), "completed slots must be reused instead of appending forever");
            assertEquals(Set.of(p2, p5), Set.copyOf(dw.pageIds()));
            assertEquals(30, dw.latestCopy(p2).orElseThrow()[200]);
        }
    }

    @Test
    void doesNotOverwriteInFlightSlotBeforeDataFileWriteCompletes() {
        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(dir.resolve("dw.dat"), PS, 1)) {
            RecoverableDoublewriteStrategy strategy = new RecoverableDoublewriteStrategy(dw);
            FlushPageSnapshot p2 = stamped(PageId.of(SPACE, PageNo.of(2)), 10, 10);
            FlushPageSnapshot p5 = stamped(PageId.of(SPACE, PageNo.of(5)), 20, 20);

            strategy.beforeDataFileWrite(p2);

            assertThrows(FlushWriteException.class, () -> strategy.beforeDataFileWrite(p5),
                    "尚未完成 data file force 的 slot 不能被后续 flush 覆盖");

            strategy.afterDataFileWrite(p2);
            assertDoesNotThrow(() -> writeCompleted(strategy, p5));
        }
    }

    @Test
    void skipsCorruptSlotWhenEnumeratingAndChoosingLatestCopy() throws Exception {
        Path path = dir.resolve("dw.dat");
        PageId p2 = PageId.of(SPACE, PageNo.of(2));
        PageId p5 = PageId.of(SPACE, PageNo.of(5));
        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(path, PS, 2)) {
            RecoverableDoublewriteStrategy strategy = new RecoverableDoublewriteStrategy(dw);
            writeCompleted(strategy, stamped(p2, 10, 10));
            writeCompleted(strategy, stamped(p5, 20, 20));
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer corruptMagic = ByteBuffer.allocate(Integer.BYTES);
            corruptMagic.putInt(0x11111111);
            corruptMagic.flip();
            channel.write(corruptMagic, 0);
        }

        try (DoublewriteFileRepository dw = DoublewriteFileRepository.open(path, PS, 2)) {
            assertEquals(Set.of(p5), Set.copyOf(dw.pageIds()));
            assertFalse(dw.latestCopy(p2).isPresent());
            assertEquals(20, dw.latestCopy(p5).orElseThrow()[200]);
        }
    }

    /** 模拟 FlushCoordinator 中 data file force 成功后的策略回调，使 slot 可被后续 flush 复用。 */
    private static void writeCompleted(RecoverableDoublewriteStrategy strategy, FlushPageSnapshot snapshot) {
        strategy.beforeDataFileWrite(snapshot);
        strategy.afterDataFileWrite(snapshot);
    }

    /** 构造带可区分 payload 的有效页镜像；page LSN 单调递增时，同页恢复应选择最新副本。 */
    private static FlushPageSnapshot stamped(PageId pageId, long pageLsn, int marker) {
        byte[] image = new byte[PS.bytes()];
        ByteBuffer.wrap(image).putLong(PageEnvelopeLayout.PAGE_LSN, pageLsn);
        image[200] = (byte) marker;
        PageImageChecksum.stamp(image, PS);
        return new FlushPageSnapshot(pageId, Lsn.of(pageLsn), pageLsn, image);
    }
}
