package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 0.18 文件环与既有 redo durable/recovery 链路的集成：证明 {@link RotatingRedoLogRepository} 可经
 * {@link RedoLogFileRepository} 接口直接替换单文件仓储，插入真实的
 * {@code RedoLogManager.durable → writer/flusher → RedoRecoveryReader} 流程，且跨轮转与恢复续写都保持 LSN 连续。
 */
class RotatingRedoRecoveryIntegrationTest {

    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId P = PageId.of(SPACE, PageNo.of(3));
    private static final int PAYLOAD = 10;

    @TempDir
    Path dir;

    private static PageBytesRecord record() {
        return new PageBytesRecord(P, 38, new byte[PAYLOAD]);
    }

    /** 单批帧在磁盘上的字节数，用于把每文件容量设成"恰好一批"或"恰好两批"。 */
    private static int oneFrameBytes() {
        PageBytesRecord r = record();
        return RedoBatchFrameCodec.encodeFrame(
                new RedoLogBatch(new LogRange(Lsn.of(0), Lsn.of(r.byteLength())), List.of(r))).remaining();
    }

    @Test
    void durableManagerRoundTripsThroughRotatedRing() {
        Lsn lastEnd;
        try (RotatingRedoLogRepository ring = RedoLogFileRepository.openRing(dir, 3, oneFrameBytes())) {
            RedoLogManager mgr = RedoLogManager.durable(ring);
            mgr.append(List.of(record()));
            mgr.append(List.of(record()));
            lastEnd = mgr.append(List.of(record())).end(); // 第三批 → 第三个文件
            assertEquals(lastEnd, mgr.flush(), "三批跨文件写出后 durable LSN 到达最后一批 end");
        }
        try (RotatingRedoLogRepository ring = RedoLogFileRepository.openRing(dir, 3, oneFrameBytes())) {
            RedoRecoveryReader reader = new RedoRecoveryReader(ring);
            List<RedoLogBatch> batches = reader.readBatches();
            assertEquals(3, batches.size(), "跨三个文件恢复出全部批次");
            assertEquals(lastEnd, reader.recoveredToLsn());
            assertEquals(0L, batches.get(0).range().start().value());
        }
    }

    @Test
    void continuesInActiveFileAfterRecoveredBoundary() {
        long capacity = 2L * oneFrameBytes(); // 每文件容两批，留出续写余量
        long firstEnd;
        try (RotatingRedoLogRepository ring = RedoLogFileRepository.openRing(dir, 3, capacity)) {
            RedoLogManager mgr = RedoLogManager.durable(ring);
            firstEnd = mgr.append(List.of(record())).end().value();
            mgr.flush();
        }
        long secondEnd;
        try (RotatingRedoLogRepository ring = RedoLogFileRepository.openRing(dir, 3, capacity)) {
            RedoRecoveryReader reader = new RedoRecoveryReader(ring);
            reader.readBatches();
            Lsn recoveredTo = reader.recoveredToLsn();
            assertEquals(firstEnd, recoveredTo.value());

            int activeBefore = ring.activeFileId();
            RedoLogManager mgr = RedoLogManager.durable(ring);
            mgr.restoreRecoveredBoundary(recoveredTo);
            secondEnd = mgr.append(List.of(record())).end().value();
            mgr.flush();
            assertEquals(activeBefore, ring.activeFileId(), "active 文件有余量，续写不触发轮转");
        }
        try (RotatingRedoLogRepository ring = RedoLogFileRepository.openRing(dir, 3, capacity)) {
            List<RedoLogBatch> batches = new RedoRecoveryReader(ring).readBatches();
            assertEquals(2, batches.size());
            assertEquals(0L, batches.get(0).range().start().value());
            assertEquals(firstEnd, batches.get(1).range().start().value(), "续写批次与恢复边界 LSN 连续");
            assertEquals(secondEnd, batches.get(1).range().end().value());
        }
    }
}
