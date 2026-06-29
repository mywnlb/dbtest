package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 0.18 redo 文件环仓储测试：轮转、跨文件恢复扫描、checkpoint 回收、环满 fail-closed。
 *
 * <p>容量以「单个文件可容纳的帧字节上限（不含文件头）」表达；测试把每文件容量设为恰好一个批次的帧大小，
 * 使轮转点精确可预测。批次按 LSN 连续追加（每批 {@code [k*L,(k+1)*L)}），与 {@link RedoLogManager} 的真实
 * append 语义一致。
 */
class RotatingRedoLogRepositoryTest {

    private static final SpaceId SPACE = SpaceId.of(1);
    private static final PageId P = PageId.of(SPACE, PageNo.of(3));
    private static final int PAYLOAD = 10;
    /** 单批逻辑 redo 字节数（= PageBytesRecord.byteLength），用于推算连续批次的 LSN 起点。 */
    private static final long L = PAYLOAD + 21;

    @TempDir
    Path dir;

    /** 构造一个起点为 start、固定大小的连续批次（单条 PAGE_BYTES）。 */
    private static RedoLogBatch batch(long start) {
        PageBytesRecord rec = new PageBytesRecord(P, 38, new byte[PAYLOAD]);
        return new RedoLogBatch(new LogRange(Lsn.of(start), Lsn.of(start + rec.byteLength())), List.of(rec));
    }

    /** 单批帧在磁盘上的字节数（含外层 frame header），用于把文件容量设成"恰好一批"。 */
    private static int oneFrameBytes() {
        return RedoBatchFrameCodec.encodeFrame(batch(0)).remaining();
    }

    @Test
    void rotatesToNextFileWhenActiveFull() {
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(dir, 3, oneFrameBytes())) {
            repo.append(batch(0));
            int firstActive = repo.activeFileId();

            repo.append(batch(L)); // 放不下第一个文件 → 轮转到下一个

            assertNotEquals(firstActive, repo.activeFileId(), "active full 后应轮转到新文件");
            List<RedoLogBatch> all = repo.readBatches();
            assertEquals(2, all.size());
            assertEquals(0L, all.get(0).range().start().value());
            assertEquals(L, all.get(1).range().start().value());
        }
    }

    @Test
    void recoveryScansAcrossRotatedFiles() {
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(dir, 3, oneFrameBytes())) {
            repo.append(batch(0));
            repo.append(batch(L));
            repo.append(batch(2 * L));
            repo.force();
        }
        // 重开（模拟崩溃后恢复）：跨三个文件按 LSN 顺序扫描出全部批次。
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(dir, 3, oneFrameBytes())) {
            List<RedoLogBatch> all = repo.readBatches();
            assertEquals(3, all.size());
            assertEquals(0L, all.get(0).range().start().value());
            assertEquals(L, all.get(1).range().start().value());
            assertEquals(2 * L, all.get(2).range().start().value());
        }
    }

    @Test
    void reclaimsFileOnlyBehindCheckpoint() {
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(dir, 2, oneFrameBytes())) {
            repo.append(batch(0));     // file0 = [0, L)
            repo.append(batch(L));     // file1 = [L, 2L)

            // 环已满：回收边界还是 0，没有文件落在 checkpoint 之内 → 不能覆盖未 checkpoint 的 redo。
            assertThrows(RedoLogCapacityExceededException.class, () -> repo.append(batch(2 * L)));

            // 推进 checkpoint 到 L：file0(end=L) 落入回收边界，可复用；file1(end=2L) 仍不可回收。
            repo.advanceReclaimBoundary(Lsn.of(L));
            repo.append(batch(2 * L)); // 复用 file0

            List<RedoLogBatch> all = repo.readBatches();
            assertEquals(2, all.size(), "旧 file0 的 [0,L) 被新一代覆盖，只剩 [L,2L) 与 [2L,3L)");
            assertEquals(L, all.get(0).range().start().value());
            assertEquals(2 * L, all.get(1).range().start().value());
        }
    }

    @Test
    void refusesAppendWhenRingFullWithoutCheckpointAndKeepsOldRedo() {
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(dir, 2, oneFrameBytes())) {
            repo.append(batch(0));
            repo.append(batch(L));

            assertThrows(RedoLogCapacityExceededException.class, () -> repo.append(batch(2 * L)));

            // 失败的 append 必须不破坏已有 redo（未做半轮转/半覆盖）。
            List<RedoLogBatch> all = repo.readBatches();
            assertEquals(2, all.size());
            assertEquals(0L, all.get(0).range().start().value());
            assertEquals(L, all.get(1).range().start().value());
        }
    }

    @Test
    void rejectsBatchLargerThanFileCapacity() {
        try (RotatingRedoLogRepository repo = RotatingRedoLogRepository.open(dir, 2, oneFrameBytes())) {
            PageBytesRecord big = new PageBytesRecord(P, 38, new byte[5000]);
            RedoLogBatch batch = new RedoLogBatch(
                    new LogRange(Lsn.of(0), Lsn.of(big.byteLength())), List.of(big));
            assertThrows(DatabaseValidationException.class, () -> repo.append(batch),
                    "单批超过文件容量是配置错误，不可重试");
        }
    }
}
