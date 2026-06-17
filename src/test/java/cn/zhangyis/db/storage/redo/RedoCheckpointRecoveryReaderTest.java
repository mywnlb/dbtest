package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.page.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * R2 redo recovery reader 测试：checkpoint label 覆盖的旧批次不再交给 replay，避免重复扫描已 checkpoint 的历史。
 */
class RedoCheckpointRecoveryReaderTest {

    private static final PageId PAGE = PageId.of(SpaceId.of(1), PageNo.of(3));
    private static final int PAYLOAD_OFFSET = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES + 32;

    @TempDir
    Path dir;

    @Test
    void checkpointSkipsBatchesWhoseEndLsnIsCovered() {
        Path redo = dir.resolve("redo.log");
        LogRange first;
        LogRange second;

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redo)) {
            RedoLogManager manager = RedoLogManager.durable(repo);
            first = manager.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            second = manager.append(List.of(new PageBytesRecord(PAGE, PAYLOAD_OFFSET, new byte[]{9, 9, 9})));
            manager.flush();
        }

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redo)) {
            RedoRecoveryReader reader = new RedoRecoveryReader(repo, first.end());
            List<RedoLogBatch> batches = reader.readBatches();

            assertEquals(1, batches.size());
            assertEquals(second, batches.get(0).range());
            assertEquals(second.end(), reader.recoveredToLsn());
        }
    }

    @Test
    void checkpointInsideBatchKeepsBatchForPageLsnIdempotence() {
        Path redo = dir.resolve("redo.log");
        LogRange range;

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redo)) {
            RedoLogManager manager = RedoLogManager.durable(repo);
            range = manager.append(List.of(
                    new PageInitRecord(PAGE, PageType.INDEX),
                    new PageBytesRecord(PAGE, PAYLOAD_OFFSET, new byte[]{1, 2, 3})));
            manager.flush();
        }

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redo)) {
            Lsn cutThroughBatch = Lsn.of(range.start().value() + 1);
            RedoRecoveryReader reader = new RedoRecoveryReader(repo, cutThroughBatch);
            List<RedoLogBatch> batches = reader.readBatches();

            assertEquals(1, batches.size());
            assertEquals(range, batches.get(0).range());
            assertEquals(range.end(), reader.recoveredToLsn());
        }
    }

    @Test
    void checkpointAheadOfRedoIsReportedAsCorruption() {
        Path redo = dir.resolve("redo.log");
        LogRange only;

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redo)) {
            RedoLogManager manager = RedoLogManager.durable(repo);
            only = manager.append(List.of(new PageInitRecord(PAGE, PageType.INDEX)));
            manager.flush();
        }

        try (RedoLogFileRepository repo = RedoLogFileRepository.open(redo)) {
            // checkpoint 落在最后一条完整 redo 批次之后：control 文件领先于 redo（截断/丢失/不匹配）。
            // 必须报致命损坏，而不是静默返回空 replay 集让页停留在 checkpoint 之前的半成品状态。
            RedoRecoveryReader reader = new RedoRecoveryReader(repo, Lsn.of(only.end().value() + 1));
            assertThrows(RedoLogCorruptedException.class, reader::readBatches);
        }
    }
}
