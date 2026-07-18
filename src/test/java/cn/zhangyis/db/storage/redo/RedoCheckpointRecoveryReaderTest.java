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

    /**
     * 验证 {@code checkpointSkipsBatchesWhoseEndLsnIsCovered} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
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

    /**
     * 验证 {@code checkpointInsideBatchKeepsBatchForPageLsnIdempotence} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
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

    /**
     * 验证 {@code checkpointAheadOfRedoIsReportedAsCorruption} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
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

    /**
     * 文件环回收后，第一条保留批次可以从非零 LSN 开始，但 checkpoint 必须已经覆盖此前区间；
     * 否则恢复所需 redo 已被回收，不能把缺口误当作“无需 replay”。
     */
    @Test
    void checkpointBeforeFirstRetainedBatchIsReportedAsCorruption() {
        RedoLogBatch retained = batchAt(100, new PageInitRecord(PAGE, PageType.INDEX));
        RedoRecoveryReader reader = new RedoRecoveryReader(
                new StubRedoRepository(List.of(retained)), Lsn.of(99));

        assertThrows(RedoLogCorruptedException.class, reader::readBatches);
    }

    /**
     * repository 返回的完整批次必须首尾相接；即使每个批次自身可解码，中间缺失一个 LSN 区间也表示
     * ring 文件丢失、错误排序或控制信息不一致，recovery 必须 fail closed。
     */
    @Test
    void gapBetweenRetainedBatchesIsReportedAsCorruption() {
        RedoLogBatch first = batchAt(0, new PageInitRecord(PAGE, PageType.INDEX));
        RedoLogBatch second = batchAt(first.range().end().value() + 1,
                new PageBytesRecord(PAGE, PAYLOAD_OFFSET, new byte[]{7}));
        RedoRecoveryReader reader = new RedoRecoveryReader(
                new StubRedoRepository(List.of(first, second)), Lsn.of(0));

        assertThrows(RedoLogCorruptedException.class, reader::readBatches);
    }

    private static RedoLogBatch batchAt(long startLsn, RedoRecord record) {
        return new RedoLogBatch(new LogRange(
                Lsn.of(startLsn), Lsn.of(startLsn + record.byteLength())), List.of(record));
    }

    /** 仅为恢复读取器边界测试提供确定批次，不接触磁盘和写路径。 */
    private static final class StubRedoRepository implements RedoLogFileRepository {

        private final List<RedoLogBatch> batches;

        private StubRedoRepository(List<RedoLogBatch> batches) {
            this.batches = List.copyOf(batches);
        }

        @Override
        public void append(RedoLogBatch batch) {
            throw new AssertionError("recovery reader must not append redo");
        }

        @Override
        public void force() {
            throw new AssertionError("recovery reader must not force redo");
        }

        @Override
        public List<RedoLogBatch> readBatches() {
            return batches;
        }

        @Override
        public void close() {
            // 内存 stub 无需释放资源。
        }
    }
}
