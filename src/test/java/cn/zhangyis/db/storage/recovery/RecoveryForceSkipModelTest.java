package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.fil.io.FileChannelPageStore;
import cn.zhangyis.db.storage.redo.RedoApplyContext;
import cn.zhangyis.db.storage.redo.RedoApplyDispatcher;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogFileRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FORCE_SKIP_CORRUPT_TABLESPACE 的请求/报告模型测试。该模式必须显式携带不可变 skip policy，
 * 并把跳过诊断带到恢复报告，避免 normal/read-only 路径出现隐式跳过数据的行为。
 */
class RecoveryForceSkipModelTest {

    private static final PageSize PS = PageSize.ofBytes(16 * 1024);

    @TempDir
    Path dir;

    /**
     * 验证 {@code skipPolicyMatchesBySpaceAndPage} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test
    void skipPolicyMatchesBySpaceAndPage() {
        RecoverySkipPolicy policy = RecoverySkipPolicy.of(Set.of(SpaceId.of(7)));

        assertTrue(policy.shouldSkip(SpaceId.of(7)));
        assertTrue(policy.shouldSkip(PageId.of(SpaceId.of(7), PageNo.of(3))));
        assertFalse(policy.shouldSkip(SpaceId.of(8)));
        assertEquals(Set.of(SpaceId.of(7)), policy.skippedSpaces());
    }

    /**
     * 验证 {@code forceSkipRequestRequiresNonEmptySkippedSpaces} 所描述的空间分配或复用路径，并断言 extent/segment 所有权、链表和重复释放边界。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void forceSkipRequestRequiresNonEmptySkippedSpaces() throws Exception {
        try (FileChannelPageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            RedoApplyContext context = new RedoApplyContext(store, PS);

            assertThrows(DatabaseValidationException.class, () ->
                    RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                            RedoApplyDispatcher.pageDispatcher(), context, Set.of()));
        }
    }

    /**
     * 验证 {@code forceSkipRequestCarriesImmutablePolicy} 对应的崩溃恢复行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void forceSkipRequestCarriesImmutablePolicy() throws Exception {
        try (FileChannelPageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            RecoveryRequest request = RecoveryRequest.forceSkip(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), new RedoApplyContext(store, PS),
                    Set.of(SpaceId.of(11)));

            assertEquals(RecoveryMode.FORCE_SKIP_CORRUPT_TABLESPACE, request.mode());
            assertTrue(request.skipPolicy().shouldSkip(SpaceId.of(11)));
            assertThrows(UnsupportedOperationException.class,
                    () -> request.skipPolicy().skippedSpaces().add(SpaceId.of(12)));
        }
    }

    /**
     * 验证 {@code normalAndReadOnlyRequestsDoNotCarrySkipPolicy} 对应的崩溃恢复行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void normalAndReadOnlyRequestsDoNotCarrySkipPolicy() throws Exception {
        try (FileChannelPageStore store = new FileChannelPageStore();
             RedoLogFileRepository redoRepo = RedoLogFileRepository.open(dir.resolve("redo.log"));
             RedoCheckpointStore checkpointStore = RedoCheckpointStore.open(dir.resolve("redo-control"))) {
            RedoApplyContext context = new RedoApplyContext(store, PS);

            assertTrue(RecoveryRequest.normal(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), context).skipPolicy().isEmpty());
            assertTrue(RecoveryRequest.readOnlyValidate(checkpointStore, redoRepo,
                    RedoApplyDispatcher.pageDispatcher(), context).skipPolicy().isEmpty());
        }
    }

    /**
     * 验证 {@code forceSkipReportCapturesSkippedDiagnostics} 对应的崩溃恢复行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void forceSkipReportCapturesSkippedDiagnostics() {
        RecoveryReport report = RecoveryReport.forceSkip(RecoveryState.OPEN,
                Lsn.of(1), Lsn.of(9), 2, 1, 3,
                Set.of(SpaceId.of(7)), 4, 5, 6,
                List.of(RecoveryStageName.TRAFFIC_CLOSED, RecoveryStageName.OPEN_TRAFFIC));

        assertEquals(Set.of(SpaceId.of(7)), report.skippedSpaces());
        assertEquals(4, report.skippedDoublewritePageCount());
        assertEquals(5, report.skippedRedoRecordCount());
        assertEquals(6, report.skippedReconcileSpaceCount());
    }
}
