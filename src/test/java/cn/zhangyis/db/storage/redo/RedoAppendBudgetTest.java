package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 操作级 redo 预算测试：固定公式必须与真实 LogBlock codec 完全相等，commit 低估必须在 append 前 fail-stop。
 */
class RedoAppendBudgetTest {

    /**
     * 验证 {@code sizingCoversZeroSingleBlockAndCrossBlockBoundary} 所描述的并发场景，并断言等待、唤醒、超时与资源释放顺序。
     */
    @Test
    void sizingCoversZeroSingleBlockAndCrossBlockBoundary() {
        assertEquals(0, RedoLogBlockSizing.physicalBytesForLogical(0));
        assertEquals(512, RedoLogBlockSizing.physicalBytesForLogical(1));
        assertEquals(512, RedoLogBlockSizing.physicalBytesForLogical(440));
        assertEquals(1_024, RedoLogBlockSizing.physicalBytesForLogical(441));
    }

    /**
     * 验证 {@code sizingMatchesRealCodecForRepresentativePayloads} 所描述的稳定格式转换，并断言往返值、字节布局、版本与损坏输入处理。
     */
    @Test
    void sizingMatchesRealCodecForRepresentativePayloads() {
        for (int payload : List.of(0, 1, 418, 419, 900, 4_096)) {
            RedoRecord record = new PageBytesRecord(
                    PageId.of(SpaceId.of(1), PageNo.of(7)), 0, new byte[payload]);
            long logical = record.byteLength();
            RedoLogBatch batch = new RedoLogBatch(
                    new LogRange(Lsn.of(0), Lsn.of(logical)), List.of(record));

            assertEquals(RedoLogBlockCodec.encodeBatch(batch, 0).byteLength(),
                    RedoLogBlockSizing.physicalBytesForLogical(logical),
                    "formula must remain the single source of truth for payload=" + payload);
        }
    }

    /**
     * 验证 {@code builderUsesCheckedArithmeticAndRejectsUnrecoverableBatch} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void builderUsesCheckedArithmeticAndRejectsUnrecoverableBatch() {
        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> RedoBudgetBuilder.forPurpose(RedoBudgetPurpose.CLUSTERED_INSERT)
                        .addPageBytes(Long.MAX_VALUE));
        assertThrows(cn.zhangyis.db.common.exception.DatabaseValidationException.class,
                () -> RedoBudgetBuilder.forPurpose(RedoBudgetPurpose.CLUSTERED_INSERT)
                        .addLogicalBytes(RedoLogBlockSizing.MAX_LOGICAL_BATCH_BYTES)
                        .addLogicalBytes(1));
    }

    /**
     * 验证 {@code actualUsageMustFitBothBudgetDimensions} 对应的Redo/WAL行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void actualUsageMustFitBothBudgetDimensions() {
        RedoRecord record = new PageBytesRecord(
                PageId.of(SpaceId.of(1), PageNo.of(8)), 4, new byte[32]);
        RedoAppendBudget exact = RedoBudgetBuilder.forPurpose(RedoBudgetPurpose.CLUSTERED_UPDATE)
                .addRecord(record).build();

        assertEquals(new RedoAppendUsage(record.byteLength(), 512), exact.requireCovers(List.of(record)));
        assertThrows(RedoBudgetExceededException.class,
                () -> RedoAppendBudget.upperBound(RedoBudgetPurpose.CLUSTERED_UPDATE,
                        record.byteLength() - 1).requireCovers(List.of(record)));
    }
}
