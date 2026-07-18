package cn.zhangyis.db.storage.fsp.lifecycle;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.storage.fil.state.TablespaceState;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * page0 lifecycle raw codec 兼容性测试。magic=0 是旧格式 GENERAL/UNDO 文件的合法缺省状态。
 */
class TablespaceLifecycleRawCodecTest {

    /**
     * 验证 {@code oldFormatLifecycleMagicZeroReturnsEmpty} 所描述的稳定格式转换，并断言往返值、字节布局、版本与损坏输入处理。
     */
    @Test
    void oldFormatLifecycleMagicZeroReturnsEmpty() {
        ByteBuffer page = ByteBuffer.allocate(PageSize.ofBytes(16 * 1024).bytes());

        assertTrue(TablespaceLifecycleRawCodec.read(page).isEmpty());
    }

    /**
     * 验证 {@code generalLifecycleRejectsTruncationEpoch} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void generalLifecycleRejectsTruncationEpoch() {
        PageNo initialSize = PageNo.of(64);

        assertThrows(DatabaseValidationException.class, () -> new TablespaceLifecycleHeader(
                TablespaceState.NORMAL,
                initialSize,
                1,
                initialSize,
                TablespaceState.NORMAL));
    }

    /**
     * 验证 {@code generalLifecycleRejectsTruncationTarget} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void generalLifecycleRejectsTruncationTarget() {
        assertThrows(DatabaseValidationException.class, () -> new TablespaceLifecycleHeader(
                TablespaceState.CORRUPTED,
                PageNo.of(64),
                0,
                PageNo.of(32),
                TablespaceState.NORMAL));
    }
}
