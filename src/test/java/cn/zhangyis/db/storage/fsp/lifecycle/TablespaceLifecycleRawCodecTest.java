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

    @Test
    void oldFormatLifecycleMagicZeroReturnsEmpty() {
        ByteBuffer page = ByteBuffer.allocate(PageSize.ofBytes(16 * 1024).bytes());

        assertTrue(TablespaceLifecycleRawCodec.read(page).isEmpty());
    }

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
