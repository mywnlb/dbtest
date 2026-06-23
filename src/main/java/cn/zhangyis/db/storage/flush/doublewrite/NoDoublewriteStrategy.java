package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;

/**
 * 关闭 doublewrite 的策略。它不提供 torn page 修复能力，只用于聚焦 WAL gate 或低可靠性测试。
 */
public final class NoDoublewriteStrategy implements DoublewriteStrategy {

    @Override
    public DoublewriteMode mode() {
        return DoublewriteMode.OFF;
    }

    @Override
    public void beforeDataFileWrite(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
    }
}
