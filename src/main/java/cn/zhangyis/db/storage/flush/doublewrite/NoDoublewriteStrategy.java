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

    /**
     * 校验输入与当前状态后修改脏页刷盘与 checkpoint领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void beforeDataFileWrite(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
    }
}
