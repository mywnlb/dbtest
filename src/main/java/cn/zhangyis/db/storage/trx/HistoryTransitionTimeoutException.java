package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** history append/unlink 等待另一个跨 IO 转换结束超时；尚未修改任何新页面，可由上层重试。 */
public final class HistoryTransitionTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建 {@code HistoryTransitionTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public HistoryTransitionTimeoutException(String message) {
        super(message);
    }
}
