package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 无法在超时内把 redo/dirty/checkpoint 安全边界推进到目标 LSN；生命周期操作应保持可恢复中间态。 */
public final class FlushBarrierTimeoutException extends DatabaseRuntimeException {

    /**
     * 创建 {@code FlushBarrierTimeoutException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public FlushBarrierTimeoutException(String message) {
        super(message);
    }
}
