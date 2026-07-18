package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Redo 文件环容量耗尽。
 *
 * <p>当文件环里所有文件都还持有「尚未被 checkpoint 覆盖」的 redo（即没有任何文件的最高 LSN 落在回收边界之内），
 * 轮转时无文件可复用。此时绝不能覆盖未 checkpoint 的 redo（否则崩溃恢复会丢失必须 replay 的修改），因此 append
 * fail-closed 抛出本异常。
 *
 * <p>它是 {@link DatabaseRuntimeException}：调用方应推进 checkpoint（刷脏 + 写 checkpoint label）让旧文件可回收后重试，
 * 而不是直接崩溃。真正的容量压力 throttle（async/sync/hard 分级前台等待）是后续 0.6 的职责，本异常只是底线保护。
 */
public class RedoLogCapacityExceededException extends DatabaseRuntimeException {

    /**
     * 创建 {@code RedoLogCapacityExceededException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public RedoLogCapacityExceededException(String message) {
        super(message);
    }
}
