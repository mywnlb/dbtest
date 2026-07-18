package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * mini-transaction 状态/绑定异常（设计 §17）。用于非法状态流转、终态后复用、嵌套 begin、
 * 跨线程或未绑定 commit/rollback、无当前 MTR 时 current()、savepoint 跨 MTR 误用。
 * 可恢复运行时异常：调用方应回滚或重建 MTR。
 */
public class MtrStateException extends DatabaseRuntimeException {

    /**
     * 创建 {@code MtrStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public MtrStateException(String message) {
        super(message);
    }

    /**
     * 创建 {@code MtrStateException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public MtrStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
