package cn.zhangyis.db.storage.api.undotruncate;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** undo 截断状态、格式或持久化边界不满足；marker 已落盘时调用方应通过 recovery 续作。 */
public class UndoTablespaceTruncationException extends DatabaseRuntimeException {

    /**
     * 创建 {@code UndoTablespaceTruncationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoTablespaceTruncationException(String message) {
        super(message);
    }

    /**
     * 创建 {@code UndoTablespaceTruncationException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UndoTablespaceTruncationException(String message, Throwable cause) {
        super(message, cause);
    }
}
