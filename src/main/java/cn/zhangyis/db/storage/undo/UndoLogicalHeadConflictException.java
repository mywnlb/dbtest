package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 持久 logical undo 头与调用方预期不一致。该异常通常表示同一 undo segment 出现并发写者，或内存
 * {@code UndoContext} 已落后于页内权威状态；调用方必须停止 statement commit，并转入完整 rollback/恢复诊断，
 * 不能覆盖较新的持久边界。
 */
public class UndoLogicalHeadConflictException extends DatabaseRuntimeException {

    /**
     * 创建 {@code UndoLogicalHeadConflictException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoLogicalHeadConflictException(String message) {
        super(message);
    }

    /**
     * 创建 {@code UndoLogicalHeadConflictException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UndoLogicalHeadConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
