package cn.zhangyis.db.engine.adapter.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * DD immutable aggregate 无法形成权威 storage metadata 快照。该异常表示 catalog/binding 不完整、对象生命周期不允许
 * 打开，或逻辑 index 与物理 binding 不一致；调用方不能猜测默认 root/segment 继续执行。
 */
public class DictionaryStorageMappingException extends DatabaseRuntimeException {

    /** 创建只包含领域上下文的映射异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public DictionaryStorageMappingException(String message) {
        super(message);
    }

    /** 创建保留底层校验根因的映射异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public DictionaryStorageMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
