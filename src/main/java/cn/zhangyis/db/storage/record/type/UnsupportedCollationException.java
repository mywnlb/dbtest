package cn.zhangyis.db.storage.record.type;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** charset/collation 组合未注册或不兼容；比较不得静默回退，否则会破坏 B+Tree 全局顺序。 */
public class UnsupportedCollationException extends DatabaseRuntimeException {

    /** 创建不支持排序规则异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UnsupportedCollationException(String message) {
        super(message);
    }

    /** 创建保留根因的不支持排序规则异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UnsupportedCollationException(String message, Throwable cause) {
        super(message, cause);
    }
}
