package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/** 编码后的记录超过 inline 上限（本片以 2 字节 recordLength 上限 65535 近似；overflow 链未实现）。可恢复。 */
public class RecordTooLargeException extends DatabaseRuntimeException {

    /**
     * 创建 {@code RecordTooLargeException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public RecordTooLargeException(String message) {
        super(message);
    }

    /**
     * 创建 {@code RecordTooLargeException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public RecordTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }
}
