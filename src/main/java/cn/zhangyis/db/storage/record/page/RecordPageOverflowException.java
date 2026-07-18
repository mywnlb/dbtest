package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 页内记录区/目录空间不足（innodb-record-design §13.4 RecordPageOverflow 信号）。
 * 这是预期内的可恢复结果，不代表损坏：B+Tree 收到后应释放当前 page latch 并走 split 流程申请新页，再重试插入。
 */
public class RecordPageOverflowException extends DatabaseRuntimeException {

    /**
     * 创建 {@code RecordPageOverflowException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public RecordPageOverflowException(String message) {
        super(message);
    }

    /**
     * 创建 {@code RecordPageOverflowException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public RecordPageOverflowException(String message, Throwable cause) {
        super(message, cause);
    }
}
