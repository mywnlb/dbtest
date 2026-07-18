package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 批量 secondary range materialization 超过教学实现的安全上限。调用方不得把已读取前缀当作完整 SQL 结果；
 * 可缩小查询范围，或在后续 cursor/fetch 协议落地后重试。
 */
public final class SecondaryRangeCapacityException extends DatabaseRuntimeException {

    /**
     * 创建 {@code SecondaryRangeCapacityException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public SecondaryRangeCapacityException(String message) {
        super(message);
    }

    /**
     * 创建 {@code SecondaryRangeCapacityException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public SecondaryRangeCapacityException(String message, Throwable cause) {
        super(message, cause);
    }
}
