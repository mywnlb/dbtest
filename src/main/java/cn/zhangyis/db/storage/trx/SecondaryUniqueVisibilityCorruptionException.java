package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * logical unique 二级点查在同一 ReadView 中解析出多个不同聚簇行。该状态表示二级约束、回表版本链或 metadata
 * 已经不一致；调用方不得任选一行返回，否则会把物理损坏伪装成合法 SQL 结果。
 */
public final class SecondaryUniqueVisibilityCorruptionException extends DatabaseRuntimeException {

    /**
     * 创建保留 table/index/key 诊断上下文的可报告数据库异常。
     *
     * @param message 描述冲突 logical key 与不同聚簇 identity 的错误消息。
     */
    public SecondaryUniqueVisibilityCorruptionException(String message) {
        super(message);
    }

    /**
     * 包装比较、解码或版本构造过程中揭示唯一可见性损坏的底层根因。
     *
     * @param message 领域诊断消息。
     * @param cause   原始异常；不能丢失。
     */
    public SecondaryUniqueVisibilityCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
