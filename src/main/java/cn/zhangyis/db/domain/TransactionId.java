package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务 id（对应聚簇记录隐藏列 DB_TRX_ID）。单调非负；{@code 0} 为 NONE 哨兵，表示只读或尚未分配写者。
 *
 * <p>读写事务首次写入时由 {@code TransactionManager} 分配单调 id；只读事务保持 {@link #NONE}。
 * 该值落到聚簇记录隐藏区 8 字节无符号字段，是 MVCC 可见性判断（后续片）的权威写者标识。
 *
 * @param value 非负事务序号；{@code 0} 表示无写者。
 */
public record TransactionId(long value) {

    /** 无写者哨兵（只读事务或尚未分配写 id）。 */
    public static final TransactionId NONE = new TransactionId(0);

    public TransactionId {
        if (value < 0) {
            throw new DatabaseValidationException("transaction id must be non-negative: " + value);
        }
    }

    /** 构造一个事务 id。 */
    public static TransactionId of(long value) {
        return new TransactionId(value);
    }

    /** 是否为「无写者」哨兵（只读/未分配）。 */
    public boolean isNone() {
        return value == 0;
    }
}
