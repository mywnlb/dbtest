package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 事务提交序号（DB_TRX_NO）。commit 时由 {@code TransactionManager} 给读写事务分配，单调递增；
 * 用于后续 purge 边界与 history list 排序。{@code 0} 为 NONE 哨兵，表示未提交或只读事务未分配。
 *
 * @param value 非负提交序号；{@code 0} 表示未分配。
 */
public record TransactionNo(long value) {

    /** 未分配提交序号哨兵（未提交/只读事务）。 */
    public static final TransactionNo NONE = new TransactionNo(0);

    public TransactionNo {
        if (value < 0) {
            throw new DatabaseValidationException("transaction no must be non-negative: " + value);
        }
    }

    /** 构造一个提交序号。 */
    public static TransactionNo of(long value) {
        return new TransactionNo(value);
    }

    /** 是否为「未分配」哨兵。 */
    public boolean isNone() {
        return value == 0;
    }
}
