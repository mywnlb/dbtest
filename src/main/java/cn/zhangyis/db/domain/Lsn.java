package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Redo 逻辑序列号。WAL、pageLSN、checkpoint 后续都通过该值对象表达边界。
 *
 * @param value redo 日志逻辑序列号；越大的值代表越新的物理修改顺序。
 */
public record Lsn(long value) {

    public Lsn {
        if (value < 0) {
            throw new DatabaseValidationException("lsn must be non-negative");
        }
    }

    /**
     * 创建 redo LSN；负数不能表示有效 WAL 顺序边界。
     *
     * @param value redo 逻辑序列号。
     * @return 通过校验的 LSN。
     */
    public static Lsn of(long value) {
        return new Lsn(value);
    }
}
