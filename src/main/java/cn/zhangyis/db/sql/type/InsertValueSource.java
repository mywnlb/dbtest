package cn.zhangyis.db.sql.type;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * INSERT 单元格的值来源。Binder 只能发布已类型化常量或自增请求，禁止以 Java {@code null}
 * 暗示稍后生成，否则批量行在优化和执行层无法区分“缺失 bug”与“合法自增”。
 */
public sealed interface InsertValueSource
        permits InsertValueSource.Constant, InsertValueSource.AutoIncrement {

    /**
     * 已完成列类型强制转换的常量来源。
     *
     * @param value 包含 SQL NULL 在内的非 Java-null 类型值
     */
    record Constant(SqlValue value) implements InsertValueSource {
        public Constant {
            if (value == null) {
                throw new DatabaseValidationException(
                        "INSERT constant source must contain a SQL value");
            }
        }
    }

    /** 自增分配请求；无可变状态，物理 high-water 由 storage API 决定。 */
    enum AutoIncrement implements InsertValueSource {
        /** 当前单元格必须在语句执行前取得一个持久自增值。 */
        INSTANCE
    }
}
