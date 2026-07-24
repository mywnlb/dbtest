package cn.zhangyis.db.sql.executor.row;

import cn.zhangyis.db.sql.type.SqlValue;

/**
 * Executor 在当前 StorageCursor 位置可见的逻辑行。实现可以惰性 hydrate 外置 LOB，但不得
 * 暴露 record/page 引用；调用方只能在 cursor 下一次 advance 或 close 之前使用本视图。
 */
public interface SqlRowView {

    /**
     * 返回当前输出 schema 的列数。
     *
     * @return 非负列数；访问叶等于 table 列数，Project 后等于公开投影列数
     */
    int width();

    /**
     * 读取一个 SQL typed value；外置值可在本调用内按需 hydrate。
     *
     * @param ordinal 当前视图 schema 中的列位置；必须满足 {@code 0 <= ordinal < width()}
     * @return 不含 storage reference 的 SQL 值；SQL NULL 使用 {@link SqlValue.NullValue}
     */
    SqlValue valueAt(int ordinal);

    /**
     * 判断列是否为 SQL NULL；该操作不得为了 NULL 判断 hydrate 外置 LOB。
     *
     * @param ordinal 当前视图 schema 中的合法列位置
     * @return 仅显式 SQL NULL 时为 {@code true}
     */
    boolean isNullAt(int ordinal);

    /**
     * 使用 exact DD/Record 类型和 collation 比较当前列与已绑定 literal。
     *
     * @param ordinal 当前视图 schema 中的合法列位置
     * @param literal Binder 已转换为该列 exact SQL 类型的非 NULL literal
     * @return 小于、等于或大于的负数、零或正数
     */
    int compareLiteral(int ordinal, SqlValue literal);

    /**
     * 按 relation/local ordinal 读取列。单表视图只接受 relation 0；连接视图覆盖本方法。
     *
     * @param relationOrdinal statement 输入序号
     * @param columnOrdinal relation 内的列位置
     * @return 当前 typed SQL value
     */
    default SqlValue valueAt(
            int relationOrdinal, int columnOrdinal) {
        requireSingleRelation(relationOrdinal);
        return valueAt(columnOrdinal);
    }

    /**
     * relation-aware NULL 探针；默认单表映射到既有 local ordinal。
     */
    default boolean isNullAt(
            int relationOrdinal, int columnOrdinal) {
        requireSingleRelation(relationOrdinal);
        return isNullAt(columnOrdinal);
    }

    /**
     * relation-aware exact comparator；默认单表映射到既有 local ordinal。
     */
    default int compareLiteral(
            int relationOrdinal, int columnOrdinal,
            SqlValue literal) {
        requireSingleRelation(relationOrdinal);
        return compareLiteral(columnOrdinal, literal);
    }

    private static void requireSingleRelation(
            int relationOrdinal) {
        if (relationOrdinal != 0) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "single-relation row only accepts relation ordinal 0");
        }
    }
}
