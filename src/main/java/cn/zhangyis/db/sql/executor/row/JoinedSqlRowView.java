package cn.zhangyis.db.sql.executor.row;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.type.SqlValue;

/**
 * 当前 outer/inner 行的零拷贝连接视图。扁平 ordinal 先覆盖左行再覆盖右行；
 * relation-aware 访问保留 Binder 的 local ordinal。任一 child cursor 前进后，其自身
 * generation 校验会使本视图随即失效。
 */
public final class JoinedSqlRowView implements SqlRowView {
    /** 当前仍有效的 SQL 左行。 */
    private final SqlRowView left;
    /** 当前仍有效的 SQL 右行。 */
    private final SqlRowView right;
    /** 构造时稳定的左行宽度，用于扁平 ordinal 分界。 */
    private final int leftWidth;
    /** 构造时稳定的右行宽度，用于总宽度与边界校验。 */
    private final int rightWidth;

    /**
     * @param left 当前 outer cursor row
     * @param right 当前 inner cursor row
     * @throws DatabaseValidationException 任一视图缺失时抛出
     */
    public JoinedSqlRowView(
            SqlRowView left, SqlRowView right) {
        if (left == null || right == null) {
            throw new DatabaseValidationException(
                    "joined row requires left and right views");
        }
        this.left = left;
        this.right = right;
        this.leftWidth = left.width();
        this.rightWidth = right.width();
    }

    @Override
    public int width() {
        return leftWidth + rightWidth;
    }

    @Override
    public SqlValue valueAt(int ordinal) {
        return ordinal < leftWidth
                ? left.valueAt(requireFlat(ordinal))
                : right.valueAt(
                requireFlat(ordinal) - leftWidth);
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return ordinal < leftWidth
                ? left.isNullAt(requireFlat(ordinal))
                : right.isNullAt(
                requireFlat(ordinal) - leftWidth);
    }

    @Override
    public int compareLiteral(
            int ordinal, SqlValue literal) {
        return ordinal < leftWidth
                ? left.compareLiteral(
                requireFlat(ordinal), literal)
                : right.compareLiteral(
                requireFlat(ordinal) - leftWidth,
                literal);
    }

    @Override
    public SqlValue valueAt(
            int relationOrdinal, int columnOrdinal) {
        return relation(relationOrdinal)
                .valueAt(columnOrdinal);
    }

    @Override
    public boolean isNullAt(
            int relationOrdinal, int columnOrdinal) {
        return relation(relationOrdinal)
                .isNullAt(columnOrdinal);
    }

    @Override
    public int compareLiteral(
            int relationOrdinal, int columnOrdinal,
            SqlValue literal) {
        return relation(relationOrdinal)
                .compareLiteral(columnOrdinal, literal);
    }

    private SqlRowView relation(int ordinal) {
        return switch (ordinal) {
            case 0 -> left;
            case 1 -> right;
            default -> throw new DatabaseValidationException(
                    "joined row relation ordinal must be 0 or 1");
        };
    }

    private int requireFlat(int ordinal) {
        if (ordinal < 0
                || ordinal >= leftWidth + rightWidth) {
            throw new DatabaseValidationException(
                    "joined row ordinal is outside schema");
        }
        return ordinal;
    }
}
