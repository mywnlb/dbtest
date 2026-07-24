package cn.zhangyis.db.sql.executor.row;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 只保存 ordinal 映射的零拷贝投影视图。其有效期与输入 row view 完全一致，LOB hydration 和
 * exact comparison 仍委托给访问叶提供的权威语义。
 */
public final class ProjectedSqlRowView implements SqlRowView {
    /** 当前 cursor 行；advance 后由其实现拒绝旧视图访问。 */
    private final SqlRowView input;
    /** 输出 ordinal 到输入 ordinal 的不可变映射。 */
    private final List<Integer> ordinals;

    /**
     * 创建投影视图。
     *
     * @param input 当前有效的 child row view
     * @param ordinals 已由 PhysicalProject 校验的输入列位置
     * @throws DatabaseValidationException 输入或 ordinal 缺失、越界时抛出
     */
    public ProjectedSqlRowView(SqlRowView input, List<Integer> ordinals) {
        if (input == null || ordinals == null || ordinals.isEmpty()) {
            throw new DatabaseValidationException(
                    "projected row input/ordinals must not be null or empty");
        }
        for (Integer ordinal : ordinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= input.width()) {
                throw new DatabaseValidationException(
                        "projected row ordinal is outside child schema");
            }
        }
        this.input = input;
        this.ordinals = List.copyOf(ordinals);
    }

    @Override
    public int width() {
        return ordinals.size();
    }

    @Override
    public SqlValue valueAt(int ordinal) {
        return input.valueAt(mapped(ordinal));
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return input.isNullAt(mapped(ordinal));
    }

    @Override
    public int compareLiteral(int ordinal, SqlValue literal) {
        return input.compareLiteral(mapped(ordinal), literal);
    }

    /**
     * 校验输出位置并转换为 child ordinal。
     *
     * @param ordinal 当前投影视图位置
     * @return 对应 child schema 位置
     * @throws DatabaseValidationException ordinal 越界时抛出
     */
    private int mapped(int ordinal) {
        if (ordinal < 0 || ordinal >= ordinals.size()) {
            throw new DatabaseValidationException(
                    "projected row ordinal is outside output schema");
        }
        return ordinals.get(ordinal);
    }
}
