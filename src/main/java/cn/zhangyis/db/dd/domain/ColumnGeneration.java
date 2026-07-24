package cn.zhangyis.db.dd.domain;

/**
 * 列值生成语义。该枚举属于数据字典逻辑定义，不保存物理计数器；自增 high-water 由表空间页 0
 * 持久化，DD 只决定某列是否允许请求该分配器。
 */
public enum ColumnGeneration {
    /** 普通列，缺失输入按 defaultDefinition 解析。 */
    NONE(0),
    /** 整数聚簇主键第一列，由 storage API 的持久 high-water 分配。 */
    AUTO_INCREMENT(1);

    /** catalog/SDI 使用的稳定编码，禁止持久化 Java ordinal。 */
    private final int stableCode;

    ColumnGeneration(int stableCode) {
        this.stableCode = stableCode;
    }

    /**
     * @return 跨版本稳定的持久编码
     */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 从可信格式边界内的稳定编码恢复生成语义。
     *
     * @param stableCode catalog/SDI 中读取的非负编码
     * @return 对应生成方式
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 编码未知时抛出；恢复方必须停止采用该元数据
     */
    public static ColumnGeneration fromStableCode(int stableCode) {
        return switch (stableCode) {
            case 0 -> NONE;
            case 1 -> AUTO_INCREMENT;
            default -> throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "unknown column generation stable code: " + stableCode);
        };
    }
}
