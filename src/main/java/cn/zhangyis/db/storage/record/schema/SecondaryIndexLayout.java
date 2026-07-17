package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.ArrayList;
import java.util.List;

/**
 * 二级索引 leaf entry 的不可变物理布局。它把完整聚簇行投影为“声明的二级 key parts + 完整聚簇主键 parts”，
 * 并保存每个物理字段对应的源表 ordinal。即使二级 key 已包含主键列，完整主键仍再次附加，令回表身份和物理
 * duplicate 检查不依赖隐式去重规则。
 *
 * <p>entry schema 始终非聚簇，不编码 DB_TRX_ID/DB_ROLL_PTR；事务可见性由调用方提取主键后回到聚簇索引判断。
 * 本类只负责纯值投影，不访问页、B+Tree、DD repository 或事务状态。</p>
 */
public final class SecondaryIndexLayout {

    /** 完整用户行的权威类型和 schema version。 */
    private final TableSchema tableSchema;
    /** 二级 leaf record 实际编码使用的紧凑 schema。 */
    private final TableSchema entrySchema;
    /** 覆盖全部 entry 字段的物理 key；indexId 与二级索引一致。 */
    private final IndexKeyDef physicalKeyDef;
    /** 每个 entry ordinal 对应的完整表 source ordinal，允许重复。 */
    private final List<Integer> sourceOrdinals;
    /** 声明的二级逻辑 key part 数；其后的字段全部属于聚簇主键后缀。 */
    private final int logicalKeyPartCount;
    /** 完整聚簇主键 part 数。 */
    private final int clusterKeyPartCount;

    /**
     * 冻结已经完成一致性校验的二级 entry 布局。
     *
     * @param tableSchema         完整聚簇表 schema，是所有 source ordinal 的解释上下文。
     * @param entrySchema         实际写入二级 leaf 的非聚簇紧凑 schema。
     * @param physicalKeyDef      覆盖“逻辑二级 key + 完整聚簇主键后缀”的物理 key definition。
     * @param sourceOrdinals      每个紧凑 entry 字段在完整表行中的来源 ordinal；构造时复制。
     * @param logicalKeyPartCount 紧凑 entry 前缀中属于声明二级 key 的字段数。
     * @param clusterKeyPartCount 紧凑 entry 尾部中属于完整聚簇主键的字段数。
     */
    private SecondaryIndexLayout(TableSchema tableSchema, TableSchema entrySchema,
                                 IndexKeyDef physicalKeyDef, List<Integer> sourceOrdinals,
                                 int logicalKeyPartCount, int clusterKeyPartCount) {
        this.tableSchema = tableSchema;
        this.entrySchema = entrySchema;
        this.physicalKeyDef = physicalKeyDef;
        this.sourceOrdinals = List.copyOf(sourceOrdinals);
        this.logicalKeyPartCount = logicalKeyPartCount;
        this.clusterKeyPartCount = clusterKeyPartCount;
    }

    /**
     * 从完整表 schema、二级逻辑 key 和聚簇 key 构造紧凑布局。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验输入 identity、聚簇属性和所有 source ordinal，避免 metadata 错配延迟到页解析期。</li>
     *     <li>按二级声明顺序复制逻辑 key 字段，保留 ASC/DESC 与 prefix；LOB/JSON 在此 fail-closed。</li>
     *     <li>按聚簇 key 顺序再次复制完整主键字段，强制 prefix=0，形成稳定回表后缀。</li>
     *     <li>发布非聚簇 entry schema 与覆盖全部字段的 physical key definition，不产生页或 redo 副作用。</li>
     * </ol>
     *
     * @param tableSchema  完整聚簇表 schema；必须标记为 clustered。
     * @param secondaryKey DD 声明的二级逻辑 key，保留 part 顺序、ASC/DESC 与 prefix 语义。
     * @param clusteredKey 聚簇索引的完整主键，用作每个二级 physical key 的稳定回表后缀。
     * @return 不可变的紧凑 entry schema、物理 key definition 和字段投影关系。
     * @throws DatabaseValidationException 输入缺失、索引 identity 冲突、source ordinal 越界，或 v1 不支持的
     *                                     LOB/JSON key 被声明时抛出。
     */
    public static SecondaryIndexLayout create(TableSchema tableSchema, IndexKeyDef secondaryKey,
                                              IndexKeyDef clusteredKey) {
        // 1. 先拒绝身份/范围错误，避免构造部分 layout 后再失败。
        if (tableSchema == null || secondaryKey == null || clusteredKey == null) {
            throw new DatabaseValidationException("secondary layout schema/keys must not be null");
        }
        if (!tableSchema.clustered()) {
            throw new DatabaseValidationException("secondary layout requires the complete clustered table schema");
        }
        if (secondaryKey.indexId() == clusteredKey.indexId()) {
            throw new DatabaseValidationException("secondary and clustered index ids must differ");
        }

        List<ColumnDef> physicalColumns = new ArrayList<>();
        List<KeyPartDef> physicalParts = new ArrayList<>();
        List<Integer> sources = new ArrayList<>();

        // 2. 复制二级逻辑字段；其比较属性是 SQL/唯一约束语义的权威来源。
        for (int i = 0; i < secondaryKey.parts().size(); i++) {
            KeyPartDef sourcePart = secondaryKey.parts().get(i);
            ColumnDef sourceColumn = sourceColumn(tableSchema, sourcePart.columnId().value(), "secondary");
            if (sourceColumn.type().storageKind() == StorageKind.OVERFLOW_CAPABLE) {
                throw new DatabaseValidationException(
                        "secondary v1 does not support LOB/JSON key column: " + sourceColumn.name());
            }
            int physicalOrdinal = physicalColumns.size();
            physicalColumns.add(physicalColumn(physicalOrdinal, "sec$" + i + "$" + sourceColumn.name(),
                    sourceColumn.type()));
            physicalParts.add(new KeyPartDef(new ColumnId(physicalOrdinal), sourcePart.order(),
                    sourcePart.prefixBytes()));
            sources.add(sourceColumn.ordinal());
        }

        // 3. 主键后缀始终完整复制；即使逻辑 key 已含同列也不去重。
        for (int i = 0; i < clusteredKey.parts().size(); i++) {
            KeyPartDef sourcePart = clusteredKey.parts().get(i);
            ColumnDef sourceColumn = sourceColumn(tableSchema, sourcePart.columnId().value(), "clustered");
            if (sourceColumn.type().storageKind() == StorageKind.OVERFLOW_CAPABLE) {
                throw new DatabaseValidationException(
                        "clustered key used by secondary layout cannot be LOB/JSON: " + sourceColumn.name());
            }
            int physicalOrdinal = physicalColumns.size();
            physicalColumns.add(physicalColumn(physicalOrdinal, "pk$" + i + "$" + sourceColumn.name(),
                    sourceColumn.type()));
            physicalParts.add(new KeyPartDef(new ColumnId(physicalOrdinal), sourcePart.order(), 0));
            sources.add(sourceColumn.ordinal());
        }

        // 4. 构造后对象完全不可变；本阶段没有 IO、锁或 redo 副作用。
        TableSchema entrySchema = new TableSchema(tableSchema.schemaVersion(), physicalColumns, false);
        return new SecondaryIndexLayout(tableSchema, entrySchema,
                new IndexKeyDef(secondaryKey.indexId(), physicalParts), sources,
                secondaryKey.parts().size(), clusteredKey.parts().size());
    }

    /**
     * 把完整聚簇行投影为紧凑二级 entry；DB_TRX_ID/DB_ROLL_PTR 等隐藏列不会进入二级记录。
     *
     * @param row     与 {@link #tableSchema()} 版本和列数一致的完整表行。
     * @param deleted 目标二级 entry 是否以 delete-marked 状态物化。
     * @return 按 source ordinal 投影、采用 entry schema version 且不含隐藏列的 conventional record。
     * @throws DatabaseValidationException 行为空、schema version 或列数不匹配时抛出。
     */
    public LogicalRecord toEntry(LogicalRecord row, boolean deleted) {
        requireTableRow(row);
        List<ColumnValue> values = sourceOrdinals.stream().map(row.columnValues()::get).toList();
        return new LogicalRecord(entrySchema.schemaVersion(), values, deleted, RecordType.CONVENTIONAL, null);
    }

    /**
     * 从紧凑 entry 提取 DD 声明的逻辑二级 key 前缀。
     *
     * @param entry 与当前 layout 精确匹配的二级物理记录。
     * @return 仅含前 {@code logicalKeyPartCount} 个值的搜索键，用于 logical unique 比较与加锁。
     * @throws DatabaseValidationException entry 形状或 schema version 不匹配时抛出。
     */
    public SearchKey logicalKey(LogicalRecord entry) {
        requireEntry(entry);
        return new SearchKey(List.copyOf(entry.columnValues().subList(0, logicalKeyPartCount)));
    }

    /**
     * 从紧凑 entry 尾部提取回表所需的完整聚簇主键。
     *
     * @param entry 与当前 layout 精确匹配的二级物理记录。
     * @return 保持聚簇 key part 顺序的完整搜索键；不能用 logical key 代替。
     * @throws DatabaseValidationException entry 形状或 schema version 不匹配时抛出。
     */
    public SearchKey clusterKey(LogicalRecord entry) {
        requireEntry(entry);
        int from = logicalKeyPartCount;
        return new SearchKey(List.copyOf(entry.columnValues().subList(from, from + clusterKeyPartCount)));
    }

    /**
     * 从紧凑 entry 提取覆盖全部字段的物理 B+Tree key。
     *
     * @param entry 与当前 layout 精确匹配的二级物理记录。
     * @return “逻辑二级 key + 完整聚簇主键后缀”的完整 physical identity。
     * @throws DatabaseValidationException entry 形状或 schema version 不匹配时抛出。
     */
    public SearchKey physicalKey(LogicalRecord entry) {
        requireEntry(entry);
        return new SearchKey(entry.columnValues());
    }

    /**
     * 返回完整表行的权威 schema。
     *
     * @return 创建 layout 时固定的 clustered table schema。
     */
    public TableSchema tableSchema() {
        return tableSchema;
    }

    /**
     * 返回二级 leaf record 的紧凑 schema。
     *
     * @return 非聚簇且不含隐藏列定义的 entry schema。
     */
    public TableSchema entrySchema() {
        return entrySchema;
    }

    /**
     * 返回覆盖全部紧凑字段的物理 key definition。
     *
     * @return 与二级 B+Tree descriptor 精确匹配的 key definition。
     */
    public IndexKeyDef physicalKeyDef() {
        return physicalKeyDef;
    }

    /**
     * 返回 entry 字段到完整表列的投影关系。
     *
     * @return 不可变 source ordinal 列表；列表允许同一表列因 key 后缀规则重复出现。
     */
    public List<Integer> sourceOrdinals() {
        return sourceOrdinals;
    }

    /**
     * 返回紧凑 entry 中 logical secondary key 的字段数。
     *
     * @return 大于零且等于 DD secondary key part 数的计数。
     */
    public int logicalKeyPartCount() {
        return logicalKeyPartCount;
    }

    /**
     * 返回紧凑 entry 尾部完整聚簇主键的字段数。
     *
     * @return 大于零且等于 clustered key part 数的计数。
     */
    public int clusterKeyPartCount() {
        return clusterKeyPartCount;
    }

    /**
     * 校验投影输入确实是当前 layout 所属表版本的完整行。
     *
     * @param row 待投影的聚簇完整行。
     * @throws DatabaseValidationException 行为空、schema version 或列数不匹配时抛出。
     */
    private void requireTableRow(LogicalRecord row) {
        if (row == null || row.schemaVersion() != tableSchema.schemaVersion()
                || row.columnValues().size() != tableSchema.columnCount()) {
            throw new DatabaseValidationException("secondary projection row does not match complete table schema");
        }
    }

    /**
     * 校验 key 提取输入确实是当前 layout 编码的紧凑二级 entry。
     *
     * @param entry 待提取 logical/cluster/physical key 的二级记录。
     * @throws DatabaseValidationException 记录为空、版本/列数不匹配或错误携带聚簇隐藏列时抛出。
     */
    private void requireEntry(LogicalRecord entry) {
        if (entry == null || entry.schemaVersion() != entrySchema.schemaVersion()
                || entry.columnValues().size() != entrySchema.columnCount() || entry.hiddenColumns() != null) {
            throw new DatabaseValidationException("secondary entry does not match compact non-clustered schema");
        }
    }

    /**
     * 按 key part 引用解析完整表列，并在布局创建期拒绝越界 ordinal。
     *
     * @param schema  完整聚簇表 schema。
     * @param ordinal key part 声明的 source column ordinal。
     * @param role    用于异常诊断的索引角色，如 secondary 或 clustered。
     * @return 对应 source ordinal 的列定义。
     * @throws DatabaseValidationException ordinal 不在 schema 列范围内时抛出。
     */
    private static ColumnDef sourceColumn(TableSchema schema, int ordinal, String role) {
        if (ordinal < 0 || ordinal >= schema.columnCount()) {
            throw new DatabaseValidationException(role + " key references missing source ordinal: " + ordinal);
        }
        return schema.column(ordinal);
    }

    /**
     * 创建供紧凑 entry schema 使用的物理列定义。
     *
     * @param ordinal entry schema 中连续且从零开始的物理 ordinal。
     * @param name    仅用于诊断的派生列名，包含 logical/clustered 来源角色。
     * @param type    从完整表 source column 复制的稳定类型。
     * @return column id 与 ordinal 一致的紧凑列定义。
     */
    private static ColumnDef physicalColumn(int ordinal, String name, ColumnType type) {
        return new ColumnDef(new ColumnId(ordinal), name, type, ordinal);
    }
}
