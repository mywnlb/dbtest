package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.HashSet;
import java.util.List;

/**
 * 完整 non-unique logical secondary key 的物理 prefix-range 访问计划。当前切片只接受单 key-part、无 prefix
 * secondary，因此一个 SQL 等值谓词会在完整 physical key（logical key + clustered suffix）上返回多行。
 *
 * @param table              statement lease 固定的 exact DD table version。
 * @param projectionOrdinals 公开结果投影 ordinal。
 * @param accessIndexId      non-unique secondary stable id。
 * @param logicalKeyValues   按声明 logical key part 顺序完成类型转换的值。
 * @param lockMode           一致性读、共享锁定读或排他锁定读。
 */
public record BoundSecondaryRangeSelect(TableDefinition table, List<Integer> projectionOrdinals,
                                        long accessIndexId, List<SqlValue> logicalKeyValues,
                                        SelectLockMode lockMode) implements BoundStatement {

    /**
     * 冻结并校验 non-unique secondary prefix-range plan。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验必填字段和 logical value 容器，阻止不完整计划进入 Executor。</li>
     *     <li>在 exact table version 中解析 stable index，确认它是单列、无 prefix、普通 non-unique secondary。</li>
     *     <li>拒绝 LOB/JSON key，避免 SQL range 旁路当前不支持的外置 key 比较。</li>
     *     <li>校验并复制投影和值列表，隔离调用方后续可变集合修改。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 字段、索引形状、key 值或投影与 exact DD version 不一致时抛出。
     */
    public BoundSecondaryRangeSelect {
        // 1. 所有执行必需字段必须在构造期完整，SqlValue.NullValue 是合法 SQL 值而 Java null 不是。
        if (table == null || projectionOrdinals == null || projectionOrdinals.isEmpty()
                || logicalKeyValues == null || logicalKeyValues.stream().anyMatch(java.util.Objects::isNull)
                || lockMode == null) {
            throw new DatabaseValidationException("invalid bound secondary range SELECT fields");
        }
        // 2. 当前 teaching slice 明确限定单 logical part；物理 clustered suffix 由 storage layout 追加。
        IndexDefinition index = table.indexes().stream()
                .filter(candidate -> candidate.id().value() == accessIndexId)
                .findFirst().orElseThrow(() -> new DatabaseValidationException(
                        "bound secondary range SELECT references missing DD index: " + accessIndexId));
        if (index.clustered() || index.unique() || index.keyParts().size() != 1
                || index.keyParts().getFirst().prefixBytes() != 0 || logicalKeyValues.size() != 1) {
            throw new DatabaseValidationException(
                    "bound secondary range SELECT requires one-part non-prefix non-unique secondary");
        }
        // 3. 当前 record key pipeline 不把 external reference 当稳定排序值，LOB/JSON 必须在 Binder 前置拒绝。
        long keyColumnId = index.keyParts().getFirst().columnId();
        var keyColumn = table.columns().stream().filter(column -> column.columnId() == keyColumnId)
                .findFirst().orElseThrow(() -> new DatabaseValidationException(
                        "bound secondary range index references missing DD column"));
        if (isLobKey(keyColumn.type().typeId())) {
            throw new DatabaseValidationException("bound secondary range SELECT does not support LOB/JSON key");
        }
        // 4. 投影顺序属于公开结果契约；重复/越界必须在存储访问前失败。
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size() || !unique.add(ordinal)) {
                throw new DatabaseValidationException("bound secondary range SELECT projection is invalid/duplicate");
            }
        }
        projectionOrdinals = List.copyOf(projectionOrdinals);
        logicalKeyValues = List.copyOf(logicalKeyValues);
    }

    private static boolean isLobKey(DictionaryTypeId type) {
        return switch (type) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> true;
            default -> false;
        };
    }
}
