package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 单列普通二级索引 logical-key 等值访问叶。该专用叶保留现有 logical-prefix 锁与
 * secondary MVCC 语义，Filter 仍须在回聚簇后的完整行上判断最终真值。
 *
 * @param table exact DD table version
 * @param accessIndexId 普通二级索引稳定 id
 * @param logicalKeyValues logical key-part 顺序的 typed values
 * @param lockMode consistent 或 current locking read
 */
public record PhysicalSecondaryPrefixAccess(
        TableDefinition table, long accessIndexId, List<SqlValue> logicalKeyValues,
        SelectLockMode lockMode) implements PhysicalAccess {

    /**
     * 校验当前教学切片支持的单列、无 prefix、non-unique secondary 访问。
     *
     * @throws DatabaseValidationException 字段、索引形状或 logical key 无效时抛出
     */
    public PhysicalSecondaryPrefixAccess {
        if (table == null || logicalKeyValues == null
                || logicalKeyValues.stream().anyMatch(java.util.Objects::isNull)
                || logicalKeyValues.stream().anyMatch(SqlValue.NullValue.class::isInstance)
                || lockMode == null) {
            throw new DatabaseValidationException(
                    "invalid physical secondary-prefix access fields");
        }
        IndexDefinition index = PhysicalPlanValidation.requireIndex(table, accessIndexId);
        if (index.clustered() || index.unique() || index.keyParts().size() != 1
                || index.keyParts().getFirst().prefixBytes() != 0
                || logicalKeyValues.size() != 1) {
            throw new DatabaseValidationException(
                    "secondary-prefix access requires one-part non-prefix non-unique index");
        }
        PhysicalPlanValidation.rejectLobKey(table, index);
        logicalKeyValues = List.copyOf(logicalKeyValues);
    }
}
