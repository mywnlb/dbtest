package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;

/**
 * 已选择聚簇主键或唯一二级键的等值访问叶。
 *
 * @param table exact DD table version
 * @param accessIndexId 所选索引的稳定 id
 * @param accessKind 聚簇主键或唯一二级访问种类
 * @param keyValues 按索引 key-part 顺序排列的完整、非 NULL typed key
 */
public record PhysicalPointAccess(
        TableDefinition table, long accessIndexId, PointAccessKind accessKind,
        List<SqlValue> keyValues) implements PhysicalAccess {

    /**
     * 校验点访问叶的索引种类与完整 key；最终 SQL truth 由父级 Filter 单独校验。
     *
     * <ol>
     *     <li>拒绝缺失字段、Java null 与 SQL NULL，避免执行期猜测 key 语义。</li>
     *     <li>在 exact table version 中解析索引并校验完整、无 prefix、非 LOB key。</li>
     *     <li>核对 clustered/unique 属性与访问种类，并冻结 key 容器。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 字段、索引或 key 不满足点访问契约时抛出
     */
    public PhysicalPointAccess {
        // 1、点访问必须在打开 cursor 前拥有完整、稳定的物理身份。
        if (table == null || accessKind == null || keyValues == null
                || keyValues.stream().anyMatch(java.util.Objects::isNull)
                || keyValues.stream().anyMatch(SqlValue.NullValue.class::isInstance)) {
            throw new DatabaseValidationException("invalid physical point access fields");
        }
        // 2、prefix/LOB key 不能由当前 point codec 无损定位。
        IndexDefinition index = PhysicalPlanValidation.requireIndex(table, accessIndexId);
        if (keyValues.size() != index.keyParts().size()
                || index.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            throw new DatabaseValidationException(
                    "physical point access requires complete non-prefix index key");
        }
        PhysicalPlanValidation.rejectLobKey(table, index);
        // 3、访问种类决定 adapter 是否回聚簇，不能与 DD 属性错配。
        boolean validKind = switch (accessKind) {
            case CLUSTERED_PRIMARY -> index.clustered() && index.unique();
            case UNIQUE_SECONDARY -> !index.clustered() && index.unique();
        };
        if (!validKind) {
            throw new DatabaseValidationException(
                    "physical point access kind/index metadata mismatch");
        }
        keyValues = List.copyOf(keyValues);
    }
}
