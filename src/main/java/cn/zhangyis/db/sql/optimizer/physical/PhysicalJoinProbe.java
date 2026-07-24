package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefinition;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.type.SqlValue;

import java.util.List;
import java.util.Optional;

/**
 * INNER JOIN 右输入的参数化访问模板。模板不持 outer row 或 cursor；每次循环由
 * Executor 传入非 NULL typed key，构造一个普通 {@link PhysicalAccess}。
 *
 * @param kind 全扫、唯一点探针或普通二级前缀探针
 * @param table SQL 右表 exact version
 * @param accessIndexId 右侧访问索引 stable id
 * @param outerColumnOrdinal 左表中提供 probe key 的 local ordinal
 * @param innerColumnOrdinal 右表 ON 列 local ordinal
 * @param pointAccessKind POINT 时的聚簇/唯一二级种类；其它策略为空
 */
public record PhysicalJoinProbe(
        PhysicalJoinProbeKind kind,
        TableDefinition table,
        long accessIndexId,
        int outerColumnOrdinal,
        int innerColumnOrdinal,
        Optional<PointAccessKind> pointAccessKind) {

    public PhysicalJoinProbe {
        if (kind == null || table == null
                || outerColumnOrdinal < 0
                || innerColumnOrdinal < 0
                || innerColumnOrdinal >= table.columns().size()
                || pointAccessKind == null) {
            throw new DatabaseValidationException(
                    "invalid physical JOIN probe fields");
        }
        IndexDefinition index = PhysicalPlanValidation.requireIndex(
                table, accessIndexId);
        if (kind == PhysicalJoinProbeKind.POINT) {
            if (pointAccessKind.isEmpty()
                    || index.keyParts().size() != 1
                    || index.keyParts().getFirst().prefixBytes() != 0
                    || index.keyParts().getFirst().columnId()
                    != table.columns().get(innerColumnOrdinal).columnId()) {
                throw new DatabaseValidationException(
                        "JOIN point probe requires the exact one-part ON index");
            }
        } else if (kind == PhysicalJoinProbeKind.SECONDARY_PREFIX) {
            if (pointAccessKind.isPresent()
                    || index.clustered() || index.unique()
                    || index.keyParts().size() != 1
                    || index.keyParts().getFirst().prefixBytes() != 0
                    || index.keyParts().getFirst().columnId()
                    != table.columns().get(innerColumnOrdinal).columnId()) {
                throw new DatabaseValidationException(
                        "JOIN secondary probe requires exact non-unique one-part ON index");
            }
        } else if (pointAccessKind.isPresent()
                || !index.clustered()) {
            throw new DatabaseValidationException(
                    "JOIN full-scan probe requires clustered index");
        }
    }

    /**
     * 用当前 outer key 创建一次右输入访问；FULL_SCAN 不消费 key 值。
     *
     * @param key outer ON 列的 exact typed 非 NULL 值；FULL_SCAN 可传任意非 Java-null 值
     * @return 尚未打开的普通物理访问叶
     * @throws DatabaseValidationException key 缺失或为 SQL NULL 时抛出；JoinNode 应在调用前跳过 NULL
     */
    public PhysicalAccess accessFor(SqlValue key) {
        if (key == null || key instanceof SqlValue.NullValue) {
            throw new DatabaseValidationException(
                    "JOIN probe key must be a non-null SQL value");
        }
        return switch (kind) {
            case FULL_SCAN -> new PhysicalRangeAccess(
                    table, accessIndexId,
                    IndexRange.unbounded(),
                    SelectLockMode.CONSISTENT, false);
            case POINT -> new PhysicalPointAccess(
                    table, accessIndexId,
                    pointAccessKind.orElseThrow(),
                    List.of(key));
            case SECONDARY_PREFIX ->
                    new PhysicalSecondaryPrefixAccess(
                            table, accessIndexId,
                            List.of(key),
                            SelectLockMode.CONSISTENT);
        };
    }

    /**
     * 返回 ON 右列的 exact DD 定义，供运行期 probe key 类型诊断。
     *
     * @return 当前右表版本中的 ON 列
     */
    public ColumnDefinition innerColumn() {
        return table.columns().get(innerColumnOrdinal);
    }
}
