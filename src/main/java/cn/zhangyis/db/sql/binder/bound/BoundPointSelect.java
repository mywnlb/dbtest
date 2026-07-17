package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.HashSet;
import java.util.List;

/**
 * Binder 已确定访问索引的等值点查。keyValues 按所选 DD index 的 logical key part 顺序排列；SQL 层只携带稳定
 * index id 与路径种类，不接触 storage descriptor、secondary layout 或聚簇回表实现。
 *
 * @param table              statement metadata lease 固定的 exact DD table version。
 * @param projectionOrdinals 用户投影列 ordinal，保持请求顺序且不可重复。
 * @param accessIndexId      所选 DD index 的稳定 id。
 * @param accessKind         聚簇主键点查或 logical unique secondary 回表点查。
 * @param keyValues          按所选 index keyParts 顺序完成类型转换的等值值；secondary 可包含 SQL NULL。
 */
public record BoundPointSelect(TableDefinition table, List<Integer> projectionOrdinals,
                               long accessIndexId, PointAccessKind accessKind,
                               List<SqlValue> keyValues) implements BoundStatement {

    /**
     * 校验并冻结 point-select bound plan，确保访问路径、索引形状、参数值和投影都属于同一 exact DD 版本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>先校验 table、访问种类、key 与投影容器，失败时不发布任何可执行计划。</li>
     *     <li>按稳定 index id 在当前 table version 中定位索引，并拒绝缺失、prefix 或不完整 key。</li>
     *     <li>核对聚簇/唯一二级访问种类与 DD 属性，防止 executor 把错误路径下发给 storage gateway。</li>
     *     <li>校验投影 ordinal 的范围与唯一性，随后复制列表，隔离调用方后续可变集合修改。</li>
     * </ol>
     *
     * @param table              metadata lease 固定的 exact DD table version，不能为 {@code null}。
     * @param projectionOrdinals 用户请求的列 ordinal；必须非空、范围有效且不可重复。
     * @param accessIndexId      当前 table version 中所选访问索引的稳定 id。
     * @param accessKind         binder 已确定的聚簇主键或 logical unique secondary 访问种类。
     * @param keyValues          按所选 DD index key-part 顺序完成类型转换的完整等值值，元素不能为 {@code null}。
     * @throws DatabaseValidationException 字段缺失、索引不存在、访问种类错配、prefix/incomplete key，或投影非法时抛出。
     */
    public BoundPointSelect {
        // 1. 容器和必填字段先于 DD 查找校验，避免空计划进入 executor。
        if (table == null || projectionOrdinals == null || projectionOrdinals.isEmpty()
                || accessKind == null || keyValues == null || keyValues.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid bound point SELECT fields");
        }
        // 2. stable index id 必须在当前 metadata lease 中可解析，且点查只接受完整无 prefix key。
        IndexDefinition index = table.indexes().stream()
                .filter(candidate -> candidate.id().value() == accessIndexId)
                .findFirst().orElseThrow(() -> new DatabaseValidationException(
                        "bound point SELECT references missing DD index: " + accessIndexId));
        if (keyValues.size() != index.keyParts().size()
                || index.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            throw new DatabaseValidationException("bound point SELECT requires complete non-prefix index key");
        }
        // 3. 访问种类是 SQL/DD 层契约；错配会导致 gateway 选择错误 MVCC 路径，必须在 bind 结果构造期拒绝。
        boolean validKind = switch (accessKind) {
            case CLUSTERED_PRIMARY -> index.clustered() && index.unique();
            case UNIQUE_SECONDARY -> !index.clustered() && index.unique();
        };
        if (!validKind) {
            throw new DatabaseValidationException("bound point SELECT access kind/index metadata mismatch");
        }
        // 4. 投影保持用户顺序但禁止重复；复制后的列表不会被调用方在执行期篡改。
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projectionOrdinals) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size() || !unique.add(ordinal)) {
                throw new DatabaseValidationException("bound SELECT projection ordinal is invalid/duplicate");
            }
        }
        projectionOrdinals = List.copyOf(projectionOrdinals);
        keyValues = List.copyOf(keyValues);
    }
}
