package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.List;

/** Point-write bound plans share one defensive primary-key shape check.
 *
 * SQL 名称绑定与类型推导的 {@code BoundPrimaryKeyValidation} 无状态校验组件；它集中检查跨字段与类型约束，合法输入不产生副作用，非法输入抛出领域异常。
 */
final class BoundPrimaryKeyValidation {
    private BoundPrimaryKeyValidation() { }

    /**
     * 校验 {@code validate} 涉及的SQL 名称绑定与类型推导结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param values 参与 {@code validate} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param operation 传给 {@code validate} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static void validate(TableDefinition table, List<SqlValue> values, String operation) {
        if (table == null || values == null || values.stream().anyMatch(java.util.Objects::isNull)
                || values.stream().anyMatch(SqlValue.NullValue.class::isInstance)
                || values.size() != table.primaryIndex().keyParts().size()
                || table.primaryIndex().keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            throw new DatabaseValidationException("invalid bound " + operation + " primary key");
        }
        for (var part : table.primaryIndex().keyParts()) {
            var column = table.columns().stream()
                    .filter(candidate -> candidate.columnId() == part.columnId()).findFirst().orElseThrow();
            if (isLob(column.type().typeId())) {
                throw new DatabaseValidationException(
                        "bound point write does not support LOB/JSON primary key");
            }
        }
    }

    private static boolean isLob(DictionaryTypeId type) {
        return switch (type) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,
                    TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> true;
            default -> false;
        };
    }
}
