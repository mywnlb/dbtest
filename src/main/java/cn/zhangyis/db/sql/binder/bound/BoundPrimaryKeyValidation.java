package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryTypeId;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.executor.SqlValue;

import java.util.List;

/** Point-write bound plans share one defensive primary-key shape check. */
final class BoundPrimaryKeyValidation {
    private BoundPrimaryKeyValidation() { }

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
