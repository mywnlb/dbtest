package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.ColumnDefaultDefinition;
import cn.zhangyis.db.dd.domain.ColumnTypeDefinition;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.storage.api.ddl.StorageDefaultValue;

import java.util.Optional;

/**
 * DD 可消费的通用 ALTER action。类型和值已脱离 SQL AST，但仍不携带 table/index/space identity；
 * coordinator 必须在 table MDL X 下按顺序绑定到当前 staged definition。
 */
public sealed interface AlterTableAction permits AlterTableAction.AddColumn,
        AlterTableAction.DropColumn, AlterTableAction.AddIndex, AlterTableAction.DropIndex,
        AlterTableAction.Rename, AlterTableAction.Comment,
        AlterTableAction.DefaultCharset, AlterTableAction.ConvertCharset {

    /** 新列及其相对位置；column id/ordinal 由 staged table 分配。 */
    record AddColumn(ObjectName name, ColumnTypeDefinition type,
                     ColumnDefaultDefinition defaultDefinition,
                     Optional<StorageDefaultValue> storageDefault, Position position)
            implements AlterTableAction {
        public AddColumn {
            if (name == null || type == null || defaultDefinition == null
                    || storageDefault == null || position == null
                    || defaultDefinition.kind() == ColumnDefaultDefinition.Kind.CONSTANT
                    && storageDefault.isEmpty()
                    || defaultDefinition.kind() == ColumnDefaultDefinition.Kind.IMPLICIT_NULL
                    && storageDefault.filter(StorageDefaultValue.NullValue.class::isInstance).isEmpty()
                    || defaultDefinition.kind() == ColumnDefaultDefinition.Kind.REQUIRED
                    && storageDefault.isPresent()) {
                throw new DatabaseValidationException("ALTER ADD COLUMN action is invalid");
            }
        }
    }

    /** 删除列；聚簇 key 列由 coordinator 拒绝。 */
    record DropColumn(ObjectName name) implements AlterTableAction {
        public DropColumn {
            if (name == null) {
                throw new DatabaseValidationException("ALTER DROP COLUMN action is invalid");
            }
        }
    }

    /** 添加二级索引；稳定 index id 在 table X 下预留。 */
    record AddIndex(CreateIndexSpec index) implements AlterTableAction {
        public AddIndex {
            if (index == null || index.clustered()) {
                throw new DatabaseValidationException(
                        "ALTER ADD INDEX requires a non-clustered definition");
            }
        }
    }

    /** 删除二级索引；PRIMARY 不能通过本 action 删除。 */
    record DropIndex(ObjectName name) implements AlterTableAction {
        public DropIndex {
            if (name == null) {
                throw new DatabaseValidationException("ALTER DROP INDEX action is invalid");
            }
        }
    }

    /** 单表 rename，可跨 schema 但不改变 table identity。 */
    record Rename(QualifiedTableName target) implements AlterTableAction {
        public Rename {
            if (target == null) {
                throw new DatabaseValidationException("ALTER RENAME action is invalid");
            }
        }
    }

    /** 表 comment metadata action。 */
    record Comment(String value) implements AlterTableAction {
        public Comment {
            if (value == null) {
                throw new DatabaseValidationException("ALTER COMMENT action is invalid");
            }
        }
    }

    /** 只修改未来新字符列使用的默认 charset/collation。 */
    record DefaultCharset(int charsetId, int collationId) implements AlterTableAction {
        public DefaultCharset {
            requireCharset(charsetId, collationId);
        }
    }

    /** 修改表默认值并重写全部既有字符列。 */
    record ConvertCharset(int charsetId, int collationId) implements AlterTableAction {
        public ConvertCharset {
            requireCharset(charsetId, collationId);
        }
    }

    /**
     * 新列位置。
     *
     * @param kind LAST/FIRST/AFTER
     * @param afterColumn 仅 AFTER 携带的 staged 列名
     */
    record Position(PositionKind kind, Optional<ObjectName> afterColumn) {
        public Position {
            if (kind == null || afterColumn == null
                    || (kind == PositionKind.AFTER) != afterColumn.isPresent()) {
                throw new DatabaseValidationException("ALTER column position is invalid");
            }
        }
    }

    /** 新列插入 staged columns 的相对位置类别。 */
    enum PositionKind {
        LAST,
        FIRST,
        AFTER
    }

    private static void requireCharset(int charsetId, int collationId) {
        if (charsetId <= 0 || collationId <= 0) {
            throw new DatabaseValidationException(
                    "ALTER charset/collation ids must be positive");
        }
    }
}
