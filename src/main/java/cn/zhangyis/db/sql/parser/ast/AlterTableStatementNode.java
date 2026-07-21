package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;
import java.util.Optional;

/**
 * 通用阻塞式 {@code ALTER TABLE} 语法树。actions 保留用户声明顺序，Binder/DD 必须依次作用于同一个
 * staged definition，不能重排后改变 FIRST/AFTER、DROP 后 ADD 等可观察语义。
 *
 * @param table 源表限定名
 * @param actions 一个或多个按 SQL 顺序排列的 action
 */
public record AlterTableStatementNode(QualifiedNameNode table, List<Action> actions)
        implements StatementNode {

    public AlterTableStatementNode {
        if (table == null || actions == null || actions.isEmpty()
                || actions.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException(
                    "ALTER TABLE AST requires table and non-empty actions");
        }
        actions = List.copyOf(actions);
    }

    /** 通用 ALTER action 根；DISCARD/IMPORT 使用独立 AST，不能与这些 action 混合。 */
    public sealed interface Action permits AddColumn, DropColumn, AddIndex, DropIndex,
            Rename, Comment, DefaultCharset, ConvertCharset {
    }

    /**
     * 新列声明。
     *
     * @param name 列名
     * @param type parser 级类型 shape
     * @param defaultLiteral 显式 DEFAULT；空表示由 nullable 推导 REQUIRED/IMPLICIT_NULL
     * @param position LAST、FIRST 或 AFTER
     */
    public record AddColumn(IdentifierNode name, ColumnType type,
                            Optional<LiteralNode> defaultLiteral, ColumnPosition position)
            implements Action {
        public AddColumn {
            if (name == null || type == null || defaultLiteral == null || position == null) {
                throw new DatabaseValidationException("ALTER ADD COLUMN fields must not be null");
            }
        }
    }

    /** @param name 待删除列名 */
    public record DropColumn(IdentifierNode name) implements Action {
        public DropColumn {
            if (name == null) {
                throw new DatabaseValidationException("ALTER DROP COLUMN name must not be null");
            }
        }
    }

    /** ALTER 内联 ADD INDEX；key part shape 与独立 CREATE INDEX 一致。 */
    public record AddIndex(IdentifierNode name, boolean unique, List<IndexKeyPartNode> keyParts)
            implements Action {
        public AddIndex {
            if (name == null || keyParts == null || keyParts.isEmpty()) {
                throw new DatabaseValidationException("ALTER ADD INDEX fields must not be empty");
            }
            keyParts = List.copyOf(keyParts);
        }
    }

    /** @param name 待删除二级索引名 */
    public record DropIndex(IdentifierNode name) implements Action {
        public DropIndex {
            if (name == null) {
                throw new DatabaseValidationException("ALTER DROP INDEX name must not be null");
            }
        }
    }

    /** @param target 单表新限定名；支持跨 schema，不改变 table id */
    public record Rename(QualifiedNameNode target) implements Action {
        public Rename {
            if (target == null) {
                throw new DatabaseValidationException("ALTER RENAME target must not be null");
            }
        }
    }

    /** @param value 新表注释，允许空字符串 */
    public record Comment(String value) implements Action {
        public Comment {
            if (value == null) {
                throw new DatabaseValidationException("ALTER COMMENT value must not be null");
            }
        }
    }

    /** @param charsetId 新列默认 charset 稳定 id @param collationId 配套 collation 稳定 id */
    public record DefaultCharset(int charsetId, int collationId) implements Action {
        public DefaultCharset {
            if (charsetId <= 0 || collationId <= 0) {
                throw new DatabaseValidationException(
                        "ALTER DEFAULT charset/collation ids must be positive");
            }
        }
    }

    /** CONVERT 会重写全部字符列并触发 shadow rebuild。 */
    public record ConvertCharset(int charsetId, int collationId) implements Action {
        public ConvertCharset {
            if (charsetId <= 0 || collationId <= 0) {
                throw new DatabaseValidationException(
                        "ALTER CONVERT charset/collation ids must be positive");
            }
        }
    }

    /**
     * parser 级列类型；charset/collation 由 staged table options 提供，ENUM/SET symbols 暂不在本 SQL
     * shape 中创建，但现有列仍可被 metadata/rename action 保留。
     *
     * @param name 类型关键字
     * @param length 可选长度/精度，0 表示类型默认
     * @param scale DECIMAL 标度，未声明为 0
     * @param unsigned 是否 UNSIGNED
     * @param nullable 是否允许 NULL
     */
    public record ColumnType(String name, int length, int scale,
                             boolean unsigned, boolean nullable) {
        public ColumnType {
            if (name == null || name.isBlank() || length < 0 || scale < 0) {
                throw new DatabaseValidationException("ALTER column type shape is invalid");
            }
        }
    }

    /**
     * 新列相对位置。
     *
     * @param kind LAST、FIRST 或 AFTER
     * @param afterColumn 仅 AFTER 存在
     */
    public record ColumnPosition(PositionKind kind, Optional<IdentifierNode> afterColumn) {
        public ColumnPosition {
            if (kind == null || afterColumn == null
                    || (kind == PositionKind.AFTER) != afterColumn.isPresent()) {
                throw new DatabaseValidationException("ALTER column position is invalid");
            }
        }

        /** @return 默认追加到现有列之后的位置 */
        public static ColumnPosition last() {
            return new ColumnPosition(PositionKind.LAST, Optional.empty());
        }
    }

    /** 新列位置类别。 */
    public enum PositionKind {
        LAST,
        FIRST,
        AFTER
    }
}
