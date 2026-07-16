package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPrimaryPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundStatement;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;
import cn.zhangyis.db.sql.binder.exception.UnknownColumnException;
import cn.zhangyis.db.sql.binder.exception.UnsupportedSqlShapeException;
import cn.zhangyis.db.sql.executor.SqlValue;
import cn.zhangyis.db.sql.parser.ast.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** primary-point v1 Binder：固定 exact DD version，输出 INSERT 或完整聚簇主键点查意图。 */
public final class DefaultSqlBinder {
    private final SqlTypeCoercion coercion;

    public DefaultSqlBinder(SqlTypeCoercion coercion) {
        if (coercion == null) throw new DatabaseValidationException("SQL type coercion must not be null");
        this.coercion = coercion;
    }

    /**
     * 先取得/复用 metadata lease，再完成全部名称、shape 和值转换，最后才 publish statement lease。任何绑定失败
     * 都关闭 statement staging；Binder 不创建 storage value，也不访问 B+Tree/Record/LOB。
     */
    public BoundStatement bind(StatementNode statement, SqlBindingContext context) {
        if (statement == null || context == null) {
            throw new DatabaseValidationException("binding statement/context must not be null");
        }
        try {
            BoundStatement bound = switch (statement) {
                case InsertStatementNode insert -> bindInsert(insert, context);
                case SelectStatementNode select -> bindSelect(select, context);
                default -> throw new UnsupportedSqlShapeException(
                        "statement is handled by Session rather than SQL Binder: " + statement.getClass().getSimpleName());
            };
            context.metadataScope().publish();
            return bound;
        } catch (RuntimeException failure) {
            try { context.metadataScope().close(); }
            catch (RuntimeException closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
    }

    private BoundClusteredInsert bindInsert(InsertStatementNode insert, SqlBindingContext context) {
        TableDefinition table = openActiveBoundTable(insert.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        if (insert.columns().size() != table.columns().size()) {
            throw new UnsupportedSqlShapeException("INSERT must name every table column exactly once");
        }
        List<SqlValue> values = new ArrayList<>(java.util.Collections.nCopies(table.columns().size(), null));
        HashSet<ObjectName> assigned = new HashSet<>();
        HashSet<Long> primaryColumns = new HashSet<>();
        for (IndexKeyPart keyPart : table.primaryIndex().keyParts()) primaryColumns.add(keyPart.columnId());
        for (int i = 0; i < insert.columns().size(); i++) {
            ObjectName name = ObjectName.of(insert.columns().get(i).value());
            ColumnDefinition column = requireColumn(columns, name);
            if (!assigned.add(name)) throw new UnsupportedSqlShapeException("duplicate INSERT column: " + name);
            values.set(column.ordinal(), coercion.coerce(insert.values().get(i), column.type(), context.zoneId(),
                    primaryColumns.contains(column.columnId())));
        }
        if (values.stream().anyMatch(java.util.Objects::isNull)) {
            throw new UnsupportedSqlShapeException("INSERT must assign the complete row");
        }
        return new BoundClusteredInsert(table, values);
    }

    private BoundPrimaryPointSelect bindSelect(SelectStatementNode select, SqlBindingContext context) {
        TableDefinition table = openActiveBoundTable(select.table(), context, TableAccessIntent.READ);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        List<Integer> projections = projectionOrdinals(select, table, columns);
        IndexDefinition primary = table.primaryIndex();
        if (primary.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            throw new UnsupportedSqlShapeException("primary-point SELECT does not support prefix primary keys");
        }
        if (select.predicates().size() != primary.keyParts().size()) {
            throw new UnsupportedSqlShapeException("SELECT predicates must equal the complete primary key");
        }
        Map<ObjectName, LiteralNode> predicates = new LinkedHashMap<>();
        for (EqualityPredicateNode predicate : select.predicates()) {
            ObjectName name = ObjectName.of(predicate.column().value());
            requireColumn(columns, name);
            if (predicates.putIfAbsent(name, predicate.value()) != null) {
                throw new UnsupportedSqlShapeException("duplicate SELECT predicate: " + name);
            }
        }
        List<SqlValue> keys = new ArrayList<>(primary.keyParts().size());
        for (IndexKeyPart part : primary.keyParts()) {
            ColumnDefinition column = table.columns().stream()
                    .filter(candidate -> candidate.columnId() == part.columnId()).findFirst()
                    .orElseThrow(() -> new SqlBindingException("primary key references missing DD column"));
            if (isLobKey(column.type().typeId())) {
                throw new UnsupportedSqlShapeException("primary-point SELECT does not support LOB/JSON primary key");
            }
            LiteralNode literal = predicates.remove(column.name());
            if (literal == null) {
                throw new UnsupportedSqlShapeException("SELECT must constrain every primary key column");
            }
            keys.add(coercion.coerce(literal, column.type(), context.zoneId(), true));
        }
        if (!predicates.isEmpty()) {
            throw new UnsupportedSqlShapeException("SELECT contains non-primary-key predicate");
        }
        return new BoundPrimaryPointSelect(table, projections, keys);
    }

    private static TableDefinition openActiveBoundTable(QualifiedNameNode syntax, SqlBindingContext context,
                                                        TableAccessIntent intent) {
        QualifiedTableName name = qualify(syntax, context);
        TableDefinition table = context.metadataScope().openTable(name, intent);
        if (table.state() != TableState.ACTIVE) throw new SqlBindingException("table is not ACTIVE: " + name.canonicalKey());
        if (table.storageBinding().isEmpty()) throw new SqlBindingException("table has no physical storage binding: " + name.canonicalKey());
        return table;
    }

    private static QualifiedTableName qualify(QualifiedNameNode name, SqlBindingContext context) {
        List<IdentifierNode> parts = name.parts();
        return switch (parts.size()) {
            case 1 -> new QualifiedTableName(ObjectName.of("def"), context.currentSchema().orElseThrow(() ->
                    new SqlBindingException("unqualified table requires current schema")), ObjectName.of(parts.getFirst().value()));
            case 2 -> new QualifiedTableName(ObjectName.of("def"), ObjectName.of(parts.get(0).value()),
                    ObjectName.of(parts.get(1).value()));
            case 3 -> {
                ObjectName catalog = ObjectName.of(parts.get(0).value());
                if (!catalog.equals(ObjectName.of("def"))) throw new SqlBindingException("catalog must be def");
                yield new QualifiedTableName(catalog, ObjectName.of(parts.get(1).value()),
                        ObjectName.of(parts.get(2).value()));
            }
            default -> throw new SqlBindingException("table name must contain one to three parts");
        };
    }

    private static Map<ObjectName, ColumnDefinition> columns(TableDefinition table) {
        LinkedHashMap<ObjectName, ColumnDefinition> result = new LinkedHashMap<>();
        for (ColumnDefinition column : table.columns()) result.put(column.name(), column);
        return result;
    }

    private static ColumnDefinition requireColumn(Map<ObjectName, ColumnDefinition> columns, ObjectName name) {
        ColumnDefinition column = columns.get(name);
        if (column == null) throw new UnknownColumnException("unknown column: " + name.displayName());
        return column;
    }

    private static List<Integer> projectionOrdinals(SelectStatementNode select, TableDefinition table,
                                                    Map<ObjectName, ColumnDefinition> columns) {
        if (select.star()) return table.columns().stream().map(ColumnDefinition::ordinal).toList();
        ArrayList<Integer> result = new ArrayList<>();
        HashSet<ObjectName> unique = new HashSet<>();
        for (IdentifierNode projection : select.projections()) {
            ObjectName name = ObjectName.of(projection.value());
            if (!unique.add(name)) throw new UnsupportedSqlShapeException("duplicate SELECT projection: " + name);
            result.add(requireColumn(columns, name).ordinal());
        }
        return List.copyOf(result);
    }

    private static boolean isLobKey(DictionaryTypeId type) {
        return switch (type) {
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB, JSON -> true;
            default -> false;
        };
    }
}
