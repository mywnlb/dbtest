package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.PointAccessKind;
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

    private BoundPointSelect bindSelect(SelectStatementNode select, SqlBindingContext context) {
        TableDefinition table = openActiveBoundTable(select.table(), context, TableAccessIntent.READ);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        List<Integer> projections = projectionOrdinals(select, table, columns);
        Map<ObjectName, LiteralNode> predicates = new LinkedHashMap<>();
        for (EqualityPredicateNode predicate : select.predicates()) {
            ObjectName name = ObjectName.of(predicate.column().value());
            requireColumn(columns, name);
            if (predicates.putIfAbsent(name, predicate.value()) != null) {
                throw new UnsupportedSqlShapeException("duplicate SELECT predicate: " + name);
            }
        }
        IndexDefinition access = choosePointAccess(table, predicates.keySet());
        List<SqlValue> keys = new ArrayList<>(access.keyParts().size());
        for (IndexKeyPart part : access.keyParts()) {
            ColumnDefinition column = table.columns().stream()
                    .filter(candidate -> candidate.columnId() == part.columnId()).findFirst()
                    .orElseThrow(() -> new SqlBindingException("point access index references missing DD column"));
            if (isLobKey(column.type().typeId())) {
                throw new UnsupportedSqlShapeException("point SELECT does not support LOB/JSON index key");
            }
            LiteralNode literal = predicates.remove(column.name());
            if (literal == null) {
                throw new UnsupportedSqlShapeException("SELECT must constrain every selected index key column");
            }
            keys.add(coercion.coerce(literal, column.type(), context.zoneId(), access.clustered()));
        }
        if (!predicates.isEmpty()) {
            throw new UnsupportedSqlShapeException("SELECT contains predicate outside selected point index");
        }
        return new BoundPointSelect(table, projections, access.id().value(),
                access.clustered() ? PointAccessKind.CLUSTERED_PRIMARY : PointAccessKind.UNIQUE_SECONDARY,
                keys);
    }

    /** 完整主键优先；否则选择完整、无 prefix 的 logical unique secondary，多个候选取稳定最小 index id。 */
    private static IndexDefinition choosePointAccess(TableDefinition table, java.util.Set<ObjectName> predicates) {
        IndexDefinition primary = table.primaryIndex();
        if (matchesExactKey(table, primary, predicates)
                && primary.keyParts().stream().allMatch(part -> part.prefixBytes() == 0)) {
            return primary;
        }
        return table.indexes().stream()
                .filter(index -> !index.clustered() && index.unique())
                .filter(index -> index.keyParts().stream().allMatch(part -> part.prefixBytes() == 0))
                .filter(index -> matchesExactKey(table, index, predicates))
                .min(java.util.Comparator.comparingLong(index -> index.id().value()))
                .orElseThrow(() -> new UnsupportedSqlShapeException(
                        "SELECT predicates must exactly cover the primary key or one non-prefix unique secondary"));
    }

    /** 判断谓词列集合是否与索引 key column 集合完全一致；part 顺序只影响后续 keyValues 排列。 */
    private static boolean matchesExactKey(TableDefinition table, IndexDefinition index,
                                           java.util.Set<ObjectName> predicates) {
        if (predicates.size() != index.keyParts().size()) {
            return false;
        }
        java.util.Set<ObjectName> keyColumns = index.keyParts().stream().map(part -> table.columns().stream()
                .filter(column -> column.columnId() == part.columnId()).findFirst()
                .orElseThrow(() -> new SqlBindingException("index references missing DD column")).name())
                .collect(java.util.stream.Collectors.toSet());
        return keyColumns.equals(predicates);
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
