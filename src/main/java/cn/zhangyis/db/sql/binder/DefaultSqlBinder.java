package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.sql.binder.bound.BoundClusteredInsert;
import cn.zhangyis.db.sql.binder.bound.BoundPointSelect;
import cn.zhangyis.db.sql.binder.bound.BoundUpdate;
import cn.zhangyis.db.sql.binder.bound.BoundDelete;
import cn.zhangyis.db.sql.binder.bound.BoundSecondaryRangeSelect;
import cn.zhangyis.db.sql.binder.bound.PointAccessKind;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;
import cn.zhangyis.db.sql.binder.bound.BoundStatement;
import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
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
import java.util.Optional;
import cn.zhangyis.db.dd.ddl.CreateIndexKeyPartSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexSpec;
import cn.zhangyis.db.dd.ddl.CreateSecondaryIndexCommand;

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
                case UpdateStatementNode update -> bindUpdate(update, context);
                case DeleteStatementNode delete -> bindDelete(delete, context);
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

    /**
     * 绑定不需要 transaction metadata scope 的 DDL 语法。这里只解析 current schema、名称和重复 key part；
     * table/index/column 的 committed existence 必须由随后持 MDL X 的 DD coordinator 重新验证。
     *
     * @param statement parser 已归一的 CREATE INDEX / ALTER ADD INDEX AST
     * @param currentSchema Session 可选当前 schema；单段表名必须依赖它
     * @return 不持 DD lease、可交给 DDL gateway 的不可变命令
     * @throws SqlBindingException 表名无法限定或 key parts 重复时抛出，且不会触发 implicit commit
     */
    public BoundCreateIndex bindDdl(CreateIndexStatementNode statement,
                                    Optional<ObjectName> currentSchema) {
        if (statement == null || currentSchema == null) {
            throw new DatabaseValidationException("DDL binding statement/current schema must not be null");
        }
        QualifiedTableName table = qualify(statement.table(), currentSchema);
        HashSet<ObjectName> uniqueColumns = new HashSet<>();
        List<CreateIndexKeyPartSpec> parts = statement.keyParts().stream().map(part -> {
            ObjectName column = ObjectName.of(part.column().value());
            if (!uniqueColumns.add(column)) {
                throw new UnsupportedSqlShapeException(
                        "CREATE INDEX key parts repeat column: " + column.displayName());
            }
            return new CreateIndexKeyPartSpec(column,
                    part.order() == IndexKeyOrderNode.ASC ? IndexOrder.ASC : IndexOrder.DESC, 0);
        }).toList();
        return new BoundCreateIndex(new CreateSecondaryIndexCommand(
                table, new CreateIndexSpec(
                ObjectName.of(statement.indexName().value()), statement.unique(), false, parts)));
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

    private BoundStatement bindSelect(SelectStatementNode select, SqlBindingContext context) {
        TableAccessIntent intent = select.lockingClause() == SelectLockingClause.NONE
                ? TableAccessIntent.READ : TableAccessIntent.WRITE;
        TableDefinition table = openActiveBoundTable(select.table(), context, intent);
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
        java.util.Optional<IndexDefinition> point = choosePointAccess(table, predicates.keySet());
        if (point.isEmpty()) {
            return bindSecondaryRange(select, table, projections, predicates, context);
        }
        if (select.lockingClause() != SelectLockingClause.NONE) {
            throw new UnsupportedSqlShapeException(
                    "locking SELECT currently requires a non-unique secondary logical range");
        }
        IndexDefinition access = point.orElseThrow();
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

    /**
     * 把完整普通二级 logical key 等值谓词绑定为物理 prefix range。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>只从 exact table version 中选择完整单列、无 prefix、non-unique 的最小稳定 id 索引，
     *         避免同一 SQL 因集合迭代顺序改变访问路径。</li>
     *     <li>解析索引列并拒绝 LOB/JSON key、缺失谓词和额外谓词，保证一个 logical equality
     *         可以安全映射为 {@code logical key + clustered suffix} 的物理前缀范围。</li>
     *     <li>把 AST locking clause 映射成一致性读或共享/排他 current-read 模式，并按列类型完成
     *         key coercion；失败不发布部分 bound plan，也不访问 storage。</li>
     * </ol>
     *
     * @param select      原始 SELECT AST，提供 locking clause。
     * @param table       statement metadata scope 固定的 exact table version。
     * @param projections 已验证的公开投影 ordinal。
     * @param predicates  已完成列名解析且尚未消费的等值谓词。
     * @param context     提供 SQL 时区与 metadata scope 的绑定上下文。
     * @return non-unique secondary range bound plan。
     * @throws UnsupportedSqlShapeException 没有精确单列普通二级访问路径或出现额外谓词时抛出。
     */
    private BoundSecondaryRangeSelect bindSecondaryRange(SelectStatementNode select, TableDefinition table,
                                                         List<Integer> projections,
                                                         Map<ObjectName, LiteralNode> predicates,
                                                         SqlBindingContext context) {
        // 1. 稳定 index id 是无 optimizer 阶段下的确定性选择规则；不把复合/前缀索引误宣称为已支持。
        IndexDefinition access = table.indexes().stream()
                .filter(index -> !index.clustered() && !index.unique())
                .filter(index -> index.keyParts().size() == 1
                        && index.keyParts().getFirst().prefixBytes() == 0)
                .filter(index -> matchesExactKey(table, index, predicates.keySet()))
                .min(java.util.Comparator.comparingLong(index -> index.id().value()))
                .orElseThrow(() -> new UnsupportedSqlShapeException(
                        "SELECT predicates must exactly cover primary/unique point key or one-part non-unique secondary"));
        // 2. logical equality 必须恰好消费唯一声明 part；额外过滤条件尚无 residual predicate executor。
        IndexKeyPart part = access.keyParts().getFirst();
        ColumnDefinition column = table.columns().stream()
                .filter(candidate -> candidate.columnId() == part.columnId()).findFirst()
                .orElseThrow(() -> new SqlBindingException("secondary range index references missing DD column"));
        if (isLobKey(column.type().typeId())) {
            throw new UnsupportedSqlShapeException("secondary range SELECT does not support LOB/JSON index key");
        }
        LiteralNode literal = predicates.remove(column.name());
        if (literal == null || !predicates.isEmpty()) {
            throw new UnsupportedSqlShapeException("secondary range SELECT contains incomplete/extra predicate");
        }
        // 3. locking mode 随 bound plan 下传；具体事务与锁资源只在 gateway/storage 阶段创建。
        SelectLockMode lockMode = switch (select.lockingClause()) {
            case NONE -> SelectLockMode.CONSISTENT;
            case FOR_SHARE -> SelectLockMode.FOR_SHARE;
            case FOR_UPDATE -> SelectLockMode.FOR_UPDATE;
        };
        return new BoundSecondaryRangeSelect(table, projections, access.id().value(),
                List.of(coercion.coerce(literal, column.type(), context.zoneId(), false)), lockMode);
    }

    private BoundUpdate bindUpdate(UpdateStatementNode update, SqlBindingContext context) {
        TableDefinition table = openActiveBoundTable(update.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        HashSet<Long> primaryColumnIds = table.primaryIndex().keyParts().stream()
                .map(IndexKeyPart::columnId).collect(java.util.stream.Collectors.toCollection(HashSet::new));
        HashSet<ObjectName> assigned = new HashSet<>();
        ArrayList<BoundAssignment> assignments = new ArrayList<>();
        for (AssignmentNode assignment : update.assignments()) {
            ObjectName name = ObjectName.of(assignment.column().value());
            ColumnDefinition column = requireColumn(columns, name);
            if (!assigned.add(name)) {
                throw new UnsupportedSqlShapeException("duplicate UPDATE assignment: " + name);
            }
            if (primaryColumnIds.contains(column.columnId())) {
                throw new UnsupportedSqlShapeException("UPDATE cannot assign a primary-key column: " + name);
            }
            SqlValue value = coercion.coerce(assignment.value(), column.type(), context.zoneId(), false);
            assignments.add(new BoundAssignment(column.ordinal(), value));
        }
        assignments.sort(java.util.Comparator.comparingInt(BoundAssignment::ordinal));
        return new BoundUpdate(table, assignments.stream().map(BoundAssignment::ordinal).toList(),
                assignments.stream().map(BoundAssignment::value).toList(),
                bindPrimaryKey(update.predicates(), table, columns, context));
    }

    private BoundDelete bindDelete(DeleteStatementNode delete, SqlBindingContext context) {
        TableDefinition table = openActiveBoundTable(delete.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        return new BoundDelete(table, bindPrimaryKey(delete.predicates(), table, columns, context));
    }

    private List<SqlValue> bindPrimaryKey(List<EqualityPredicateNode> predicates, TableDefinition table,
                                          Map<ObjectName, ColumnDefinition> columns,
                                          SqlBindingContext context) {
        Map<ObjectName, LiteralNode> values = new LinkedHashMap<>();
        for (EqualityPredicateNode predicate : predicates) {
            ObjectName name = ObjectName.of(predicate.column().value());
            requireColumn(columns, name);
            if (values.putIfAbsent(name, predicate.value()) != null) {
                throw new UnsupportedSqlShapeException("duplicate primary-key predicate: " + name);
            }
        }
        IndexDefinition primary = table.primaryIndex();
        if (primary.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)
                || !matchesExactKey(table, primary, values.keySet())) {
            throw new UnsupportedSqlShapeException(
                    "point write predicates must exactly cover the complete non-prefix primary key");
        }
        ArrayList<SqlValue> keys = new ArrayList<>(primary.keyParts().size());
        for (IndexKeyPart part : primary.keyParts()) {
            ColumnDefinition column = table.columns().stream()
                    .filter(candidate -> candidate.columnId() == part.columnId()).findFirst()
                    .orElseThrow(() -> new SqlBindingException("primary index references missing DD column"));
            if (isLobKey(column.type().typeId())) {
                throw new UnsupportedSqlShapeException("point write does not support LOB/JSON primary key");
            }
            keys.add(coercion.coerce(values.get(column.name()), column.type(), context.zoneId(), true));
        }
        return List.copyOf(keys);
    }

    /** 完整主键优先；否则选择完整、无 prefix 的 logical unique secondary，多个候选取稳定最小 index id。 */
    private static java.util.Optional<IndexDefinition> choosePointAccess(
            TableDefinition table, java.util.Set<ObjectName> predicates) {
        IndexDefinition primary = table.primaryIndex();
        if (matchesExactKey(table, primary, predicates)
                && primary.keyParts().stream().allMatch(part -> part.prefixBytes() == 0)) {
            return java.util.Optional.of(primary);
        }
        return table.indexes().stream()
                .filter(index -> !index.clustered() && index.unique())
                .filter(index -> index.keyParts().stream().allMatch(part -> part.prefixBytes() == 0))
                .filter(index -> matchesExactKey(table, index, predicates))
                .min(java.util.Comparator.comparingLong(index -> index.id().value()));
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
        return qualify(name, context.currentSchema());
    }

    /** DML 与无 metadata scope 的 DDL 共用同一限定名规则。 */
    private static QualifiedTableName qualify(QualifiedNameNode name, Optional<ObjectName> currentSchema) {
        List<IdentifierNode> parts = name.parts();
        return switch (parts.size()) {
            case 1 -> new QualifiedTableName(ObjectName.of("def"), currentSchema.orElseThrow(() ->
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

    private record BoundAssignment(int ordinal, SqlValue value) { }
}
