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
    /**
     * 本对象持有的 {@code coercion} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SqlTypeCoercion coercion;

    /**
     * 创建 {@code DefaultSqlBinder}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param coercion SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlBinder(SqlTypeCoercion coercion) {
        if (coercion == null) throw new DatabaseValidationException("SQL type coercion must not be null");
        this.coercion = coercion;
    }

    /**
     * 先取得/复用 metadata lease，再完成全部名称、shape 和值转换，最后才 publish statement lease。任何绑定失败
     * 都关闭 statement staging；Binder 不创建 storage value，也不访问 B+Tree/Record/LOB。
     *
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param context 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @return {@code bind} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws UnsupportedSqlShapeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
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
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param statement parser 已归一的 CREATE INDEX / ALTER ADD INDEX AST
     * @param currentSchema Session 可选当前 schema；单段表名必须依赖它
     * @return 不持 DD lease、可交给 DDL gateway 的不可变命令
     * @throws SqlBindingException 表名无法限定或 key parts 重复时抛出，且不会触发 implicit commit
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BoundCreateIndex bindDdl(CreateIndexStatementNode statement,
                                    Optional<ObjectName> currentSchema) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        if (statement == null || currentSchema == null) {
            throw new DatabaseValidationException("DDL binding statement/current schema must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        QualifiedTableName table = qualify(statement.table(), currentSchema);
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return new BoundCreateIndex(new CreateSecondaryIndexCommand(
                table, new CreateIndexSpec(
                ObjectName.of(statement.indexName().value()), statement.unique(), false, parts)));
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param insert SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param context 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @return {@code bindInsert} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws UnsupportedSqlShapeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    private BoundClusteredInsert bindInsert(InsertStatementNode insert, SqlBindingContext context) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        TableDefinition table = openActiveBoundTable(insert.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        if (insert.columns().size() != table.columns().size()) {
            throw new UnsupportedSqlShapeException("INSERT must name every table column exactly once");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        List<SqlValue> values = new ArrayList<>(java.util.Collections.nCopies(table.columns().size(), null));
        HashSet<ObjectName> assigned = new HashSet<>();
        HashSet<Long> primaryColumns = new HashSet<>();
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return new BoundClusteredInsert(table, values);
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param select SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param context 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @return {@code bindSelect} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws UnsupportedSqlShapeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    private BoundStatement bindSelect(SelectStatementNode select, SqlBindingContext context) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        TableAccessIntent intent = select.lockingClause() == SelectLockingClause.NONE
                ? TableAccessIntent.READ : TableAccessIntent.WRITE;
        TableDefinition table = openActiveBoundTable(select.table(), context, intent);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        List<Integer> projections = projectionOrdinals(select, table, columns);
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
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
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return new BoundSecondaryRangeSelect(table, projections, access.id().value(),
                List.of(coercion.coerce(literal, column.type(), context.zoneId(), false)), lockMode);
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param update SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @param context 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @return {@code bindUpdate} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws UnsupportedSqlShapeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    private BoundUpdate bindUpdate(UpdateStatementNode update, SqlBindingContext context) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        TableDefinition table = openActiveBoundTable(update.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        HashSet<Long> primaryColumnIds = table.primaryIndex().keyParts().stream()
                .map(IndexKeyPart::columnId).collect(java.util.stream.Collectors.toCollection(HashSet::new));
        HashSet<ObjectName> assigned = new HashSet<>();
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return new BoundUpdate(table, assignments.stream().map(BoundAssignment::ordinal).toList(),
                assignments.stream().map(BoundAssignment::value).toList(),
                bindPrimaryKey(update.predicates(), table, columns, context));
    }

    private BoundDelete bindDelete(DeleteStatementNode delete, SqlBindingContext context) {
        TableDefinition table = openActiveBoundTable(delete.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        return new BoundDelete(table, bindPrimaryKey(delete.predicates(), table, columns, context));
    }

    /**
     * 把语法对象绑定到稳定元数据与类型；绑定期间保持版本一致，失败不发布半绑定结果。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验语法/命令、会话状态与元数据身份，构造统一 deadline，纯输入错误在事务或持久副作用前失败。</li>
     *     <li>按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，并在等待后复核版本与状态。</li>
     *     <li>调用 binder、executor、字典或 storage 稳定接口完成领域动作，成功后才发布缓存、事务或结果状态。</li>
     *     <li>关闭 scope 并返回不可变结果；异常保留 cause/suppressed 图，按 autocommit 或显式事务边界回滚。</li>
     * </ol>
     *
     * @param predicates 参与 {@code bindPrimaryKey} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param table 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param columns 参与 {@code bindPrimaryKey} 的键值映射；不得为 {@code null}，空映射表示没有条目，键和值均不得包含 Java {@code null}
     * @param context 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @return {@code bindPrimaryKey} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     * @throws UnsupportedSqlShapeException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    private List<SqlValue> bindPrimaryKey(List<EqualityPredicateNode> predicates, TableDefinition table,
                                          Map<ObjectName, ColumnDefinition> columns,
                                          SqlBindingContext context) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        Map<ObjectName, LiteralNode> values = new LinkedHashMap<>();
        for (EqualityPredicateNode predicate : predicates) {
            ObjectName name = ObjectName.of(predicate.column().value());
            requireColumn(columns, name);
            if (values.putIfAbsent(name, predicate.value()) != null) {
                throw new UnsupportedSqlShapeException("duplicate primary-key predicate: " + name);
            }
        }
        // 2、继续完成范围、身份与候选校验；通过后，按 session、transaction、MDL 与 metadata scope 顺序取得受控资源，保持处理顺序与资源边界。
        IndexDefinition primary = table.primaryIndex();
        if (primary.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)
                || !matchesExactKey(table, primary, values.keySet())) {
            throw new UnsupportedSqlShapeException(
                    "point write predicates must exactly cover the complete non-prefix primary key");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，调用 binder、executor、字典或 storage 稳定接口完成领域动作，并维持领域不变量。
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
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
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

    /**
     * 根据调用参数创建或转换 {@code openActiveBoundTable} 返回的 {@code TableDefinition}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param syntax 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param context 本次操作的不可变上下文或权威快照；不得为 {@code null}，其中的版本、owner 与资源边界必须来自同一次调用链
     * @param intent 由组合根提供的 {@code TableAccessIntent} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code openActiveBoundTable} 调用
     * @return {@code openActiveBoundTable} 形成的不可变定义、计划或元数据快照；成功时不为 {@code null}，内部身份、版本和范围已完成交叉校验
     * @throws SqlBindingException SQL 绑定、会话准入或事务结果无法按当前状态完成时抛出；调用方应报告错误并按事务边界回滚或关闭
     */
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

    /**
     * 封装SQL 名称绑定与类型推导中 {@code BoundAssignment} 的绑定结果或元数据租约；schema 版本与释放责任在创建后固定，执行结束必须按所有权关闭。
     *
     * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     * @param value SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     */
    private record BoundAssignment(int ordinal, SqlValue value) { }
}
