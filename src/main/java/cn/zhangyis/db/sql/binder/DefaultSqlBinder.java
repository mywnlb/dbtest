package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.sql.binder.bound.*;
import cn.zhangyis.db.dd.ddl.DropSecondaryIndexCommand;
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
import java.time.ZoneId;
import cn.zhangyis.db.dd.ddl.CreateIndexKeyPartSpec;
import cn.zhangyis.db.dd.ddl.CreateIndexSpec;
import cn.zhangyis.db.dd.ddl.CreateSecondaryIndexCommand;
import cn.zhangyis.db.dd.ddl.AlterTableAction;
import cn.zhangyis.db.dd.ddl.AlterTableCommand;
import cn.zhangyis.db.storage.api.ddl.StorageDefaultValue;

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
     * 把 DROP INDEX 纯语法绑定为不持有 DD lease 的稳定命令。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 AST 与 current schema 容器，保证纯输入错误早于 Session implicit commit。</li>
     *     <li>按 catalog/schema/table 规则补全限定名，不打开 table metadata 或选择物理索引。</li>
     *     <li>规范化索引标识符，形成只携带逻辑名称的 DD command。</li>
     *     <li>返回不可变 bound 对象；目标存在性和 clustered 属性留给 table MDL X 下的 coordinator 重验。</li>
     * </ol>
     *
     * @param statement Parser 已归一的 DROP INDEX / ALTER DROP INDEX AST
     * @param currentSchema Session 可选当前 schema；单段表名必须依赖它
     * @return 不持 DD lease、可在 implicit commit 后交给 DDL gateway 的命令
     * @throws SqlBindingException 表名无法限定时抛出，且不会触发事务提交
     * @throws DatabaseValidationException statement/currentSchema 容器缺失时抛出
     */
    public BoundDropIndex bindDdl(DropIndexStatementNode statement,
                                  Optional<ObjectName> currentSchema) {
        // 1、输入容器校验不访问 DD，失败不会改变 Session transaction。
        if (statement == null || currentSchema == null) {
            throw new DatabaseValidationException("DROP INDEX binding statement/current schema must not be null");
        }
        // 2、沿 CREATE INDEX 共用的限定名规则补全 catalog/schema。
        QualifiedTableName table = qualify(statement.table(), currentSchema);
        // 3、ObjectName 负责稳定大小写规范化；此处不猜测 IndexId。
        DropSecondaryIndexCommand command = new DropSecondaryIndexCommand(
                table, ObjectName.of(statement.indexName().value()));
        // 4、返回纯命令，DD coordinator 后续在 MDL X 下重读 ACTIVE aggregate。
        return new BoundDropIndex(command);
    }

    /**
     * 补全 DISCARD/IMPORT TABLESPACE 的逻辑表名；目标状态、文件身份和受控路径只能由持 table MDL X
     * 的 DD coordinator 决定，Binder 不读取可过期的字典快照。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 AST 与 current schema 容器，纯输入错误不得触发 DDL implicit commit。</li>
     *     <li>按照其它 DDL 共用的 catalog/schema 规则补全表名，不打开 metadata lease。</li>
     *     <li>保留 parser 已交叉校验的 DISCARD/IMPORT 枚举，不在 Binder 猜测物理路径。</li>
     *     <li>返回不持资源的 bound 意图，由 Session 在 implicit commit 后交给独立 DDL owner。</li>
     * </ol>
     *
     * @param statement parser 产生的表空间生命周期 AST；不得为 {@code null}
     * @param currentSchema Session 当前 schema；单段表名要求该值存在
     * @return 已限定逻辑表名和动作，不持 DD lease
     * @throws SqlBindingException 单段表名缺少当前 schema 时抛出，且不改变 Session 事务
     */
    public BoundAlterTablespace bindDdl(AlterTablespaceStatementNode statement,
                                        Optional<ObjectName> currentSchema) {
        // 1、输入校验早于 Session 的 implicit commit。
        if (statement == null || currentSchema == null) {
            throw new DatabaseValidationException(
                    "ALTER TABLESPACE binding statement/current schema must not be null");
        }
        // 2、名称补全只消费语法与 Session 快照，不建立 DD pin。
        QualifiedTableName table = qualify(statement.table(), currentSchema);
        // 3、动作枚举已由 AST 构造器保证非空，此处不映射为任意文件操作。
        AlterTablespaceStatementNode.Action action = statement.action();
        // 4、目标文件和身份留给 table MDL X 内的 coordinator 计算。
        return new BoundAlterTablespace(table, action);
    }

    /**
     * 将通用 ALTER AST 转为与 SQL 层解耦的 DD command。action 顺序原样保留；类型和常量先在
     * implicit commit 前校验，但 table-dependent 名称、FIRST/AFTER、索引引用与冲突由 table X 下的
     * coordinator 在 staged definition 上依次解析。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>补全源表名并冻结 Session zone；缺少 current schema 等纯输入错误不改变事务。</li>
     *     <li>按原顺序映射 action；ADD COLUMN 将类型 shape 转为 DD 类型并严格校验显式 default。</li>
     *     <li>将索引 key part、rename 目标和 options 规范化为值对象，不读取当前字典版本。</li>
     *     <li>返回不持 DD lease 的命令；后续 table X 内仍须重验全部 table-dependent 不变量。</li>
     * </ol>
     *
     * @param statement parser 产生的通用 ALTER AST
     * @param currentSchema Session 当前 schema；源表单段名依赖它
     * @param zoneId Session 时区；TIMESTAMP default 的严格转换使用该值
     * @return 保序且不持资源的通用 ALTER command
     * @throws UnsupportedSqlShapeException 类型、default 或 action shape 超出当前阻塞式 ALTER 范围时抛出
     */
    public BoundAlterTable bindDdl(AlterTableStatementNode statement,
                                   Optional<ObjectName> currentSchema,
                                   ZoneId zoneId) {
        // 1、输入和逻辑限定名必须在 DDL implicit commit 前完成。
        if (statement == null || currentSchema == null || zoneId == null) {
            throw new DatabaseValidationException(
                    "ALTER TABLE binding statement/schema/zone must not be null");
        }
        QualifiedTableName source = qualify(statement.table(), currentSchema);
        List<AlterTableAction> actions = new ArrayList<>(statement.actions().size());
        // 2、每个 action 只做不依赖当前 table 的确定性转换。
        for (AlterTableStatementNode.Action action : statement.actions()) {
            actions.add(switch (action) {
                case AlterTableStatementNode.AddColumn add -> bindAlterAddColumn(add, zoneId);
                case AlterTableStatementNode.DropColumn drop ->
                        new AlterTableAction.DropColumn(ObjectName.of(drop.name().value()));
                case AlterTableStatementNode.AddIndex add ->
                        new AlterTableAction.AddIndex(bindAlterIndex(add));
                case AlterTableStatementNode.DropIndex drop ->
                        new AlterTableAction.DropIndex(ObjectName.of(drop.name().value()));
                case AlterTableStatementNode.Rename rename ->
                        new AlterTableAction.Rename(qualify(
                                rename.target(), Optional.of(source.schema())));
                case AlterTableStatementNode.Comment comment ->
                        new AlterTableAction.Comment(comment.value());
                case AlterTableStatementNode.DefaultCharset charset ->
                        new AlterTableAction.DefaultCharset(
                                charset.charsetId(), charset.collationId());
                case AlterTableStatementNode.ConvertCharset charset ->
                        new AlterTableAction.ConvertCharset(
                                charset.charsetId(), charset.collationId());
            });
        }
        // 3、rename 等目标存在性仍由 MDL X 下的 DD 快照裁决。
        List<AlterTableAction> ordered = List.copyOf(actions);
        // 4、命令不包含 table id、space id 或路径，避免绑定后到执行前身份陈旧。
        return new BoundAlterTable(new AlterTableCommand(source, ordered));
    }

    /** 把 ADD COLUMN 类型/default 转为 DD action；charset=0 表示在 staged table options 下继承。 */
    private AlterTableAction.AddColumn bindAlterAddColumn(
            AlterTableStatementNode.AddColumn add, ZoneId zoneId) {
        ColumnTypeDefinition type = alterColumnType(add.type());
        ColumnDefaultDefinition defaultDefinition;
        Optional<StorageDefaultValue> storageDefault;
        if (add.defaultLiteral().isEmpty()) {
            defaultDefinition = type.nullable()
                    ? ColumnDefaultDefinition.implicitNull()
                    : ColumnDefaultDefinition.required();
            storageDefault = type.nullable()
                    ? Optional.of(StorageDefaultValue.NullValue.INSTANCE) : Optional.empty();
        } else if (add.defaultLiteral().orElseThrow() instanceof NullLiteralNode) {
            if (!type.nullable()) {
                throw new UnsupportedSqlShapeException(
                        "NOT NULL ADD COLUMN cannot use DEFAULT NULL");
            }
            defaultDefinition = ColumnDefaultDefinition.implicitNull();
            storageDefault = Optional.of(StorageDefaultValue.NullValue.INSTANCE);
        } else {
            LiteralNode literal = add.defaultLiteral().orElseThrow();
            SqlValue value = coercion.coerce(
                    literal, typeForDefaultValidation(type), zoneId, false);
            defaultDefinition = ColumnDefaultDefinition.constant(literal.lexeme());
            storageDefault = Optional.of(storageDefault(value));
        }
        AlterTableAction.Position position = new AlterTableAction.Position(
                switch (add.position().kind()) {
                    case LAST -> AlterTableAction.PositionKind.LAST;
                    case FIRST -> AlterTableAction.PositionKind.FIRST;
                    case AFTER -> AlterTableAction.PositionKind.AFTER;
                },
                add.position().afterColumn().map(value -> ObjectName.of(value.value())));
        return new AlterTableAction.AddColumn(
                ObjectName.of(add.name().value()), type, defaultDefinition,
                storageDefault, position);
    }

    /** SQL value 到 storage rebuild 常量 DTO 的穷尽映射；DD 只透传稳定值，不解析 SQL AST。 */
    private static StorageDefaultValue storageDefault(SqlValue value) {
        return switch (value) {
            case SqlValue.NullValue ignored -> StorageDefaultValue.NullValue.INSTANCE;
            case SqlValue.IntegerValue integer ->
                    new StorageDefaultValue.IntegerValue(integer.value());
            case SqlValue.FloatingValue floating ->
                    new StorageDefaultValue.FloatingValue(floating.value());
            case SqlValue.DecimalValue decimal ->
                    new StorageDefaultValue.DecimalValue(decimal.value());
            case SqlValue.StringValue string ->
                    new StorageDefaultValue.StringValue(string.value());
            case SqlValue.BytesValue bytes ->
                    new StorageDefaultValue.BytesValue(bytes.value());
            case SqlValue.TemporalValue temporal ->
                    new StorageDefaultValue.TemporalValue(
                            StorageDefaultValue.TemporalKind.valueOf(temporal.kind().name()),
                            temporal.value());
            case SqlValue.BitValue bits ->
                    new StorageDefaultValue.BitValue(bits.bytes());
            case SqlValue.EnumValue enumeration ->
                    new StorageDefaultValue.EnumValue(enumeration.ordinal());
            case SqlValue.SetValue set ->
                    new StorageDefaultValue.SetValue(set.bitmap());
        };
    }

    /** 映射内联 ADD INDEX 并拒绝重复 key part，列存在性留给 staged table。 */
    private static CreateIndexSpec bindAlterIndex(AlterTableStatementNode.AddIndex add) {
        HashSet<ObjectName> unique = new HashSet<>();
        List<CreateIndexKeyPartSpec> parts = add.keyParts().stream().map(part -> {
            ObjectName column = ObjectName.of(part.column().value());
            if (!unique.add(column)) {
                throw new UnsupportedSqlShapeException(
                        "ALTER ADD INDEX repeats column: " + column.displayName());
            }
            return new CreateIndexKeyPartSpec(column,
                    part.order() == IndexKeyOrderNode.ASC ? IndexOrder.ASC : IndexOrder.DESC, 0);
        }).toList();
        return new CreateIndexSpec(
                ObjectName.of(add.name().value()), add.unique(), false, parts);
    }

    /** 将 parser 类型名/参数映射到 DD 28 类型；ENUM/SET 新声明需要 symbols，当前 SQL shape 明确拒绝。 */
    private static ColumnTypeDefinition alterColumnType(
            AlterTableStatementNode.ColumnType syntax) {
        DictionaryTypeId typeId;
        try {
            typeId = DictionaryTypeId.valueOf(syntax.name().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException unknown) {
            throw new UnsupportedSqlShapeException(
                    "unsupported ALTER column type: " + syntax.name(), unknown);
        }
        if (typeId == DictionaryTypeId.ENUM || typeId == DictionaryTypeId.SET) {
            throw new UnsupportedSqlShapeException(
                    "ALTER ADD COLUMN ENUM/SET requires symbol-list syntax not implemented by this slice");
        }
        int length = syntax.length() > 0 ? syntax.length() : defaultTypeLength(typeId);
        // 0 是 ALTER staged table 默认值的继承哨兵；非字符类型的 mapper 同样要求该字段保持 0。
        int charset = 0;
        int collation = 0;
        return new ColumnTypeDefinition(typeId, syntax.unsigned(), syntax.nullable(),
                length, syntax.scale(), charset, collation, List.of());
    }

    /** default 校验需要正 charset/collation；DD 执行时仍以 staged table options 替换继承哨兵。 */
    private static ColumnTypeDefinition typeForDefaultValidation(ColumnTypeDefinition type) {
        if (!characterType(type.typeId())) {
            return type;
        }
        return new ColumnTypeDefinition(type.typeId(), type.unsigned(), type.nullable(),
                type.length(), type.scale(), 1, 1, type.symbols());
    }

    /** 未显式参数时使用与 Record codec 相符的确定上界。 */
    private static int defaultTypeLength(DictionaryTypeId type) {
        return switch (type) {
            case CHAR, VARCHAR, BINARY, VARBINARY -> 255;
            case DECIMAL -> 10;
            case BIT -> 1;
            case TINYTEXT, TINYBLOB -> 255;
            case TEXT, BLOB -> 65_535;
            case MEDIUMTEXT, MEDIUMBLOB -> 16_777_215;
            case LONGTEXT, LONGBLOB, JSON -> Integer.MAX_VALUE;
            default -> 0;
        };
    }

    /** 字符串/JSON 类型需要从 staged table options 继承 charset/collation。 */
    private static boolean characterType(DictionaryTypeId type) {
        return switch (type) {
            case CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, JSON -> true;
            default -> false;
        };
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
        // 1、固定 exact table version 并解析投影；locking read 用 WRITE intent 保证 MDL 生命周期覆盖事务锁。
        TableAccessIntent intent = select.lockingClause() == SelectLockingClause.NONE
                ? TableAccessIntent.READ : TableAccessIntent.WRITE;
        TableDefinition table = openActiveBoundTable(select.table(), context, intent);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        List<Integer> projections = projectionOrdinals(select, table, columns);
        // 2、全部谓词先按列类型转换并求交；该结果同时驱动访问范围与最终 residual 真值，索引边界不是过滤权威。
        PredicateBinding predicates = bindPredicates(select.predicates(), table, columns, context);
        Map<ObjectName, LiteralNode> exactEqualities = exactEqualityLiterals(select.predicates(), columns);

        // 3、普通一致性读仍优先复用已验证的 point/单列 non-unique equality 生产链；
        // locking point 与任意比较进入通用 current-range，避免 point 计划丢失锁模式。
        if (!predicates.empty() && exactEqualities != null) {
            Optional<IndexDefinition> point = choosePointAccess(table, exactEqualities.keySet());
            if (point.isPresent() && select.lockingClause() == SelectLockingClause.NONE) {
                return bindPointSelect(table, projections, point.orElseThrow(), exactEqualities, context);
            }
            if (point.isEmpty()) {
                Optional<BoundSecondaryRangeSelect> legacy = tryBindLegacySecondaryRange(
                        select, table, projections, exactEqualities, context);
                if (legacy.isPresent()) {
                    return legacy.orElseThrow();
                }
            }
        }

        // 4、按最长连续前缀与 stable index id 选择范围；无候选时显式绑定聚簇全扫，矛盾范围保留 empty 标记。
        RangeAccess access = chooseRangeAccess(table, predicates.constraints(), predicates.empty());
        return new BoundRangeSelect(table, projections, access.index().id().value(), access.range(),
                predicates.predicates(), lockMode(select.lockingClause()), predicates.empty());
    }

    /**
     * 把完整 equality point 绑定到既有 point plan；该方法只在调用方确认没有 residual 与 locking mode 后使用。
     */
    private BoundPointSelect bindPointSelect(TableDefinition table, List<Integer> projections,
                                             IndexDefinition access,
                                             Map<ObjectName, LiteralNode> predicates,
                                             SqlBindingContext context) {
        List<SqlValue> keys = new ArrayList<>(access.keyParts().size());
        for (IndexKeyPart part : access.keyParts()) {
            ColumnDefinition column = columnById(table, part.columnId(), "point access");
            if (isLobKey(column.type().typeId())) {
                throw new UnsupportedSqlShapeException("point SELECT does not support LOB/JSON index key");
            }
            LiteralNode literal = predicates.get(column.name());
            if (literal == null) {
                throw new UnsupportedSqlShapeException("SELECT must constrain every selected index key column");
            }
            keys.add(coercion.coerce(literal, column.type(), context.zoneId(), access.clustered()));
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
    private Optional<BoundSecondaryRangeSelect> tryBindLegacySecondaryRange(
            SelectStatementNode select, TableDefinition table, List<Integer> projections,
            Map<ObjectName, LiteralNode> predicates, SqlBindingContext context) {
        // 1. 稳定 index id 是无 optimizer 阶段下的确定性选择规则；不把复合/前缀索引误宣称为已支持。
        IndexDefinition access = table.indexes().stream()
                .filter(index -> !index.clustered() && !index.unique())
                .filter(index -> index.keyParts().size() == 1
                        && index.keyParts().getFirst().prefixBytes() == 0)
                .filter(index -> matchesExactKey(table, index, predicates.keySet()))
                .min(java.util.Comparator.comparingLong(index -> index.id().value()))
                .orElse(null);
        if (access == null) {
            return Optional.empty();
        }
        // 2. logical equality 必须恰好消费唯一声明 part；额外过滤条件尚无 residual predicate executor。
        IndexKeyPart part = access.keyParts().getFirst();
        ColumnDefinition column = table.columns().stream()
                .filter(candidate -> candidate.columnId() == part.columnId()).findFirst()
                .orElseThrow(() -> new SqlBindingException("secondary range index references missing DD column"));
        if (isLobKey(column.type().typeId())) {
            throw new UnsupportedSqlShapeException("secondary range SELECT does not support LOB/JSON index key");
        }
        LiteralNode literal = predicates.get(column.name());
        // 3. locking mode 随 bound plan 下传；具体事务与锁资源只在 gateway/storage 阶段创建。
        SelectLockMode lockMode = switch (select.lockingClause()) {
            case NONE -> SelectLockMode.CONSISTENT;
            case FOR_SHARE -> SelectLockMode.FOR_SHARE;
            case FOR_UPDATE -> SelectLockMode.FOR_UPDATE;
        };
        // 4、关闭 scope 并返回不可变结果，以稳定返回或领域异常完成收口。
        return Optional.of(new BoundSecondaryRangeSelect(table, projections, access.id().value(),
                List.of(coercion.coerce(literal, column.type(), context.zoneId(), false)), lockMode));
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
    private BoundStatement bindUpdate(UpdateStatementNode update, SqlBindingContext context) {
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
        // 4、完整聚簇 equality 保留 point DML；其余范围先冻结候选/残余计划，Executor 不得边扫边改。
        if (isExactPrimaryEquality(update.predicates(), table, columns)) {
            return new BoundUpdate(table, assignments.stream().map(BoundAssignment::ordinal).toList(),
                    assignments.stream().map(BoundAssignment::value).toList(),
                    bindPrimaryKey(update.predicates(), table, columns, context));
        }
        PredicateBinding predicates = bindPredicates(update.predicates(), table, columns, context);
        RangeAccess access = chooseRangeAccess(table, predicates.constraints(), predicates.empty());
        return new BoundRangeUpdate(table, assignments.stream().map(BoundAssignment::ordinal).toList(),
                assignments.stream().map(BoundAssignment::value).toList(), access.index().id().value(),
                access.range(), predicates.predicates(), predicates.empty());
    }

    private BoundStatement bindDelete(DeleteStatementNode delete, SqlBindingContext context) {
        TableDefinition table = openActiveBoundTable(delete.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        if (isExactPrimaryEquality(delete.predicates(), table, columns)) {
            return new BoundDelete(table, bindPrimaryKey(delete.predicates(), table, columns, context));
        }
        PredicateBinding predicates = bindPredicates(delete.predicates(), table, columns, context);
        RangeAccess access = chooseRangeAccess(table, predicates.constraints(), predicates.empty());
        return new BoundRangeDelete(table, access.index().id().value(), access.range(),
                predicates.predicates(), predicates.empty());
    }

    /**
     * 将 comparison/BETWEEN 绑定为 typed residual，并按列求可安全证明的交集。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按规范化列名解析 exact DD column，未知列和 LOB/JSON comparison 在计划发布前失败。</li>
     *     <li>把 literal 转换为列类型；BETWEEN 展开成闭合下界与闭合上界，NULL 直接标记 empty。</li>
     *     <li>同列上下界按可证明的值序求交；依赖 collation 的不同字符串不在 Binder 猜顺序，保留 residual 复核。</li>
     *     <li>冻结全部 residual 与列约束；后续索引选择只能缩小候选，不能删除 residual。</li>
     * </ol>
     *
     * @param syntaxPredicates Parser 产生的非空 conjunction
     * @param table statement lease 固定的 exact table version
     * @param columns 规范化列名到 exact column 的映射
     * @param context 提供时区和 metadata scope 的绑定上下文
     * @return typed residual、可用于索引前缀的列约束及 empty 证明
     * @throws UnsupportedSqlShapeException LOB/JSON 比较或同列重复 equality 时抛出
     */
    private PredicateBinding bindPredicates(List<PredicateNode> syntaxPredicates, TableDefinition table,
                                            Map<ObjectName, ColumnDefinition> columns,
                                            SqlBindingContext context) {
        // 1、列解析与 LOB 边界在任何 bound plan 发布前完成。
        ArrayList<BoundRowPredicate> bound = new ArrayList<>();
        LinkedHashMap<Integer, ColumnConstraint> constraints = new LinkedHashMap<>();
        boolean empty = false;
        for (PredicateNode syntax : syntaxPredicates) {
            ColumnDefinition column = requireColumn(columns, ObjectName.of(syntax.column().value()));
            if (isLobKey(column.type().typeId())) {
                throw new UnsupportedSqlShapeException(
                        "comparison predicate does not support LOB/JSON column: " + column.name());
            }
            ColumnConstraint constraint = constraints.computeIfAbsent(
                    column.ordinal(), ignored -> new ColumnConstraint());
            // 2、每一种 AST 都展开为统一的 typed comparison；NULL 遵循 UNKNOWN，不进入物理 key。
            if (syntax instanceof EqualityPredicateNode equality) {
                if (constraint.hasEquality()) {
                    throw new UnsupportedSqlShapeException(
                            "duplicate equality predicate: " + column.name());
                }
                SqlValue value = coercePredicate(equality.value(), column, context);
                bound.add(new BoundRowPredicate(column.ordinal(), BoundRowPredicateOperator.EQUAL, value));
                empty |= constraint.add(BoundRowPredicateOperator.EQUAL, value);
            } else if (syntax instanceof ComparisonPredicateNode comparison) {
                BoundRowPredicateOperator operator = switch (comparison.operator()) {
                    case LESS_THAN -> BoundRowPredicateOperator.LESS_THAN;
                    case LESS_THAN_OR_EQUAL -> BoundRowPredicateOperator.LESS_THAN_OR_EQUAL;
                    case GREATER_THAN -> BoundRowPredicateOperator.GREATER_THAN;
                    case GREATER_THAN_OR_EQUAL -> BoundRowPredicateOperator.GREATER_THAN_OR_EQUAL;
                };
                SqlValue value = coercePredicate(comparison.value(), column, context);
                bound.add(new BoundRowPredicate(column.ordinal(), operator, value));
                empty |= constraint.add(operator, value);
            } else if (syntax instanceof BetweenPredicateNode between) {
                SqlValue lower = coercePredicate(between.lowerInclusive(), column, context);
                SqlValue upper = coercePredicate(between.upperInclusive(), column, context);
                bound.add(new BoundRowPredicate(
                        column.ordinal(), BoundRowPredicateOperator.GREATER_THAN_OR_EQUAL, lower));
                bound.add(new BoundRowPredicate(
                        column.ordinal(), BoundRowPredicateOperator.LESS_THAN_OR_EQUAL, upper));
                empty |= constraint.add(BoundRowPredicateOperator.GREATER_THAN_OR_EQUAL, lower);
                empty |= constraint.add(BoundRowPredicateOperator.LESS_THAN_OR_EQUAL, upper);
            } else {
                throw new UnsupportedSqlShapeException(
                        "unsupported predicate AST: " + syntax.getClass().getSimpleName());
            }
        }
        // 3、ColumnConstraint 只在类型值序确定时收紧；未知 collation 顺序不产生可能漏行的推断。
        // 4、复制后的 residual 是最终 SQL 真值权威，访问路径后续不得移除其中任何一项。
        return new PredicateBinding(List.copyOf(bound), Map.copyOf(constraints), empty);
    }

    /** comparison 中的 NULL 即使面对 NOT NULL 列也合法，只是永远不能得到 TRUE。 */
    private SqlValue coercePredicate(LiteralNode literal, ColumnDefinition column,
                                     SqlBindingContext context) {
        if (literal instanceof NullLiteralNode) {
            return SqlValue.NullValue.INSTANCE;
        }
        return coercion.coerce(literal, column.type(), context.zoneId(), false);
    }

    /**
     * 选择最长连续索引前缀；同长度按 stable index id，完全无首列约束时回退聚簇全扫。
     */
    private RangeAccess chooseRangeAccess(TableDefinition table,
                                          Map<Integer, ColumnConstraint> constraints,
                                          boolean empty) {
        if (empty) {
            return new RangeAccess(table.primaryIndex(), BoundIndexRange.unbounded(), 0);
        }
        RangeAccess best = null;
        for (IndexDefinition index : table.indexes()) {
            RangeAccess candidate = rangeForIndex(table, index, constraints);
            if (candidate == null) {
                continue;
            }
            if (best == null || candidate.score() > best.score()
                    || candidate.score() == best.score()
                    && candidate.index().id().value() < best.index().id().value()) {
                best = candidate;
            }
        }
        if (best != null) {
            return best;
        }
        IndexDefinition primary = table.primaryIndex();
        if (primary.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            throw new UnsupportedSqlShapeException(
                    "clustered full scan does not support prefix primary key");
        }
        return new RangeAccess(primary, BoundIndexRange.unbounded(), 0);
    }

    /**
     * 将一个 DD 索引的连续 equality-prefix + 最多一个 range part 转成物理排序边界。
     * DESC part 会交换 SQL 下/上界；短 key 由 B+Tree prefix comparison 覆盖后续 key 与 clustered suffix。
     */
    private RangeAccess rangeForIndex(TableDefinition table, IndexDefinition index,
                                      Map<Integer, ColumnConstraint> constraints) {
        if (index.keyParts().stream().anyMatch(part -> part.prefixBytes() != 0)) {
            return null;
        }
        ArrayList<SqlValue> equalityPrefix = new ArrayList<>();
        BoundRangeEndpoint lower = null;
        BoundRangeEndpoint upper = null;
        int score = 0;
        for (IndexKeyPart part : index.keyParts()) {
            ColumnDefinition column = columnById(table, part.columnId(), "range access");
            if (isLobKey(column.type().typeId())) {
                return null;
            }
            ColumnConstraint constraint = constraints.get(column.ordinal());
            if (constraint == null) {
                break;
            }
            if (constraint.hasEquality()
                    && !(constraint.equality() instanceof SqlValue.NullValue)) {
                equalityPrefix.add(constraint.equality());
                score++;
                continue;
            }
            BoundRangeEndpoint logicalLower = endpoint(equalityPrefix, constraint.lower());
            BoundRangeEndpoint logicalUpper = endpoint(equalityPrefix, constraint.upper());
            if (logicalLower == null && logicalUpper == null) {
                break;
            }
            if (part.order() == IndexOrder.ASC) {
                lower = logicalLower;
                upper = logicalUpper;
            } else {
                lower = logicalUpper;
                upper = logicalLower;
            }
            score++;
            break;
        }
        if (score == 0) {
            return null;
        }
        if (lower == null && upper == null && !equalityPrefix.isEmpty()) {
            lower = new BoundRangeEndpoint(equalityPrefix, true);
            upper = new BoundRangeEndpoint(equalityPrefix, true);
        }
        return new RangeAccess(index, new BoundIndexRange(
                Optional.ofNullable(lower), Optional.ofNullable(upper)), score);
    }

    private static BoundRangeEndpoint endpoint(List<SqlValue> prefix, BoundValue value) {
        if (value == null) {
            return null;
        }
        ArrayList<SqlValue> keys = new ArrayList<>(prefix);
        keys.add(value.value());
        return new BoundRangeEndpoint(keys, value.inclusive());
    }

    /** 返回仅由不重复 equality 组成的名称→literal 映射；包含 comparison/BETWEEN 时返回 null。 */
    private static Map<ObjectName, LiteralNode> exactEqualityLiterals(
            List<PredicateNode> predicates, Map<ObjectName, ColumnDefinition> columns) {
        LinkedHashMap<ObjectName, LiteralNode> values = new LinkedHashMap<>();
        for (PredicateNode syntax : predicates) {
            if (!(syntax instanceof EqualityPredicateNode equality)) {
                return null;
            }
            ObjectName name = ObjectName.of(equality.column().value());
            requireColumn(columns, name);
            if (values.putIfAbsent(name, equality.value()) != null) {
                throw new UnsupportedSqlShapeException("duplicate equality predicate: " + name);
            }
        }
        return values;
    }

    private static boolean isExactPrimaryEquality(List<PredicateNode> predicates, TableDefinition table,
                                                  Map<ObjectName, ColumnDefinition> columns) {
        Map<ObjectName, LiteralNode> equalities = exactEqualityLiterals(predicates, columns);
        return equalities != null && matchesExactKey(table, table.primaryIndex(), equalities.keySet());
    }

    private static SelectLockMode lockMode(SelectLockingClause clause) {
        return switch (clause) {
            case NONE -> SelectLockMode.CONSISTENT;
            case FOR_SHARE -> SelectLockMode.FOR_SHARE;
            case FOR_UPDATE -> SelectLockMode.FOR_UPDATE;
        };
    }

    private static ColumnDefinition columnById(TableDefinition table, long columnId, String operation) {
        return table.columns().stream().filter(column -> column.columnId() == columnId).findFirst()
                .orElseThrow(() -> new SqlBindingException(
                        operation + " index references missing DD column " + columnId));
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
    private List<SqlValue> bindPrimaryKey(List<PredicateNode> predicates, TableDefinition table,
                                          Map<ObjectName, ColumnDefinition> columns,
                                          SqlBindingContext context) {
        // 1、校验语法/命令、会话状态与元数据身份，在共享或持久副作用前拒绝非法状态。
        Map<ObjectName, LiteralNode> values = new LinkedHashMap<>();
        for (PredicateNode syntaxPredicate : predicates) {
            if (!(syntaxPredicate instanceof EqualityPredicateNode predicate)) {
                throw new UnsupportedSqlShapeException(
                        "comparison predicate write requires a range DML plan");
            }
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
     * 同列 comparison 的可安全索引约束。不同字符串/二进制值的排序依赖 DD collation，
     * Binder 不复制 Record 比较器，因此无法证明顺序时保留第一条边界并依赖 residual。
     */
    private static final class ColumnConstraint {
        private SqlValue equality;
        private BoundValue lower;
        private BoundValue upper;

        private boolean hasEquality() {
            return equality != null;
        }

        private SqlValue equality() {
            return equality;
        }

        private BoundValue lower() {
            return lower;
        }

        private BoundValue upper() {
            return upper;
        }

        /**
         * 合并一个 typed comparison，并报告是否已能证明 conjunction 为空。
         *
         * @param operator residual comparison 操作符
         * @param value 已完成 DD 类型转换的右值
         * @return NULL、交叉上下界或 equality 与边界冲突时为 {@code true}
         */
        private boolean add(BoundRowPredicateOperator operator, SqlValue value) {
            if (value instanceof SqlValue.NullValue) {
                return true;
            }
            switch (operator) {
                case EQUAL -> equality = value;
                case GREATER_THAN -> lower = stricterLower(lower, new BoundValue(value, false));
                case GREATER_THAN_OR_EQUAL -> lower = stricterLower(lower, new BoundValue(value, true));
                case LESS_THAN -> upper = stricterUpper(upper, new BoundValue(value, false));
                case LESS_THAN_OR_EQUAL -> upper = stricterUpper(upper, new BoundValue(value, true));
            }
            return contradictory();
        }

        private boolean contradictory() {
            if (equality != null) {
                Integer lowerOrder = lower == null ? null : compareKnown(equality, lower.value());
                if (lowerOrder != null && (lowerOrder < 0 || lowerOrder == 0 && !lower.inclusive())) {
                    return true;
                }
                Integer upperOrder = upper == null ? null : compareKnown(equality, upper.value());
                if (upperOrder != null && (upperOrder > 0 || upperOrder == 0 && !upper.inclusive())) {
                    return true;
                }
            }
            if (lower != null && upper != null) {
                Integer order = compareKnown(lower.value(), upper.value());
                return order != null && (order > 0
                        || order == 0 && (!lower.inclusive() || !upper.inclusive()));
            }
            return false;
        }

        private static BoundValue stricterLower(BoundValue current, BoundValue candidate) {
            if (current == null) {
                return candidate;
            }
            Integer order = compareKnown(candidate.value(), current.value());
            if (order == null || order < 0) {
                return current;
            }
            if (order > 0) {
                return candidate;
            }
            return new BoundValue(current.value(), current.inclusive() && candidate.inclusive());
        }

        private static BoundValue stricterUpper(BoundValue current, BoundValue candidate) {
            if (current == null) {
                return candidate;
            }
            Integer order = compareKnown(candidate.value(), current.value());
            if (order == null || order > 0) {
                return current;
            }
            if (order < 0) {
                return candidate;
            }
            return new BoundValue(current.value(), current.inclusive() && candidate.inclusive());
        }
    }

    /**
     * 只比较不依赖 charset/collation 的同类 typed value；返回 null 表示不能在 Binder 安全推断顺序。
     */
    private static Integer compareKnown(SqlValue left, SqlValue right) {
        if (left.equals(right)) {
            return 0;
        }
        if (left instanceof SqlValue.IntegerValue a && right instanceof SqlValue.IntegerValue b) {
            return a.value().compareTo(b.value());
        }
        if (left instanceof SqlValue.FloatingValue a && right instanceof SqlValue.FloatingValue b) {
            return Double.compare(a.value(), b.value());
        }
        if (left instanceof SqlValue.DecimalValue a && right instanceof SqlValue.DecimalValue b) {
            return a.value().compareTo(b.value());
        }
        if (left instanceof SqlValue.TemporalValue a && right instanceof SqlValue.TemporalValue b
                && a.kind() == b.kind()) {
            return Long.compare(a.value(), b.value());
        }
        if (left instanceof SqlValue.EnumValue a && right instanceof SqlValue.EnumValue b) {
            return Integer.compare(a.ordinal(), b.ordinal());
        }
        if (left instanceof SqlValue.SetValue a && right instanceof SqlValue.SetValue b) {
            return Long.compareUnsigned(a.bitmap(), b.bitmap());
        }
        if (left instanceof SqlValue.BitValue a && right instanceof SqlValue.BitValue b
                && a.bitWidth() == b.bitWidth()) {
            byte[] av = a.bytes();
            byte[] bv = b.bytes();
            for (int i = 0; i < av.length; i++) {
                int order = Integer.compare(Byte.toUnsignedInt(av[i]), Byte.toUnsignedInt(bv[i]));
                if (order != 0) {
                    return order;
                }
            }
            return 0;
        }
        return null;
    }

    /** 单侧索引边界值与开闭属性。 */
    private record BoundValue(SqlValue value, boolean inclusive) {
    }

    /** 全部 residual、按 ordinal 聚合的范围约束与 empty 证明。 */
    private record PredicateBinding(List<BoundRowPredicate> predicates,
                                    Map<Integer, ColumnConstraint> constraints,
                                    boolean empty) {
    }

    /** 确定性访问索引、其物理范围与连续前缀得分。 */
    private record RangeAccess(IndexDefinition index, BoundIndexRange range, int score) {
    }

    /**
     * 封装SQL 名称绑定与类型推导中 {@code BoundAssignment} 的绑定结果或元数据租约；schema 版本与释放责任在创建后固定，执行结束必须按所有权关闭。
     *
     * @param ordinal 参与 {@code 构造} 的零基位置 {@code ordinal}；必须非负且小于所属页面、集合或持久结构的容量
     * @param value SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     */
    private record BoundAssignment(int ordinal, SqlValue value) { }
}
