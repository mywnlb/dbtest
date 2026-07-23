package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.*;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.sql.binder.bound.*;
import cn.zhangyis.db.dd.ddl.DropSecondaryIndexCommand;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;
import cn.zhangyis.db.sql.binder.exception.UnknownColumnException;
import cn.zhangyis.db.sql.binder.exception.UnsupportedSqlShapeException;
import cn.zhangyis.db.sql.expression.BoundColumnReference;
import cn.zhangyis.db.sql.expression.BoundComparison;
import cn.zhangyis.db.sql.expression.BoundComparisonOperator;
import cn.zhangyis.db.sql.expression.BoundConjunction;
import cn.zhangyis.db.sql.expression.BoundDisjunction;
import cn.zhangyis.db.sql.expression.BoundExpression;
import cn.zhangyis.db.sql.expression.BoundLiteral;
import cn.zhangyis.db.sql.expression.BoundNegation;
import cn.zhangyis.db.sql.expression.BoundNullTest;
import cn.zhangyis.db.sql.expression.BoundNullTestOperator;
import cn.zhangyis.db.sql.type.SqlValue;
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

/**
 * Calcite-Lite 语义 Binder：固定 exact DD version，完成名称解析、类型转换与 M3 boolean
 * 表达式绑定，只输出不含访问路径的 Bound IR。
 */
public final class DefaultSqlBinder implements SqlBinder {
    /**
     * 本对象持有的 {@code coercion} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SqlTypeCoercion coercion;

    /**
     * 创建 {@code DefaultSqlBinder}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param coercion DD column type 与 SQL literal 之间的严格转换器；不得为 {@code null}，
     *                 生命周期必须覆盖 Binder
     * @throws DatabaseValidationException coercion 缺失时抛出；不会发布半初始化 Binder
     */
    public DefaultSqlBinder(SqlTypeCoercion coercion) {
        if (coercion == null) throw new DatabaseValidationException("SQL type coercion must not be null");
        this.coercion = coercion;
    }

    /**
     * 在调用方已建立的 metadata scope 内完成名称、shape 和值转换。Binder 不发布或关闭 scope，
     * 因为只有 Binder、Logical Converter 和 Optimizer 全部成功后，Session 才能原子发布可执行计划。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 AST 与 binding context，纯输入错误不会访问 metadata。</li>
     *     <li>按关系语句种类分派到 INSERT/SELECT/UPDATE/DELETE 语义绑定，不接收 DDL 或事务命令。</li>
     *     <li>返回不含 index/range 的不可变 Bound IR；metadata scope 仍保持 OPEN 并归调用方所有。</li>
     * </ol>
     *
     * @param statement Parser 已完整消费的数据访问 AST；不得为 {@code null}，且必须是
     *                  INSERT、SELECT、UPDATE 或 DELETE
     * @param context 当前 schema、时区和仍为 OPEN 的 statement metadata scope；不得为
     *                {@code null}，scope 所有权始终留给 Compiler/Session
     * @return 绑定 exact table version、typed values/predicates 与读写意图的不可变语义 IR
     * @throws DatabaseValidationException statement 或 context 缺失时抛出；metadata scope 不发生发布
     * @throws SqlBindingException 表、列、类型或 metadata lease 无法绑定时抛出；调用方应关闭 scope
     * @throws UnsupportedSqlShapeException AST 不是当前关系语句或 SQL shape 超出当前切片时抛出
     */
    @Override
    public BoundRelationalStatement bind(StatementNode statement, SqlBindingContext context) {
        // 1、空 AST/context 在 metadata lease 获取前失败。
        if (statement == null || context == null) {
            throw new DatabaseValidationException("binding statement/context must not be null");
        }
        // 2、只分派关系数据语句；DDL/session control 必须保持各自的独立生命周期。
        // 3、各分支只返回 semantic Bound，成功与失败都不改变 scope 所有权。
        return switch (statement) {
            case InsertStatementNode insert -> bindInsert(insert, context);
            case SelectStatementNode select -> bindSelect(select, context);
            case UpdateStatementNode update -> bindUpdate(update, context);
            case DeleteStatementNode delete -> bindDelete(delete, context);
            default -> throw new UnsupportedSqlShapeException(
                    "statement is handled by Session rather than SQL Binder: "
                            + statement.getClass().getSimpleName());
        };
    }

    /**
     * 绑定不需要 transaction metadata scope 的 DDL 语法。这里只解析 current schema、名称和重复 key part；
     * table/index/column 的 committed existence 必须由随后持 MDL X 的 DD coordinator 重新验证。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 CREATE INDEX AST 与 current schema 容器，纯输入错误早于 implicit commit。</li>
     *     <li>按 catalog/schema/table 规则补全限定名；本阶段不打开 table metadata lease。</li>
     *     <li>规范化 index/key-part 名称与顺序并拒绝重复列，不验证目标对象是否已存在。</li>
     *     <li>返回不可变 DD command；目标重验和持久副作用留给独立 DDL owner 下的 coordinator。</li>
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
        // 1、AST/container 校验早于 Session implicit commit。
        if (statement == null || currentSchema == null) {
            throw new DatabaseValidationException("DDL binding statement/current schema must not be null");
        }
        // 2、只补全逻辑限定名，不打开 table lease 或读取物理 binding。
        QualifiedTableName table = qualify(statement.table(), currentSchema);
        // 3、key part 规范化保持声明顺序，并在 command 创建前拒绝重复列。
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
        // 4、返回纯 DD command；对象存在性由 coordinator 在 MDL X 下重验。
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
     * 把完整单行 INSERT 绑定为 exact table version 下按 column ordinal 排列的 typed values。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 WRITE intent 打开 ACTIVE exact table version，并建立规范化 column map。</li>
     *     <li>要求 SQL 显式列数等于 table width，创建尚未发布的 ordinal value 容器。</li>
     *     <li>逐列拒绝重复/未知名称，并按 DD 类型和主键上下文转换 literal；缺列在返回前失败。</li>
     *     <li>冻结 {@link BoundInsert}；不 publish/close metadata scope，也不创建 storage row。</li>
     * </ol>
     *
     * @param insert Parser 产生的单行 INSERT；必须显式列出与目标表等宽的列和值
     * @param context 当前 schema、时区和未发布 statement scope
     * @return 按 exact table ordinal 排列的完整 typed row
     * @throws SqlBindingException 目标表、列或 metadata lease 无法解析时抛出
     * @throws UnsupportedSqlShapeException 列缺失、重复、未知或 literal 不能按 DD 类型转换时抛出
     */
    private BoundInsert bindInsert(InsertStatementNode insert, SqlBindingContext context) {
        // 1、WRITE lease 固定后续 logical/physical/executor 共用的 exact table version。
        TableDefinition table = openActiveBoundTable(insert.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        if (insert.columns().size() != table.columns().size()) {
            throw new UnsupportedSqlShapeException("INSERT must name every table column exactly once");
        }
        // 2、完整行约束在值转换前确认，避免发布部分 ordinal 布局。
        List<SqlValue> values = new ArrayList<>(java.util.Collections.nCopies(table.columns().size(), null));
        HashSet<ObjectName> assigned = new HashSet<>();
        HashSet<Long> primaryColumns = new HashSet<>();
        // 3、主键列转换使用 key 约束，但不选择具体 B+Tree 访问路径。
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
        // 4、返回 semantic INSERT；scope 仍由 compiler/Session 管理。
        return new BoundInsert(table, values);
    }

    /**
     * 把单表 SELECT 绑定为投影、typed residual 与不可改写的读取模式。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按 consistent/locking read 选择 READ/WRITE metadata intent，并固定 exact table version。</li>
     *     <li>把 star 或显式投影解析为保持用户顺序的唯一 column ordinals。</li>
     *     <li>把 WHERE conjunction 转为 typed residual；BETWEEN 展开但不求物理 range。</li>
     *     <li>冻结 {@link BoundSelect} 和 read mode；不选择索引、不发布 metadata scope。</li>
     * </ol>
     *
     * @param select Parser 产生的单表 SELECT；必须包含非空 WHERE boolean condition
     * @param context 当前 schema、时区和未发布 statement scope
     * @return 保持投影顺序、全部 typed residual 与 locking intent 的语义 SELECT
     * @throws SqlBindingException 目标表、投影列、谓词列或 metadata lease 无法解析时抛出
     * @throws UnsupportedSqlShapeException 投影重复、谓词重复或 LOB/JSON comparison 不受支持时抛出
     */
    private BoundSelect bindSelect(SelectStatementNode select, SqlBindingContext context) {
        // 1、固定 exact table version；locking read 用 WRITE intent，使 MDL 生命周期覆盖后续事务锁。
        TableAccessIntent intent = select.lockingClause() == SelectLockingClause.NONE
                ? TableAccessIntent.READ : TableAccessIntent.WRITE;
        TableDefinition table = openActiveBoundTable(select.table(), context, intent);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        // 2、投影只解析为 table ordinal，不携带 record layout 或访问索引。
        List<Integer> projections = projectionOrdinals(select, table, columns);
        // 3、WHERE 只完成名称解析、类型转换和 BETWEEN 展开；索引选择留给 Optimizer。
        BoundExpression condition =
                bindCondition(select.condition(), table, columns, context);
        // 4、冻结读取语义；本阶段不发布 metadata scope，也不创建执行资源。
        return new BoundSelect(
                table, projections, condition, lockMode(select.lockingClause()));
    }

    /**
     * 把单表 UPDATE 绑定为稳定赋值集与 typed residual，point/range 分类留给 Optimizer。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 WRITE intent 固定 ACTIVE exact table version 和 column map。</li>
     *     <li>解析赋值，拒绝重复列和聚簇主键修改，并按目标 DD 类型转换 literal。</li>
     *     <li>按 table ordinal 排序赋值，使语义 IR 不依赖 SQL SET 子句顺序。</li>
     *     <li>绑定并完整保留 WHERE residual，返回 {@link BoundUpdate}；不形成 primary key 或 range。</li>
     * </ol>
     *
     * @param update Parser 产生的单表 UPDATE；必须包含非空 assignment 与 WHERE conjunction
     * @param context 当前 schema、时区和未发布 statement scope
     * @return ordinal 升序的 typed patch 与完整 typed residual
     * @throws SqlBindingException 目标表、assignment/predicate 列或 metadata lease 无法解析时抛出
     * @throws UnsupportedSqlShapeException assignment 重复、修改主键或 literal/谓词不受支持时抛出
     */
    private BoundUpdate bindUpdate(UpdateStatementNode update, SqlBindingContext context) {
        // 1、在 WRITE metadata intent 下固定 exact table version，后续计划不得切换版本。
        TableDefinition table = openActiveBoundTable(update.table(), context, TableAccessIntent.WRITE);
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        // 2、解析赋值并拒绝主键修改；该限制维持当前 storage update API 的记录身份不变量。
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
        // 3、赋值按 table ordinal 排序，形成与语法顺序无关的稳定语义表示。
        assignments.sort(java.util.Comparator.comparingInt(BoundAssignment::ordinal));
        // 4、全部 predicate 作为 residual 冻结；point/range 决策及范围原子性由后续层承担。
        return new BoundUpdate(
                table, assignments.stream().map(BoundAssignment::ordinal).toList(),
                assignments.stream().map(BoundAssignment::value).toList(),
                bindCondition(update.condition(), table, columns, context));
    }

    /**
     * 把单表 DELETE 绑定为 exact table version 与 typed residual，不预先形成主键或范围定位值。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 WRITE intent 打开 ACTIVE table，使后续 current-read 锁与 MDL 生命周期一致。</li>
     *     <li>从 exact table version 建立规范化 column map，不读取 storage descriptor。</li>
     *     <li>解析并转换全部 WHERE conjunction，保留 SQL 三值语义所需的 residual。</li>
     *     <li>冻结 {@link BoundDelete}；point/range 选择及语句原子性分别留给 Optimizer 和 Data Port。</li>
     * </ol>
     *
     * @param delete Parser 产生的单表 DELETE AST；必须包含受支持的非空 conjunction
     * @param context 当前 schema、时区和未发布 metadata scope
     * @return 不含访问路径的不可变 DELETE 语义
     * @throws SqlBindingException 表、列、类型或 predicate 形状无法安全绑定时抛出
     */
    private BoundDelete bindDelete(DeleteStatementNode delete, SqlBindingContext context) {
        // 1、WRITE intent 固定 current-read 所需的 metadata 生命周期。
        TableDefinition table = openActiveBoundTable(delete.table(), context, TableAccessIntent.WRITE);
        // 2、列解析始终基于同一 exact table version。
        Map<ObjectName, ColumnDefinition> columns = columns(table);
        // 3、typed residual 是最终真值权威，不能在 Binder 中按索引近似删除。
        // 4、返回 semantic DELETE，不 publish scope 或创建 storage statement guard。
        return new BoundDelete(
                table, bindCondition(
                delete.condition(), table, columns, context));
    }

    /**
     * 将完整 boolean AST 递归绑定为 typed residual；Binder 不推导访问路径。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>按节点种类递归保持 AND/OR/NOT 树形，复合节点不访问 DD 或存储。</li>
     *     <li>原子谓词解析 exact DD column；comparison 转换 literal，BETWEEN 展开闭区间。</li>
     *     <li>每个正向 conjunction 独立拒绝直接重复 equality；OR 分支不共享该集合。</li>
     *     <li>冻结完整 typed residual；Optimizer 只能派生安全候选范围，不能删除条件。</li>
     * </ol>
     *
     * @param syntaxCondition Parser 产生的非空 boolean 条件
     * @param table statement lease 固定的 exact table version
     * @param columns 规范化列名到 exact column 的映射
     * @param context 提供时区和 metadata scope 的绑定上下文
     * @return 保持 SQL 优先级、operand 顺序和三值语义的 typed residual
     * @throws UnsupportedSqlShapeException LOB/JSON comparison、重复 equality 或未知节点时抛出
     */
    private BoundExpression bindCondition(
            BooleanExpressionNode syntaxCondition, TableDefinition table,
            Map<ObjectName, ColumnDefinition> columns, SqlBindingContext context) {
        // 1、封闭节点集合是 Parser/Binder 协议；缺失条件不能以 TRUE 猜测继续。
        if (syntaxCondition == null) {
            throw new UnsupportedSqlShapeException(
                    "WHERE boolean condition must not be null");
        }
        // 2、复合节点只控制 boolean 结构；原子分支才解析 column 和 exact type。
        return switch (syntaxCondition) {
            case ConjunctionExpressionNode conjunction -> {
                // 3、只在同一正向 AND 域检查重复 equality；OR/NOT 是语义屏障。
                validateConjunctionEqualities(
                        conjunction, columns, new HashSet<>());
                yield new BoundConjunction(
                        conjunction.operands().stream()
                                .map(operand -> bindCondition(
                                        operand, table, columns, context))
                                .toList());
            }
            case DisjunctionExpressionNode disjunction ->
                    new BoundDisjunction(
                            disjunction.operands().stream()
                                    .map(operand -> bindCondition(
                                            operand, table, columns, context))
                                    .toList());
            case NegationExpressionNode negation ->
                    new BoundNegation(
                            bindCondition(
                                    negation.operand(), table,
                                    columns, context),
                            negation.position());
            case PredicateNode predicate ->
                    bindPredicate(predicate, columns, context);
        };
    }

    /**
     * 绑定一个列原子谓词；comparison 与 null-test 使用不同的 LOB 边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>从 exact table column map 解析列身份，未知列立即失败。</li>
     *     <li>NULL 检查只读取列值是否为空，允许 LOB/JSON 并构造非 nullable boolean。</li>
     *     <li>comparison/BETWEEN 拒绝当前 comparator 不支持的 LOB/JSON，再转换 literal。</li>
     *     <li>返回 column-literal 规范表达式或闭区间 conjunction，不选择索引。</li>
     * </ol>
     *
     * @param syntax Parser 产生的原子谓词
     * @param columns exact table 的规范化列映射
     * @param context 当前时区与绑定作用域
     * @return 完整 typed 原子表达式
     * @throws UnsupportedSqlShapeException LOB comparison 或未知 predicate 节点时抛出
     */
    private BoundExpression bindPredicate(
            PredicateNode syntax,
            Map<ObjectName, ColumnDefinition> columns,
            SqlBindingContext context) {
        // 1、所有分支共享同一个 exact column identity。
        ColumnDefinition column = requireColumn(
                columns, ObjectName.of(syntax.column().value()));
        // 2、null-test 不执行值序比较，外置 LOB 也能通过 NullValue 标记判断。
        if (syntax instanceof NullTestPredicateNode nullTest) {
            return new BoundNullTest(
                    columnReference(column, nullTest.position()),
                    switch (nullTest.operator()) {
                        case IS_NULL -> BoundNullTestOperator.IS_NULL;
                        case IS_NOT_NULL ->
                                BoundNullTestOperator.IS_NOT_NULL;
                    },
                    nullTest.position());
        }
        // 3、当前 Record comparator 不接受 LOB/JSON scalar comparison。
        if (isLobKey(column.type().typeId())) {
            throw new UnsupportedSqlShapeException(
                    "comparison predicate does not support LOB/JSON column: "
                            + column.name());
        }
        // 4、普通 comparison 保留 SQL NULL；BETWEEN 只做语义展开，不比较端点。
        if (syntax instanceof EqualityPredicateNode equality) {
            SqlValue value =
                    coercePredicate(equality.value(), column, context);
            return comparison(
                    column, equality.column().position(),
                    equality.value(), BoundComparisonOperator.EQUAL, value);
        }
        if (syntax instanceof ComparisonPredicateNode comparison) {
            BoundComparisonOperator operator = switch (comparison.operator()) {
                case LESS_THAN -> BoundComparisonOperator.LESS_THAN;
                case LESS_THAN_OR_EQUAL ->
                        BoundComparisonOperator.LESS_THAN_OR_EQUAL;
                case GREATER_THAN -> BoundComparisonOperator.GREATER_THAN;
                case GREATER_THAN_OR_EQUAL ->
                        BoundComparisonOperator.GREATER_THAN_OR_EQUAL;
            };
            SqlValue value =
                    coercePredicate(comparison.value(), column, context);
            return comparison(
                    column, comparison.column().position(),
                    comparison.value(), operator, value);
        }
        if (syntax instanceof BetweenPredicateNode between) {
            SqlValue lower = coercePredicate(
                    between.lowerInclusive(), column, context);
            SqlValue upper = coercePredicate(
                    between.upperInclusive(), column, context);
            return new BoundConjunction(List.of(
                    comparison(
                            column, between.column().position(),
                            between.lowerInclusive(),
                            BoundComparisonOperator.GREATER_THAN_OR_EQUAL,
                            lower),
                    comparison(
                            column, between.column().position(),
                            between.upperInclusive(),
                            BoundComparisonOperator.LESS_THAN_OR_EQUAL,
                            upper)));
        }
        throw new UnsupportedSqlShapeException(
                "unsupported predicate AST: "
                        + syntax.getClass().getSimpleName());
    }

    /**
     * 收集同一正向 AND 域中的直接 equality column，保留 M2 的重复条件拒绝语义。
     *
     * @param expression 当前 conjunction 子树
     * @param columns exact table 的规范化列映射
     * @param equalityColumns 当前 AND 域已出现的 column ordinal
     * @throws UnsupportedSqlShapeException 同列 equality 重复时抛出；OR/NOT 内部不参与本集合
     */
    private static void validateConjunctionEqualities(
            BooleanExpressionNode expression,
            Map<ObjectName, ColumnDefinition> columns,
            HashSet<Integer> equalityColumns) {
        if (expression instanceof ConjunctionExpressionNode conjunction) {
            conjunction.operands().forEach(operand ->
                    validateConjunctionEqualities(
                            operand, columns, equalityColumns));
            return;
        }
        if (expression instanceof EqualityPredicateNode equality) {
            ColumnDefinition column = requireColumn(
                    columns, ObjectName.of(equality.column().value()));
            if (!equalityColumns.add(column.ordinal())) {
                throw new UnsupportedSqlShapeException(
                        "duplicate equality predicate: " + column.name());
            }
        }
    }

    /**
     * 构造 exact table version 的 typed 列引用。
     *
     * @param column 当前 statement 固定的 DD 列定义
     * @param position 原始列 token 的源位置
     * @return 携带稳定 id、ordinal、exact type 与位置的列引用
     */
    private static BoundColumnReference columnReference(
            ColumnDefinition column,
            cn.zhangyis.db.sql.parser.SourcePosition position) {
        return new BoundColumnReference(
                column.columnId(), column.ordinal(),
                column.type(), position);
    }

    /**
     * 把 exact column 与已转换 literal 组装为 typed comparison。
     *
     * @param column 当前 statement table version 中解析出的列
     * @param columnPosition 列标识符的源起始位置
     * @param literal Parser 保留源位置的原始 literal
     * @param operator 语义 comparison 操作符
     * @param value 已按 column exact type 转换的值
     * @return column-literal 方向的不可变 comparison
     */
    private static BoundComparison comparison(
            ColumnDefinition column,
            cn.zhangyis.db.sql.parser.SourcePosition columnPosition,
            LiteralNode literal, BoundComparisonOperator operator,
            SqlValue value) {
        return new BoundComparison(
                columnReference(column, columnPosition),
                operator,
                new BoundLiteral(value, column.type(), literal.position()));
    }

    /** comparison 中的 NULL 即使面对 NOT NULL 列也合法，只是永远不能得到 TRUE。 */
    private SqlValue coercePredicate(LiteralNode literal, ColumnDefinition column,
                                     SqlBindingContext context) {
        if (literal instanceof NullLiteralNode) {
            return SqlValue.NullValue.INSTANCE;
        }
        return coercion.coerce(literal, column.type(), context.zoneId(), false);
    }

    private static SelectLockMode lockMode(SelectLockingClause clause) {
        return switch (clause) {
            case NONE -> SelectLockMode.CONSISTENT;
            case FOR_SHARE -> SelectLockMode.FOR_SHARE;
            case FOR_UPDATE -> SelectLockMode.FOR_UPDATE;
        };
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
