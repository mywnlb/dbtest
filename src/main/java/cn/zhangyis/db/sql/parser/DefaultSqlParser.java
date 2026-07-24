package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.ast.*;
import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** v1 递归下降 parser；实例只持最大输入配置，单次 parse 的 token/cursor 均为局部状态。 */
public final class DefaultSqlParser implements SqlParser {
    /**
     * 类级校验或资源上界；所有实例以该值拒绝超限输入，调整时必须复核容量、等待与格式约束。
     */
    public static final int DEFAULT_MAX_SQL_LENGTH = 1_048_576;
    /**
     * 记录 {@code maxSqlLength} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
     */
    private final int maxSqlLength;

    /**
     * 创建 {@code DefaultSqlParser}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     */
    public DefaultSqlParser() { this(DEFAULT_MAX_SQL_LENGTH); }

    /**
     * 创建 {@code DefaultSqlParser}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param maxSqlLength 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlParser(int maxSqlLength) {
        if (maxSqlLength <= 0) throw new DatabaseValidationException("max SQL length must be positive");
        this.maxSqlLength = maxSqlLength;
    }

    /** 解析恰好一条 v1 statement，并严格消费可选分号与 EOF。
     *
     * @param sql 传给 {@code parse} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code parse} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SqlSyntaxException SQL 或协议输入不符合受支持语法时抛出；调用方应修正输入，不能据此提交事务副作用
     */
    @Override
    public StatementNode parse(String sql) {
        if (sql == null) throw new DatabaseValidationException("SQL text must not be null");
        if (sql.length() > maxSqlLength) {
            throw new SqlSyntaxException("SQL input exceeds configured limit",
                    new SourcePosition(maxSqlLength, 1, maxSqlLength + 1), sql);
        }
        Parser parser = new Parser(sql, new SqlLexer(sql).lex());
        return parser.parse();
    }

    /**
     * SQL 词法与语法解析的 {@code Parser} 解析组件；输入与游标由单次解析拥有，成功完整消费目标结构，失败报告稳定位置且不发布半成品。
     */
    private static final class Parser {
        /**
         * 构造时冻结的 {@code sql} 文本；保存规范化标识、SQL 或诊断上下文，不得为空白，字符顺序在本对象生命周期内保持不变。
         */
        private final String sql;
        /**
         * 本对象拥有的 {@code tokens} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
        private final List<Token> tokens;
        /**
         * 记录 {@code cursor} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
         */
        private int cursor;

        private Parser(String sql, List<Token> tokens) { this.sql = sql; this.tokens = tokens; }

        /**
         * 从当前 SQL token 与局部游标解析 {@code parse} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         * @return {@code parse} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
         */
        private StatementNode parse() {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            StatementNode statement;
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            if (keyword("XA")) statement = xa();
            else if (keyword("INSERT")) statement = insert();
            else if (keyword("CREATE")) statement = create();
            else if (keyword("DROP")) statement = drop();
            else if (keyword("ALTER")) statement = alterIndex();
            else if (keyword("UPDATE")) statement = update();
            else if (keyword("DELETE")) statement = delete();
            else if (keyword("SELECT")) statement = select();
            else if (keyword("SET")) statement = set();
            else if (keyword("BEGIN")) { take(); statement = new TransactionControlNode(TransactionControlNode.Kind.BEGIN); }
            else if (keyword("START")) {
                take(); requireKeyword("TRANSACTION");
                statement = new TransactionControlNode(TransactionControlNode.Kind.BEGIN);
            } else if (keyword("COMMIT")) {
                take(); statement = new TransactionControlNode(TransactionControlNode.Kind.COMMIT);
            } else if (keyword("ROLLBACK")) {
                take();
                if (keyword("TO")) {
                    take();
                    if (keyword("SAVEPOINT")) {
                        take();
                    }
                    statement = new SavepointStatementNode(
                            SavepointStatementNode.Kind.ROLLBACK_TO, identifier());
                } else {
                    statement = new TransactionControlNode(TransactionControlNode.Kind.ROLLBACK);
                }
            } else if (keyword("SAVEPOINT")) {
                take();
                statement = new SavepointStatementNode(
                        SavepointStatementNode.Kind.CREATE, identifier());
            } else if (keyword("RELEASE")) {
                take();
                requireKeyword("SAVEPOINT");
                statement = new SavepointStatementNode(
                        SavepointStatementNode.Kind.RELEASE, identifier());
            } else {
                throw syntax("expected data, DDL, transaction or savepoint statement",
                        current());
            }
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            if (match(TokenType.SEMICOLON)) take();
            require(TokenType.EOF, "end of statement");
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return statement;
        }

        /**
         * 解析 XA START/BEGIN、END、PREPARE、COMMIT、ROLLBACK 与 RECOVER。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>消费 XA 与命令关键字，RECOVER 独立解析可选 CONVERT XID。</li>
         *     <li>其它命令按 `gtrid[, bqual[, formatId]]` 解码 XID，字符串使用 UTF-8、HEX 使用原字节。</li>
         *     <li>解析命令专属 JOIN/RESUME、SUSPEND/FOR MIGRATE 或 ONE PHASE，拒绝跨命令选项。</li>
         *     <li>构造已交叉校验 AST；外层统一消费分号与 EOF，任何多余 token 都失败。</li>
         * </ol>
         *
         * @return 完整 XA AST
         */
        private XaStatementNode xa() {
            // 1、RECOVER 不携带单一 XID，CONVERT XID 只改变结果显示请求。
            requireKeyword("XA");
            if (keyword("RECOVER")) {
                take();
                boolean convert = false;
                if (keyword("CONVERT")) {
                    take();
                    requireKeyword("XID");
                    convert = true;
                }
                return new XaStatementNode(XaStatementNode.Kind.RECOVER, java.util.Optional.empty(),
                        XaStatementNode.StartMode.NONE, XaStatementNode.EndMode.NONE,
                        false, convert);
            }

            XaStatementNode.Kind kind;
            if (keyword("START") || keyword("BEGIN")) {
                take();
                kind = XaStatementNode.Kind.START;
            } else if (keyword("END")) {
                take();
                kind = XaStatementNode.Kind.END;
            } else if (keyword("PREPARE")) {
                take();
                kind = XaStatementNode.Kind.PREPARE;
            } else if (keyword("COMMIT")) {
                take();
                kind = XaStatementNode.Kind.COMMIT;
            } else if (keyword("ROLLBACK")) {
                take();
                kind = XaStatementNode.Kind.ROLLBACK;
            } else {
                throw syntax("expected XA START, END, PREPARE, COMMIT, ROLLBACK or RECOVER",
                        current());
            }

            // 2、XID 字节及 signed format id 在语法层完成确定性解码。
            XaIdentifierNode xid = xaIdentifier();
            XaStatementNode.StartMode startMode = XaStatementNode.StartMode.NONE;
            XaStatementNode.EndMode endMode = XaStatementNode.EndMode.NONE;
            boolean onePhase = false;

            // 3、每类命令只接受自己的尾部选项。
            if (kind == XaStatementNode.Kind.START) {
                if (keyword("JOIN")) {
                    take();
                    startMode = XaStatementNode.StartMode.JOIN;
                } else if (keyword("RESUME")) {
                    take();
                    startMode = XaStatementNode.StartMode.RESUME;
                }
            } else if (kind == XaStatementNode.Kind.END && keyword("SUSPEND")) {
                take();
                endMode = XaStatementNode.EndMode.SUSPEND;
                if (keyword("FOR")) {
                    take();
                    requireKeyword("MIGRATE");
                    endMode = XaStatementNode.EndMode.FOR_MIGRATE;
                }
            } else if (kind == XaStatementNode.Kind.COMMIT && keyword("ONE")) {
                take();
                requireKeyword("PHASE");
                onePhase = true;
            }

            // 4、构造器再次交叉校验 kind/option，避免后续分派接受非法组合。
            return new XaStatementNode(kind, java.util.Optional.of(xid), startMode, endMode,
                    onePhase, false);
        }

        /**
         * 解析 MySQL 风格 XID 三元组。未给 bqual 时为空，未给 formatId 时为 1。
         */
        private XaIdentifierNode xaIdentifier() {
            Token first = current();
            byte[] gtrid = xaBytes();
            byte[] bqual = new byte[0];
            int formatId = 1;
            if (match(TokenType.COMMA)) {
                take();
                bqual = xaBytes();
                if (match(TokenType.COMMA)) {
                    take();
                    Token format = require(TokenType.NUMBER, "signed XA format id");
                    try {
                        if (format.text().contains(".") || format.text().contains("e")
                                || format.text().contains("E")) {
                            throw new NumberFormatException("non-integral XA format id");
                        }
                        formatId = Integer.parseInt(format.text());
                    } catch (NumberFormatException invalid) {
                        throw syntax("XA format id must be a signed 32-bit integer", format);
                    }
                }
            }
            try {
                return new XaIdentifierNode(formatId, gtrid, bqual, first.position());
            } catch (DatabaseValidationException invalid) {
                throw syntax(invalid.getMessage(), first);
            }
        }

        /** 把 STRING/HEX XID 组件转换为确定字节序列。 */
        private byte[] xaBytes() {
            Token token = current();
            if (token.type() == TokenType.STRING) {
                take();
                return token.text().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            if (token.type() == TokenType.HEX) {
                take();
                try {
                    return java.util.HexFormat.of().parseHex(token.text());
                } catch (IllegalArgumentException invalid) {
                    throw syntax("XA hex literal must contain complete hexadecimal bytes", token);
                }
            }
            throw syntax("expected XA string or hex literal", token);
        }

        /**
         * 从当前 SQL token 与局部游标解析 {@code insert} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         * @return {@code insert} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
         */
        private InsertStatementNode insert() {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            requireKeyword("INSERT"); requireKeyword("INTO");
            QualifiedNameNode table = qualifiedName();
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            List<IdentifierNode> columns = List.of();
            if (match(TokenType.LPAREN)) {
                take();
                columns = identifierList();
                require(TokenType.RPAREN, "')'");
            }
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            requireKeyword("VALUES");
            List<List<LiteralNode>> rows = new ArrayList<>();
            do {
                require(TokenType.LPAREN, "'('");
                rows.add(literalList());
                require(TokenType.RPAREN, "')'");
                if (!match(TokenType.COMMA)) {
                    break;
                }
                take();
            } while (true);
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return new InsertStatementNode(table, columns, rows);
        }

        /**
         * 分派 CREATE TABLE 与 CREATE [UNIQUE] INDEX。只在消费 CREATE 后检查对象类别，未知类别
         * 以当前 token 的稳定位置失败，不能回退成其它语句或发布部分 AST。
         *
         * @return CREATE TABLE 或既有 CREATE INDEX 纯语法对象
         */
        private StatementNode create() {
            requireKeyword("CREATE");
            if (keyword("SCHEMA") || keyword("DATABASE")) {
                take();
                boolean ifNotExists = false;
                if (keyword("IF")) {
                    take();
                    requireKeyword("NOT");
                    requireKeyword("EXISTS");
                    ifNotExists = true;
                }
                return new CreateSchemaStatementNode(
                        identifier(), ifNotExists);
            }
            if (keyword("TABLE")) {
                take();
                return createTable();
            }
            boolean unique = false;
            if (keyword("UNIQUE")) {
                take();
                unique = true;
            }
            requireKeyword("INDEX");
            IdentifierNode indexName = identifier();
            requireKeyword("ON");
            QualifiedNameNode table = qualifiedName();
            return new CreateIndexStatementNode(table, indexName, unique, indexKeyParts());
        }

        /**
         * 分派 DROP INDEX、原子多表 DROP TABLE 与 DROP SCHEMA/DATABASE。
         *
         * @return 保留 IF 语义和完整目标列表的 DDL AST
         * @throws SqlSyntaxException DROP 对象类别或 IF 子句非法时抛出
         */
        private StatementNode drop() {
            requireKeyword("DROP");
            if (keyword("INDEX")) {
                take();
                IdentifierNode indexName = identifier();
                requireKeyword("ON");
                return new DropIndexStatementNode(
                        qualifiedName(), indexName);
            }
            if (keyword("TABLE")) {
                take();
                boolean ifExists = false;
                if (keyword("IF")) {
                    take();
                    requireKeyword("EXISTS");
                    ifExists = true;
                }
                List<QualifiedNameNode> tables = new ArrayList<>();
                tables.add(qualifiedName());
                while (match(TokenType.COMMA)) {
                    take();
                    tables.add(qualifiedName());
                }
                return new DropTableStatementNode(tables, ifExists);
            }
            if (keyword("SCHEMA") || keyword("DATABASE")) {
                take();
                boolean ifExists = false;
                if (keyword("IF")) {
                    take();
                    requireKeyword("EXISTS");
                    ifExists = true;
                }
                return new DropSchemaStatementNode(
                        identifier(), ifExists);
            }
            throw syntax(
                    "expected INDEX, TABLE, SCHEMA or DATABASE after DROP",
                    current());
        }

        /**
         * 解析基础 CREATE TABLE。当前 shape 支持 IF NOT EXISTS、整数显示宽度、列
         * NULL/default/AUTO_INCREMENT/COMMENT、显式 PRIMARY KEY、UNIQUE/普通 BTREE INDEX
         * 以及表 COMMENT；foreign/generated/check 与其它 table option 仍显式拒绝，防止把尚无
         * DD/恢复语义的语法静默吞掉。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取目标限定名和左括号；空定义列表在 AST 发布前失败。</li>
         *     <li>逐项区分表级索引与列定义；列尾的 PRIMARY/UNIQUE 被规范为同一索引 AST。</li>
         *     <li>每项只消费自身括号/default，外层逗号保持定义顺序并阻止尾随空项。</li>
         *     <li>右括号闭合后冻结列/索引列表；名称、类型与主键数量留给 Binder 校验。</li>
         * </ol>
         *
         * @return 不含 DD identity 或物理配置的 CREATE TABLE AST
         * @throws SqlSyntaxException 语法超出当前基础 shape 或定义列表不闭合时抛出
         */
        private CreateTableStatementNode createTable() {
            // 1、IF NOT EXISTS 只作为 AST 语义保存；对象存在性必须在 DD 锁内重验。
            boolean ifNotExists = false;
            if (keyword("IF")) {
                take();
                requireKeyword("NOT");
                requireKeyword("EXISTS");
                ifNotExists = true;
            }
            QualifiedNameNode table = qualifiedName();
            require(TokenType.LPAREN, "'('");
            if (match(TokenType.RPAREN)) {
                throw syntax("CREATE TABLE requires at least one column", current());
            }

            List<CreateTableStatementNode.Column> columns = new ArrayList<>();
            List<CreateTableStatementNode.Index> indexes = new ArrayList<>();
            while (true) {
                // 2、表级约束先于普通列名判定；关键字仍是 IDENT，分支顺序决定其语法角色。
                if (keyword("PRIMARY")) {
                    indexes.add(createPrimaryIndex());
                } else if (keyword("UNIQUE")) {
                    indexes.add(createNamedTableIndex(true));
                } else if (keyword("INDEX") || keyword("KEY")) {
                    indexes.add(createNamedTableIndex(false));
                } else {
                    createColumn(columns, indexes);
                }

                // 3、定义项之间必须恰好一个逗号；尾随逗号会在下一轮以 RPAREN 位置失败。
                if (!match(TokenType.COMMA)) {
                    break;
                }
                take();
                if (match(TokenType.RPAREN)) {
                    throw syntax("CREATE TABLE does not allow a trailing comma", current());
                }
            }

            // 4、完整闭合后才构造不可变 AST，失败不泄漏局部列表。
            Token closing = require(TokenType.RPAREN, "')'");
            if (columns.isEmpty()) {
                throw syntax(
                        "CREATE TABLE requires at least one column",
                        closing);
            }
            String tableComment = "";
            if (keyword("COMMENT")) {
                take();
                if (match(TokenType.EQUALS)) {
                    take();
                }
                tableComment = require(
                        TokenType.STRING, "string table comment").text();
            }
            return new CreateTableStatementNode(
                    table, columns, indexes, ifNotExists, tableComment);
        }

        /**
         * 解析一个列定义，并把列尾 PRIMARY KEY / UNIQUE 规范为表级索引 AST。
         *
         * @param columns 当前语句拥有的列列表；成功追加恰好一个元素
         * @param indexes 当前语句拥有的索引列表；存在内联约束时追加恰好一个元素
         */
        private void createColumn(
                List<CreateTableStatementNode.Column> columns,
                List<CreateTableStatementNode.Index> indexes) {
            IdentifierNode name = identifier();
            CreateTableStatementNode.ColumnType type = createColumnType();
            Optional<LiteralNode> defaultLiteral = Optional.empty();
            String comment = "";
            CreateTableStatementNode.Generation generation =
                    CreateTableStatementNode.Generation.NONE;
            boolean inlineConstraint = false;
            while (!match(TokenType.COMMA) && !match(TokenType.RPAREN)) {
                if (keyword("DEFAULT") && defaultLiteral.isEmpty()) {
                    take();
                    defaultLiteral = Optional.of(literal());
                    continue;
                }
                if (keyword("PRIMARY") && !inlineConstraint) {
                    Token primary = take();
                    requireKeyword("KEY");
                    indexes.add(new CreateTableStatementNode.Index(
                            new IdentifierNode("PRIMARY", primary.position()),
                            true, true, List.of(new IndexKeyPartNode(
                            name, IndexKeyOrderNode.ASC))));
                    inlineConstraint = true;
                    continue;
                }
                if (keyword("UNIQUE") && !inlineConstraint) {
                    take();
                    if (keyword("KEY")) {
                        take();
                    }
                    indexes.add(new CreateTableStatementNode.Index(
                            name, true, false, List.of(new IndexKeyPartNode(
                            name, IndexKeyOrderNode.ASC))));
                    inlineConstraint = true;
                    continue;
                }
                if (keyword("AUTO_INCREMENT")
                        && generation == CreateTableStatementNode.Generation.NONE) {
                    take();
                    generation = CreateTableStatementNode.Generation.AUTO_INCREMENT;
                    continue;
                }
                if (keyword("COMMENT") && comment.isEmpty()) {
                    take();
                    comment = require(TokenType.STRING, "string column comment").text();
                    continue;
                }
                throw syntax("unsupported CREATE TABLE column clause", current());
            }
            columns.add(new CreateTableStatementNode.Column(
                    name, type, defaultLiteral, comment, generation));
        }

        /** 解析表级 PRIMARY KEY，并固定逻辑/物理主索引名称为 PRIMARY。 */
        private CreateTableStatementNode.Index createPrimaryIndex() {
            Token primary = take();
            requireKeyword("KEY");
            boolean usingBefore = consumeBtreeAlgorithm();
            List<IndexKeyPartNode> keyParts = indexKeyParts();
            if (usingBefore && keyword("USING")) {
                throw syntax("index algorithm may be declared only once", current());
            }
            consumeBtreeAlgorithm();
            return new CreateTableStatementNode.Index(
                    new IdentifierNode("PRIMARY", primary.position()),
                    true, true, keyParts,
                    CreateTableStatementNode.IndexAlgorithm.BTREE);
        }

        /**
         * 解析具名 UNIQUE INDEX/KEY 或普通 INDEX/KEY；当前不为匿名 secondary 猜测名称。
         *
         * @param unique 是否为逻辑唯一二级索引
         * @return 保留 key part 顺序且非聚簇的索引 AST
         */
        private CreateTableStatementNode.Index createNamedTableIndex(boolean unique) {
            if (unique) {
                requireKeyword("UNIQUE");
                if (keyword("INDEX") || keyword("KEY")) {
                    take();
                } else {
                    throw syntax("UNIQUE table constraint requires INDEX or KEY", current());
                }
            } else {
                take();
            }
            IdentifierNode name = identifier();
            boolean usingBefore = consumeBtreeAlgorithm();
            List<IndexKeyPartNode> keyParts = indexKeyParts();
            if (usingBefore && keyword("USING")) {
                throw syntax("index algorithm may be declared only once", current());
            }
            consumeBtreeAlgorithm();
            return new CreateTableStatementNode.Index(
                    name, unique, false, keyParts,
                    CreateTableStatementNode.IndexAlgorithm.BTREE);
        }

        /**
         * 消费可选 {@code USING BTREE}；未知算法必须失败，不能把索引算法静默规范为 BTREE。
         *
         * @return 当前游标是否消费了显式算法
         */
        private boolean consumeBtreeAlgorithm() {
            if (!keyword("USING")) {
                return false;
            }
            take();
            Token algorithm = require(TokenType.IDENT, "index algorithm");
            if (!algorithm.text().equalsIgnoreCase("BTREE")) {
                throw syntax("only USING BTREE is supported", algorithm);
            }
            return true;
        }

        /**
         * 解析独立 DROP INDEX。v1 不接受 IF EXISTS，避免不存在目标时静默吞掉 metadata 错误。
         *
         * @return 与 ALTER TABLE DROP INDEX 共用的纯语法 AST
         */
        /**
         * 解析 ALTER TABLE 的索引与表空间生命周期动作；索引分支归一为独立 CREATE/DROP 使用的 AST，
         * DISCARD/IMPORT 保留专属节点，防止 Session 把文件生命周期动作交给普通 executor。
         *
         * @return 规范化的索引 DDL AST；不支持的 ALTER action 以语法异常拒绝
         */
        private StatementNode alterIndex() {
            requireKeyword("ALTER");
            requireKeyword("TABLE");
            QualifiedNameNode table = qualifiedName();
            if (keyword("DISCARD")) {
                take();
                requireKeyword("TABLESPACE");
                return new AlterTablespaceStatementNode(
                        table, AlterTablespaceStatementNode.Action.DISCARD);
            }
            if (keyword("IMPORT")) {
                take();
                requireKeyword("TABLESPACE");
                return new AlterTablespaceStatementNode(
                        table, AlterTablespaceStatementNode.Action.IMPORT);
            }
            List<AlterTableStatementNode.Action> actions = new ArrayList<>();
            actions.add(alterAction());
            while (match(TokenType.COMMA)) {
                take();
                actions.add(alterAction());
            }
            // 单索引 action 保持既有 AST/DDL 状态机；多 action 必须作为一次 staged ALTER 原子发布。
            if (actions.size() == 1 && actions.getFirst() instanceof AlterTableStatementNode.AddIndex add) {
                return new CreateIndexStatementNode(table, add.name(), add.unique(), add.keyParts());
            }
            if (actions.size() == 1 && actions.getFirst() instanceof AlterTableStatementNode.DropIndex drop) {
                return new DropIndexStatementNode(table, drop.name());
            }
            return new AlterTableStatementNode(table, actions);
        }

        /**
         * 解析通用 ALTER 的一个 action；调用方消费 action 间逗号，索引 key part 与类型参数内部逗号由本分支消费。
         *
         * @return 保留用户顺序的单个 action
         */
        private AlterTableStatementNode.Action alterAction() {
            if (keyword("ADD")) {
                take();
                boolean unique = false;
                if (keyword("UNIQUE")) {
                    take();
                    unique = true;
                }
                if (keyword("INDEX")) {
                    take();
                    IdentifierNode name = identifier();
                    return new AlterTableStatementNode.AddIndex(
                            name, unique, indexKeyParts());
                }
                if (unique) {
                    throw syntax("UNIQUE must be followed by INDEX", current());
                }
                if (keyword("COLUMN")) {
                    take();
                }
                IdentifierNode name = identifier();
                AlterTableStatementNode.ColumnType type = alterColumnType();
                Optional<LiteralNode> defaultLiteral = Optional.empty();
                if (keyword("DEFAULT")) {
                    take();
                    defaultLiteral = Optional.of(literal());
                }
                AlterTableStatementNode.ColumnPosition position =
                        AlterTableStatementNode.ColumnPosition.last();
                if (keyword("FIRST")) {
                    take();
                    position = new AlterTableStatementNode.ColumnPosition(
                            AlterTableStatementNode.PositionKind.FIRST, Optional.empty());
                } else if (keyword("AFTER")) {
                    take();
                    position = new AlterTableStatementNode.ColumnPosition(
                            AlterTableStatementNode.PositionKind.AFTER,
                            Optional.of(identifier()));
                }
                return new AlterTableStatementNode.AddColumn(
                        name, type, defaultLiteral, position);
            }
            if (keyword("DROP")) {
                take();
                if (keyword("INDEX")) {
                    take();
                    return new AlterTableStatementNode.DropIndex(identifier());
                }
                if (keyword("COLUMN")) {
                    take();
                }
                return new AlterTableStatementNode.DropColumn(identifier());
            }
            if (keyword("RENAME")) {
                take();
                if (keyword("TO") || keyword("AS")) {
                    take();
                }
                return new AlterTableStatementNode.Rename(qualifiedName());
            }
            if (keyword("COMMENT")) {
                take();
                if (match(TokenType.EQUALS)) {
                    take();
                }
                Token comment = require(TokenType.STRING, "string table comment");
                return new AlterTableStatementNode.Comment(comment.text());
            }
            if (keyword("DEFAULT")) {
                take();
                requireKeyword("CHARACTER");
                requireKeyword("SET");
                if (match(TokenType.EQUALS)) {
                    take();
                }
                int charset = positiveDdlNumber("charset id");
                requireKeyword("COLLATE");
                if (match(TokenType.EQUALS)) {
                    take();
                }
                return new AlterTableStatementNode.DefaultCharset(
                        charset, positiveDdlNumber("collation id"));
            }
            if (keyword("CONVERT")) {
                take();
                requireKeyword("TO");
                requireKeyword("CHARACTER");
                requireKeyword("SET");
                int charset = positiveDdlNumber("charset id");
                requireKeyword("COLLATE");
                return new AlterTableStatementNode.ConvertCharset(
                        charset, positiveDdlNumber("collation id"));
            }
            throw syntax("unsupported ALTER TABLE action", current());
        }

        /**
         * 解析 ADD COLUMN 的类型 shape。当前 SQL 切片支持 DD 中除 ENUM/SET 外的类型关键字、
         * 可选一个或两个数值参数、UNSIGNED 与 NULL/NOT NULL。
         */
        private AlterTableStatementNode.ColumnType alterColumnType() {
            ColumnTypeShape shape = columnTypeShape("ALTER");
            return new AlterTableStatementNode.ColumnType(
                    shape.name(), shape.length(), shape.scale(),
                    shape.unsigned(), shape.nullable());
        }

        /** CREATE 与 ALTER 共用同一类型 token 规则，只映射到各自封闭 AST。 */
        private CreateTableStatementNode.ColumnType createColumnType() {
            ColumnTypeShape shape = columnTypeShape("CREATE TABLE");
            return new CreateTableStatementNode.ColumnType(
                    shape.name(), shape.length(), shape.scale(),
                    shape.unsigned(), shape.nullable());
        }

        /**
         * 解析 DDL 列类型的公共 token shape；本阶段只做语法范围，具体类型能力由 Binder 决定。
         *
         * @param operation 用于稳定诊断的 DDL 类别
         * @return 类型名、长度/精度、scale、unsigned 和 nullable 的不可变局部结果
         */
        private ColumnTypeShape columnTypeShape(String operation) {
            Token type = require(
                    TokenType.IDENT, operation + " column type");
            int length = 0;
            int scale = 0;
            if (match(TokenType.LPAREN)) {
                take();
                length = positiveDdlNumber("column length or precision");
                if (match(TokenType.COMMA)) {
                    take();
                    Token scaleToken = require(TokenType.NUMBER, "column scale");
                    try {
                        scale = Integer.parseInt(scaleToken.text());
                    } catch (NumberFormatException overflow) {
                        throw syntax("column scale exceeds integer range", scaleToken);
                    }
                    if (scale < 0) {
                        throw syntax("column scale must be non-negative", scaleToken);
                    }
                }
                require(TokenType.RPAREN, "')'");
            }
            boolean unsigned = false;
            boolean nullable = true;
            if (keyword("UNSIGNED")) {
                take();
                unsigned = true;
            }
            if (keyword("NOT")) {
                take();
                requireKeyword("NULL");
                nullable = false;
            } else if (keyword("NULL")) {
                take();
            }
            return new ColumnTypeShape(
                    type.text(), length, scale, unsigned, nullable);
        }

        /** 读取 DDL 中必须为正且可表示为 int 的稳定数值 id/长度。 */
        private int positiveDdlNumber(String label) {
            Token number = require(TokenType.NUMBER, label);
            try {
                int value = Integer.parseInt(number.text());
                if (value <= 0) {
                    throw syntax(label + " must be positive", number);
                }
                return value;
            } catch (NumberFormatException overflow) {
                throw syntax(label + " exceeds integer range", number);
            }
        }

        /** Parser 内部共享的列类型临时结果，不进入 AST 或 DD。 */
        private record ColumnTypeShape(
                String name, int length, int scale, boolean unsigned,
                boolean nullable) { }

        /** key part v1 仅允许完整列与可选 ASC/DESC，不把前缀长度或表达式静默吞掉。 */
        private List<IndexKeyPartNode> indexKeyParts() {
            require(TokenType.LPAREN, "'('");
            List<IndexKeyPartNode> parts = new ArrayList<>();
            parts.add(indexKeyPart());
            while (match(TokenType.COMMA)) {
                take();
                parts.add(indexKeyPart());
            }
            require(TokenType.RPAREN, "')'");
            return List.copyOf(parts);
        }

        private IndexKeyPartNode indexKeyPart() {
            IdentifierNode column = identifier();
            IndexKeyOrderNode order = IndexKeyOrderNode.ASC;
            if (keyword("ASC")) {
                take();
            } else if (keyword("DESC")) {
                take();
                order = IndexKeyOrderNode.DESC;
            }
            return new IndexKeyPartNode(column, order);
        }

        /**
         * 从当前 SQL token 与局部游标解析 {@code update} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
         *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
         *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
         *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
         * </ol>
         *
         * @return {@code update} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
         */
        private UpdateStatementNode update() {
            // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
            requireKeyword("UPDATE"); QualifiedNameNode table = qualifiedName(); requireKeyword("SET");
            // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
            List<AssignmentNode> assignments = new ArrayList<>();
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            assignments.add(assignment());
            while (match(TokenType.COMMA)) { take(); assignments.add(assignment()); }
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            requireKeyword("WHERE");
            return new UpdateStatementNode(table, assignments, condition());
        }

        private AssignmentNode assignment() { return new AssignmentNode(identifier(), equalsLiteral()); }
        private LiteralNode equalsLiteral() { require(TokenType.EQUALS, "'='"); return literal(); }

        /**
         * 从当前 SQL token 与局部游标解析 {@code delete} 对应的语法结构；成功推进到确定边界，失败报告位置且不发布半解析 AST。
         *
         * @return {@code delete} 产生的 SQL 语句、绑定或执行对象；成功时不为 {@code null}，并保留当前 schema 版本和会话语义
         */
        private DeleteStatementNode delete() {
            requireKeyword("DELETE"); requireKeyword("FROM"); QualifiedNameNode table = qualifiedName();
            requireKeyword("WHERE");
            return new DeleteStatementNode(table, condition());
        }

        /**
         * 从 WHERE 起点按 {@code NOT > AND > OR} 解析完整 boolean 语法树。
         *
         * @return 保持用户优先级、operand 顺序和源位置的不可变条件
         * @throws SqlSyntaxException 条件缺失、括号不闭合或原子谓词非法时抛出
         */
        private BooleanExpressionNode condition() {
            return disjunction();
        }

        /**
         * 解析最低优先级 OR；每个 operand 已先完整消费其 AND 子树。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取第一个 conjunction，确保 OR 左侧是完整 boolean 表达式。</li>
         *     <li>按用户顺序消费零个或多个 OR 和右侧 conjunction。</li>
         *     <li>单元素保持原节点，多元素冻结为 n-ary disjunction，避免无意义包装。</li>
         * </ol>
         *
         * @return OR 优先级层的不可变表达式
         */
        private BooleanExpressionNode disjunction() {
            // 1、右侧 AND 必须先归组，防止把 a OR b AND c 错解为 (a OR b) AND c。
            ArrayList<BooleanExpressionNode> operands = new ArrayList<>();
            operands.add(conjunction());
            // 2、OR token 只在本层消费，括号内会递归进入新的 disjunction。
            while (keyword("OR")) {
                take();
                operands.add(conjunction());
            }
            // 3、保留最小 AST 形状；多 operand 的顺序后续不得重排。
            return operands.size() == 1
                    ? operands.getFirst()
                    : new DisjunctionExpressionNode(operands);
        }

        /**
         * 解析中间优先级 AND；BETWEEN 的内部 AND 由原子谓词先行消费。
         *
         * @return 保持用户顺序的单表达式或 n-ary conjunction
         */
        private BooleanExpressionNode conjunction() {
            ArrayList<BooleanExpressionNode> operands = new ArrayList<>();
            operands.add(negation());
            while (keyword("AND")) {
                take();
                operands.add(negation());
            }
            return operands.size() == 1
                    ? operands.getFirst()
                    : new ConjunctionExpressionNode(operands);
        }

        /**
         * 解析最高 boolean 前缀优先级 NOT、括号或原子谓词。
         *
         * @return 保留连续 NOT 和括号改变后树形的 boolean 表达式
         * @throws SqlSyntaxException 右括号缺失或括号内条件非法时抛出
         */
        private BooleanExpressionNode negation() {
            if (keyword("NOT")) {
                Token not = take();
                return new NegationExpressionNode(
                        negation(), not.position());
            }
            if (match(TokenType.LPAREN)) {
                take();
                BooleanExpressionNode nested = disjunction();
                require(TokenType.RPAREN, "')'");
                return nested;
            }
            return predicate();
        }

        private SelectStatementNode select() {
            requireKeyword("SELECT");
            boolean star = false;
            List<ColumnReferenceNode> projections = List.of();
            if (match(TokenType.STAR)) { take(); star = true; }
            else projections = columnReferenceList();
            requireKeyword("FROM");
            QualifiedNameNode table = qualifiedName();
            Optional<IdentifierNode> tableAlias = optionalTableAlias();
            Optional<InnerJoinClauseNode> join = Optional.empty();
            if (keyword("INNER") || keyword("JOIN")) {
                if (keyword("INNER")) {
                    take();
                }
                requireKeyword("JOIN");
                QualifiedNameNode rightTable = qualifiedName();
                Optional<IdentifierNode> rightAlias =
                        optionalTableAlias();
                requireKeyword("ON");
                ColumnReferenceNode leftColumn =
                        columnReference();
                require(TokenType.EQUALS, "'='");
                ColumnReferenceNode rightColumn =
                        columnReference();
                join = Optional.of(new InnerJoinClauseNode(
                        rightTable, rightAlias,
                        leftColumn, rightColumn));
            }
            requireKeyword("WHERE");
            BooleanExpressionNode condition = condition();
            List<OrderByItemNode> orderBy = new ArrayList<>();
            if (keyword("ORDER")) {
                take();
                requireKeyword("BY");
                do {
                    ColumnReferenceNode column = columnReference();
                    SortDirectionNode direction = SortDirectionNode.ASC;
                    if (keyword("ASC")) {
                        take();
                    } else if (keyword("DESC")) {
                        take();
                        direction = SortDirectionNode.DESC;
                    }
                    orderBy.add(new OrderByItemNode(column, direction));
                    if (!match(TokenType.COMMA)) {
                        break;
                    }
                    take();
                } while (true);
            }
            Optional<LimitClauseNode> limit = Optional.empty();
            if (keyword("LIMIT")) {
                take();
                long first = limitInteger();
                long offset = 0L;
                long count = first;
                if (match(TokenType.COMMA)) {
                    take();
                    offset = first;
                    count = limitInteger();
                } else if (keyword("OFFSET")) {
                    take();
                    offset = limitInteger();
                }
                limit = Optional.of(new LimitClauseNode(offset, count));
            }
            SelectLockingClause lockingClause = SelectLockingClause.NONE;
            if (keyword("FOR")) {
                take();
                if (keyword("SHARE")) {
                    take();
                    lockingClause = SelectLockingClause.FOR_SHARE;
                } else if (keyword("UPDATE")) {
                    take();
                    lockingClause = SelectLockingClause.FOR_UPDATE;
                } else {
                    throw syntax("expected SHARE or UPDATE after FOR", current());
                }
            }
            return new SelectStatementNode(
                    star, projections, table, tableAlias, join, condition,
                    orderBy, limit, lockingClause);
        }

        /**
         * 解析 LIMIT 的非负 64 位十进制整数；小数、指数、负数和溢出均在 AST 发布前失败。
         *
         * @return 可安全进入 offset/count 算术的非负 long
         * @throws SqlSyntaxException 当前 token 不是规范非负整数或超过 long 上界时抛出
         */
        private long limitInteger() {
            Token token = require(TokenType.NUMBER, "non-negative LIMIT integer");
            if (token.text().isEmpty()
                    || token.text().chars().anyMatch(character -> !Character.isDigit(character))) {
                throw syntax("LIMIT value must be a non-negative integer", token);
            }
            try {
                return Long.parseLong(token.text());
            } catch (NumberFormatException invalid) {
                throw syntax("LIMIT value exceeds signed 64-bit range", token);
            }
        }

        /**
         * 解析一个列对 literal 的 comparison、BETWEEN 或 NULL 检查。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取左侧列标识符；列解析失败时不消费任何字面量。</li>
         *     <li>优先识别 BETWEEN，并在节点内部消费其语法专用 AND。</li>
         *     <li>识别 IS [NOT] NULL；NOT 只属于 null-test，不与前缀 boolean NOT 混用。</li>
         *     <li>否则消费一个已支持的二元比较符并读取右侧 literal。</li>
         *     <li>构造保留开闭边界、NULL 操作符与源位置的不可变谓词。</li>
         * </ol>
         *
         * @return 尚未绑定 DD 类型和索引路径的不可变谓词
         * @throws SqlSyntaxException 操作符、BETWEEN 边界或字面量缺失时抛出
         */
        private PredicateNode predicate() {
            // 1、列名只表达语法身份；Parser 不在这里访问 DD。
            ColumnReferenceNode column = columnReference();
            // 2、BETWEEN 自己拥有中间 AND，外层 conjunction 循环只消费后续 AND。
            if (keyword("BETWEEN")) {
                take();
                LiteralNode lower = literal();
                requireKeyword("AND");
                return new BetweenPredicateNode(column, lower, literal());
            }
            // 3、IS NULL 的结果永远是二值 boolean；Parser 不根据列 nullable 提前折叠。
            if (keyword("IS")) {
                take();
                boolean negated = false;
                if (keyword("NOT")) {
                    take();
                    negated = true;
                }
                requireKeyword("NULL");
                return new NullTestPredicateNode(
                        column, negated
                        ? NullTestOperator.IS_NOT_NULL
                        : NullTestOperator.IS_NULL);
            }
            // 4、操作符 token 决定范围方向和开闭性，等值保留既有 AST 类型。
            Token operator = current();
            take();
            // literal 缺失会抛稳定位置异常，不发布半构造谓词。
            return switch (operator.type()) {
                case EQUALS -> new EqualityPredicateNode(column, literal());
                case LESS_THAN -> new ComparisonPredicateNode(
                        column, ComparisonOperator.LESS_THAN, literal());
                case LESS_EQUAL -> new ComparisonPredicateNode(
                        column, ComparisonOperator.LESS_THAN_OR_EQUAL, literal());
                case GREATER_THAN -> new ComparisonPredicateNode(
                        column, ComparisonOperator.GREATER_THAN, literal());
                case GREATER_EQUAL -> new ComparisonPredicateNode(
                        column, ComparisonOperator.GREATER_THAN_OR_EQUAL, literal());
                default -> throw syntax("expected =, <, <=, >, >= or BETWEEN", operator);
            };
        }

        private SetAutocommitNode set() {
            requireKeyword("SET"); requireKeyword("AUTOCOMMIT"); require(TokenType.EQUALS, "'='");
            Token value = require(TokenType.NUMBER, "0 or 1");
            if (!value.text().equals("0") && !value.text().equals("1")) {
                throw syntax("autocommit accepts only 0 or 1", value);
            }
            return new SetAutocommitNode(value.text().equals("1"));
        }

        private QualifiedNameNode qualifiedName() {
            List<IdentifierNode> parts = new ArrayList<>();
            parts.add(identifier());
            while (match(TokenType.DOT)) {
                take();
                if (parts.size() == 3) throw syntax("table name has more than three parts", current());
                parts.add(identifier());
            }
            if (parts.size() == 3 && !parts.getFirst().value().equalsIgnoreCase("def")) {
                throw syntax("three-part table name catalog must be def", tokenAt(parts.getFirst().position()));
            }
            return new QualifiedNameNode(parts);
        }

        private List<IdentifierNode> identifierList() {
            List<IdentifierNode> result = new ArrayList<>();
            result.add(identifier());
            while (match(TokenType.COMMA)) { take(); result.add(identifier()); }
            return List.copyOf(result);
        }

        /**
         * 解析 SELECT/ORDER/WHERE 中的一段或两段列引用列表。
         *
         * @return 保持用户顺序的不可变列引用
         */
        private List<ColumnReferenceNode> columnReferenceList() {
            List<ColumnReferenceNode> result = new ArrayList<>();
            result.add(columnReference());
            while (match(TokenType.COMMA)) {
                take();
                result.add(columnReference());
            }
            return List.copyOf(result);
        }

        /**
         * 解析 {@code column} 或 {@code qualifier.column}；超过两段时由外层 EOF/关键字
         * 校验拒绝，避免误把三段表名规则应用到列引用。
         */
        private ColumnReferenceNode columnReference() {
            List<IdentifierNode> parts =
                    new ArrayList<>();
            parts.add(identifier());
            if (match(TokenType.DOT)) {
                take();
                parts.add(identifier());
            }
            return new ColumnReferenceNode(parts);
        }

        /**
         * 解析 FROM/JOIN 表后的 AS 或隐式 alias。只有确定不是后续子句关键字的标识符
         * 才会被消费，防止把 INNER、JOIN、WHERE 等误当别名。
         */
        private Optional<IdentifierNode> optionalTableAlias() {
            if (keyword("AS")) {
                take();
                return Optional.of(identifier());
            }
            if (current().type() == TokenType.IDENT
                    && !keyword("INNER") && !keyword("JOIN")
                    && !keyword("ON") && !keyword("WHERE")
                    && !keyword("ORDER") && !keyword("LIMIT")
                    && !keyword("FOR")) {
                return Optional.of(identifier());
            }
            return Optional.empty();
        }

        private List<LiteralNode> literalList() {
            List<LiteralNode> result = new ArrayList<>();
            result.add(literal());
            while (match(TokenType.COMMA)) { take(); result.add(literal()); }
            return List.copyOf(result);
        }

        private IdentifierNode identifier() {
            Token token = require(TokenType.IDENT, "identifier");
            return new IdentifierNode(token.text(), token.position());
        }

        private LiteralNode literal() {
            Token token = current();
            return switch (token.type()) {
                case IDENT -> {
                    if (!token.text().equalsIgnoreCase("NULL")) throw syntax("expected literal", token);
                    take(); yield new NullLiteralNode(token.lexeme(), token.position());
                }
                case NUMBER -> { take(); yield new NumericLiteralNode(token.text(), token.lexeme(), token.position()); }
                case STRING -> { take(); yield new StringLiteralNode(token.text(), token.lexeme(), token.position()); }
                case HEX -> { take(); yield new HexLiteralNode(token.text(), token.lexeme(), token.position()); }
                case BIT -> { take(); yield new BitLiteralNode(token.text(), token.lexeme(), token.position()); }
                default -> throw syntax("expected NULL, numeric, string, hex or bit literal", token);
            };
        }

        private boolean keyword(String expected) {
            return current().type() == TokenType.IDENT
                    && current().text().toUpperCase(Locale.ROOT).equals(expected);
        }
        private void requireKeyword(String expected) {
            if (!keyword(expected)) throw syntax("expected keyword " + expected, current());
            take();
        }
        private boolean match(TokenType type) { return current().type() == type; }
        private Token require(TokenType type, String expected) {
            if (!match(type)) throw syntax("expected " + expected, current());
            return take();
        }
        private Token take() { return tokens.get(cursor++); }
        private Token current() { return tokens.get(cursor); }
        private Token tokenAt(SourcePosition position) {
            return tokens.stream().filter(token -> token.position().equals(position)).findFirst().orElse(current());
        }
        private SqlSyntaxException syntax(String message, Token token) {
            return new SqlSyntaxException(message, token.position(), sql);
        }
    }
}
