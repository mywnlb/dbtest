package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.ast.*;
import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** v1 递归下降 parser；实例只持最大输入配置，单次 parse 的 token/cursor 均为局部状态。 */
public final class DefaultSqlParser {
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
            else if (keyword("CREATE")) statement = createIndex();
            else if (keyword("DROP")) statement = dropIndex();
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
            require(TokenType.LPAREN, "'('");
            List<IdentifierNode> columns = identifierList();
            require(TokenType.RPAREN, "')'");
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            requireKeyword("VALUES"); require(TokenType.LPAREN, "'('");
            List<LiteralNode> values = literalList();
            require(TokenType.RPAREN, "')'");
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return new InsertStatementNode(table, columns, values);
        }

        /** 解析独立 CREATE [UNIQUE] INDEX，并直接归一为共享 index AST。 */
        private CreateIndexStatementNode createIndex() {
            requireKeyword("CREATE");
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
         * 解析独立 DROP INDEX。v1 不接受 IF EXISTS，避免不存在目标时静默吞掉 metadata 错误。
         *
         * @return 与 ALTER TABLE DROP INDEX 共用的纯语法 AST
         */
        private DropIndexStatementNode dropIndex() {
            requireKeyword("DROP");
            requireKeyword("INDEX");
            IdentifierNode indexName = identifier();
            requireKeyword("ON");
            QualifiedNameNode table = qualifiedName();
            return new DropIndexStatementNode(table, indexName);
        }

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
                int charset = positiveAlterNumber("charset id");
                requireKeyword("COLLATE");
                if (match(TokenType.EQUALS)) {
                    take();
                }
                return new AlterTableStatementNode.DefaultCharset(
                        charset, positiveAlterNumber("collation id"));
            }
            if (keyword("CONVERT")) {
                take();
                requireKeyword("TO");
                requireKeyword("CHARACTER");
                requireKeyword("SET");
                int charset = positiveAlterNumber("charset id");
                requireKeyword("COLLATE");
                return new AlterTableStatementNode.ConvertCharset(
                        charset, positiveAlterNumber("collation id"));
            }
            throw syntax("unsupported ALTER TABLE action", current());
        }

        /**
         * 解析 ADD COLUMN 的类型 shape。当前 SQL 切片支持 DD 中除 ENUM/SET 外的类型关键字、
         * 可选一个或两个数值参数、UNSIGNED 与 NULL/NOT NULL。
         */
        private AlterTableStatementNode.ColumnType alterColumnType() {
            Token type = require(TokenType.IDENT, "column type");
            int length = 0;
            int scale = 0;
            if (match(TokenType.LPAREN)) {
                take();
                length = positiveAlterNumber("column length or precision");
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
            return new AlterTableStatementNode.ColumnType(
                    type.text(), length, scale, unsigned, nullable);
        }

        /** 读取 ALTER 中必须为正且可表示为 int 的稳定数值 id/长度。 */
        private int positiveAlterNumber(String label) {
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
            requireKeyword("WHERE"); return new UpdateStatementNode(table, assignments, predicates());
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
            requireKeyword("WHERE"); return new DeleteStatementNode(table, predicates());
        }

        private List<PredicateNode> predicates() {
            List<PredicateNode> result = new ArrayList<>(); result.add(predicate());
            while (keyword("AND")) { take(); result.add(predicate()); } return List.copyOf(result);
        }

        private SelectStatementNode select() {
            requireKeyword("SELECT");
            boolean star = false;
            List<IdentifierNode> projections = List.of();
            if (match(TokenType.STAR)) { take(); star = true; }
            else projections = identifierList();
            requireKeyword("FROM");
            QualifiedNameNode table = qualifiedName();
            requireKeyword("WHERE");
            List<PredicateNode> predicates = new ArrayList<>();
            predicates.add(predicate());
            while (keyword("AND")) { take(); predicates.add(predicate()); }
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
            return new SelectStatementNode(star, projections, table, predicates, lockingClause);
        }

        /**
         * 解析一个列对 literal 的 comparison 或 BETWEEN。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取左侧列标识符；列解析失败时不消费任何字面量。</li>
         *     <li>优先识别 BETWEEN，并在节点内部消费其语法专用 AND。</li>
         *     <li>否则消费一个已支持的二元比较符；未支持操作符在 AST 发布前失败。</li>
         *     <li>读取右侧 literal 并构造保留开闭边界的不可变谓词。</li>
         * </ol>
         *
         * @return 尚未绑定 DD 类型和索引路径的不可变谓词
         * @throws SqlSyntaxException 操作符、BETWEEN 边界或字面量缺失时抛出
         */
        private PredicateNode predicate() {
            // 1、列名只表达语法身份；Parser 不在这里访问 DD。
            IdentifierNode column = identifier();
            // 2、BETWEEN 自己拥有中间 AND，外层 conjunction 循环只消费后续 AND。
            if (keyword("BETWEEN")) {
                take();
                LiteralNode lower = literal();
                requireKeyword("AND");
                return new BetweenPredicateNode(column, lower, literal());
            }
            // 3、操作符 token 决定范围方向和开闭性，等值保留既有 AST 类型。
            Token operator = current();
            take();
            // 4、literal 缺失会抛稳定位置异常，不发布半构造谓词。
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
