package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.ast.*;
import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
            if (keyword("INSERT")) statement = insert();
            else if (keyword("CREATE")) statement = createIndex();
            else if (keyword("ALTER")) statement = alterAddIndex();
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
                take(); statement = new TransactionControlNode(TransactionControlNode.Kind.ROLLBACK);
            } else {
                throw syntax("expected INSERT, CREATE, ALTER, UPDATE, DELETE, SELECT, SET, BEGIN, START, COMMIT or ROLLBACK",
                        current());
            }
            // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
            if (match(TokenType.SEMICOLON)) take();
            require(TokenType.EOF, "end of statement");
            // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
            return statement;
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

        /** v1 ALTER 只接受 TABLE ... ADD [UNIQUE] INDEX，并与独立语法产生同一 AST 类型。 */
        private CreateIndexStatementNode alterAddIndex() {
            requireKeyword("ALTER");
            requireKeyword("TABLE");
            QualifiedNameNode table = qualifiedName();
            requireKeyword("ADD");
            boolean unique = false;
            if (keyword("UNIQUE")) {
                take();
                unique = true;
            }
            requireKeyword("INDEX");
            IdentifierNode indexName = identifier();
            return new CreateIndexStatementNode(table, indexName, unique, indexKeyParts());
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

        private List<EqualityPredicateNode> predicates() {
            List<EqualityPredicateNode> result = new ArrayList<>(); result.add(predicate());
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
            List<EqualityPredicateNode> predicates = new ArrayList<>();
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

        private EqualityPredicateNode predicate() {
            IdentifierNode column = identifier();
            require(TokenType.EQUALS, "'='");
            return new EqualityPredicateNode(column, literal());
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
