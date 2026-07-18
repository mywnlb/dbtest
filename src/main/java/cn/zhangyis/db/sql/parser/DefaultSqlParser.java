package cn.zhangyis.db.sql.parser;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.parser.ast.*;
import cn.zhangyis.db.sql.parser.exception.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** v1 递归下降 parser；实例只持最大输入配置，单次 parse 的 token/cursor 均为局部状态。 */
public final class DefaultSqlParser {
    public static final int DEFAULT_MAX_SQL_LENGTH = 1_048_576;
    private final int maxSqlLength;

    public DefaultSqlParser() { this(DEFAULT_MAX_SQL_LENGTH); }

    public DefaultSqlParser(int maxSqlLength) {
        if (maxSqlLength <= 0) throw new DatabaseValidationException("max SQL length must be positive");
        this.maxSqlLength = maxSqlLength;
    }

    /** 解析恰好一条 v1 statement，并严格消费可选分号与 EOF。 */
    public StatementNode parse(String sql) {
        if (sql == null) throw new DatabaseValidationException("SQL text must not be null");
        if (sql.length() > maxSqlLength) {
            throw new SqlSyntaxException("SQL input exceeds configured limit",
                    new SourcePosition(maxSqlLength, 1, maxSqlLength + 1), sql);
        }
        Parser parser = new Parser(sql, new SqlLexer(sql).lex());
        return parser.parse();
    }

    private static final class Parser {
        private final String sql;
        private final List<Token> tokens;
        private int cursor;

        private Parser(String sql, List<Token> tokens) { this.sql = sql; this.tokens = tokens; }

        private StatementNode parse() {
            StatementNode statement;
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
            if (match(TokenType.SEMICOLON)) take();
            require(TokenType.EOF, "end of statement");
            return statement;
        }

        private InsertStatementNode insert() {
            requireKeyword("INSERT"); requireKeyword("INTO");
            QualifiedNameNode table = qualifiedName();
            require(TokenType.LPAREN, "'('");
            List<IdentifierNode> columns = identifierList();
            require(TokenType.RPAREN, "')'");
            requireKeyword("VALUES"); require(TokenType.LPAREN, "'('");
            List<LiteralNode> values = literalList();
            require(TokenType.RPAREN, "')'");
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

        private UpdateStatementNode update() {
            requireKeyword("UPDATE"); QualifiedNameNode table = qualifiedName(); requireKeyword("SET");
            List<AssignmentNode> assignments = new ArrayList<>();
            assignments.add(assignment());
            while (match(TokenType.COMMA)) { take(); assignments.add(assignment()); }
            requireKeyword("WHERE"); return new UpdateStatementNode(table, assignments, predicates());
        }

        private AssignmentNode assignment() { return new AssignmentNode(identifier(), equalsLiteral()); }
        private LiteralNode equalsLiteral() { require(TokenType.EQUALS, "'='"); return literal(); }

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
