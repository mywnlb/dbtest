package cn.zhangyis.db.sql.parser.ast;
/**
 * 表示SQL 词法与语法解析中的 {@code SetAutocommitNode} 不可变命令 AST；节点固定事务控制或会话选项语义，执行层只读取其值，不允许解析后改写。
 *
 * @param enabled 调用方请求的开关目标状态；{@code true} 表示启用，{@code false} 表示禁用，实际切换仍须通过当前状态机校验
 */
public record SetAutocommitNode(boolean enabled) implements StatementNode { }
