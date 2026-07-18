package cn.zhangyis.db.sql.parser.ast;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
/**
 * 表示SQL 词法与语法解析中的 {@code TransactionControlNode} 不可变命令 AST；节点固定事务控制或会话选项语义，执行层只读取其值，不允许解析后改写。
 *
 * @param kind 选择 {@code 构造} 分支的 {@code Kind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
 */
public record TransactionControlNode(Kind kind) implements StatementNode {
    public TransactionControlNode {
        if (kind == null) throw new DatabaseValidationException("transaction command kind must not be null");
    }
    /**
     * 定义SQL 词法与语法解析的 {@code Kind} 状态或类别；枚举值用于显式分派领域行为，不得用声明顺序代替稳定编码。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code BEGIN}：表示“BEGIN”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code COMMIT}：表示“提交”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code ROLLBACK}：表示“回滚”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     * </ul>
     */
    public enum Kind { BEGIN, COMMIT, ROLLBACK }
}
