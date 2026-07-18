package cn.zhangyis.db.sql.parser.ast;

/** CREATE INDEX key part 的纯语法排序方向；未写方向由 parser 规范为 ASC。
 *
 * <p>未单独声明 Javadoc 的枚举值语义：</p>
 * <ul>
 *     <li>{@code ASC}：表示“ASC”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 *     <li>{@code DESC}：表示“DESC”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
 * </ul>
 */
public enum IndexKeyOrderNode {
    ASC,
    DESC
}
