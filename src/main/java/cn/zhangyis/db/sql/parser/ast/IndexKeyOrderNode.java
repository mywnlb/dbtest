package cn.zhangyis.db.sql.parser.ast;

/** CREATE INDEX key part 的纯语法排序方向；未写方向由 parser 规范为 ASC。 */
public enum IndexKeyOrderNode {
    ASC,
    DESC
}
