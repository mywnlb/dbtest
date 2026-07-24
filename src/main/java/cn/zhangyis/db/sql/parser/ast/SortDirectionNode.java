package cn.zhangyis.db.sql.parser.ast;

/**
 * ORDER BY 排序方向。枚举只保留 SQL 语法语义，是否能由索引物理顺序满足由 Optimizer 判定。
 */
public enum SortDirectionNode {
    /** 升序；未显式书写方向时也规范为该值。 */
    ASC,
    /** 降序。 */
    DESC
}
