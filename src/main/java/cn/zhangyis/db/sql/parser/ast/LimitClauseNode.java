package cn.zhangyis.db.sql.parser.ast;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 已规范为 offset/count 的 LIMIT 语法。
 *
 * @param offset 从有序结果前端跳过的非负行数
 * @param count 最多发布的非负行数；零表示不打开下游即可返回空集
 */
public record LimitClauseNode(long offset, long count) {

    public LimitClauseNode {
        if (offset < 0 || count < 0) {
            throw new DatabaseValidationException(
                    "LIMIT offset/count must be non-negative");
        }
    }
}
