package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * Binder 冻结的 LIMIT offset/count。
 *
 * @param offset 排序完成后跳过的非负行数
 * @param count 最多发布的非负行数
 */
public record BoundLimit(long offset, long count) {

    public BoundLimit {
        if (offset < 0 || count < 0) {
            throw new DatabaseValidationException(
                    "bound LIMIT offset/count must be non-negative");
        }
    }
}
