package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 类型化保存点：记录所属 MTR id 与 memo 深度。只应由 {@link MiniTransaction#savepoint()} 创建，
 * 不要手工构造；rollbackToSavepoint 会用 mtrId 校验归属、用 depth 决定释放到哪一层。
 *
 * @param mtrId 所属 mini-transaction id。
 * @param depth 保存点对应的 memo 深度（非负）。
 */
public record MtrSavepoint(long mtrId, int depth) {

    public MtrSavepoint {
        if (depth < 0) {
            throw new DatabaseValidationException("savepoint depth must be non-negative: " + depth);
        }
    }
}
