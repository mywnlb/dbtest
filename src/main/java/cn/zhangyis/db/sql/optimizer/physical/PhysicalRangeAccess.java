package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.sql.binder.bound.SelectLockMode;

/**
 * comparison、复合前缀或聚簇全扫访问叶。
 *
 * @param table exact DD table version
 * @param accessIndexId 访问索引稳定 id
 * @param indexRange 可无界、开放或闭合的物理范围
 * @param lockMode consistent 或 current locking read
 * @param empty Optimizer 已证明 WHERE 不可能为 TRUE；打开 cursor 时不得创建 ReadView 或锁
 */
public record PhysicalRangeAccess(
        TableDefinition table, long accessIndexId, IndexRange indexRange,
        SelectLockMode lockMode, boolean empty) implements PhysicalAccess {

    /**
     * 校验索引和 endpoint；residual 由独立父级 Filter 校验。
     *
     * @throws DatabaseValidationException 字段、索引或 endpoint 与 exact table 不一致时抛出
     */
    public PhysicalRangeAccess {
        if (table == null || indexRange == null || lockMode == null) {
            throw new DatabaseValidationException("invalid physical range access fields");
        }
        PhysicalPlanValidation.validateRangeAccess(table, accessIndexId, indexRange);
    }
}
