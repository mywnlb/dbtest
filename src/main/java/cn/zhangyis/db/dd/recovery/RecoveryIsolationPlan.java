package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.engine.recovery.RecoveryUnavailableTable;
import cn.zhangyis.db.storage.recovery.RecoverySpaceExclusionPolicy;

import java.util.List;

/**
 * 启动前对象级隔离的不可变结果；DD commit 已完成，后续 discovery 与 storage recovery 必须消费同一份排除证据。
 *
 * @param exclusionPolicy 管理员和 committed DD 空间的来源保留并集
 * @param unavailableTables 当前全部持久恢复隔离对象，供组合根诊断接口发布
 */
public record RecoveryIsolationPlan(RecoverySpaceExclusionPolicy exclusionPolicy,
                                    List<RecoveryUnavailableTable> unavailableTables) {

    /** 冻结列表并保证规划器不能发布空协作者或可变诊断状态。 */
    public RecoveryIsolationPlan {
        if (exclusionPolicy == null || unavailableTables == null
                || unavailableTables.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("recovery isolation plan fields must not be null");
        }
        unavailableTables = List.copyOf(unavailableTables);
    }
}
