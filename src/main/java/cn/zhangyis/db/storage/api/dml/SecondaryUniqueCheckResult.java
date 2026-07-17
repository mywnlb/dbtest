package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 二级 publish 前检查结果。duplicate=true 时调用方必须报告唯一/物理 identity 冲突，不能消费 publishState；
 * available 时 publishState 决定 insert 或 revive。
 *
 * @param duplicate    是否存在禁止发布的 live/其它主键候选。
 * @param publishState 无冲突时目标完整物理 identity 的既有状态；不能为 null。
 */
public record SecondaryUniqueCheckResult(boolean duplicate, SecondaryPublishState publishState) {

    /**
     * 校验二级发布检查结果内部状态。
     *
     * @param duplicate    {@code true} 表示 logical unique key 或完整物理 identity 已被 live entry 占用。
     * @param publishState 无冲突时目标完整物理 identity 的既有状态；冲突结果中仅为不可消费的占位状态。
     * @throws DatabaseValidationException {@code publishState} 为空时抛出，防止调用方无法选择 insert/revive 分支。
     */
    public SecondaryUniqueCheckResult {
        if (publishState == null) {
            throw new DatabaseValidationException("secondary unique check publishState must not be null");
        }
    }

    /**
     * 构造允许发布的检查结果。
     *
     * @param state 目标完整物理 identity 在检查时的状态；{@link SecondaryPublishState#ABSENT} 对应 insert，
     *              {@link SecondaryPublishState#DELETE_MARKED} 对应 revive。
     * @return {@code duplicate=false} 且携带给前向发布阶段消费的状态。
     * @throws DatabaseValidationException {@code state} 为空时抛出。
     */
    public static SecondaryUniqueCheckResult available(SecondaryPublishState state) {
        return new SecondaryUniqueCheckResult(false, state);
    }

    /**
     * 构造禁止发布的冲突结果。
     *
     * @return {@code duplicate=true} 的结果；其中 {@code publishState} 使用 ABSENT 占位，调用方不得消费。
     */
    public static SecondaryUniqueCheckResult conflict() {
        return new SecondaryUniqueCheckResult(true, SecondaryPublishState.ABSENT);
    }
}
