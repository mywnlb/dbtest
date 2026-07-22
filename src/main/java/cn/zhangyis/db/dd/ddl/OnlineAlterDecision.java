package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.List;

/**
 * 一次无副作用策略裁决；strategy/reason为主结果，rejectedCapabilities解释显式降级条件。
 *
 * @param strategy 已冻结的执行策略
 * @param reason 与策略对应的稳定原因
 * @param rejectedCapabilities 阻止更强online策略的有序能力集合；真正online时为空
 */
public record OnlineAlterDecision(OnlineAlterStrategy strategy, OnlineAlterReason reason,
                                  List<OnlineAlterCapability> rejectedCapabilities) {

    /** 防御性冻结能力集合并拒绝自相矛盾的online裁决。 */
    public OnlineAlterDecision {
        if (strategy == null || reason == null || rejectedCapabilities == null
                || rejectedCapabilities.stream().anyMatch(java.util.Objects::isNull)
                || ((strategy == OnlineAlterStrategy.INSTANT_METADATA
                || strategy == OnlineAlterStrategy.INPLACE_INDEX
                || strategy == OnlineAlterStrategy.SHADOW_REBUILD_V1)
                && !rejectedCapabilities.isEmpty())) {
            throw new DatabaseValidationException("online ALTER decision fields are inconsistent");
        }
        rejectedCapabilities = List.copyOf(rejectedCapabilities);
    }
}
