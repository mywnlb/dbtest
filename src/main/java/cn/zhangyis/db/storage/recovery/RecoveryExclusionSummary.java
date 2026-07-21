package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.SpaceId;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 一次恢复中对象级排除的来源与效果统计；集合用于长期诊断，计数只描述本次实际跳过的物理/undo 工作。
 *
 * @param administrativeSpaces 本次 FORCE 管理员声明
 * @param dictionarySpaces committed DD 长期隔离集合
 * @param skippedDoublewritePages 被排除空间中的 doublewrite 页数
 * @param skippedRedoRecords 被排除空间中的 redo page record 数
 * @param skippedReconcileSpaces 未执行 file reconcile 的排除空间数
 * @param skippedActiveRollbackRecords recovered ACTIVE 中仅推进 head 的记录数
 * @param skippedPreparedRollbackRecords recovered PREPARED rollback 中仅推进 head 的记录数
 * @param skippedPurgeRecords recovered history purge 中仅推进 head 的记录数
 */
public record RecoveryExclusionSummary(Set<SpaceId> administrativeSpaces,
                                       Set<SpaceId> dictionarySpaces,
                                       int skippedDoublewritePages,
                                       int skippedRedoRecords,
                                       int skippedReconcileSpaces,
                                       int skippedActiveRollbackRecords,
                                       int skippedPreparedRollbackRecords,
                                       int skippedPurgeRecords) {

    /** 冻结来源集合并拒绝负统计。 */
    public RecoveryExclusionSummary {
        if (administrativeSpaces == null || dictionarySpaces == null
                || administrativeSpaces.stream().anyMatch(java.util.Objects::isNull)
                || dictionarySpaces.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("recovery exclusion summary spaces must not contain null");
        }
        if (skippedDoublewritePages < 0 || skippedRedoRecords < 0 || skippedReconcileSpaces < 0
                || skippedActiveRollbackRecords < 0 || skippedPreparedRollbackRecords < 0
                || skippedPurgeRecords < 0) {
            throw new DatabaseValidationException("recovery exclusion summary counts must not be negative");
        }
        administrativeSpaces = Set.copyOf(administrativeSpaces);
        dictionarySpaces = Set.copyOf(dictionarySpaces);
    }

    /** @return 无排除来源和跳过工作的普通摘要。 */
    public static RecoveryExclusionSummary none() {
        return new RecoveryExclusionSummary(Set.of(), Set.of(), 0, 0, 0, 0, 0, 0);
    }

    /** @return 管理员与 DD 排除集合的不可变并集。 */
    public Set<SpaceId> excludedSpaces() {
        Set<SpaceId> result = new LinkedHashSet<>(administrativeSpaces);
        result.addAll(dictionarySpaces);
        return Set.copyOf(result);
    }
}
