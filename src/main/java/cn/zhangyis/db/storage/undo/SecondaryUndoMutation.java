package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一条 undo 对单个二级索引的反向证据。它故意不复制物理 key：rollback/purge 使用 exact-version
 * {@code TableIndexMetadata}，分别从当前聚簇行和 undo old image 重建紧凑 entry，确保比较、prefix 与主键后缀
 * 永远服从对应 schema version；tail 只保存无法从行 image 推导的 action 和 UPDATE 新 entry 前态。
 *
 * @param indexId             目标二级索引 id；必须非负，且在 UndoRecord 列表中严格递增。
 * @param action              前向 DML 对该索引执行的动作；不能为 null。
 * @param newEntryBeforeState UPDATE 新 entry 发布前状态；非 CHANGE_KEY 必须为 NOT_APPLICABLE。
 */
public record SecondaryUndoMutation(long indexId,
                                    SecondaryUndoAction action,
                                    SecondaryEntryBeforeState newEntryBeforeState) {

    /**
     * 校验单索引 undo mutation 的动作与前态组合。
     *
     * @param indexId             目标二级索引稳定 id，必须非负。
     * @param action              rollback 要执行的动作，不能为 {@code null}。
     * @param newEntryBeforeState UPDATE 新 entry 的发布前状态；仅 CHANGE_KEY 允许 ABSENT/DELETE_MARKED。
     * @throws DatabaseValidationException identity 无效、字段缺失或 action/before-state 组合不可恢复时抛出。
     */
    public SecondaryUndoMutation {
        if (indexId < 0) {
            throw new DatabaseValidationException("secondary undo index id must be non-negative: " + indexId);
        }
        if (action == null || newEntryBeforeState == null) {
            throw new DatabaseValidationException("secondary undo action/before-state must not be null");
        }
        if (action == SecondaryUndoAction.CHANGE_KEY) {
            if (newEntryBeforeState == SecondaryEntryBeforeState.NOT_APPLICABLE) {
                throw new DatabaseValidationException("CHANGE_KEY requires ABSENT or DELETE_MARKED before-state");
            }
        } else if (newEntryBeforeState != SecondaryEntryBeforeState.NOT_APPLICABLE) {
            throw new DatabaseValidationException(action + " requires NOT_APPLICABLE before-state");
        }
    }

    /**
     * 构造 INSERT 发布二级 entry 的反向证据。
     *
     * @param indexId 被 INSERT 发布的二级索引稳定 id。
     * @return rollback 时物理删除本事务新 entry 的 mutation。
     * @throws DatabaseValidationException {@code indexId} 为负数时抛出。
     */
    public static SecondaryUndoMutation insertEntry(long indexId) {
        return new SecondaryUndoMutation(indexId, SecondaryUndoAction.INSERT_ENTRY,
                SecondaryEntryBeforeState.NOT_APPLICABLE);
    }

    /**
     * 构造 UPDATE 改变 logical key 的反向证据。
     *
     * @param indexId    被 UPDATE 改键的二级索引稳定 id。
     * @param beforeState 新 entry 发布前是不存在还是 delete-marked；决定 rollback 执行 remove 还是 re-mark。
     * @return 先撤销新 entry、再 revive 旧 entry 所需的 mutation。
     * @throws DatabaseValidationException 索引 id 无效或前态为 NOT_APPLICABLE 时抛出。
     */
    public static SecondaryUndoMutation changeKey(long indexId, SecondaryEntryBeforeState beforeState) {
        return new SecondaryUndoMutation(indexId, SecondaryUndoAction.CHANGE_KEY, beforeState);
    }

    /**
     * 构造 DELETE 标记二级 entry 的反向证据。
     *
     * @param indexId 被 DELETE 标记的二级索引稳定 id。
     * @return rollback 时 revive 同一完整物理 entry 的 mutation。
     * @throws DatabaseValidationException {@code indexId} 为负数时抛出。
     */
    public static SecondaryUndoMutation deleteMarkEntry(long indexId) {
        return new SecondaryUndoMutation(indexId, SecondaryUndoAction.DELETE_MARK_ENTRY,
                SecondaryEntryBeforeState.NOT_APPLICABLE);
    }
}
