package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.SegmentRef;

/**
 * system.ibd page 3 的不可变权威投影。
 *
 * @param state header 生命周期
 * @param configuredMode 创建/最近显式持久化的模式
 * @param rootPageId 全局树稳定 root，必须属于 SpaceId 0
 * @param rootLevel 当前 B+Tree 高度快照
 * @param indexId 内部树稳定 index identity
 * @param leafSegment split 分配 leaf 页的专用 segment
 * @param nonLeafSegment 长高分配内部页的专用 segment
 * @param nextSequence 下一条 mutation 使用的正序号
 * @param pendingOperations 尚未 consume 的记录数
 * @param formatEpoch 不兼容重建代际，必须为正
 */
public record ChangeBufferHeaderSnapshot(ChangeBufferHeaderState state, ChangeBufferMode configuredMode,
                                         PageId rootPageId, int rootLevel, long indexId,
                                         SegmentRef leafSegment, SegmentRef nonLeafSegment,
                                         long nextSequence, long pendingOperations, long formatEpoch) {

    /** 系统表空间的稳定 identity。 */
    public static final SpaceId SYSTEM_SPACE_ID = SpaceId.of(0);

    public ChangeBufferHeaderSnapshot {
        if (state == null || configuredMode == null || rootPageId == null
                || leafSegment == null || nonLeafSegment == null) {
            throw new DatabaseValidationException("change buffer header fields must not be null");
        }
        if (!rootPageId.spaceId().equals(SYSTEM_SPACE_ID)
                || !leafSegment.spaceId().equals(SYSTEM_SPACE_ID)
                || !nonLeafSegment.spaceId().equals(SYSTEM_SPACE_ID)) {
            throw new DatabaseValidationException("change buffer root and segments must belong to system space 0");
        }
        if (rootLevel < 0 || indexId <= 0 || nextSequence <= 0 || pendingOperations < 0 || formatEpoch <= 0) {
            throw new DatabaseValidationException("invalid change buffer header numeric state");
        }
        if (leafSegment.equals(nonLeafSegment)) {
            throw new DatabaseValidationException(
                    "change buffer leaf and non-leaf segments must have distinct identities");
        }
    }

    /**
     * 为追加一条记录生成后置 header，保持 root/segment identity 并推进 sequence/count。
     *
     * @param newRootLevel B+Tree insert 后从结果取得的非负层级
     * @return nextSequence 加一、pending 加一的新快照
     * @throws ChangeBufferStateException sequence 或 pending 已到 {@link Long#MAX_VALUE} 时抛出；调用方必须停止追加
     */
    public ChangeBufferHeaderSnapshot afterAppend(int newRootLevel) {
        if (nextSequence == Long.MAX_VALUE || pendingOperations == Long.MAX_VALUE) {
            throw new ChangeBufferStateException(
                    "change buffer sequence/pending counter is exhausted: next="
                            + nextSequence + ", pending=" + pendingOperations);
        }
        return new ChangeBufferHeaderSnapshot(state, configuredMode, rootPageId, newRootLevel, indexId,
                leafSegment, nonLeafSegment, nextSequence + 1L,
                pendingOperations + 1L, formatEpoch);
    }

    /**
     * 为成功 consume 若干记录生成后置 header。
     *
     * @param newRootLevel B+Tree delete/merge 后层级
     * @param consumed 本 MTR 实际移除的正记录数
     * @return pending 精确扣减的新快照
     * @throws DatabaseValidationException consumed 非正或超过 pending 时抛出
     */
    public ChangeBufferHeaderSnapshot afterConsume(int newRootLevel, long consumed) {
        if (consumed <= 0 || consumed > pendingOperations) {
            throw new DatabaseValidationException("change buffer consumed count is invalid: " + consumed);
        }
        return new ChangeBufferHeaderSnapshot(state, configuredMode, rootPageId, newRootLevel, indexId,
                leafSegment, nonLeafSegment, nextSequence, pendingOperations - consumed, formatEpoch);
    }
}
