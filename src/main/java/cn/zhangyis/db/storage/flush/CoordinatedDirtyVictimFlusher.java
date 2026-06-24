package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.DirtyVictimFlusher;

/**
 * 把 Buffer Pool 的淘汰端口 {@link DirtyVictimFlusher} 适配到 {@link FlushCoordinator#singlePageFlush}。
 * 由 {@code flush} 实现（依赖方向 flush→buf），使 {@code buf} 淘汰脏帧时复用既有 WAL gate + checksum +
 * doublewrite 单页 flush 管线，而无需 import {@code flush}。
 */
public final class CoordinatedDirtyVictimFlusher implements DirtyVictimFlusher {

    private final FlushCoordinator coordinator;

    public CoordinatedDirtyVictimFlusher(FlushCoordinator coordinator) {
        if (coordinator == null) {
            throw new DatabaseValidationException("flush coordinator must not be null");
        }
        this.coordinator = coordinator;
    }

    /**
     * 按 {@link FlushResultStatus} 映射淘汰端口契约：
     * <ul>
     *   <li>{@code CLEAN}→true（已落盘清脏，帧可复用）；</li>
     *   <li>{@code KEPT_DIRTY}/{@code SKIPPED_NOT_DIRTY}/{@code SKIPPED_REDO_NOT_DURABLE}→false（本轮未清成，调用方另选）；</li>
     *   <li>{@code FAILED}→抛出所携带的领域异常根因（真 IO/doublewrite/force 失败绝不能被吞成"可另选"，
     *       否则掩盖盘故障）。</li>
     * </ul>
     */
    @Override
    public boolean flushVictim(PageId pageId) {
        FlushResult result = coordinator.singlePageFlush(pageId);
        return switch (result.status()) {
            case CLEAN -> true;
            case KEPT_DIRTY, SKIPPED_NOT_DIRTY, SKIPPED_REDO_NOT_DURABLE -> false;
            case FAILED -> throw result.failure();
        };
    }
}
