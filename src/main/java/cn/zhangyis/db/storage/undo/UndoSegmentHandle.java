package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.SpaceId;

/**
 * 一条 undo log segment 的逻辑与物理定位。该值对象属于 undo 模块自身，不暴露
 * {@code storage.api.SegmentRef}，用于隔离 undo 物理页链与 DiskSpaceManager 的直接依赖。
 *
 * <p>{@code inodeSlot}/{@code segmentId} 会落到 undo page header 中，reopen 时可重建续分配所需的
 * segment 身份；{@code firstPageId}/{@code lastPageId} 是当前 undo log 页链端点。对象不可变，跨页生长
 * 通过 {@link #withLastPage(PageId)} 生成新 handle，避免把链尾推进隐藏在共享可变状态中。
 *
 * @param spaceId     segment 所属 undo 表空间；本片保持单 undo space 假设，但仍显式携带以保护页定位。
 * @param inodeSlot   FSP segment inode 槽下标；非法槽会导致后续续分配写入错误 segment。
 * @param segmentId   UNDO segment 逻辑编号；0 不代表真实业务 segment，本对象要求正数。
 * @param firstPageId 链首页，包含 undo log header；必须与 {@code spaceId} 同表空间。
 * @param lastPageId  链尾页，append 目标；必须与 {@code spaceId} 同表空间。
 */
public record UndoSegmentHandle(SpaceId spaceId, int inodeSlot, SegmentId segmentId,
                                PageId firstPageId, PageId lastPageId) {

    public UndoSegmentHandle {
        if (spaceId == null || segmentId == null || firstPageId == null || lastPageId == null) {
            throw new DatabaseValidationException("undo segment handle fields must not be null");
        }
        if (inodeSlot < 0) {
            throw new DatabaseValidationException("inode slot must be non-negative: " + inodeSlot);
        }
        if (segmentId.value() <= 0) {
            throw new DatabaseValidationException("segment id must be positive: " + segmentId.value());
        }
        if (!firstPageId.spaceId().equals(spaceId) || !lastPageId.spaceId().equals(spaceId)) {
            throw new DatabaseValidationException("undo segment handle page space mismatch with " + spaceId);
        }
    }

    /**
     * 返回仅替换链尾页的新 handle。调用方在 FIL 链和 first 页 LAST_PAGE_NO 已写入后使用它推进内存端点；
     * 原 handle 保持不变，便于异常路径确认未发生半生长状态泄漏。
     *
     * @param newLast 新链尾页，必须与 handle 的 undo 表空间一致。
     * @return 链尾推进后的新 handle。
     */
    public UndoSegmentHandle withLastPage(PageId newLast) {
        return new UndoSegmentHandle(spaceId, inodeSlot, segmentId, firstPageId, newLast);
    }
}
