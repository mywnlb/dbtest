package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.SegmentId;

import java.util.Optional;

/**
 * XDES entry 投影（不含 bitmap，bitmap 按位单独访问，避免大数组拷贝）。ownerSegmentRaw==0 表无主。
 *
 * @param extentId       该 descriptor 所描述的 extent。
 * @param state          extent 分配状态。
 * @param ownerSegmentRaw 拥有者 segment id 原始值；0 表无主（真实 segment id 从 1 起）。
 * @param prev           所在 extent list 的前驱节点地址（NULL 表链头/无）。
 * @param next           所在 extent list 的后继节点地址（NULL 表链尾/无）。
 */
public record ExtentDescriptor(
        ExtentId extentId,
        ExtentState state,
        long ownerSegmentRaw,
        FileAddress prev,
        FileAddress next) {

    /** 拥有者 segment；raw==0 → 空（无主）。 */
    public Optional<SegmentId> ownerSegment() {
        return ownerSegmentRaw == 0 ? Optional.empty() : Optional.of(SegmentId.of(ownerSegmentRaw));
    }
}
