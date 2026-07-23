package cn.zhangyis.db.storage.fsp.extent;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.ExtentId;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;

/**
 * 一个 XDES entry 的稳定物理定位结果。
 *
 * @param extentId 被描述 extent 的全局身份；反向解析必须恢复为同一值
 * @param descriptorPageId 承载 entry 的 page0 或独立 XDES 页
 * @param slotInPage entry 在 descriptor 区中的零基槽号，不是全局 extentNo
 * @param entryOffset entry 相对物理页首的字节偏移，必须从固定 XDES_BASE 开始按 68 字节对齐
 * @param listNodeAddress entry 内 PREV 字段的页内地址，FLST 用它作为节点身份
 */
public record ExtentDescriptorLocation(
        ExtentId extentId,
        PageId descriptorPageId,
        int slotInPage,
        int entryOffset,
        FileAddress listNodeAddress) {

    /** 创建后立即校验冗余地址，避免 repository 与 FLST 使用不一致的页或偏移。 */
    public ExtentDescriptorLocation {
        if (extentId == null || descriptorPageId == null || listNodeAddress == null) {
            throw new DatabaseValidationException("XDES location fields must not be null");
        }
        if (!extentId.spaceId().equals(descriptorPageId.spaceId())) {
            throw new DatabaseValidationException("XDES location space identity mismatch");
        }
        if (slotInPage < 0 || entryOffset < 0 || listNodeAddress.isNull()) {
            throw new DatabaseValidationException("XDES location slot/offset/node must be concrete");
        }
        if (!descriptorPageId.pageNo().equals(listNodeAddress.pageNo())
                || listNodeAddress.offset() != entryOffset + ExtentDescriptorLayout.PREV) {
            throw new DatabaseValidationException("XDES location list node does not match entry address");
        }
    }
}
