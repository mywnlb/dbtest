package cn.zhangyis.db.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 记录内 external LOB 引用。它只描述稳定物理定位和完整性信息，不持有页、buffer fix 或 segment 生命周期。
 *
 * @param spaceId 页链所属表空间。
 * @param firstPageNo 页链首页。
 * @param totalLength 完整逻辑 payload 字节数。
 * @param pageCount 页链精确页数。
 * @param segmentId 拥有该链的 LOB segment identity。
 * @param inodeSlot segment inode 槽。
 * @param crc32 完整 payload 的 unsigned CRC32（0..2^32-1）。
 */
public record LobReference(SpaceId spaceId, PageNo firstPageNo, int totalLength, int pageCount,
                           SegmentId segmentId, int inodeSlot, long crc32) {

    public LobReference {
        if (spaceId == null || firstPageNo == null || segmentId == null) {
            throw new DatabaseValidationException("LOB reference identity fields must not be null");
        }
        if (totalLength <= 0 || pageCount <= 0 || segmentId.value() <= 0 || inodeSlot < 0) {
            throw new DatabaseValidationException("LOB reference length/count/segment fields must be positive");
        }
        if (firstPageNo.value() < 4 || firstPageNo.value() >= 0xFFFF_FFFFL) {
            throw new DatabaseValidationException(
                    "LOB first page must be a data page in FIL u32 range: " + firstPageNo.value());
        }
        if (crc32 < 0 || crc32 > 0xFFFF_FFFFL) {
            throw new DatabaseValidationException("LOB reference CRC32 out of unsigned range: " + crc32);
        }
    }
}
