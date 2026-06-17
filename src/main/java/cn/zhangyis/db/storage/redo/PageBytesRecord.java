package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

/**
 * 物理字节区间写：把 {@code bytes} 覆盖到 {@code pageId} 的 {@code offset} 处。recovery（R1）按此重放即可幂等重建页内容。
 *
 * @param pageId 目标页。
 * @param offset 页内偏移（非负）。
 * @param bytes  写入字节（防御性 clone；accessor 也返回 clone，避免泄漏可变状态）。
 */
public record PageBytesRecord(PageId pageId, int offset, byte[] bytes) implements RedoRecord {

    /** R1 文件编码头：tag(1) + spaceId(4) + pageNo(8) + offset(4) + payloadLen(4)。 */
    private static final int HEADER_BYTES = 21;

    public PageBytesRecord {
        if (pageId == null) {
            throw new DatabaseValidationException("page bytes record pageId must not be null");
        }
        if (offset < 0) {
            throw new DatabaseValidationException("page bytes record offset must be non-negative: " + offset);
        }
        if (bytes == null) {
            throw new DatabaseValidationException("page bytes record bytes must not be null");
        }
        bytes = bytes.clone();
    }

    /** 返回防御性副本，避免外部改动内部状态（Java record 数组 accessor 默认会泄漏内部数组）。 */
    @Override
    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public int byteLength() {
        return HEADER_BYTES + bytes.length;
    }
}
