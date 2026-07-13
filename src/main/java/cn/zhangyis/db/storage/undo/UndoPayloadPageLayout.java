package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/** External undo payload 页的稳定 v1 body 布局；统一 FIL header/trailer 继续承载页链与校验信息。 */
final class UndoPayloadPageLayout {

    private UndoPayloadPageLayout() {
    }

    /** ASCII "UEP1"。 */
    static final int MAGIC_VALUE = 0x55455031;
    static final int VERSION_VALUE = 1;
    static final int MAGIC = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    static final int VERSION = MAGIC + Integer.BYTES;
    static final int CHUNK_INDEX = VERSION + Integer.BYTES;
    static final int CHUNK_LENGTH = CHUNK_INDEX + Integer.BYTES;
    static final int SEGMENT_ID = CHUNK_LENGTH + Integer.BYTES;
    static final int INODE_SLOT = SEGMENT_ID + Long.BYTES;
    static final int TRANSACTION_ID = INODE_SLOT + Integer.BYTES;
    static final int UNDO_NO = TRANSACTION_ID + Long.BYTES;
    static final int TOTAL_LENGTH = UNDO_NO + Long.BYTES;
    static final int PAGE_COUNT = TOTAL_LENGTH + Integer.BYTES;
    static final int WHOLE_CRC32 = PAGE_COUNT + Integer.BYTES;
    static final int DATA = WHOLE_CRC32 + Integer.BYTES;

    /** 单页 chunk 容量；排除统一 FIL trailer。 */
    static int payloadCapacity(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("undo payload page size must not be null");
        }
        int capacity = PageEnvelopeLayout.trailerOffset(pageSize) - DATA;
        if (capacity <= 0) {
            throw new DatabaseValidationException("page size too small for external undo payload layout: "
                    + pageSize.bytes());
        }
        return capacity;
    }
}
