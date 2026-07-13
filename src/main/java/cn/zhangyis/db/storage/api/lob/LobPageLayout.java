package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/** BLOB 页 body 的稳定 v1 布局；页首/页尾继续使用统一 FIL envelope。 */
public final class LobPageLayout {

    private LobPageLayout() {
    }

    /** ASCII "LOB1"，用于区别未格式化或错误复用的已分配页。 */
    public static final int MAGIC_VALUE = 0x4C4F4231;
    /** v1 body 版本；布局变化必须追加版本并保留旧解码。 */
    public static final int VERSION_VALUE = 1;
    public static final int MAGIC = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    public static final int VERSION = MAGIC + Integer.BYTES;
    public static final int CHUNK_INDEX = VERSION + Integer.BYTES;
    public static final int CHUNK_LENGTH = CHUNK_INDEX + Integer.BYTES;
    public static final int SEGMENT_ID = CHUNK_LENGTH + Integer.BYTES;
    public static final int INODE_SLOT = SEGMENT_ID + Long.BYTES;
    public static final int TOTAL_LENGTH = INODE_SLOT + Integer.BYTES;
    public static final int WHOLE_CRC32 = TOTAL_LENGTH + Integer.BYTES;
    public static final int PAGE_COUNT = WHOLE_CRC32 + Integer.BYTES;
    /** chunk payload 首字节。 */
    public static final int DATA = PAGE_COUNT + Integer.BYTES;

    /** 计算单页最大 chunk，排除统一 trailer；过小页配置 fail-closed。 */
    public static int payloadCapacity(PageSize pageSize) {
        if (pageSize == null) {
            throw new DatabaseValidationException("LOB page size must not be null");
        }
        int capacity = PageEnvelopeLayout.trailerOffset(pageSize) - DATA;
        if (capacity <= 0) {
            throw new DatabaseValidationException("page size too small for LOB layout: " + pageSize.bytes());
        }
        return capacity;
    }
}
