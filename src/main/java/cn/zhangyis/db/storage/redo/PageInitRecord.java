package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.page.PageType;

/**
 * 页初始化：{@code pageId} 在本 LSN 处以 {@code pageType} 创建。recovery（R1）据此初始化页骨架，随后的 PAGE_BYTES 填充内容。
 *
 * @param pageId   新页。
 * @param pageType 页类型。
 */
public record PageInitRecord(PageId pageId, PageType pageType) implements RedoRecord {

    /** R1 文件编码长度：tag(1) + spaceId(4) + pageNo(8) + pageType(4)。 */
    private static final int LENGTH = 17;

    public PageInitRecord {
        if (pageId == null || pageType == null) {
            throw new DatabaseValidationException("page init record pageId/pageType must not be null");
        }
    }

    @Override
    public int byteLength() {
        return LENGTH;
    }
}
