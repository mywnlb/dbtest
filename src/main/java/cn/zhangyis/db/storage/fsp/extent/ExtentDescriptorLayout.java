package cn.zhangyis.db.storage.fsp.extent;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderLayout;


import cn.zhangyis.db.domain.PageSize;

/**
 * XDES entry 布局：内嵌 page 0、从 SpaceHeaderLayout.XDES_BASE 起、每条 ENTRY_SIZE 字节（条内偏移如下）。
 * bitmap 固定 32 字节（256 位，1 位/页，1=已分配），仅前 pagesPerExtent 位有效。
 */
public final class ExtentDescriptorLayout {

    private ExtentDescriptorLayout() {
    }

    public static final int STATE = 0;                 // int（ExtentState ordinal）
    public static final int OWNER_SEGMENT = STATE + 4; // 4 long（0=无主）
    public static final int PREV = OWNER_SEGMENT + 8;  // 12 FileAddress(12)
    public static final int NEXT = PREV + 12;          // 24 FileAddress(12)
    public static final int BITMAP = NEXT + 12;        // 36
    public static final int BITMAP_BYTES = 32;         // 256 位
    public static final int ENTRY_SIZE = BITMAP + BITMAP_BYTES; // 68

    public static long maxEntriesInPage0(PageSize pageSize) {
        return (long) (pageSize.bytes() - SpaceHeaderLayout.XDES_BASE) / ENTRY_SIZE;
    }

    public static int entryOffset(long extentNo) {
        return Math.toIntExact(SpaceHeaderLayout.XDES_BASE + extentNo * ENTRY_SIZE);
    }
}
