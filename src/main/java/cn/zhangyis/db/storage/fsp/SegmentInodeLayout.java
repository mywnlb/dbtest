package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * SegmentInode entry 布局（page 2，从 INODE_BASE 起，每条 ENTRY_SIZE 字节）。used==0 空闲槽；fragment 槽空哨兵=0。
 * 三个 extent list 头为 FLST base（32B）。
 */
final class SegmentInodeLayout {

    private SegmentInodeLayout() {
    }

    static final int INODE_BASE = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES; // 38

    static final int USED = 0;                          // int（0=空闲,1=在用）
    static final int SEGMENT_ID = USED + 4;             // 4 long
    static final int PURPOSE = SEGMENT_ID + 8;          // 12 int（SegmentPurpose ordinal）
    static final int USED_PAGE_COUNT = PURPOSE + 4;     // 16 long
    static final int RESERVED_PAGE_COUNT = USED_PAGE_COUNT + 8; // 24 long
    static final int FREE_EXTENT_LIST_BASE = RESERVED_PAGE_COUNT + 8;            // 32 FlstBase(32)
    static final int NOT_FULL_EXTENT_LIST_BASE = FREE_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 64
    static final int FULL_EXTENT_LIST_BASE = NOT_FULL_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 96
    static final int FRAGMENT_SLOTS = FULL_EXTENT_LIST_BASE + FlstBaseLayout.SIZE;            // 128
    static final int FRAGMENT_SLOT_COUNT = 32;
    static final int ENTRY_SIZE = FRAGMENT_SLOTS + FRAGMENT_SLOT_COUNT * 8; // 384

    static long maxInodesInPage(PageSize pageSize) {
        return (long) (pageSize.bytes() - INODE_BASE) / ENTRY_SIZE;
    }

    static int slotOffset(int inodeSlot) {
        return INODE_BASE + inodeSlot * ENTRY_SIZE;
    }

    static int fragmentSlotOffset(int inodeSlot, int fragIdx) {
        return slotOffset(inodeSlot) + FRAGMENT_SLOTS + fragIdx * 8;
    }
}
