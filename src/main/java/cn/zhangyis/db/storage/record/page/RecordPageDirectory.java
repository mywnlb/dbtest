package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * PageDirectory 槽数组视图（innodb-record-design §7，R3）。绑定 {@link PageGuard}，在页尾 trailer 之前向低地址增长的
 * 稀疏槽数组上读写：每槽 2 字节 u16，存所属 group **最后一条记录**的页内偏移。槽数权威来源为 page header 的 N_DIR_SLOTS。
 *
 * <p>槽序：slot[0] 紧贴 trailer（最高地址），index 越大地址越低；逻辑上 slot[0]=infimum 组、slot[n-1]=supremum 组。
 * 每组成员数 {@code n_owned} 记在组末记录的 RecordHeader 上，由调用方（R4 insert/purge）维护，本类只管槽数组本身。
 *
 * <p>并发：写原语要求 PageGuard 为 EXCLUSIVE（由 PageGuard 自校验）。本类不拥有 latch/buffer fix 生命周期。
 */
public final class RecordPageDirectory {

    private final PageGuard guard;
    private final PageSize pageSize;

    public RecordPageDirectory(PageGuard guard, PageSize pageSize) {
        if (guard == null || pageSize == null) {
            throw new DatabaseValidationException("directory guard/pageSize must not be null");
        }
        this.pageSize = pageSize;
        this.guard = guard;
    }

    /** 槽数（读 page header N_DIR_SLOTS）。 */
    public int slotCount() {
        return PageU16.get(guard, IndexPageHeaderLayout.N_DIR_SLOTS);
    }

    /** 第 i 槽存的记录偏移；i 越界视为目录损坏。 */
    public int slot(int i) {
        checkIndex(i, slotCount());
        return PageU16.get(guard, slotAddr(i));
    }

    /** 改写第 i 槽（要求 X）；i 越界视为目录损坏。 */
    public void setSlot(int i, int recordOffset) {
        checkIndex(i, slotCount());
        PageU16.put(guard, slotAddr(i), recordOffset);
    }

    /** 线扫返回指向 {@code recordOffset} 的槽下标；未找到返回 -1。供 purge/update 定位 owner 的槽。 */
    public int indexOf(int recordOffset) {
        int n = slotCount();
        for (int i = 0; i < n; i++) {
            if (slot(i) == recordOffset) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 在第 {@code at} 槽位置插入新槽（要求 X）：原 [at, n) 槽逻辑右移一格（向更低地址多占 2 字节），N_DIR_SLOTS+1。
     *
     * <p>容量校验：新增一槽使 dirStart 下移 2 字节，若撞上 heapTop（free space 不足）则抛 {@link RecordPageOverflowException}。
     * 数据流：从高 index 向 at 复制（先把 old slot n-1..at 搬到 n..at+1，避免覆盖），再写新槽、再更新槽数。
     *
     * @param at           插入位置，{@code 0..n}。
     * @param recordOffset 新槽指向的记录偏移。
     */
    public void insertSlot(int at, int recordOffset) {
        int n = slotCount();
        if (at < 0 || at > n) {
            throw new PageDirectoryCorruptedException("insert slot index out of range: " + at + " (n=" + n + ")");
        }
        int newDirStart = dirEnd() - (n + 1) * IndexPageLayout.DIR_SLOT_BYTES;
        int heapTop = PageU16.get(guard, IndexPageHeaderLayout.HEAP_TOP);
        if (newDirStart < heapTop) {
            throw new RecordPageOverflowException("no room for new directory slot; heapTop=" + heapTop
                    + " newDirStart=" + newDirStart);
        }
        for (int i = n; i > at; i--) {
            PageU16.put(guard, slotAddr(i), PageU16.get(guard, slotAddr(i - 1)));
        }
        PageU16.put(guard, slotAddr(at), recordOffset);
        PageU16.put(guard, IndexPageHeaderLayout.N_DIR_SLOTS, n + 1);
    }

    /**
     * 删除第 {@code at} 槽（要求 X）：原 (at, n) 槽逻辑左移一格，N_DIR_SLOTS-1。
     * 不允许降到 2 槽以下（infimum/supremum 必须各保留一槽），否则视为目录损坏。
     */
    public void removeSlot(int at) {
        int n = slotCount();
        if (n <= 2) {
            throw new PageDirectoryCorruptedException("cannot remove slot below minimum 2: n=" + n);
        }
        checkIndex(at, n);
        for (int i = at; i < n - 1; i++) {
            PageU16.put(guard, slotAddr(i), PageU16.get(guard, slotAddr(i + 1)));
        }
        PageU16.put(guard, IndexPageHeaderLayout.N_DIR_SLOTS, n - 1);
    }

    private int dirEnd() {
        return pageSize.bytes() - IndexPageLayout.FIL_PAGE_TRAILER_BYTES;
    }

    private int slotAddr(int i) {
        return dirEnd() - (i + 1) * IndexPageLayout.DIR_SLOT_BYTES;
    }

    private static void checkIndex(int i, int n) {
        if (i < 0 || i >= n) {
            throw new PageDirectoryCorruptedException("slot index out of range: " + i + " (n=" + n + ")");
        }
    }
}
