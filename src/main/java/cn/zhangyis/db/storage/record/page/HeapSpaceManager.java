package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.format.RecordHeader;

/**
 * 页内 heap 空间分配/回收策略（innodb-record-design §7 GarbageList + §10.1 step4，Strategy）。绑定单页、单 X latch 内使用。
 *
 * <p>GarbageList：header {@code FREE}=空闲碎片链头（0=空），各碎片用 {@code next_record} 字段串接（碎片已离开用户链，
 * 复用该字段安全），碎片容量取其保留的 {@link RecordHeader#recordLength()}。{@code GARBAGE}=已跟踪垃圾字节数
 * （= free 累加整条 − 复用扣减整条 + 原地缩短累加；非物理总死空间，oversized 复用余量不计入，见 spec §3.2）。
 *
 * <p>分配 first-fit + 整块消费：{@link #allocate} 先扫 FREE 链找首个容量≥需求的碎片复用（沿用其 heapNo、nHeap 不变、
 * GARBAGE 扣整块），余量作未跟踪内部碎片留 reorganize 回收；找不到回退 {@link RecordPage#allocateFromFreeSpace}（新 heapNo）。
 */
public final class HeapSpaceManager {

    /** 分配结果：记录落点偏移、其 heapNo、是否复用 garbage 碎片。 */
    public record Allocation(int offset, int heapNo, boolean reused) {
    }

    private final RecordPage page;

    public HeapSpaceManager(RecordPage page) {
        if (page == null) {
            throw new DatabaseValidationException("heap space manager page must not be null");
        }
        this.page = page;
    }

    /**
     * 为一条 {@code neededBytes} 的新记录分配空间（要求 X）。first-fit 复用 GarbageList，否则回退 FreeSpace。
     *
     * @throws RecordPageOverflowException FreeSpace 不足（由 {@link RecordPage#allocateFromFreeSpace} 抛出）。
     * @throws PageDirectoryCorruptedException FREE 链成环。
     */
    public Allocation allocate(int neededBytes) {
        int maxSteps = page.header().nHeap();
        int prevFrag = 0; // 0 表示当前考察的是 FREE 链头本身
        int frag = page.header().free();
        int steps = 0;
        while (frag != 0) {
            if (steps++ > maxSteps) {
                throw new PageDirectoryCorruptedException("garbage free-list cycle detected");
            }
            RecordHeader fh = page.recordHeaderAt(frag);
            int cap = fh.recordLength();
            int nextFrag = page.nextRecord(frag);
            if (cap >= neededBytes) {
                // 从 FREE 链摘除 frag：头则改 FREE 字段，非头则改前驱碎片的 next。
                if (prevFrag == 0) {
                    page.writeHeader(page.header().withFree(nextFrag));
                } else {
                    page.setNextRecord(prevFrag, nextFrag);
                }
                page.writeHeader(page.header().withGarbage(page.header().garbage() - cap));
                return new Allocation(frag, fh.heapNo(), true);
            }
            prevFrag = frag;
            frag = nextFrag;
        }
        int heapNo = page.nextHeapNo();
        int offset = page.allocateFromFreeSpace(neededBytes);
        return new Allocation(offset, heapNo, false);
    }

    /**
     * 把 {@code offset} 记录占用的空间压入 GarbageList 头（要求 X）：{@code next_record(offset)=旧 FREE}、{@code FREE=offset}、
     * {@code GARBAGE += recordLength}。调用方须保证该记录已离开用户 next_record 链（purge/update 搬迁先摘链）。
     */
    public void free(int offset) {
        int cap = page.recordHeaderAt(offset).recordLength();
        int oldHead = page.header().free();
        page.setNextRecord(offset, oldHead);
        page.writeHeader(page.header().withFree(offset).withGarbage(page.header().garbage() + cap));
    }
}
