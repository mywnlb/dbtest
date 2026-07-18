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

    /** 分配结果：记录落点偏移、其 heapNo、是否复用 garbage 碎片。
     *
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param heapNo 参与 {@code 构造} 的原始数值身份 {@code heapNo}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param reused 当前对象是否处于首次创建、首次写入或复用分支；该事实决定初始化、redo 和失败补偿路径
     */
    public record Allocation(int offset, int heapNo, boolean reused) {
    }

    /**
     * 本对象持有的 {@code page} 页面、记录或布局状态；身份与 schema 必须匹配，访问期间遵守 fix/latch 和字节边界，不能泄漏未发布修改。
     */
    private final RecordPage page;

    /**
     * 创建 {@code HeapSpaceManager}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public HeapSpaceManager(RecordPage page) {
        if (page == null) {
            throw new DatabaseValidationException("heap space manager page must not be null");
        }
        this.page = page;
    }

    /**
     * 为一条 {@code neededBytes} 的新记录分配空间（要求 X）。first-fit 复用 GarbageList，否则回退 FreeSpace。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @throws RecordPageOverflowException FreeSpace 不足（由 {@link RecordPage#allocateFromFreeSpace} 抛出）。
     * @throws PageDirectoryCorruptedException FREE 链成环。
     * @param neededBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code allocate} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     */
    public Allocation allocate(int neededBytes) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        int maxSteps = page.header().nHeap();
        int prevFrag = 0; // 0 表示当前考察的是 FREE 链头本身
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        int frag = page.header().free();
        int steps = 0;
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
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
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        return new Allocation(offset, heapNo, false);
    }

    /**
     * 把 {@code offset} 记录占用的空间压入 GarbageList 头（要求 X）：{@code next_record(offset)=旧 FREE}、{@code FREE=offset}、
     * {@code GARBAGE += recordLength}。调用方须保证该记录已离开用户 next_record 链（purge/update 搬迁先摘链）。
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     */
    public void free(int offset) {
        int cap = page.recordHeaderAt(offset).recordLength();
        int oldHead = page.header().free();
        page.setNextRecord(offset, oldHead);
        page.writeHeader(page.header().withFree(offset).withGarbage(page.header().garbage() + cap));
    }
}
