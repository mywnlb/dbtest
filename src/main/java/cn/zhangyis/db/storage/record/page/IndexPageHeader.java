package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.PageGuard;

/**
 * INDEX page header 值对象（innodb-record-design §7，不可变）。承载页内记录区的权威几何与统计：
 * 槽数、heap 顶、heap/用户记录计数、GarbageList 头、插入方向等。读写均经 {@link PageGuard}（写要求 X）。
 *
 * @param nDirSlots   PageDirectory 槽数；空页 = 2（infimum/supremum 各一槽），不得 &lt; 2。
 * @param heapTop     首个空闲字节偏移；free space = dirStart - heapTop。
 * @param nHeap       heap 记录总数（含 infimum/supremum）；新记录 heapNo = 分配前的本值，不得 &lt; 2。
 * @param free        GarbageList 头记录偏移（0=空）；本片只读写字段，复用算法归 R4 purge。
 * @param garbage     删除记录占用字节总数；本片只读写字段。
 * @param lastInsert  上次插入记录偏移（0=无）。
 * @param direction   插入方向。
 * @param nDirection  同方向连续插入计数。
 * @param nRecs       用户记录数（不含 infimum/supremum，含 delete-marked）。
 * @param level       B+Tree 层（0=leaf）。
 * @param indexId     索引 id。
 */
public record IndexPageHeader(int nDirSlots, int heapTop, int nHeap, int free, int garbage,
                              int lastInsert, IndexPageDirection direction, int nDirection,
                              int nRecs, int level, long indexId) {

    public IndexPageHeader {
        requireU16("nDirSlots", nDirSlots);
        requireU16("heapTop", heapTop);
        requireU16("nHeap", nHeap);
        requireU16("free", free);
        requireU16("garbage", garbage);
        requireU16("lastInsert", lastInsert);
        requireU16("nDirection", nDirection);
        requireU16("nRecs", nRecs);
        requireU16("level", level);
        if (direction == null) {
            throw new DatabaseValidationException("page direction must not be null");
        }
        // 空页至少 infimum/supremum 两槽两 heap 记录；小于 2 必为损坏或误用。
        if (nDirSlots < 2) {
            throw new DatabaseValidationException("nDirSlots must be >= 2: " + nDirSlots);
        }
        if (nHeap < 2) {
            throw new DatabaseValidationException("nHeap must be >= 2: " + nHeap);
        }
        if (indexId < 0) {
            throw new DatabaseValidationException("indexId must be non-negative: " + indexId);
        }
    }

    /** 写全部字段到页 header 区（要求 X）。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     */
    public void writeTo(PageGuard guard) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        PageU16.put(guard, IndexPageHeaderLayout.N_DIR_SLOTS, nDirSlots);
        PageU16.put(guard, IndexPageHeaderLayout.HEAP_TOP, heapTop);
        PageU16.put(guard, IndexPageHeaderLayout.N_HEAP, nHeap);
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        PageU16.put(guard, IndexPageHeaderLayout.FREE, free);
        PageU16.put(guard, IndexPageHeaderLayout.GARBAGE, garbage);
        PageU16.put(guard, IndexPageHeaderLayout.LAST_INSERT, lastInsert);
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        PageU16.put(guard, IndexPageHeaderLayout.DIRECTION, direction.code());
        PageU16.put(guard, IndexPageHeaderLayout.N_DIRECTION, nDirection);
        PageU16.put(guard, IndexPageHeaderLayout.N_RECS, nRecs);
        PageU16.put(guard, IndexPageHeaderLayout.LEVEL, level);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        guard.writeLong(IndexPageHeaderLayout.INDEX_ID, indexId);
    }

    /** 从页 header 区读全部字段（S/X 均可）。
     *
     * @param guard 调用方持有的 {@code PageGuard} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @return {@code readFrom} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public static IndexPageHeader readFrom(PageGuard guard) {
        return new IndexPageHeader(
                PageU16.get(guard, IndexPageHeaderLayout.N_DIR_SLOTS),
                PageU16.get(guard, IndexPageHeaderLayout.HEAP_TOP),
                PageU16.get(guard, IndexPageHeaderLayout.N_HEAP),
                PageU16.get(guard, IndexPageHeaderLayout.FREE),
                PageU16.get(guard, IndexPageHeaderLayout.GARBAGE),
                PageU16.get(guard, IndexPageHeaderLayout.LAST_INSERT),
                IndexPageDirection.fromCode(PageU16.get(guard, IndexPageHeaderLayout.DIRECTION)),
                PageU16.get(guard, IndexPageHeaderLayout.N_DIRECTION),
                PageU16.get(guard, IndexPageHeaderLayout.N_RECS),
                PageU16.get(guard, IndexPageHeaderLayout.LEVEL),
                guard.readLong(IndexPageHeaderLayout.INDEX_ID));
    }

    /** 返回仅 {@code free}（GarbageList 头）不同的副本（其余字段不变）。供 HeapSpaceManager 做 header RMW。
     * @param newFree 写入页内记录链或空闲链的偏移 {@code newFree}；必须是当前页面中的合法记录边界，哨兵值只按页格式约定解释
     * @return {@code withFree} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public IndexPageHeader withFree(int newFree) {
        return new IndexPageHeader(nDirSlots, heapTop, nHeap, newFree, garbage, lastInsert, direction,
                nDirection, nRecs, level, indexId);
    }

    /** 返回仅 {@code garbage}（已跟踪垃圾字节数）不同的副本。
     * @param newGarbage 待写入页头或目录的记录计数 {@code newGarbage}；必须非负并满足当前页容量及 page-directory 分组上界
     * @return {@code withGarbage} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public IndexPageHeader withGarbage(int newGarbage) {
        return new IndexPageHeader(nDirSlots, heapTop, nHeap, free, newGarbage, lastInsert, direction,
                nDirection, nRecs, level, indexId);
    }

    /** 返回仅 {@code nRecs}（用户记录数，含 delete-marked）不同的副本。供 purge/reorganize 维护计数。
     * @param newNRecs 待写入页头或目录的记录计数 {@code newNRecs}；必须非负并满足当前页容量及 page-directory 分组上界
     * @return {@code withNRecs} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public IndexPageHeader withNRecs(int newNRecs) {
        return new IndexPageHeader(nDirSlots, heapTop, nHeap, free, garbage, lastInsert, direction,
                nDirection, newNRecs, level, indexId);
    }

    private static void requireU16(String name, int v) {
        if (v < 0 || v > 0xFFFF) {
            throw new DatabaseValidationException(name + " out of u16 range: " + v);
        }
    }
}
