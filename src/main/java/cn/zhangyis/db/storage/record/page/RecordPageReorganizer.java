package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.ArrayList;
import java.util.List;

/**
 * 页内重组算子（innodb-record-design §10.4）：按逻辑 key 顺序把记录稠密重写，回收 GarbageList 与所有内部碎片、
 * 重建 next_record 链与 PageDirectory（含 n_owned），并 **重排 heapNo**（稠密 2..n+1）。
 *
 * <p>语义：保留 delete-marked 记录（仍在 next_record 链中，按 key 序重写并保留其 deleted 位）；已 purge 的记录已离链，
 * 自然丢弃。重排 heapNo 会使所有旧 {@link RecordRef} 失效（本片不提供失效检测，调用方在重组后须重新定位）。
 * 无状态、线程安全；要求页 X latch。
 *
 * <p>目录重建规则（确定性，保证不变量）：每第 {@code MAX_N_OWNED} 条用户记录作一个中间组末（slot），n_owned=MAX；
 * 尾部不足 MAX 的记录归 supremum 组（n_owned=尾数+1）。故中间组恒=MAX∈[MIN..MAX]，supremum 组∈[1..MAX]。
 */
public final class RecordPageReorganizer {

    /**
     * 重组 page（要求 X）。先按链快照（含 delete-marked），format 重置，再稠密重排 + 重建目录。
     */
    public void reorganize(RecordPage page) {
        if (page == null) {
            throw new DatabaseValidationException("reorganize page must not be null");
        }
        IndexPageHeader h0 = page.header();
        long indexId = h0.indexId();
        int level = h0.level();

        // 1. 按链（key）序快照记录字节（含 delete-marked，保留 flags）。
        List<byte[]> records = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            records.add(page.readRecordBytes(off));
        }

        // 2. 重置页（infimum/supremum、2 槽、heapTop=98、nHeap=2、nRecs=0、FREE=0、GARBAGE=0）。
        page.format(indexId, level);

        // 3. 稠密重排 + 串链。每条显式 setNOwned(0)（清掉快照里旧布局的 n_owned）。
        int prev = page.infimumOffset();
        List<Integer> offsets = new ArrayList<>(records.size());
        for (byte[] bytes : records) {
            int heapNo = page.nextHeapNo();
            int off2 = page.allocateFromFreeSpace(bytes.length);
            page.writeRecordBytes(off2, bytes);
            page.setHeapNo(off2, heapNo);
            page.setNOwned(off2, 0);
            page.setNextRecord(prev, off2);
            prev = off2;
            offsets.add(off2);
        }
        page.setNextRecord(prev, page.supremumOffset());

        // 4. 重建目录 + n_owned。
        RecordPageDirectory dir = page.directory();
        int count = offsets.size();
        int lastGroupEnd = 0; // 已成中间组的记录数
        for (int i = 0; i < count; i++) {
            if ((i + 1) % RecordPageInserter.MAX_N_OWNED == 0) {
                dir.insertSlot(dir.slotCount() - 1, offsets.get(i)); // 插在 supremum 槽前
                page.setNOwned(offsets.get(i), RecordPageInserter.MAX_N_OWNED);
                lastGroupEnd = i + 1;
            }
        }
        int tail = count - lastGroupEnd; // 末中间组之后归 supremum 组的记录数
        page.setNOwned(page.supremumOffset(), tail + 1);

        // 5. nRecs = 用户记录数（含 delete-marked）。
        page.writeHeader(page.header().withNRecs(count));
    }
}
