package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.record.format.HiddenColumnLayout;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.RecordHeader;
import cn.zhangyis.db.storage.record.format.RecordType;

import java.util.ArrayList;
import java.util.List;

/**
 * INDEX 页内记录区访问器（innodb-record-design §7、§14.1 的结构子集，R3）。绑定一个有效 {@link PageGuard}
 * 在页体 {@code [38, pageSize-8)} 上工作，提供空页初始化、INDEX page header 读写、系统记录读、heap 空间分配、
 * next_record 链遍历等**结构原语**。
 *
 * <p>不含 key 语义：key 比较、key 有序插入定位、PageDirectory 二分查找、{@code RecordCursor} 均归 R4。
 * 本类只负责物理结构，调用方（R4 insert/B+Tree）负责把分配出的 heap 空间按 key 顺序 wire 进 next_record 链与目录。
 *
 * <p>并发：不拥有 page latch / buffer fix 生命周期（§13.1）；写原语要求 {@link PageGuard} 为 EXCLUSIVE
 * （由 PageGuard 自身校验并抛 {@link DatabaseValidationException}）。本类不缓存任何跨 PageGuard 生命周期的状态。
 * 信封（FilePageHeader/PageType.INDEX）由调用方在建页时盖，本类不碰 {@code [0,38)} 与 {@code [pageSize-8,pageSize)}。
 */
public final class RecordPage {

    /** 受控页句柄；持有期间由调用方保证有效（close 后再用，PageGuard 自身抛异常）。 */
    private final PageGuard guard;
    /** 页大小，用于定位页尾的 PageDirectory 与 trailer。PageGuard 不暴露页大小，故由调用方传入。 */
    private final PageSize pageSize;

    public RecordPage(PageGuard guard, PageSize pageSize) {
        if (guard == null || pageSize == null) {
            throw new DatabaseValidationException("record page guard/pageSize must not be null");
        }
        this.pageSize = pageSize;
        this.guard = guard;
    }

    // ---------------------------------------------------------------------
    // 空页初始化
    // ---------------------------------------------------------------------

    /**
     * 初始化为空 INDEX 页记录区（要求 X）：写 INDEX page header、infimum/supremum 系统记录、初始 2 槽 PageDirectory。
     *
     * <p>数据流：header（nDirSlots=2、heapTop=98、nHeap=2、nRecs=0、direction=NO_DIRECTION）→ infimum（next 指向 supremum）
     * → supremum（next=0 表链尾）→ 目录 slot[0]=infimum、slot[1]=supremum。不盖 FilePageHeader（调用方负责 PageType.INDEX）。
     *
     * @param indexId 本页所属索引 id。
     * @param level   B+Tree 层（0=leaf）。
     */
    public void format(long indexId, int level) {
        new IndexPageHeader(2, IndexPageLayout.USER_RECORDS_START, 2, 0, 0, 0,
                IndexPageDirection.NO_DIRECTION, 0, 0, level, indexId).writeTo(guard);
        writeSystemRecord(IndexPageLayout.INFIMUM_OFFSET, RecordType.INFIMUM, 0,
                IndexPageLayout.SUPREMUM_OFFSET, IndexPageLayout.INFIMUM_LABEL);
        writeSystemRecord(IndexPageLayout.SUPREMUM_OFFSET, RecordType.SUPREMUM, 1,
                0, IndexPageLayout.SUPREMUM_LABEL);
        PageU16.put(guard, slotAddr(0), IndexPageLayout.INFIMUM_OFFSET);
        PageU16.put(guard, slotAddr(1), IndexPageLayout.SUPREMUM_OFFSET);
    }

    /** 写一条系统记录（8 字节定长头 + 8 字节标签）。nOwned=1：infimum/supremum 各自成组、各占一槽。 */
    private void writeSystemRecord(int offset, RecordType type, int heapNo, int next, byte[] label) {
        byte[] header = new byte[IndexPageLayout.REC_HEADER_BYTES];
        new RecordHeader(false, false, type, heapNo, 1, next, IndexPageLayout.SYS_REC_BYTES).writeTo(header, 0);
        guard.writeBytes(offset, header);
        guard.writeBytes(offset + IndexPageLayout.REC_HEADER_BYTES, label);
    }

    // ---------------------------------------------------------------------
    // 读 + 几何
    // ---------------------------------------------------------------------

    /** 读 INDEX page header（S/X 均可）。 */
    public IndexPageHeader header() {
        return IndexPageHeader.readFrom(guard);
    }

    /** infimum 系统记录页内偏移（常量）。 */
    public int infimumOffset() {
        return IndexPageLayout.INFIMUM_OFFSET;
    }

    /** supremum 系统记录页内偏移（常量）。 */
    public int supremumOffset() {
        return IndexPageLayout.SUPREMUM_OFFSET;
    }

    /** 读某偏移处的记录头（前向布局，头在记录起始）。 */
    public RecordHeader recordHeaderAt(int offset) {
        return RecordHeader.readFrom(guard.readBytes(offset, IndexPageLayout.REC_HEADER_BYTES), 0);
    }

    /** 读系统记录的 8 字节标签（紧跟记录头），用于校验/诊断。 */
    public byte[] systemLabelAt(int offset) {
        return guard.readBytes(offset + IndexPageLayout.REC_HEADER_BYTES, 8);
    }

    /** 写整条记录字节到 offset（要求 X）。供 R4 insert 把编码好的记录落页。 */
    public void writeRecordBytes(int offset, byte[] bytes) {
        guard.writeBytes(offset, bytes);
    }

    /** 读整条记录字节（先读头取 recordLength，再读该长度）。供 RecordCursor 一次性拷出后做字段解析。 */
    public byte[] readRecordBytes(int offset) {
        RecordHeader header = recordHeaderAt(offset);
        return guard.readBytes(offset, header.recordLength());
    }

    /** 改写某记录的 heapNo（要求 X）。编码记录默认 heapNo=0，insert 落页后须改为真实页内 heap 序号。 */
    public void setHeapNo(int offset, int heapNo) {
        PageU16.put(guard, offset + IndexPageLayout.REC_HEAPNO_FIELD_OFFSET, heapNo);
    }

    /**
     * 置/清某记录的 delete-mark（要求 X）。**只读-改-写 flags 字节的 bit0**，保留 bit1（min-rec）与 bit2-3（recordType）——
     * 不能用 {@link #writeRecordBytes}（会覆盖整头）。delete-mark 保留记录在 next_record 链中（供历史版本/后续 purge）。
     */
    public void setDeleted(int offset, boolean deleted) {
        int flags = guard.readBytes(offset + IndexPageLayout.REC_FLAGS_FIELD_OFFSET, 1)[0] & 0xFF;
        flags = deleted ? (flags | 0x01) : (flags & ~0x01);
        guard.writeBytes(offset + IndexPageLayout.REC_FLAGS_FIELD_OFFSET, new byte[]{(byte) flags});
    }

    /**
     * 改写聚簇记录的隐藏列 DB_TRX_ID/DB_ROLL_PTR（要求 X，T1.3f）。**外科修补尾部 15B**——隐藏区贴在记录字节末尾
     * （{@code offset + recordLength - HIDDEN_BYTES}），仅覆写这 15 字节，不动用户列、记录头与 next/heap/flags 字段。
     * 供 delete-mark/取消标记改版本指针而保持列值与记录长度不变（与 {@link #setDeleted} 配对，两步纯写、无内容 undo
     * 风险）。调用方须保证该记录为聚簇记录（带隐藏区）；非聚簇记录无隐藏区，调用即破坏记录字节。
     */
    public void writeHiddenColumns(int offset, HiddenColumns hidden) {
        if (hidden == null) {
            throw new DatabaseValidationException("hidden columns must not be null");
        }
        int recordLength = recordHeaderAt(offset).recordLength();
        byte[] buf = new byte[HiddenColumnLayout.HIDDEN_BYTES];
        HiddenColumnLayout.encode(buf, 0, hidden.dbTrxId(), hidden.dbRollPtr());
        guard.writeBytes(offset + recordLength - HiddenColumnLayout.HIDDEN_BYTES, buf);
    }

    /**
     * 改写某记录的 n_owned（组成员数，要求 X）。仅 group 末记录（slot 指向者）非 0；insert 后 owner.n_owned++，
     * 组 split 时由 inserter 重新分配两组的 n_owned。值域 u8（0..255），越界抛 {@link DatabaseValidationException}
     * 防止把高位写进相邻的 next_record 字段。
     */
    public void setNOwned(int offset, int nOwned) {
        if (nOwned < 0 || nOwned > 0xFF) {
            throw new DatabaseValidationException("nOwned out of u8 range: " + nOwned);
        }
        guard.writeBytes(offset + IndexPageLayout.REC_NOWNED_FIELD_OFFSET, new byte[]{(byte) nOwned});
    }

    /** 返回绑定同 guard/pageSize 的 PageDirectory 视图，作为 search/inserter 取槽的桥（RecordPage 持有 guard，不外泄）。 */
    public RecordPageDirectory directory() {
        return new RecordPageDirectory(guard, pageSize);
    }

    /**
     * 整体回写 INDEX page header（要求 X）。insert 在单页 X latch 内做 header 计数器的 read-modify-write：
     * 先 {@link #header()} 读出，再以更新后的 {@link IndexPageHeader} 调本方法落盘。RecordPage 不暴露 guard，
     * 故 header 写回须经此桥；本类只忠实落字段，哪些字段随 insert 变（nRecs/lastInsert/direction）由 inserter 决定。
     */
    public void writeHeader(IndexPageHeader header) {
        if (header == null) {
            throw new DatabaseValidationException("index page header must not be null");
        }
        header.writeTo(guard);
    }

    /** 页尾（trailer 前）地址，PageDirectory slot[0] 紧贴此处向低地址增长。 */
    private int dirEnd() {
        return pageSize.bytes() - IndexPageLayout.FIL_PAGE_TRAILER_BYTES;
    }

    /** 第 i 槽在页内的地址（slot[0] 最高、向低地址递增 index）。 */
    private int slotAddr(int i) {
        return dirEnd() - (i + 1) * IndexPageLayout.DIR_SLOT_BYTES;
    }

    /** 目录起始地址（最低槽地址）= dirEnd - nDirSlots*2；free space 上界。 */
    public int dirStart() {
        return dirEnd() - PageU16.get(guard, IndexPageHeaderLayout.N_DIR_SLOTS) * IndexPageLayout.DIR_SLOT_BYTES;
    }

    /** 连续可用空间字节数 = dirStart - heapTop。 */
    public int freeSpace() {
        return dirStart() - PageU16.get(guard, IndexPageHeaderLayout.HEAP_TOP);
    }

    /**
     * 捕获当前 INDEX 页最终结构 after-image。数据流：读取权威 header 得到 level/record count/heapTop，
     * 再复制固定 header、已使用 heap 和目录三段；不复制 free-space，不修改页面，也不触发 redo listener。
     * 调用方必须仍持有当前页的 S/X latch；B+Tree 结构 redo 在写路径中使用 X latch 下的稳定快照。
     */
    public RecordPageStructureSnapshot structureSnapshot() {
        IndexPageHeader current = header();
        int directoryStart = dirStart();
        int trailerStart = pageSize.bytes() - IndexPageLayout.FIL_PAGE_TRAILER_BYTES;
        return new RecordPageStructureSnapshot(
                current.level(),
                current.nRecs(),
                IndexPageLayout.PAGE_HEADER_START,
                guard.readBytes(IndexPageLayout.PAGE_HEADER_START,
                        IndexPageLayout.PAGE_HEADER_END - IndexPageLayout.PAGE_HEADER_START),
                IndexPageLayout.INFIMUM_OFFSET,
                guard.readBytes(IndexPageLayout.INFIMUM_OFFSET,
                        current.heapTop() - IndexPageLayout.INFIMUM_OFFSET),
                directoryStart,
                guard.readBytes(directoryStart, trailerStart - directoryStart));
    }

    // ---------------------------------------------------------------------
    // heap 分配 + next_record 链
    // ---------------------------------------------------------------------

    /** 即将分配的新记录 heapNo（= 当前 nHeap）。 */
    public int nextHeapNo() {
        return PageU16.get(guard, IndexPageHeaderLayout.N_HEAP);
    }

    /**
     * 从 free space 顶部切出 {@code bytes} 字节给一条新记录（要求 X），返回新记录页内起始偏移（= 旧 heapTop）。
     * 推进 heapTop 与 nHeap；新记录 heapNo = 旧 nHeap。
     *
     * <p>本原语**不** wire next_record、不动 PageDirectory、不增 nRecs——按 key 顺序串接与目录维护归 R4 insert，
     * 保持「物理空间分配」与「逻辑有序插入」职责分离。空间不足抛 {@link RecordPageOverflowException}（B+Tree 据此 split）。
     *
     * @param bytes 记录字节数，必须为正。
     */
    public int allocateFromFreeSpace(int bytes) {
        if (bytes <= 0) {
            throw new DatabaseValidationException("record bytes must be positive: " + bytes);
        }
        if (bytes > freeSpace()) {
            throw new RecordPageOverflowException("no room for record of " + bytes
                    + " bytes; free=" + freeSpace());
        }
        int offset = PageU16.get(guard, IndexPageHeaderLayout.HEAP_TOP);
        PageU16.put(guard, IndexPageHeaderLayout.HEAP_TOP, offset + bytes);
        PageU16.put(guard, IndexPageHeaderLayout.N_HEAP, PageU16.get(guard, IndexPageHeaderLayout.N_HEAP) + 1);
        return offset;
    }

    /** 读某记录的 next_record（绝对页内偏移；指向下一记录头，0 表链尾哨兵）。 */
    public int nextRecord(int offset) {
        return PageU16.get(guard, offset + IndexPageLayout.REC_NEXT_FIELD_OFFSET);
    }

    /** 改写某记录的 next_record（要求 X）。target 为下一记录页内偏移；链尾指向 supremum。 */
    public void setNextRecord(int offset, int target) {
        PageU16.put(guard, offset + IndexPageLayout.REC_NEXT_FIELD_OFFSET, target);
    }

    /**
     * 从 infimum 沿 next_record 找到 {@code next == offset} 的记录偏移（即 offset 的链上前驱）。offset 为首条用户记录时返回
     * {@link #infimumOffset()}。供 purge / update 搬迁定位并重写前驱链。
     *
     * <p>守卫：遍历到 supremum 仍未命中判 offset 不在链中、步数超过 nHeap 判成环，均抛 {@link PageDirectoryCorruptedException}
     * （不静默修复）。要求 offset 为链中的用户记录；不接受 infimum/supremum 作为 offset。
     */
    public int findPredecessor(int offset) {
        int maxSteps = PageU16.get(guard, IndexPageHeaderLayout.N_HEAP);
        int cur = infimumOffset();
        int supremum = supremumOffset();
        int steps = 0;
        while (true) {
            int next = nextRecord(cur);
            if (next == offset) {
                return cur;
            }
            if (next == supremum) {
                throw new PageDirectoryCorruptedException("predecessor not found; offset not in chain: " + offset);
            }
            if (steps++ > maxSteps) {
                throw new PageDirectoryCorruptedException("next_record chain cycle while finding predecessor of " + offset);
            }
            cur = next;
        }
    }

    /**
     * 沿 next_record 从 infimum 走到 supremum，返回中间用户记录偏移（不含 infimum/supremum），即页内逻辑（key）顺序。
     *
     * <p>防御：每个用户记录偏移必须落在页体 {@code [USER_RECORDS_START, dirStart)} 内，否则判为链损坏；
     * 步数超过 nHeap 判为成环。两者均抛 {@link PageDirectoryCorruptedException}，不静默修复。
     */
    public List<Integer> recordOffsetsInOrder() {
        List<Integer> result = new ArrayList<>();
        int supremum = supremumOffset();
        int maxSteps = PageU16.get(guard, IndexPageHeaderLayout.N_HEAP);
        int lowerBound = IndexPageLayout.USER_RECORDS_START;
        int upperBound = dirStart();
        int cur = nextRecord(infimumOffset());
        int steps = 0;
        while (cur != supremum) {
            if (steps++ > maxSteps) {
                throw new PageDirectoryCorruptedException("next_record chain cycle detected (steps>" + maxSteps + ")");
            }
            if (cur < lowerBound || cur >= upperBound) {
                throw new PageDirectoryCorruptedException("next_record offset out of page body: " + cur);
            }
            result.add(cur);
            cur = nextRecord(cur);
        }
        return result;
    }
}
