package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SegmentId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * undo 页视图（PageGuard 之上）。所有状态都在页字节中，写入经 MTR 持有的 X latch guard 产生
 * PAGE_BYTES redo，commit 时由 MTR 盖 pageLSN。
 *
 * <p>本类只表达页内物理格式：page header 每页都有，log header 仅 first 页可读写；chain 页清零 log
 * header 预留区，避免旧字节被误解释。FIL prev/next 负责页链链接，first 页 log header 另存链端点和整链计数。
 */
public final class UndoPage {

    /**
     * MTR-owned 页访问 guard。UndoPage 不拥有释放职责，生命周期由 MiniTransaction commit/rollback 统一释放。
     */
    private final PageGuard guard;

    /**
     * 页大小用于追加时计算 FIL trailer 前的写入上界，防止 undo record 覆盖校验/LSN trailer 区域。
     */
    private final PageSize pageSize;

    UndoPage(PageGuard guard, PageSize pageSize) {
        if (guard == null || pageSize == null) {
            throw new DatabaseValidationException("undo page guard/pageSize must not be null");
        }
        this.guard = guard;
        this.pageSize = pageSize;
    }

    /**
     * 格式化 undo log first 页。数据流：写每页 page header（含 segment 归属和 first 标志）→ 写 log header
     * 的事务、类型、状态与链端点 → 初始化整链计数。调用方必须已通过 {@link UndoPageAccess} 写好 UNDO 页信封。
     *
     * @param kind   undo log 类型；本片仅写 INSERT。
     * @param txnId  所属事务 id，仅作为物理 header 测试字段，尚不接事务系统。
     * @param handle segment 定位；first 页必须位于 handle 所属表空间。
     */
    void formatFirstPage(UndoLogKind kind, TransactionId txnId, UndoSegmentHandle handle) {
        if (kind == null || txnId == null || handle == null) {
            throw new DatabaseValidationException("undo first page format args must not be null");
        }
        requireHandleSpace(handle);
        writePageHeader(handle, true);
        guard.writeLong(UndoPageLayout.TRANSACTION_ID, txnId.value());
        setU8(UndoPageLayout.UNDO_KIND, kind.ordinal());
        setU8(UndoPageLayout.STATE, UndoPageLayout.STATE_ACTIVE);
        long self = guard.pageId().pageNo().value();
        setU32(UndoPageLayout.FIRST_PAGE_NO, self);
        setU32(UndoPageLayout.LAST_PAGE_NO, self);
        guard.writeLong(UndoPageLayout.LOG_RECORD_COUNT, 0L);
        guard.writeLong(UndoPageLayout.LOG_LAST_UNDO_NO, 0L);
    }

    /**
     * 格式化 undo chain 页。chain 页仍预留 log header 宽度并清零 {@code [63,97)}，这样 record area 起点
     * 与 first 页完全一致；first-only 访问器会通过 {@link #isFirstPage()} 拒绝解析这些清零字节。
     *
     * @param handle segment 定位；页必须与 handle 所属表空间一致。
     */
    void formatChainPage(UndoSegmentHandle handle) {
        if (handle == null) {
            throw new DatabaseValidationException("undo chain page format handle must not be null");
        }
        requireHandleSpace(handle);
        writePageHeader(handle, false);
        guard.writeBytes(UndoPageLayout.TRANSACTION_ID,
                new byte[UndoPageLayout.LOG_HEADER_END - UndoPageLayout.TRANSACTION_ID]);
    }

    /**
     * 追加一条页内 undo record。数据流：校验 undoNo 合法 → 在任何写入前做页内容量判断 →
     * 写 record 槽 {@code [len u16][payload]} → 推进本页 header 的 free/count/pageLastUndoNo。
     *
     * <p>本方法只维护 page header，不更新 first 页 log header；整链计数由 {@link UndoLogSegment} 在 append
     * 成功后统一更新，从而能覆盖单页与跨页两种路径。
     *
     * @param payload 已编码 undo record payload。
     * @param undoNo  事务内 undo 序号，不能是 NONE。
     * @return record 槽起始 offset，供 RollPointer 编码。
     */
    int appendRecord(byte[] payload, UndoNo undoNo) {
        if (payload == null || undoNo == null) {
            throw new DatabaseValidationException("undo append payload/undoNo must not be null");
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo append undoNo must be > 0 (not NONE)");
        }
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        int need = 2 + payload.length;
        int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
        if (free + need > limit) {
            throw new UndoPageOverflowException("undo record (" + need + "B) does not fit at free="
                    + free + " limit=" + limit);
        }
        setU16(free, payload.length);
        guard.writeBytes(free + 2, payload);
        setU16(UndoPageLayout.FREE_OFFSET, free + need);
        setU16(UndoPageLayout.RECORD_COUNT, getU16(UndoPageLayout.RECORD_COUNT) + 1);
        guard.writeLong(UndoPageLayout.PAGE_LAST_UNDO_NO, undoNo.value());
        return free;
    }

    /**
     * 读取指定 offset 的 undo record payload。offset 必须落在 {@link UndoPageLayout#RECORD_AREA_START}
     * 与当前 freeOffset 之间，且 len 前缀不能越过已写区域；越界表示 roll pointer 或页内容损坏。
     */
    byte[] recordAt(int offset) {
        int free = getU16(UndoPageLayout.FREE_OFFSET);
        if (offset < UndoPageLayout.RECORD_AREA_START || offset + 2 > free) {
            throw new UndoLogFormatException("undo record offset out of area: " + offset + " free=" + free);
        }
        int len = getU16(offset);
        if (offset + 2 + len > free) {
            throw new UndoLogFormatException("undo record length out of area: off=" + offset + " len=" + len);
        }
        return guard.readBytes(offset + 2, len);
    }

    /**
     * 写 FIL NEXT 链接并保留现有 PREV。生长时先把当前尾页 next 指向新页，再在新页写 prev，避免覆盖 pageLSN
     * 或 pageType 等信封字段。
     *
     * @param next 新后继页号。
     */
    void linkNextTo(PageNo next) {
        if (next == null) {
            throw new DatabaseValidationException("undo link next must not be null");
        }
        long prev = PageEnvelope.readHeader(guard).prevPageNo();
        PageEnvelope.writeSiblingLinks(guard, prev, next.value());
    }

    /**
     * 写 FIL PREV 链接并保留现有 NEXT。用于新 chain 页接入前驱页，NEXT 初始保持 FIL_NULL。
     *
     * @param prev 新前驱页号。
     */
    void linkPrevTo(PageNo prev) {
        if (prev == null) {
            throw new DatabaseValidationException("undo link prev must not be null");
        }
        long next = PageEnvelope.readHeader(guard).nextPageNo();
        PageEnvelope.writeSiblingLinks(guard, prev.value(), next);
    }

    /**
     * 推进 first 页 log header 中的链尾页号。只能在 first 页调用；chain 页调用说明调用方把页角色弄错，
     * 属物理格式使用错误，按格式异常处理。
     */
    void setLastPageNo(PageNo last) {
        requireFirstPage();
        if (last == null) {
            throw new DatabaseValidationException("undo last page no must not be null");
        }
        setU32(UndoPageLayout.LAST_PAGE_NO, last.value());
    }

    /**
     * 更新 first 页整链 record 总数。该值是 reopen 和遍历诊断的权威整链计数，不等同于任一页的
     * {@link #recordCount()}。
     */
    void setLogRecordCount(long count) {
        requireFirstPage();
        if (count < 0) {
            throw new DatabaseValidationException("undo log record count must be non-negative: " + count);
        }
        guard.writeLong(UndoPageLayout.LOG_RECORD_COUNT, count);
    }

    /**
     * 更新 first 页整链最后 undoNo。成功 append 后由 UndoLogSegment 调用；失败路径不应提前写此字段。
     */
    void setLogLastUndoNo(long undoNo) {
        requireFirstPage();
        if (undoNo < 0) {
            throw new DatabaseValidationException("undo log last undo no must be non-negative: " + undoNo);
        }
        guard.writeLong(UndoPageLayout.LOG_LAST_UNDO_NO, undoNo);
    }

    /** 当前页 id。 */
    PageId pageId() {
        return guard.pageId();
    }

    /** 本页所属 UNDO segment id。 */
    SegmentId segmentId() {
        return SegmentId.of(guard.readLong(UndoPageLayout.SEGMENT_ID));
    }

    /** 本页所属 segment inode 槽。 */
    int inodeSlot() {
        return guard.readInt(UndoPageLayout.INODE_SLOT);
    }

    /** 是否为 undo log first 页；只有 first 页的 log header 有语义。 */
    boolean isFirstPage() {
        return (getU8(UndoPageLayout.PAGE_FLAGS) & UndoPageLayout.FLAG_FIRST_PAGE) != 0;
    }

    /** 原始 page flags，供格式诊断测试使用。 */
    int pageFlags() {
        return getU8(UndoPageLayout.PAGE_FLAGS);
    }

    /** 下一条页内追加位置。 */
    int freeOffset() {
        return getU16(UndoPageLayout.FREE_OFFSET);
    }

    /** 本页 record 数。 */
    int recordCount() {
        return getU16(UndoPageLayout.RECORD_COUNT);
    }

    /** 本页最近 undoNo；整链最近 undoNo 见 {@link #logLastUndoNo()}。 */
    UndoNo pageLastUndoNo() {
        return UndoNo.of(guard.readLong(UndoPageLayout.PAGE_LAST_UNDO_NO));
    }

    /** FIL NEXT 页号；无后继时为 {@link FilePageHeader#FIL_NULL}。 */
    long nextPageNo() {
        return PageEnvelope.readHeader(guard).nextPageNo();
    }

    /** FIL PREV 页号；无前驱时为 {@link FilePageHeader#FIL_NULL}。 */
    long prevPageNo() {
        return PageEnvelope.readHeader(guard).prevPageNo();
    }

    /** first 页 log header 中的事务 id。 */
    TransactionId transactionId() {
        requireFirstPage();
        return TransactionId.of(guard.readLong(UndoPageLayout.TRANSACTION_ID));
    }

    /** first 页 log header 中的 undo log 类型。 */
    UndoLogKind undoKind() {
        requireFirstPage();
        int idx = getU8(UndoPageLayout.UNDO_KIND);
        UndoLogKind[] all = UndoLogKind.values();
        if (idx < 0 || idx >= all.length) {
            throw new UndoLogFormatException("undo kind ordinal out of range: " + idx);
        }
        return all[idx];
    }

    /** first 页 log header 中的状态占位值。 */
    int state() {
        requireFirstPage();
        return getU8(UndoPageLayout.STATE);
    }

    /** first 页 log header 中的链首页号。 */
    long firstPageNo() {
        requireFirstPage();
        return getU32(UndoPageLayout.FIRST_PAGE_NO);
    }

    /** first 页 log header 中的当前链尾页号。 */
    long lastPageNo() {
        requireFirstPage();
        return getU32(UndoPageLayout.LAST_PAGE_NO);
    }

    /** first 页 log header 中的整链 record 总数。 */
    long logRecordCount() {
        requireFirstPage();
        return guard.readLong(UndoPageLayout.LOG_RECORD_COUNT);
    }

    /** first 页 log header 中的整链最近 undoNo。 */
    UndoNo logLastUndoNo() {
        requireFirstPage();
        return UndoNo.of(guard.readLong(UndoPageLayout.LOG_LAST_UNDO_NO));
    }

    private void writePageHeader(UndoSegmentHandle handle, boolean first) {
        setU16(UndoPageLayout.FREE_OFFSET, UndoPageLayout.RECORD_AREA_START);
        setU16(UndoPageLayout.RECORD_COUNT, 0);
        guard.writeLong(UndoPageLayout.PAGE_LAST_UNDO_NO, 0L);
        guard.writeLong(UndoPageLayout.SEGMENT_ID, handle.segmentId().value());
        setU32(UndoPageLayout.INODE_SLOT, handle.inodeSlot());
        setU8(UndoPageLayout.PAGE_FLAGS, first ? UndoPageLayout.FLAG_FIRST_PAGE : 0);
    }

    private void requireHandleSpace(UndoSegmentHandle handle) {
        if (!handle.spaceId().equals(guard.pageId().spaceId())) {
            throw new DatabaseValidationException("undo page " + guard.pageId()
                    + " space mismatch with handle " + handle.spaceId());
        }
    }

    private void requireFirstPage() {
        if (!isFirstPage()) {
            throw new UndoLogFormatException("undo page is not the log first page: " + guard.pageId());
        }
    }

    private int getU8(int off) {
        return guard.readBytes(off, 1)[0] & 0xFF;
    }

    private void setU8(int off, int v) {
        guard.writeBytes(off, new byte[]{(byte) v});
    }

    private int getU16(int off) {
        byte[] b = guard.readBytes(off, 2);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    private void setU16(int off, int v) {
        if (v < 0 || v > 0xFFFF) {
            throw new DatabaseValidationException("u16 out of range: " + v);
        }
        guard.writeBytes(off, new byte[]{(byte) (v >>> 8), (byte) v});
    }

    private long getU32(int off) {
        return guard.readInt(off) & 0xFFFFFFFFL;
    }

    private void setU32(int off, long v) {
        if (v < 0 || v > FilePageHeader.FIL_NULL) {
            throw new DatabaseValidationException("u32 out of range: " + v);
        }
        guard.writeInt(off, (int) v);
    }
}
