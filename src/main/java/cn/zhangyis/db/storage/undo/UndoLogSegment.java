package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 一条 undo log segment 的 MTR 内物理句柄。它持有 first 页（整链 log header 权威入口）、current 页
 *（append 目标）、segment handle 和本 MTR 已 fix 页缓存，用于 append、RollPointer 读回与正向遍历。
 *
 * <p>本片仍是物理基座：不接事务上下文、不接 rollback，不修改聚簇记录的 DB_ROLL_PTR。并发简化为单 writer：
 * 同一 undo segment 同时只有一个 EXCLUSIVE append 会话，多 writer 锁序与 rollback segment slot 留后续片处理。
 */
public final class UndoLogSegment {

    /**
     * 当前物理短事务。所有页 fix/latch 和 redo 收集归它管理，UndoLogSegment 不自行释放页 guard。
     */
    private final MiniTransaction mtr;

    /**
     * 页大小用于跨页生长时计算空页容量；即使单页路径也保留在对象中，避免后续生长另建状态来源。
     */
    private final PageSize pageSize;

    /**
     * 页分配端口。undo 模块只依赖此端口，不知道 DiskSpaceManager/SegmentRef。
     */
    private final UndoSpaceAllocator allocator;

    /**
     * undo record payload codec，负责将逻辑 UndoRecord 与页内字节相互转换。
     */
    private final UndoRecordCodec codec;

    /**
     * undo 页访问器。resolvePage 对未持有页只通过它打开 UNDO 页，页类型守门集中在该入口。
     */
    private final UndoPageAccess pageAccess;

    /**
     * 当前会话 latch 模式。EXCLUSIVE 才允许 append；SHARED 只允许 read/forEach。
     */
    private final PageLatchMode mode;

    /**
     * first 页是 log header 的权威入口，整链 record 计数、last undoNo 和 last page no 都在这里维护。
     */
    private final UndoPage firstPage;

    /**
     * 本 MTR 已 fix 页缓存。first/current/生长页以 X 方式持有；resolvePage 打开的历史页以 S 方式持有。
     * 已在缓存中的页直接读，避免对同一页二次 getPage 导致 latch 重入或 pageLSN 盖戳边界变复杂。
     */
    private final Map<Long, UndoPage> heldPages = new HashMap<>();

    /**
     * undo segment 定位。跨页生长时只通过 {@link UndoSegmentHandle#withLastPage(PageId)} 推进链尾。
     */
    private UndoSegmentHandle handle;

    /**
     * 当前 append 目标页。单页路径等于 first 页；跨页生长后切换到新的尾页。
     */
    private UndoPage current;

    UndoLogSegment(MiniTransaction mtr, PageSize pageSize, UndoSpaceAllocator allocator, UndoRecordCodec codec,
                   UndoPageAccess pageAccess, UndoSegmentHandle handle, UndoPage firstPage, UndoPage current,
                   PageLatchMode mode) {
        if (mtr == null || pageSize == null || allocator == null || codec == null || pageAccess == null
                || handle == null || firstPage == null || current == null || mode == null) {
            throw new DatabaseValidationException("undo log segment fields must not be null");
        }
        this.mtr = mtr;
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.codec = codec;
        this.pageAccess = pageAccess;
        this.handle = handle;
        this.firstPage = firstPage;
        this.current = current;
        this.mode = mode;
        heldPages.put(firstPage.pageId().pageNo().value(), firstPage);
        heldPages.put(current.pageId().pageNo().value(), current);
    }

    /**
     * 追加一条 undo record。数据流：要求 EXCLUSIVE 会话 → codec 编码 record payload →
     * current 页追加 record 槽 → first 页 log header 的整链计数和最新 undoNo 在成功后推进 →
     * 返回指向槽起点的 insert RollPointer。
     *
     * <p>如果 current 页放不下，会先确认一张全新 undo 页能容纳该 record，再分配并 FIL 链入新页。preflight
     * 必须早于任何页修改，否则 MTR rollbackUncommitted 不做 content undo，会留下半生长脏链。
     */
    public RollPointer append(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("append requires an EXCLUSIVE (writable) undo log segment session");
        }
        if (rec == null) {
            throw new DatabaseValidationException("undo append record must not be null");
        }
        byte[] payload = codec.encode(rec, keyDef, schema);
        int off;
        try {
            off = current.appendRecord(payload, rec.undoNo());
        } catch (UndoPageOverflowException overflow) {
            off = growAndAppend(payload, rec.undoNo(), overflow);
        }
        firstPage.setLogRecordCount(firstPage.logRecordCount() + 1);
        firstPage.setLogLastUndoNo(rec.undoNo().value());
        // RollPointer.insert 标志按记录类型决定（T1.3e）：INSERT_ROW→true、UPDATE_ROW→false。混合段中段头
        // UndoLogKind 不权威，insert 标志与每条记录的 UndoRecordType 一致，供 MVCC/版本链区分 insert vs update undo。
        boolean insert = rec.type() == UndoRecordType.INSERT_ROW;
        return new RollPointer(insert, current.pageId().pageNo(), off);
    }

    /**
     * current 页放不下时生长一页再写。数据流：先计算单条 record 槽是否能放入全新空页；若不能，直接
     * 重新抛出原 overflow，且不分配、不格式化、不链接、不更新 first 页 header。只有 preflight 通过后才
     * 分配新页、格式化 chain 页、写 FIL 双向链、推进 first 页 LAST_PAGE_NO、更新 handle/current，然后在新页
     * 重试 append。
     *
     * @param payload  已编码 payload。
     * @param undoNo   record undoNo。
     * @param overflow current 页原始溢出异常；单条超页时保持该异常语义。
     * @return 新页上的 record 槽 offset。
     */
    private int growAndAppend(byte[] payload, UndoNo undoNo, UndoPageOverflowException overflow) {
        int need = 2 + payload.length;
        int freshCapacity = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES
                - UndoPageLayout.RECORD_AREA_START;
        if (need > freshCapacity) {
            throw overflow;
        }
        // 0.14b：grow 是真实多页消费者。预留必须晚于单条容量 preflight、早于任何分配/格式化/FIL 链接/first header 修改；
        // 否则 ENOSPC 发生在中途时，MTR 无 content undo，无法撤回半生长的 undo 页链。
        try (UndoSpaceReservation ignored = allocator.reserveGrowPages(mtr, handle.spaceId(), 1L)) {
            PageId newId = allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId());
            UndoPage newPage = pageAccess.createChainPage(mtr, newId, handle);
            current.linkNextTo(newId.pageNo());
            newPage.linkPrevTo(current.pageId().pageNo());
            firstPage.setLastPageNo(newId.pageNo());
            handle = handle.withLastPage(newId);
            heldPages.put(newId.pageNo().value(), newPage);
            current = newPage;
            return current.appendRecord(payload, undoNo);
        }
    }

    /**
     * 标记本 undo segment 已提交（R 1.2/R 1.3）：写 first 页 log header {@code STATE=COMMITTED} 与
     * {@code COMMIT_NO}（要求 EXCLUSIVE 会话，redo 保护）。恢复期据此把 ACTIVE 段判为未提交事务回滚，把
     * COMMITTED 段按提交序重建 history 后交给 purge 续作。
     */
    public void markCommitted(TransactionNo commitNo) {
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("markCommitted requires an EXCLUSIVE undo log segment session");
        }
        if (commitNo == null) {
            throw new DatabaseValidationException("markCommitted commitNo must not be null");
        }
        firstPage.setLogState(UndoPageLayout.STATE_COMMITTED);
        firstPage.setCommitNo(commitNo.value()); // R 1.3：与 STATE 同 MTR 写提交序号，供恢复重建 history
    }

    /** first 页 log header 中的 undo log 状态原始值（{@code STATE_ACTIVE}/{@code STATE_COMMITTED}）。 */
    public int state() {
        return firstPage.state();
    }

    /** 是否 ACTIVE（未提交）；恢复期 ACTIVE 段需回滚。 */
    public boolean isActive() {
        return firstPage.state() == UndoPageLayout.STATE_ACTIVE;
    }

    /** 是否 COMMITTED；恢复期 COMMITTED 段不回滚，而是重建 history 交给 purge 后台续作。 */
    public boolean isCommitted() {
        return firstPage.state() == UndoPageLayout.STATE_COMMITTED;
    }

    /** 提交序号（R 1.3）；仅 COMMITTED 段有意义，恢复重建 history 用。 */
    public TransactionNo committedTransactionNo() {
        return TransactionNo.of(firstPage.commitNo());
    }

    /** 本 undo log 所属事务写 id（creator）；恢复重建 history / 计数器复位用。 */
    public TransactionId creatorTransactionId() {
        return firstPage.transactionId();
    }

    /**
     * 按 RollPointer 读回 undo record。先拒绝 NULL 指针，再定位指针页并校验 segmentId/inodeSlot 与本 handle
     * 一致；段不符代表指针指向别的 undo segment 或页内容损坏，不能继续按当前 schema 解码。
     */
    public UndoRecord readRecord(RollPointer rp, IndexKeyDef keyDef, TableSchema schema) {
        if (rp == null) {
            throw new DatabaseValidationException("undo readRecord roll pointer must not be null");
        }
        if (rp.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        UndoPage page = resolvePage(rp.pageNo());
        requireSameSegment(page, "roll pointer page " + rp.pageNo());
        byte[] payload = page.recordAt(rp.offset());
        return codec.decode(payload, 0, keyDef, schema);
    }

    /**
     * 正向遍历整条 undo log 页链。每页按 record area 中的 {@code [len u16][payload]} 槽顺序解码，
     * 然后沿 FIL NEXT 继续。链上任何页类型或 segment 归属异常都会通过 openUndoPage/requireSameSegment 抛出
     * {@link UndoLogFormatException}。
     */
    public void forEachRecord(Consumer<UndoRecord> consumer, IndexKeyDef keyDef, TableSchema schema) {
        if (consumer == null) {
            throw new DatabaseValidationException("undo forEachRecord consumer must not be null");
        }
        forEachRecordWithPointer((rec, rp) -> consumer.accept(rec), keyDef, schema);
    }

    /**
     * 正向遍历整条 undo log 页链，并向 consumer 同时给出**每条 record 自身的 {@link RollPointer} 地址**（pageNo+offset，
     * insert 标志按记录类型）。purge 用它把已提交 undo log 的每条 DELETE_MARK/UPDATE record 与聚簇记录的
     * {@code DB_ROLL_PTR} 严格比对（记录的 DB_ROLL_PTR 即写入时返回的该 undo record 地址，见 {@link #append}）。
     *
     * <p>遍历语义与 {@link #forEachRecord} 一致：每页按 record area {@code [len u16][payload]} 槽顺序解码后沿 FIL NEXT
     * 继续；页类型/段归属异常经 openUndoPage/requireSameSegment 抛 {@link UndoLogFormatException}。
     */
    public void forEachRecordWithPointer(BiConsumer<UndoRecord, RollPointer> consumer,
                                         IndexKeyDef keyDef, TableSchema schema) {
        if (consumer == null) {
            throw new DatabaseValidationException("undo forEachRecordWithPointer consumer must not be null");
        }
        long pageNoVal = handle.firstPageId().pageNo().value();
        while (true) {
            UndoPage page = resolvePage(PageNo.of(pageNoVal));
            requireSameSegment(page, "undo chain page " + pageNoVal);
            int free = page.freeOffset();
            int off = UndoPageLayout.RECORD_AREA_START;
            while (off < free) {
                byte[] payload = page.recordAt(off);
                UndoRecord rec = codec.decode(payload, 0, keyDef, schema);
                // 该 record 的地址 = 写入时返回的 RollPointer：insert 标志按类型（INSERT_ROW→true，其余 false），
                // 与 append/聚簇记录 DB_ROLL_PTR 编码一致，供 purge 严格比对。
                boolean insert = rec.type() == UndoRecordType.INSERT_ROW;
                consumer.accept(rec, new RollPointer(insert, PageNo.of(pageNoVal), off));
                off += 2 + payload.length;
            }
            long next = page.nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                break;
            }
            pageNoVal = next;
        }
    }

    /**
     * 本 undo segment 的定位 handle（spaceId/inodeSlot/segmentId/首尾页）。purge 在 read MTR 内取它（值对象，
     * MTR 关闭后仍有效）构 SegmentRef 经 {@link UndoSpaceAllocator#dropUndoSegment} 物理回收整段。
     */
    public UndoSegmentHandle handle() {
        return handle;
    }

    /** undo log 链首页 id。 */
    public PageId firstPageId() {
        return handle.firstPageId();
    }

    /** undo log 当前链尾页 id。 */
    public PageId lastPageId() {
        return handle.lastPageId();
    }

    /** first 页 log header 中的事务 id。 */
    public TransactionId transactionId() {
        return firstPage.transactionId();
    }

    /** first 页 log header 中的 undo log 类型。 */
    public UndoLogKind undoKind() {
        return firstPage.undoKind();
    }

    /** first 页 log header 中的整链 record 总数。 */
    public long logRecordCount() {
        return firstPage.logRecordCount();
    }

    /** first 页 log header 中的整链最近 undoNo。 */
    public UndoNo logLastUndoNo() {
        return firstPage.logLastUndoNo();
    }

    private UndoPage resolvePage(PageNo pageNo) {
        UndoPage held = heldPages.get(pageNo.value());
        if (held != null) {
            return held;
        }
        UndoPage page = pageAccess.openUndoPage(mtr, PageId.of(handle.spaceId(), pageNo), PageLatchMode.SHARED);
        heldPages.put(pageNo.value(), page);
        return page;
    }

    private void requireSameSegment(UndoPage page, String what) {
        if (!page.segmentId().equals(handle.segmentId()) || page.inodeSlot() != handle.inodeSlot()) {
            throw new UndoLogFormatException(what + " not in undo segment " + handle.segmentId().value()
                    + "/slot " + handle.inodeSlot());
        }
    }
}
