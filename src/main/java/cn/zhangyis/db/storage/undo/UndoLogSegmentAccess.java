package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.TransactionNo;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 跨页 undo log segment 的 MTR 生产入口。它只依赖 {@link UndoSpaceAllocator} 端口进行页分配，
 * 不反向依赖 storage.api 的 DiskSpaceManager/SegmentRef。返回的 {@link UndoLogSegment} 由 MTR memo
 * 持有页 guard，调用方不要自行 close。
 */
public final class UndoLogSegmentAccess {

    /**
     * 页大小透传给 UndoPage/UndoLogSegment，用于页内容量和生长 preflight。
     */
    private final PageSize pageSize;

    /**
     * undo 页分配端口，隐藏底层 FSP/DiskSpaceManager 细节。
     */
    private final UndoSpaceAllocator allocator;

    /**
     * record codec。Access 创建一次后复用，段句柄共享该无状态编解码器。
     */
    private final UndoRecordCodec codec;

    /**
     * undo page 访问器，负责 first/chain 页格式化和页类型守门。
     */
    private final UndoPageAccess pageAccess;
    /** external payload 页链读写设施。 */
    private final UndoPayloadStorage payloadStorage;
    /** 所有 record 槽统一解码入口。 */
    private final UndoStoredRecordResolver storedRecordResolver;
    /** 单条 external undo payload 的实例安全上限。 */
    private final int maxExternalPages;

    public UndoLogSegmentAccess(BufferPool pool, PageSize pageSize, UndoSpaceAllocator allocator,
                                TypeCodecRegistry registry) {
        this(pool, pageSize, allocator, registry, null, 16);
    }

    /**
     * 创建带运行时表空间状态准入的 undo segment access。生产组合根注入共享 {@link TablespaceRegistry}，
     * 使所有 undo first/chain 页打开和新建都先经 operation lease + registry require；低层测试仍可用四参构造
     * 专注页格式和链路行为。
     */
    public UndoLogSegmentAccess(BufferPool pool, PageSize pageSize, UndoSpaceAllocator allocator,
                                TypeCodecRegistry registry, TablespaceRegistry tablespaceRegistry) {
        this(pool, pageSize, allocator, registry, tablespaceRegistry, 16);
    }

    /** 创建带 external undo payload 页数上限的生产 access。 */
    public UndoLogSegmentAccess(BufferPool pool, PageSize pageSize, UndoSpaceAllocator allocator,
                                TypeCodecRegistry registry, TablespaceRegistry tablespaceRegistry,
                                int maxExternalPages) {
        if (pool == null || pageSize == null || allocator == null || registry == null) {
            throw new DatabaseValidationException("undo log segment access args must not be null");
        }
        if (maxExternalPages <= 0) {
            throw new DatabaseValidationException("max external undo pages must be positive: " + maxExternalPages);
        }
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.codec = new UndoRecordCodec(registry);
        this.pageAccess = new UndoPageAccess(pool, pageSize, tablespaceRegistry);
        this.payloadStorage = new UndoPayloadStorage(pool, pageSize, tablespaceRegistry);
        this.storedRecordResolver = new UndoStoredRecordResolver(codec, payloadStorage, maxExternalPages);
        this.maxExternalPages = maxExternalPages;
    }

    /**
     * 创建一条新的 INSERT 或 UPDATE undo log segment。数据流：分配端口建 UNDO segment 和首页（此时页类型为
     * ALLOCATED）→ 同一 MTR 内把首页重初始化为 UNDO first 页 → 返回 EXCLUSIVE 段句柄，current=first。
     *
     * <p>同页两次 newPage（ALLOCATED→UNDO）复用 T1.3a 的物理 redo 顺序，最终页类型由后一条 PAGE_INIT/bytes
     * 决定，本片不新增 redo 类型。
     */
    public UndoLogSegment create(MiniTransaction mtr, SpaceId undoSpace, TransactionId txnId,
                                 UndoLogKind kind) {
        if (mtr == null || undoSpace == null || txnId == null || kind == null) {
            throw new DatabaseValidationException("undo log segment create args must not be null");
        }
        if (kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("temporary undo log is not supported by ordinary undo tablespace");
        }
        UndoSegmentHandle handle = allocator.createUndoSegment(mtr, undoSpace);
        UndoPage firstPage = pageAccess.createFirstPage(mtr, handle.firstPageId(), kind, txnId, handle);
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, payloadStorage,
                storedRecordResolver, maxExternalPages, handle, firstPage, firstPage, PageLatchMode.EXCLUSIVE);
    }

    /**
     * 重开一条已有 undo log segment。数据流：按调用方请求模式打开 first 页 → 校验它确实是 log first 页 →
     * 从 first 页 page/log header 重建 {@link UndoSegmentHandle}。SHARED 会话只读遍历，仅持 first 页 S；
     * EXCLUSIVE 会话用于续 append，若尾页不是 first 页，会额外以 X 打开尾页，并校验尾页 segmentId/inodeSlot
     * 与 first header 重建的 handle 一致，防止损坏的 LAST_PAGE_NO 把其它 segment 页当作当前尾页继续写。
     *
     * @param mtr         当前物理短事务。
     * @param firstPageId undo log first 页 id，不能传 chain 页。
     * @param mode        SHARED=只读，EXCLUSIVE=续写。
     * @return MTR 内有效的 undo log segment 句柄。
     */
    public UndoLogSegment open(MiniTransaction mtr, PageId firstPageId, PageLatchMode mode) {
        if (mtr == null || firstPageId == null || mode == null) {
            throw new DatabaseValidationException("undo log segment open args must not be null");
        }
        UndoPage firstPage = pageAccess.openUndoPage(mtr, firstPageId, mode);
        if (!firstPage.isFirstPage()) {
            throw new UndoLogFormatException("page " + firstPageId + " is not the undo log first page");
        }
        long lastPageNoVal = firstPage.lastPageNo();
        PageId lastPageId = PageId.of(firstPageId.spaceId(), PageNo.of(lastPageNoVal));
        UndoSegmentHandle handle = new UndoSegmentHandle(firstPageId.spaceId(), firstPage.inodeSlot(),
                firstPage.segmentId(), firstPageId, lastPageId);
        UndoPage current = firstPage;
        if (mode == PageLatchMode.EXCLUSIVE && lastPageNoVal != firstPageId.pageNo().value()) {
            UndoPage last = pageAccess.openUndoPage(mtr, lastPageId, PageLatchMode.EXCLUSIVE);
            if (!last.segmentId().equals(handle.segmentId()) || last.inodeSlot() != handle.inodeSlot()) {
                throw new UndoLogFormatException("undo last page " + lastPageId + " segment mismatch with "
                        + handle.segmentId().value() + "/slot " + handle.inodeSlot());
            }
            if (last.undoKind() != firstPage.undoKind()) {
                throw new UndoLogFormatException("undo last page " + lastPageId + " kind " + last.undoKind()
                        + " differs from first page " + firstPage.undoKind());
            }
            current = last;
        }
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, payloadStorage,
                storedRecordResolver, maxExternalPages, handle, firstPage, current, mode);
    }

    /**
     * 恢复/规划辅助：只读打开 page3 cached first page，验证它是空单页 owner，并复制可跨 MTR 使用的 handle。
     * FSP used-page/extent 账本由调用方在独立只读 MTR 中继续交叉校验，避免 first-page latch 与 page2 逆序。
     */
    public CachedUndoSegmentRef inspectCached(MiniTransaction mtr, PageId firstPageId, UndoLogKind expectedKind) {
        if (mtr == null || firstPageId == null || expectedKind == null
                || expectedKind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("cached undo inspection args are invalid");
        }
        UndoLogSegment segment = open(mtr, firstPageId, PageLatchMode.SHARED);
        segment.requireCached(expectedKind);
        return new CachedUndoSegmentRef(expectedKind, segment.handle());
    }

    /**
     * 只读一张 first page 的 history/recovery header，不跟随 LAST_PAGE_NO 打开段尾。该入口用于持久 history 遍历，
     * 保证每个节点都在独立短 MTR 内读取，不把整条跨事务链的 page fix 同时压在 buffer pool 上。
     */
    public UndoHistoryNodeSnapshot inspectHistoryNode(MiniTransaction mtr, PageId firstPageId) {
        if (mtr == null || firstPageId == null) {
            throw new DatabaseValidationException("undo history node inspection args must not be null");
        }
        UndoPage page = pageAccess.openUndoPage(mtr, firstPageId, PageLatchMode.SHARED);
        if (!page.isFirstPage() || page.firstPageNo() != firstPageId.pageNo().value()) {
            throw new UndoLogFormatException("history node is not an undo first page: " + firstPageId);
        }
        long lastPageNo = page.lastPageNo();
        UndoSegmentHandle handle = new UndoSegmentHandle(firstPageId.spaceId(), page.inodeSlot(), page.segmentId(),
                firstPageId, PageId.of(firstPageId.spaceId(), PageNo.of(lastPageNo)));
        return new UndoHistoryNodeSnapshot(firstPageId, handle, page.undoKind(),
                UndoLogState.fromPhysical(page.state()), page.transactionId(),
                TransactionNo.of(page.commitNo()), historyLink(firstPageId, page.historyPrevPageNo()),
                historyLink(firstPageId, page.historyNextPageNo()), page.logicalHead());
    }

    /**
     * commit 最终 MTR 的 first-page-only 批处理：按 pageNo 升序获取旧 tail 与新 UPDATE 首页，避免普通
     * {@link #open} 因跟随新节点 LAST_PAGE_NO 而越过另一 first page。全部状态校验先完成，之后才写双向链接和提交头。
     */
    public void appendHistoryNode(MiniTransaction mtr, Optional<PageId> oldTailPageId,
                                  PageId newTailPageId, TransactionId creatorTransactionId,
                                  TransactionNo transactionNo) {
        appendHistoryNode(mtr, oldTailPageId, newTailPageId, creatorTransactionId, transactionNo, List.of());
    }

    /**
     * mixed commit 变体：除 history old/new first page 外，同时按同一全局 pageNo 顺序打开需要重置的 INSERT
     * cached first page。这样 page3 后的普通 undo latch 顺序不依赖各 segment 的分配先后。
     */
    public void appendHistoryNode(MiniTransaction mtr, Optional<PageId> oldTailPageId,
                                  PageId newTailPageId, TransactionId creatorTransactionId,
                                  TransactionNo transactionNo,
                                  List<CachedUndoSegmentRef> cacheResets) {
        if (mtr == null || oldTailPageId == null || newTailPageId == null
                || creatorTransactionId == null || creatorTransactionId.isNone()
                || transactionNo == null || transactionNo.isNone() || cacheResets == null
                || cacheResets.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("undo history append batch arguments are invalid");
        }
        oldTailPageId.ifPresent(old -> requireSameHistorySpace(old, newTailPageId));
        if (oldTailPageId.equals(Optional.of(newTailPageId))) {
            throw new DatabaseValidationException("undo history append cannot link a page to itself");
        }
        List<PageId> pageIds = new ArrayList<>();
        oldTailPageId.ifPresent(pageIds::add);
        pageIds.add(newTailPageId);
        cacheResets.stream().map(item -> item.handle().firstPageId()).forEach(pageIds::add);
        if (pageIds.stream().distinct().count() != pageIds.size()) {
            throw new DatabaseValidationException("history append/cache reset first pages must be distinct");
        }
        Map<PageId, UndoPage> pages = openFirstPagesSorted(mtr, pageIds);
        UndoPage newTail = pages.get(newTailPageId);
        requireHistoryFirstPage(newTail, newTailPageId);
        if (newTail.undoKind() != UndoLogKind.UPDATE || newTail.state() != UndoPageLayout.STATE_ACTIVE
                || !newTail.transactionId().equals(creatorTransactionId) || newTail.commitNo() != 0L
                || newTail.historyPrevPageNo() != FilePageHeader.FIL_NULL
                || newTail.historyNextPageNo() != FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("new history tail must be an unlinked ACTIVE UPDATE undo page: "
                    + newTailPageId);
        }
        if (oldTailPageId.isPresent()) {
            UndoPage oldTail = pages.get(oldTailPageId.orElseThrow());
            requireHistoryFirstPage(oldTail, oldTailPageId.orElseThrow());
            if (oldTail.undoKind() != UndoLogKind.UPDATE
                    || oldTail.state() != UndoPageLayout.STATE_COMMITTED
                    || oldTail.commitNo() == 0L
                    || oldTail.historyNextPageNo() != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException("old history tail is not a terminal COMMITTED UPDATE node: "
                        + oldTailPageId.orElseThrow());
            }
        }
        for (CachedUndoSegmentRef reset : cacheResets) {
            UndoPage page = pages.get(reset.handle().firstPageId());
            requireHistoryFirstPage(page, reset.handle().firstPageId());
            if (reset.kind() == UndoLogKind.TEMPORARY || page.undoKind() != reset.kind()
                    || page.state() != UndoPageLayout.STATE_ACTIVE
                    || !page.transactionId().equals(creatorTransactionId)
                    || page.historyPrevPageNo() != FilePageHeader.FIL_NULL
                    || page.historyNextPageNo() != FilePageHeader.FIL_NULL) {
                throw new UndoLogFormatException("mixed commit cache reset target changed: "
                        + reset.handle().firstPageId());
            }
        }
        oldTailPageId.ifPresent(old -> pages.get(old).setHistoryNextPageNo(newTailPageId.pageNo().value()));
        newTail.setHistoryLinks(oldTailPageId.map(page -> page.pageNo().value()).orElse(FilePageHeader.FIL_NULL),
                FilePageHeader.FIL_NULL);
        newTail.setLogState(UndoPageLayout.STATE_COMMITTED);
        newTail.setCommitNo(transactionNo.value());
        for (CachedUndoSegmentRef reset : cacheResets) {
            pages.get(reset.handle().firstPageId()).resetForCache(reset.kind(), reset.handle());
        }
    }

    /**
     * purge 最终 MTR 的 first-page-only 批处理。非缓存路径只触碰新 head；被回收首页已经由 FSP drop，不再获取其
     * latch。缓存路径同时按页号排序打开 removed/new head，把 removed 重置为 CACHED 并清除 history links。
     */
    public void unlinkHistoryHead(MiniTransaction mtr, UndoHistoryNodeSnapshot removed,
                                  Optional<UndoHistoryNodeSnapshot> newHead, boolean cacheRemoved) {
        if (mtr == null || removed == null || newHead == null || !removed.isCommitted()
                || removed.kind() != UndoLogKind.UPDATE) {
            throw new DatabaseValidationException("undo history unlink batch arguments are invalid");
        }
        if (!removed.previousHistoryPageId().isEmpty()
                || !removed.nextHistoryPageId().equals(newHead.map(UndoHistoryNodeSnapshot::firstPageId))) {
            throw new UndoLogFormatException("removed history head links do not match the frozen successor");
        }
        newHead.ifPresent(next -> {
            requireSameHistorySpace(removed.firstPageId(), next.firstPageId());
            if (!next.isCommitted() || next.kind() != UndoLogKind.UPDATE
                    || !next.previousHistoryPageId().equals(Optional.of(removed.firstPageId()))) {
                throw new UndoLogFormatException("new history head does not point back to removed node");
            }
        });
        List<PageId> ids = new ArrayList<>();
        newHead.map(UndoHistoryNodeSnapshot::firstPageId).ifPresent(ids::add);
        if (cacheRemoved) {
            ids.add(removed.firstPageId());
        }
        Map<PageId, UndoPage> pages = openFirstPagesSorted(mtr, ids);
        newHead.ifPresent(snapshot -> {
            UndoPage page = pages.get(snapshot.firstPageId());
            requireHistoryFirstPage(page, snapshot.firstPageId());
            if (page.state() != UndoPageLayout.STATE_COMMITTED
                    || page.historyPrevPageNo() != removed.firstPageId().pageNo().value()) {
                throw new UndoLogFormatException("new history head changed before unlink");
            }
            page.setHistoryPrevPageNo(FilePageHeader.FIL_NULL);
        });
        if (cacheRemoved) {
            UndoPage page = pages.get(removed.firstPageId());
            requireHistoryFirstPage(page, removed.firstPageId());
            if (page.state() != UndoPageLayout.STATE_COMMITTED
                    || page.commitNo() != removed.committedTransactionNo().value()) {
                throw new UndoLogFormatException("removed history node changed before cache reset");
            }
            page.resetForCache(UndoLogKind.UPDATE, removed.handle());
        }
    }

    private Map<PageId, UndoPage> openFirstPagesSorted(MiniTransaction mtr, List<PageId> firstPageIds) {
        Map<PageId, UndoPage> pages = new LinkedHashMap<>();
        firstPageIds.stream().distinct()
                .sorted(Comparator.comparingInt((PageId page) -> page.spaceId().value())
                        .thenComparingLong(page -> page.pageNo().value()))
                .forEach(pageId -> pages.put(pageId,
                        pageAccess.openUndoPage(mtr, pageId, PageLatchMode.EXCLUSIVE)));
        return pages;
    }

    private static void requireHistoryFirstPage(UndoPage page, PageId expectedPageId) {
        if (page == null || !page.pageId().equals(expectedPageId) || !page.isFirstPage()) {
            throw new UndoLogFormatException("history batch target is not the expected first page: "
                    + expectedPageId);
        }
    }

    private static Optional<PageId> historyLink(PageId owner, long linkedPageNo) {
        if (linkedPageNo == FilePageHeader.FIL_NULL) {
            return Optional.empty();
        }
        if (linkedPageNo < 0 || linkedPageNo > FilePageHeader.FIL_NULL) {
            throw new UndoLogFormatException("invalid undo history link on " + owner + ": " + linkedPageNo);
        }
        return Optional.of(PageId.of(owner.spaceId(), PageNo.of(linkedPageNo)));
    }

    private static void requireSameHistorySpace(PageId left, PageId right) {
        if (!left.spaceId().equals(right.spaceId())) {
            throw new DatabaseValidationException("undo history links cannot cross tablespaces");
        }
    }

    /**
     * 复用 cached segment：按 page3/内存冻结的 handle 重新 X-open、逐字段验证，再重置为新事务 ACTIVE 首页。
     */
    public UndoLogSegment activateCached(MiniTransaction mtr, CachedUndoSegmentRef cached,
                                         TransactionId transactionId) {
        if (mtr == null || cached == null || transactionId == null || transactionId.isNone()) {
            throw new DatabaseValidationException("cached undo activation args are invalid");
        }
        UndoLogSegment segment = open(mtr, cached.handle().firstPageId(), PageLatchMode.EXCLUSIVE);
        if (!segment.handle().equals(cached.handle())) {
            throw new UndoLogFormatException("cached undo handle changed before activation: expected="
                    + cached.handle() + ", current=" + segment.handle());
        }
        segment.requireCached(cached.kind());
        segment.activateCached(transactionId, cached.kind());
        return segment;
    }

    /**
     * 按 roll pointer 直读单条 undo record（T1.4 MVCC 旧版本构造用）。与 {@link UndoLogSegment#readRecord} 的区别：
     * **不依赖 segment 首页/句柄**——一致性读要读的旧版本可能落在**别的事务**的 undo 段，调用方只有 roll pointer。
     * 单 undo 空间按 {@code rollPtr}(pageNo+offset) 以 S latch 直开 UNDO 页（页类型守门在 {@link UndoPageAccess#openUndoPage}）、
     * 解码单条 record（单条不跨页，§379）。
     *
     * <p><b>损坏快速失败</b>（全部抛 {@link UndoLogFormatException}，不返回拼错的历史行）：NULL 指针；offset 越出
     * record area {@code [RECORD_AREA_START, freeOffset)} 或槽溢出已用区；指针 insert 位与解码 record 类型不一致；
     * record 的 {@code indexId} 与 {@code keyDef.indexId()} 不一致。调用方读完应即提交/回滚该只读 MTR 释放 S latch。
     *
     * @param undoSpace 单 undo 表空间（roll pointer 不编码 spaceId，由调用方提供）。
     * @return 解码出的 undo record（INSERT_ROW 或 UPDATE_ROW）。
     */
    public UndoRecord readRecordByRollPointer(MiniTransaction mtr, SpaceId undoSpace, RollPointer rollPtr,
                                              IndexKeyDef keyDef, TableSchema schema) {
        if (mtr == null || undoSpace == null || rollPtr == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("readRecordByRollPointer args must not be null");
        }
        if (rollPtr.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        UndoPage page = pageAccess.openUndoPage(mtr, PageId.of(undoSpace, rollPtr.pageNo()), PageLatchMode.SHARED);
        int off = rollPtr.offset();
        int free = page.freeOffset();
        if (off < UndoPageLayout.RECORD_AREA_START || off >= free) {
            throw new UndoLogFormatException("roll pointer offset " + off + " outside undo record area ["
                    + UndoPageLayout.RECORD_AREA_START + "," + free + ")");
        }
        byte[] payload = page.recordAt(off);
        if (off + 2 + payload.length > free) {
            throw new UndoLogFormatException("undo record slot at " + off + " overruns used area (free=" + free + ")");
        }
        UndoRecord rec = storedRecordResolver.resolve(mtr, undoSpace,
                new UndoPayloadStorage.SegmentIdentity(page.segmentId(), page.inodeSlot()),
                payload, keyDef, schema);
        requireRecordKind(page.undoKind(), rec.type(), "direct roll-pointer read");
        boolean expectInsert = rec.type() == UndoRecordType.INSERT_ROW;
        if (rollPtr.insert() != expectInsert) {
            throw new UndoLogFormatException("roll pointer insert bit " + rollPtr.insert()
                    + " inconsistent with undo record type " + rec.type());
        }
        if (rec.indexId() != keyDef.indexId()) {
            throw new UndoLogFormatException("undo record indexId " + rec.indexId()
                    + " != expected " + keyDef.indexId());
        }
        return rec;
    }

    static void requireRecordKind(UndoLogKind kind, UndoRecordType type, String operation) {
        boolean valid = switch (kind) {
            case INSERT -> type == UndoRecordType.INSERT_ROW;
            case UPDATE -> type == UndoRecordType.UPDATE_ROW || type == UndoRecordType.DELETE_MARK;
            case TEMPORARY -> false;
        };
        if (!valid) {
            throw new UndoLogFormatException(operation + " found record type " + type
                    + " in " + kind + " undo log");
        }
    }

    /** 在写 MTR admission 前冻结一条 record 的编码与 external 页数。 */
    public UndoRecordWritePlan planRecord(UndoRecord record, IndexKeyDef keyDef, TableSchema schema) {
        return UndoRecordWritePlan.create(codec, pageSize, record, keyDef, schema, maxExternalPages);
    }

    /** 根据规划快照精确计算本次会消费的 segment 首页、普通 root grow 和 external payload 页总数。 */
    public int plannedNewPages(boolean newLog, UndoAppendSnapshot snapshot, UndoRecordWritePlan recordPlan) {
        if (recordPlan == null || (!newLog && snapshot == null)) {
            throw new DatabaseValidationException("undo planned page calculation args invalid");
        }
        int rootPages;
        if (newLog) {
            rootPages = 1;
        } else {
            int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
            rootPages = snapshot.tailFreeOffset() + Short.BYTES + recordPlan.rootPayloadLength() > limit ? 1 : 0;
        }
        return Math.addExact(rootPages, recordPlan.externalPageCount());
    }

    /** 为计划中全部新页一次性预留 undo 空间；0 页计划由调用方跳过。 */
    public UndoSpaceReservation reservePages(MiniTransaction mtr, SpaceId spaceId, long pages) {
        if (pages <= 0) {
            throw new DatabaseValidationException("planned undo reservation pages must be positive: " + pages);
        }
        return allocator.reserveGrowPages(mtr, spaceId, pages);
    }

    /**
     * 用短只读 MTR物化 append 所需的 first/tail 权威快照。尾页与首页不同时单独 S-open，并验证 segment owner；
     * 调用方提交只读 MTR 后只保留该不可变值对象。
     */
    public UndoAppendSnapshot inspectAppendSnapshot(MiniTransaction mtr, PageId firstPageId) {
        if (mtr == null || firstPageId == null) {
            throw new DatabaseValidationException("undo append snapshot mtr/first page must not be null");
        }
        UndoPage first = pageAccess.openUndoPage(mtr, firstPageId, PageLatchMode.SHARED);
        if (!first.isFirstPage()) {
            throw new UndoLogFormatException("append snapshot page is not undo first page: " + firstPageId);
        }
        PageId tailId = PageId.of(firstPageId.spaceId(), PageNo.of(first.lastPageNo()));
        UndoPage tail = first;
        if (!tailId.equals(firstPageId)) {
            try (var ignored = mtr.allowOutOfOrderPageLatch(
                    "undo append planning opens persisted tail after first-page snapshot")) {
                tail = pageAccess.openUndoPage(mtr, tailId, PageLatchMode.SHARED);
            }
            if (!tail.segmentId().equals(first.segmentId()) || tail.inodeSlot() != first.inodeSlot()) {
                throw new UndoLogFormatException("undo append snapshot tail owner mismatch: " + tailId);
            }
        }
        return new UndoAppendSnapshot(firstPageId, tailId, first.segmentId(), first.inodeSlot(),
                first.transactionId(), first.logLastUndoNo(), first.logicalHead(), first.undoKind(),
                first.logRecordCount(), tail.freeOffset());
    }
}
