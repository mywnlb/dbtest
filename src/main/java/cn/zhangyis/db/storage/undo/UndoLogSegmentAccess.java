package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

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
     * 创建一条新的 INSERT undo log segment。数据流：分配端口建 UNDO segment 和首页（此时页类型为
     * ALLOCATED）→ 同一 MTR 内把首页重初始化为 UNDO first 页 → 返回 EXCLUSIVE 段句柄，current=first。
     *
     * <p>同页两次 newPage（ALLOCATED→UNDO）复用 T1.3a 的物理 redo 顺序，最终页类型由后一条 PAGE_INIT/bytes
     * 决定，本片不新增 redo 类型。
     */
    public UndoLogSegment create(MiniTransaction mtr, SpaceId undoSpace, TransactionId txnId) {
        if (mtr == null || undoSpace == null || txnId == null) {
            throw new DatabaseValidationException("undo log segment create args must not be null");
        }
        UndoSegmentHandle handle = allocator.createUndoSegment(mtr, undoSpace);
        UndoPage firstPage = pageAccess.createFirstPage(mtr, handle.firstPageId(), UndoLogKind.INSERT, txnId, handle);
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
            current = last;
        }
        return new UndoLogSegment(mtr, pageSize, allocator, codec, pageAccess, payloadStorage,
                storedRecordResolver, maxExternalPages, handle, firstPage, current, mode);
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

    /** 在写 MTR admission 前冻结一条 record 的编码与 external 页数。 */
    public UndoRecordWritePlan planRecord(UndoRecord record, IndexKeyDef keyDef, TableSchema schema) {
        return UndoRecordWritePlan.create(codec, pageSize, record, keyDef, schema, maxExternalPages);
    }

    /** 根据规划快照精确计算本次会消费的 segment 首页、普通 root grow 和 external payload 页总数。 */
    public int plannedNewPages(boolean firstWrite, UndoAppendSnapshot snapshot, UndoRecordWritePlan recordPlan) {
        if (recordPlan == null || (!firstWrite && snapshot == null)) {
            throw new DatabaseValidationException("undo planned page calculation args invalid");
        }
        int rootPages;
        if (firstWrite) {
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
                first.transactionId(), first.logLastUndoNo(), first.logicalHead(), first.logRecordCount(),
                tail.freeOffset());
    }
}
