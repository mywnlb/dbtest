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
    /** external payload 页链物理存储。 */
    private final UndoPayloadStorage payloadStorage;
    /** inline/external 槽统一解码器。 */
    private final UndoStoredRecordResolver storedRecordResolver;
    /** 低层 append 现场规划使用的 external 页数上限。 */
    private final int maxExternalPages;

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
                   UndoPageAccess pageAccess, UndoPayloadStorage payloadStorage,
                   UndoStoredRecordResolver storedRecordResolver, int maxExternalPages,
                   UndoSegmentHandle handle, UndoPage firstPage, UndoPage current, PageLatchMode mode) {
        if (mtr == null || pageSize == null || allocator == null || codec == null || pageAccess == null
                || payloadStorage == null || storedRecordResolver == null || maxExternalPages <= 0
                || handle == null || firstPage == null || current == null || mode == null) {
            throw new DatabaseValidationException("undo log segment fields must not be null");
        }
        this.mtr = mtr;
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.codec = codec;
        this.pageAccess = pageAccess;
        this.payloadStorage = payloadStorage;
        this.storedRecordResolver = storedRecordResolver;
        this.maxExternalPages = maxExternalPages;
        this.handle = handle;
        this.firstPage = firstPage;
        this.current = current;
        this.mode = mode;
        heldPages.put(firstPage.pageId().pageNo().value(), firstPage);
        heldPages.put(current.pageId().pageNo().value(), current);
    }

    /**
     * 追加一条 undo record。数据流：要求 EXCLUSIVE 会话 → codec 编码 record payload →
     * current 页追加 record 槽 → 构造该槽的 RollPointer → first 页 log header 的整链计数、物理最新 undoNo
     * 和持久 logical head 在同一 MTR 成功后推进 → 返回 RollPointer。
     *
     * <p>事务 id、索引 id、连续 undoNo、record count 容量、predecessor=当前持久头以及非空头指针均在
     * {@code appendRecord} 前预检；任何可预测失败都不能先写页，因为 MTR rollback 不撤销 buffer content。
     *
     * <p>如果 current 页放不下，会先确认一张全新 undo 页能容纳该 record，再分配并 FIL 链入新页。preflight
     * 必须早于任何页修改，否则 MTR rollbackUncommitted 不做 content undo，会留下半生长脏链。
     */
    public RollPointer append(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        UndoRecordWritePlan plan = UndoRecordWritePlan.create(codec, pageSize, rec, keyDef, schema,
                maxExternalPages);
        int pages = requiredNewPages(plan);
        if (pages == 0) {
            return appendPlanned(plan);
        }
        try (UndoSpaceReservation ignored = reserveGrowPages(pages)) {
            return appendPlanned(plan);
        }
    }

    /**
     * 执行 admission 前形成的物理计划。调用方必须已为 {@link #requiredNewPages(UndoRecordWritePlan)} 返回值预留容量；
     * 本方法不会嵌套申请 reservation，确保 external 页链与可能的 descriptor root grow 共用同一额度。
     */
    public RollPointer appendPlanned(UndoRecordWritePlan plan) {
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException("append requires an EXCLUSIVE (writable) undo log segment session");
        }
        if (plan == null) {
            throw new DatabaseValidationException("planned undo append plan must not be null");
        }
        UndoRecord rec = plan.record();
        long nextRecordCount = preflightAppend(rec, plan.keyDef(), plan.schema());
        byte[] payload;
        if (plan.external()) {
            payload = payloadStorage.write(mtr, allocator, handle, plan).encode();
        } else {
            payload = plan.encodedPayloadUnsafe();
        }
        int off;
        try {
            off = current.appendRecord(payload, rec.transactionId(), rec.undoNo());
        } catch (UndoPageOverflowException overflow) {
            off = growAndAppendReserved(payload, rec.transactionId(), rec.undoNo(), overflow);
        }
        // RollPointer.insert 标志按记录类型决定（T1.3e）：INSERT_ROW→true、UPDATE_ROW→false。混合段中段头
        // UndoLogKind 不权威，insert 标志与每条记录的 UndoRecordType 一致，供 MVCC/版本链区分 insert vs update undo。
        boolean insert = rec.type() == UndoRecordType.INSERT_ROW;
        RollPointer pointer = new RollPointer(insert, current.pageId().pageNo(), off);
        UndoLogicalHead newHead = new UndoLogicalHead(rec.undoNo(), pointer);
        firstPage.setLogRecordCount(nextRecordCount);
        firstPage.setLogLastUndoNo(rec.undoNo().value());
        firstPage.setLogicalHead(newHead);
        return pointer;
    }

    /** external 页数加 descriptor/inline root 是否需要一张普通 UNDO grow 页。 */
    public int requiredNewPages(UndoRecordWritePlan plan) {
        if (plan == null) {
            throw new DatabaseValidationException("undo page requirement plan must not be null");
        }
        int limit = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
        int rootGrow = current.freeOffset() + Short.BYTES + plan.rootPayloadLength() > limit ? 1 : 0;
        return Math.addExact(plan.externalPageCount(), rootGrow);
    }

    /**
     * 在任何 record slot/FIL/header 写入前验证 append 不变量。页内物理高水位必须严格连续，record 事务/索引
     * 必须属于本段，且 record predecessor 必须精确等于持久 logical head；否则一次错误 append 会覆盖链头并让
     * recovery/purge 永久跳过旧记录。非空持久头也会实际解码并核对 undoNo，避免把损坏 pointer 继续传播到新分支。
     *
     * @return 校验后的新整链 record count；调用方写槽成功后直接落该值，不再执行可能溢出的加法。
     */
    private long preflightAppend(UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (rec == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo append record/keyDef/schema must not be null");
        }
        TransactionId creator = firstPage.transactionId();
        if (!rec.transactionId().equals(creator)) {
            throw new DatabaseValidationException("undo append transaction " + rec.transactionId().value()
                    + " != segment creator " + creator.value());
        }
        if (rec.indexId() != keyDef.indexId()) {
            throw new DatabaseValidationException("undo append indexId " + rec.indexId()
                    + " != key definition " + keyDef.indexId());
        }
        UndoNo physicalHighWater = firstPage.logLastUndoNo();
        if (physicalHighWater.value() == Long.MAX_VALUE) {
            throw new DatabaseValidationException("undo append high-water exhausted at Long.MAX_VALUE");
        }
        long expectedUndoNo = physicalHighWater.value() + 1;
        if (rec.undoNo().value() != expectedUndoNo) {
            throw new DatabaseValidationException("undo append number " + rec.undoNo().value()
                    + " must equal physical high-water successor " + expectedUndoNo);
        }
        long recordCount = firstPage.logRecordCount();
        if (recordCount < 0) {
            throw new UndoLogFormatException("negative undo log record count: " + recordCount);
        }
        if (recordCount == Long.MAX_VALUE) {
            throw new DatabaseValidationException("undo log record count exhausted at Long.MAX_VALUE");
        }
        UndoLogicalHead head = firstPage.logicalHead();
        if (head.undoNo().value() > physicalHighWater.value()) {
            throw new UndoLogFormatException("logical undo head exceeds physical high-water: "
                    + head.undoNo().value() + " > " + physicalHighWater.value());
        }
        if (!rec.prevRollPointer().equals(head.rollPointer())) {
            throw new DatabaseValidationException("undo append predecessor " + rec.prevRollPointer()
                    + " != persistent logical head " + head.rollPointer());
        }
        if (!head.isEmpty()) {
            UndoRecord headRecord = readRecord(head.rollPointer(), keyDef, schema);
            if (!headRecord.undoNo().equals(head.undoNo())) {
                throw new UndoLogFormatException("persistent logical head pointer resolves to undoNo "
                        + headRecord.undoNo().value() + " instead of " + head.undoNo().value());
            }
        }
        return recordCount + 1;
    }

    /**
     * current 页放不下时生长一页再写。数据流：先计算单条 record 槽是否能放入全新空页；若不能，直接
     * 重新抛出原 overflow，且不分配、不格式化、不链接、不更新 first 页 header。只有 preflight 通过后才
     * 分配新页、格式化 chain 页、写 FIL 双向链、推进 first 页 LAST_PAGE_NO、更新 handle/current，然后在新页
     * 重试 append。
     *
     * @param payload  已编码 payload。
     * @param txnId    record 所属事务写 id。
     * @param undoNo   record undoNo。
     * @param overflow current 页原始溢出异常；单条超页时保持该异常语义。
     * @return 新页上的 record 槽 offset。
     */
    private int growAndAppendReserved(byte[] payload, TransactionId txnId, UndoNo undoNo,
                                      UndoPageOverflowException overflow) {
        int need = 2 + payload.length;
        int freshCapacity = pageSize.bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES
                - UndoPageLayout.RECORD_AREA_START;
        if (need > freshCapacity) {
            throw overflow;
        }
        PageId newId = allocateChainPageForAppend();
        UndoPage newPage = createChainPageForAppend(newId);
        current.linkNextTo(newId.pageNo());
        newPage.linkPrevTo(current.pageId().pageNo());
        firstPage.setLastPageNo(newId.pageNo());
        handle = handle.withLastPage(newId);
        heldPages.put(newId.pageNo().value(), newPage);
        current = newPage;
        return current.appendRecord(payload, txnId, undoNo);
    }

    /**
     * 为 undo 页链生长申请 FSP 预留。调用点通常已经持有 current undo 页 X latch，且该页可能在同一 MTR
     * 中已被写过，不能为了满足 PageId 升序而提前释放；否则 commit 盖 pageLSN 时会丢失 touched 页的 X guard。
     *
     * <p>这里允许局部越序的并发证明是：undo segment 当前实现是单 writer，外部事务不会同时以 X 写同一条
     * undo 页链；FSP 预留只获取表空间元页 latch，从不反向等待 undo 页 latch，因此不会形成
     * “Undo 页 ↔ FSP 元页”等待环。
     */
    private UndoSpaceReservation reserveGrowPages(long pages) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "undo grow reservation: single-writer undo chain, FSP never waits for undo page latches")) {
            return allocator.reserveGrowPages(mtr, handle.spaceId(), pages);
        }
    }

    /**
     * 在既有 undo segment 内续分配一张 chain 页。分配过程会访问 page0/page2/XDES 等较低 FSP 元页，
     * 但不会读取或等待 undo 页内容；局部越序证明与 {@link #reserveGrowPages(long)} 相同。
     */
    private PageId allocateChainPageForAppend() {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "undo grow allocation: single-writer undo chain, FSP never waits for undo page latches")) {
            return allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId());
        }
    }

    /**
     * 把刚分配的裸页格式化为 UNDO chain 页。该页尚未链接进任何可见 undo 链，若物理页号低于 current 页，
     * 也不存在其它线程持有它并回等 current 页的环。
     */
    private UndoPage createChainPageForAppend(PageId newId) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "undo grow format: freshly allocated chain page is not visible to other undo readers yet")) {
            return pageAccess.createChainPage(mtr, newId, handle);
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
        if (rp == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("undo readRecord roll pointer/keyDef/schema must not be null");
        }
        if (rp.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        UndoPage page = resolvePage(rp.pageNo());
        requireSameSegment(page, "roll pointer page " + rp.pageNo());
        byte[] payload = page.recordAt(rp.offset());
        UndoRecord record = storedRecordResolver.resolve(mtr, handle.spaceId(), segmentIdentity(page),
                payload, keyDef, schema);
        requirePointerMatchesRecord(rp, record, keyDef);
        return record;
    }

    /**
     * 读取 first-page header 的持久逻辑链头。该值是 crash recovery 与 purge 的权威入口；物理
     * {@link #logLastUndoNo()} 只表示 append 高水位，部分回滚后可能大于本值。
     */
    public UndoLogicalHead logicalHead() {
        return firstPage.logicalHead();
    }

    /**
     * 以 compare-and-set 语义持久化部分回滚边界。所有可预见校验均发生在 15B header 写入之前：会话必须可写、
     * 页内当前头等于 expected、目标不能向前推进或越过物理高水位，且非空 target 必须解析到本 segment 中 undoNo
     * 和 pointer 类型一致的真实 record。这样 stale context 或损坏 target 不会在 MTR 无 content undo 的前提下留下
     * 半完成 header。
     *
     * <p><b>调用前置条件</b>：本方法为物理 CAS，不在一个长 MTR 内遍历 expected→target 的完整祖先链，避免
     * 大事务把所有 undo 页同时 fixed。生产调用方 {@code RollbackService} 必须先用逐 pointer 短 MTR 精确证明
     * target 可达；本方法再校验 target 自身 record 与高水位并完成最终 CAS。
     *
     * @param expected 调用方开始 partial rollback 时看到的旧逻辑头。
     * @param target   已成功完成所有逆操作后要持久化的 savepoint/空边界。
     * @param keyDef   解码目标 undo record 的聚簇索引 key 定义。
     * @param schema   解码目标 undo record 的表 schema。
     */
    public void updateLogicalHead(UndoLogicalHead expected, UndoLogicalHead target,
                                  IndexKeyDef keyDef, TableSchema schema) {
        if (mode != PageLatchMode.EXCLUSIVE) {
            throw new DatabaseValidationException(
                    "updateLogicalHead requires an EXCLUSIVE undo log segment session");
        }
        if (expected == null || target == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("logical head update args must not be null");
        }
        UndoLogicalHead currentHead = firstPage.logicalHead();
        if (!currentHead.equals(expected)) {
            throw new UndoLogicalHeadConflictException("persistent logical undo head changed: expected="
                    + expected + ", actual=" + currentHead);
        }
        if (target.undoNo().value() > expected.undoNo().value()) {
            throw new DatabaseValidationException("partial rollback target cannot advance logical undo head: "
                    + target.undoNo().value() + " > " + expected.undoNo().value());
        }
        UndoNo physicalHighWater = firstPage.logLastUndoNo();
        if (target.undoNo().value() > physicalHighWater.value()) {
            throw new DatabaseValidationException("logical undo head exceeds physical high-water: "
                    + target.undoNo().value() + " > " + physicalHighWater.value());
        }
        if (!target.isEmpty()) {
            UndoRecord targetRecord = readRecord(target.rollPointer(), keyDef, schema);
            if (!targetRecord.undoNo().equals(target.undoNo())) {
                throw new UndoLogFormatException("logical head pointer resolves to undoNo "
                        + targetRecord.undoNo().value() + " instead of " + target.undoNo().value());
            }
        }
        if (!currentHead.equals(target)) {
            firstPage.setLogicalHead(target);
        }
    }

    /**
     * 按物理 FIL 页链正向遍历全部槽。该 API 仅供格式诊断/测试，不表达部分回滚后的当前逻辑链；recovery/purge
     * 必须从 {@link #logicalHead()} 沿 {@link UndoRecord#prevRollPointer()} 逐条短读。每页按 record area
     * 中的 {@code [len u16][payload]} 槽顺序解码，
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
     * 按物理 FIL 页链正向遍历全部槽，并向 consumer 同时给出**每条 record 自身的 {@link RollPointer} 地址**
     *（pageNo+offset，insert 标志按记录类型）。它只表达物理槽顺序，供格式诊断和测试核对 record 地址；
     * partial rollback 后该顺序还包含 detached branch，生产 rollback/recovery/purge 不得据此判断当前逻辑链。
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
                UndoRecord rec = storedRecordResolver.resolve(mtr, handle.spaceId(), segmentIdentity(page),
                        payload, keyDef, schema);
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

    /** 返回当前已持 X/S latch 会话观察到的 append 权威快照，供事务层在首次写前比较规划结果。 */
    public UndoAppendSnapshot appendSnapshot() {
        return new UndoAppendSnapshot(handle.firstPageId(), handle.lastPageId(), handle.segmentId(),
                handle.inodeSlot(), firstPage.transactionId(), firstPage.logLastUndoNo(), firstPage.logicalHead(),
                firstPage.logRecordCount(), current.freeOffset());
    }

    private void requirePointerMatchesRecord(RollPointer pointer, UndoRecord record, IndexKeyDef keyDef) {
        boolean expectedInsert = record.type() == UndoRecordType.INSERT_ROW;
        if (pointer.insert() != expectedInsert) {
            throw new UndoLogFormatException("roll pointer insert bit " + pointer.insert()
                    + " inconsistent with undo record type " + record.type());
        }
        if (!record.transactionId().equals(firstPage.transactionId())) {
            throw new UndoLogFormatException("undo record transaction " + record.transactionId().value()
                    + " != segment creator " + firstPage.transactionId().value());
        }
        if (record.indexId() != keyDef.indexId()) {
            throw new UndoLogFormatException("undo record indexId " + record.indexId()
                    + " != expected " + keyDef.indexId());
        }
    }

    private UndoPage resolvePage(PageNo pageNo) {
        UndoPage held = heldPages.get(pageNo.value());
        if (held != null) {
            return held;
        }
        UndoPage page = openChainPageForRead(pageNo);
        heldPages.put(pageNo.value(), page);
        return page;
    }

    /**
     * 按页号打开 undo 链上的历史页。undo 链的逻辑顺序由 FIL NEXT/PREV 和 RollPointer 决定，不能假设物理
     * PageNo 单调；append MTR 也可能在持有最新尾页 X latch 后，为校验旧 RollPointer 再读更小页号的历史页。
     *
     * <p>越序只限本段句柄内的 SHARED 打开：同一 segment 仍是单 writer，MVCC 跨事务直读应走
     * {@link UndoLogSegmentAccess#readRecordByRollPointer} 的短只读 MTR，而不是在别的 segment 写 MTR 中长持页链。
     */
    private UndoPage openChainPageForRead(PageNo pageNo) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "undo chain read: logical roll-pointer/page-chain order is independent from physical PageId order")) {
            return pageAccess.openUndoPage(mtr, PageId.of(handle.spaceId(), pageNo), PageLatchMode.SHARED);
        }
    }

    private void requireSameSegment(UndoPage page, String what) {
        if (!page.segmentId().equals(handle.segmentId()) || page.inodeSlot() != handle.inodeSlot()) {
            throw new UndoLogFormatException(what + " not in undo segment " + handle.segmentId().value()
                    + "/slot " + handle.inodeSlot());
        }
    }

    private static UndoPayloadStorage.SegmentIdentity segmentIdentity(UndoPage page) {
        return new UndoPayloadStorage.SegmentIdentity(page.segmentId(), page.inodeSlot());
    }
}
