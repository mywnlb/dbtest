package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.LobReference;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservation;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservationKind;
import cn.zhangyis.db.storage.fsp.segment.SegmentPurpose;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.mtr.MtrLatchOrderScope;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.LobCodec;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.record.type.UnsupportedColumnTypeException;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Off-page LOB 稳定存储门面。它是唯一同时协作 Record codec、Buffer Pool、MTR 和 FSP 的组件；Record 包只保存
 * {@link LobReference}，FSP/Buffer Pool 仍不理解 TEXT/BLOB/JSON。
 *
 * <p><b>并发与恢复</b>：写链在一个调用方 MTR 中先 BLOB reserve、再分配全部 PAGE_INIT(BLOB)、最后格式化 body；
 * commit 原子追加 FSP intent、PAGE_INIT 与 PAGE_BYTES。读链逐页 S latch、复制校验后立即释放。free 先只读验证整链，
 * 再以 page0→page2 顺序复核 LOB segment 并释放，避免 page latch 与 FSP latch 的反向等待。
 *
 * <p><b>教学简化</b>：写入期间所有新页的 X guard 由 MTR 持到 commit，因此单值大小受 Buffer Pool 可固定帧数约束；
 * 后续流式版本需要 staged/incomplete-chain 状态与独立 crash recovery 协议，不能静默拆成多个 MTR。
 */
public final class LobStorage {

    /** FSP/segment 门面，负责 reservation、分配与释放的权威账本。 */
    private final DiskSpaceManager diskSpace;
    /** 页链 body 的受控页来源；所有访问必须经调用方 MTR。 */
    private final BufferPool pool;
    /** 实例页大小，决定 chunk 容量。 */
    private final PageSize pageSize;
    /** Record 类型入口；必须与 B+Tree/Undo 使用同样的稳定编码规则。 */
    private final TypeCodecRegistry codecs;

    public LobStorage(DiskSpaceManager diskSpace, BufferPool pool, PageSize pageSize,
                      TypeCodecRegistry codecs) {
        if (diskSpace == null || pool == null || pageSize == null || codecs == null) {
            throw new DatabaseValidationException("LOB storage dependencies must not be null");
        }
        this.diskSpace = diskSpace;
        this.pool = pool;
        this.pageSize = pageSize;
        this.codecs = codecs;
    }

    /**
     * 无 IO 地冻结新的 LOB 页链计划。inline/空值不应进入 externalize 路径；segment purpose 刻意留到
     * {@link #writePlanned(MiniTransaction, LobWritePlan)} 开始物理动作前复核，使该方法可安全用于 begin admission。
     *
     * @param segment 精确 table binding 提供的候选 LOB segment identity。
     * @param type TEXT/BLOB/JSON family 类型。
     * @param value 完整 StringValue/BinaryValue，不能是已有 external reference。
     * @return 冻结 payload、CRC、页数、prefix 与 workload 的纯计划。
     */
    public LobWritePlan planWrite(SegmentRef segment, ColumnType type, ColumnValue value) {
        if (segment == null || type == null || value == null) {
            throw new DatabaseValidationException("LOB write plan arguments must not be null");
        }
        LobCodec codec = requireLobCodec(type);
        byte[] payload = codec.logicalBytesForStorage(value, type);
        if (payload.length == 0) {
            throw new DatabaseValidationException("empty LOB should remain inline instead of allocating a page chain");
        }
        if (payload.length <= LobCodec.INLINE_PAYLOAD_LIMIT) {
            throw new DatabaseValidationException("LOB payload within inline limit must not allocate a page chain: "
                    + payload.length);
        }
        int count = pageCount(payload.length, LobPageLayout.payloadCapacity(pageSize));
        return new LobWritePlan(segment, type, payload, count, crc32(payload),
                codec.inlinePrefix(payload, type), writeWorkload(payload.length));
    }

    /**
     * 在 DML begin 前验证一个非 NULL LOB 逻辑值并判断是否超过 inline 阈值。该方法无 IO、无 allocation；
     * 已经 external 的值不是 INSERT raw input，交由 LobCodec 明确拒绝。
     */
    public boolean requiresExternalization(ColumnType type, ColumnValue value) {
        if (type == null || value == null) {
            throw new DatabaseValidationException("LOB externalization check args must not be null");
        }
        LobCodec codec = requireLobCodec(type);
        return codec.logicalBytesForStorage(value, type).length > LobCodec.INLINE_PAYLOAD_LIMIT;
    }

    /**
     * 用短只读 MTR 复核 table binding 指向的 segment identity/purpose。LOB-aware DML 在业务 MTR admission 前调用，
     * 使旧 catalog 缺 binding 或错误 purpose 不会留下 undo owner、row 或新 LOB 页副作用。
     */
    public void preflightSegment(MiniTransaction mtr, SegmentRef segment) {
        if (mtr == null || segment == null) {
            throw new DatabaseValidationException("LOB segment preflight args must not be null");
        }
        requireLobSegment(mtr, segment);
    }

    /**
     * 按冻结计划写出新链并返回仍由调用方持有的 ownership guard。purpose preflight 是本方法的第一项 FSP 行为；
     * 任一 allocation/format 失败会在同一 active MTR 内尽力反序回收本次已分配页并保留原始失败。
     */
    public LobWriteAllocation writePlanned(MiniTransaction mtr, LobWritePlan plan) {
        if (mtr == null || plan == null) {
            throw new DatabaseValidationException("planned LOB write arguments must not be null");
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            throw new LobAllocationStateException("planned LOB write requires ACTIVE MTR: " + mtr.state());
        }
        validateFrozenPlan(plan);
        SegmentRef segment = plan.segment();
        byte[] payload = plan.payloadUnsafe();
        int capacity = LobPageLayout.payloadCapacity(pageSize);
        List<PageId> allocated = new ArrayList<>(plan.pageCount());

        // identity/purpose 复核严格早于 reserve/allocate；纯 plan 阶段不读取 inode，也不产生任何物理副作用。
        requireLobSegment(mtr, segment);
        try (SpaceReservation ignored = diskSpace.reserveSpace(
                mtr, segment.spaceId(), SpaceReservationKind.BLOB, plan.pageCount(), 0)) {
            try {
                for (int i = 0; i < plan.pageCount(); i++) {
                    allocated.add(diskSpace.allocatePage(mtr, segment, PageType.BLOB));
                }
                for (int i = 0; i < plan.pageCount(); i++) {
                    int offset = i * capacity;
                    int length = Math.min(capacity, payload.length - offset);
                    byte[] chunk = Arrays.copyOfRange(payload, offset, offset + length);
                    PageId pageId = allocated.get(i);
                    PageGuard guard = mtr.getPage(pool, pageId, PageLatchMode.EXCLUSIVE);
                    long previous = i == 0 ? FilePageHeader.FIL_NULL : allocated.get(i - 1).pageNo().value();
                    long next = i + 1 == plan.pageCount()
                            ? FilePageHeader.FIL_NULL : allocated.get(i + 1).pageNo().value();
                    LobPage.format(guard, pageId, previous, next, i, plan.pageCount(),
                            segment.segmentId(), segment.inodeSlot(), plan.totalLength(), plan.crc32(), chunk);
                }
            } catch (RuntimeException writeFailure) {
                reclaimPartialAllocation(mtr, segment, allocated, writeFailure);
                throw writeFailure;
            }
        }
        LobReference reference = new LobReference(segment.spaceId(), allocated.getFirst().pageNo(),
                plan.totalLength(), plan.pageCount(), segment.segmentId(), segment.inodeSlot(), plan.crc32());
        ColumnValue.ExternalValue external = new ColumnValue.ExternalValue(plan.type().typeId(), reference,
                plan.inlinePrefix());
        return new LobWriteAllocation(this, mtr, segment, allocated, external);
    }

    /**
     * 把完整逻辑值写为新的 LOB 页链并立即转移 ownership；保留给显式低层调用者。需要和 undo/row 原子发布的上层
     * 必须直接使用 plan/writePlanned guard，不能通过本便利方法丢失失败补偿窗口。
     *
     * @param mtr 调用方写 MTR；失败时调用方仍必须 rollback，释放 memo 与 redo reservation。
     * @param segment purpose=LOB 的 segment。
     * @param type TEXT/BLOB/JSON family 类型。
     * @param value 完整 StringValue/BinaryValue，不能是已有 external reference。
     * @return 带 32B 内安全 prefix 的 external value；调用方随后把它编码进聚簇记录。
     */
    public ColumnValue.ExternalValue write(MiniTransaction mtr, SegmentRef segment,
                                           ColumnType type, ColumnValue value) {
        try (LobWriteAllocation allocation = writePlanned(mtr, planWrite(segment, type, value))) {
            ColumnValue.ExternalValue external = allocation.value();
            allocation.transferOwnership();
            return external;
        }
    }

    /**
     * 读取并校验 external value 的完整页链。链中任一信封、链接、归属、长度或 CRC 不一致都 fail-closed。
     */
    public ColumnValue read(MiniTransaction mtr, ColumnType type, ColumnValue.ExternalValue external) {
        if (mtr == null || type == null || external == null) {
            throw new DatabaseValidationException("LOB read arguments must not be null");
        }
        LobCodec codec = requireLobCodec(type);
        codec.validate(external, type);
        LoadedChain chain = loadChain(mtr, external.reference());
        return codec.logicalValueFromStorage(chain.payload(), type);
    }

    /**
     * 校验后释放整条页链。先完成所有只读校验并释放 S latch，再修改 FSP；这样损坏链不会造成部分回收。
     * 释放页在同一 MTR 重新 PAGE_INIT(ALLOCATED)，使旧引用立即因 page type 不匹配而失败。
     */
    public void free(MiniTransaction mtr, SegmentRef segment, ColumnType type,
                     ColumnValue.ExternalValue external) {
        requireArguments(mtr, segment, type);
        if (external == null) {
            throw new DatabaseValidationException("external LOB value must not be null");
        }
        LobCodec codec = requireLobCodec(type);
        codec.validate(external, type);
        LobReference reference = external.reference();
        if (!reference.spaceId().equals(segment.spaceId())
                || !reference.segmentId().equals(segment.segmentId())
                || reference.inodeSlot() != segment.inodeSlot()) {
            throw new LobSegmentMismatchException("LOB reference does not belong to supplied segment");
        }
        LoadedChain chain = loadChain(mtr, reference);
        requireLobSegment(mtr, segment);
        try (MtrLatchOrderScope ignored = mtr.allowOutOfOrderPageLatch(
                "LOB free follows a fully validated acyclic reference chain; FSP page0/page2 are already ordered")) {
            for (PageId pageId : chain.pageIds()) {
                diskSpace.freePage(mtr, segment, pageId);
                reinitializeFreedPage(mtr, pageId);
            }
        }
    }

    /** 写操作在 begin 前使用的保守 redo workload；每个 LOB 页另计 FSP/INODE/XDES 与 page body 余量。 */
    public RedoBudgetWorkload writeWorkload(int logicalLength) {
        int count = pageCount(logicalLength, LobPageLayout.payloadCapacity(pageSize));
        return RedoBudgetWorkload.pageImages(checkedWorkload(8L + count * 8L));
    }

    /** free 操作在 begin 前使用的保守 redo workload。 */
    public RedoBudgetWorkload freeWorkload(int pageCount) {
        if (pageCount <= 0) {
            throw new DatabaseValidationException("LOB free page count must be positive");
        }
        return RedoBudgetWorkload.pageImages(checkedWorkload(8L + pageCount * 6L));
    }

    /** 沿 reference 精确页数读取，逐页释放 S latch，并在返回前验证整值长度/CRC。 */
    private LoadedChain loadChain(MiniTransaction mtr, LobReference reference) {
        int capacity = LobPageLayout.payloadCapacity(pageSize);
        int canonicalPageCount = pageCount(reference.totalLength(), capacity);
        if (reference.pageCount() != canonicalPageCount) {
            throw new LobPageCorruptedException("LOB reference page count is non-canonical: expected="
                    + canonicalPageCount + ", actual=" + reference.pageCount());
        }
        byte[] payload = new byte[reference.totalLength()];
        List<PageId> pageIds = new ArrayList<>(reference.pageCount());
        Set<Long> visited = new HashSet<>();
        long currentPageNo = reference.firstPageNo().value();
        long expectedPrevious = FilePageHeader.FIL_NULL;
        int payloadOffset = 0;

        for (int index = 0; index < reference.pageCount(); index++) {
            if (currentPageNo == FilePageHeader.FIL_NULL || !visited.add(currentPageNo)) {
                throw new LobPageCorruptedException("LOB chain ended early or contains a cycle at index " + index);
            }
            PageId pageId = PageId.of(reference.spaceId(), PageNo.of(currentPageNo));
            PageGuard guard = mtr.getPage(pool, pageId, PageLatchMode.SHARED);
            LobPage.Snapshot page = LobPage.read(guard, pageId, capacity);
            validatePageAgainstReference(page, reference, index, expectedPrevious, pageId);
            int expectedChunkLength = Math.min(capacity, payload.length - payloadOffset);
            if (page.chunk().length != expectedChunkLength) {
                throw new LobPageCorruptedException("LOB chunk length is non-canonical at " + pageId
                        + ": expected=" + expectedChunkLength + ", actual=" + page.chunk().length);
            }
            if (page.chunk().length > payload.length - payloadOffset) {
                throw new LobPageCorruptedException("LOB chunks exceed reference length at " + pageId);
            }
            System.arraycopy(page.chunk(), 0, payload, payloadOffset, page.chunk().length);
            payloadOffset += page.chunk().length;
            pageIds.add(pageId);
            expectedPrevious = currentPageNo;
            currentPageNo = page.nextPageNo();
            mtr.releaseLatch(pageId, guard);
        }
        if (currentPageNo != FilePageHeader.FIL_NULL || payloadOffset != payload.length) {
            throw new LobPageCorruptedException("LOB chain length/tail mismatch: pages=" + reference.pageCount()
                    + ", payload=" + payloadOffset + "/" + payload.length + ", next=" + currentPageNo);
        }
        long actualCrc = crc32(payload);
        if (actualCrc != reference.crc32()) {
            throw new LobPageCorruptedException("LOB whole-value CRC mismatch: expected="
                    + reference.crc32() + ", actual=" + actualCrc);
        }
        return new LoadedChain(payload, List.copyOf(pageIds));
    }

    private static void validatePageAgainstReference(LobPage.Snapshot page, LobReference reference, int index,
                                                     long expectedPrevious, PageId pageId) {
        boolean tailLinkValid = index + 1 < reference.pageCount()
                ? page.nextPageNo() != FilePageHeader.FIL_NULL
                : page.nextPageNo() == FilePageHeader.FIL_NULL;
        if (page.previousPageNo() != expectedPrevious || !tailLinkValid
                || page.chunkIndex() != index || page.pageCount() != reference.pageCount()
                || !page.segmentId().equals(reference.segmentId()) || page.inodeSlot() != reference.inodeSlot()
                || page.totalLength() != reference.totalLength() || page.wholeCrc32() != reference.crc32()) {
            throw new LobPageCorruptedException("LOB chain/reference metadata mismatch at " + pageId
                    + " index=" + index);
        }
    }

    private void requireLobSegment(MiniTransaction mtr, SegmentRef segment) {
        try {
            diskSpace.requireSegmentPurposeForWrite(mtr, segment, SegmentPurpose.LOB);
        } catch (FspMetadataException mismatch) {
            throw new LobSegmentMismatchException("LOB operation requires matching LOB segment: " + segment, mismatch);
        }
    }

    /** 执行前重算所有可派生字段，阻止损坏/错误来源的 plan 绕过 canonical pageCount/CRC/prefix/workload。 */
    private void validateFrozenPlan(LobWritePlan plan) {
        byte[] payload = plan.payloadUnsafe();
        int expectedPageCount = pageCount(payload.length, LobPageLayout.payloadCapacity(pageSize));
        LobCodec codec = requireLobCodec(plan.type());
        if (payload.length <= LobCodec.INLINE_PAYLOAD_LIMIT
                || plan.pageCount() != expectedPageCount
                || plan.crc32() != crc32(payload)
                || !Arrays.equals(plan.inlinePrefix(), codec.inlinePrefix(payload, plan.type()))
                || !plan.workload().equals(writeWorkload(payload.length))) {
            throw new LobAllocationStateException("frozen LOB write plan consistency check failed");
        }
    }

    /** 写失败后在同一 MTR 尽力归还已记入 FSP 的页；清理失败保留为 suppressed，不能覆盖原始根因。 */
    private void reclaimPartialAllocation(MiniTransaction mtr, SegmentRef segment, List<PageId> allocated,
                                          RuntimeException original) {
        if (allocated.isEmpty()) {
            return;
        }
        try {
            compensateAllocation(mtr, segment, allocated);
        } catch (RuntimeException cleanupFailure) {
            original.addSuppressed(cleanupFailure);
        }
    }

    /**
     * Guard 专用补偿原语：只接受当前 active MTR 刚分配且尚未发布的页，按 allocation 反序 free/reinitialize。
     * 继续尝试全部页并把后续失败压到首个根因，禁止对已发布普通 LOB 调用。
     */
    void compensateAllocation(MiniTransaction mtr, SegmentRef segment, List<PageId> allocated) {
        if (mtr == null || segment == null || allocated == null || allocated.isEmpty()) {
            throw new DatabaseValidationException("LOB allocation compensation args must not be null/empty");
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            throw new LobAllocationStateException("LOB allocation compensation requires ACTIVE MTR: " + mtr.state());
        }
        RuntimeException firstFailure = null;
        for (int i = allocated.size() - 1; i >= 0; i--) {
            PageId pageId = allocated.get(i);
            try {
                diskSpace.freePage(mtr, segment, pageId);
                reinitializeFreedPage(mtr, pageId);
            } catch (RuntimeException cleanupFailure) {
                if (firstFailure == null) {
                    firstFailure = cleanupFailure;
                } else {
                    firstFailure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (firstFailure != null) {
            throw new LobAllocationStateException("failed to compensate newly allocated LOB page chain",
                    firstFailure);
        }
    }

    /** FSP free intent 后清零页并写 PAGE_INIT(ALLOCATED)，旧 external reference 不会继续读到历史 payload。 */
    private void reinitializeFreedPage(MiniTransaction mtr, PageId pageId) {
        PageGuard guard = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.ALLOCATED);
        PageEnvelope.writeHeader(guard, new FilePageHeader(pageId.spaceId(), pageId.pageNo().value(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.ALLOCATED));
    }

    private LobCodec requireLobCodec(ColumnType type) {
        TypeCodec codec = codecs.codecFor(type);
        if (!(codec instanceof LobCodec lobCodec)) {
            throw new UnsupportedColumnTypeException("type is not overflow-capable LOB: " + type.typeId());
        }
        return lobCodec;
    }

    private static void requireArguments(MiniTransaction mtr, SegmentRef segment, ColumnType type) {
        if (mtr == null || segment == null || type == null) {
            throw new DatabaseValidationException("LOB operation arguments must not be null");
        }
    }

    private static int pageCount(int length, int capacity) {
        if (length <= 0) {
            throw new DatabaseValidationException("LOB logical length must be positive: " + length);
        }
        return (int) (((long) length + capacity - 1L) / capacity);
    }

    private static long checkedWorkload(long pageImages) {
        if (pageImages <= 0) {
            throw new DatabaseValidationException("LOB redo workload overflow");
        }
        return pageImages;
    }

    private static long crc32(byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(payload);
        return crc.getValue();
    }

    /** 完整校验后的页链快照；读 latch 均已释放，可安全进入 FSP 修改阶段。 */
    private record LoadedChain(byte[] payload, List<PageId> pageIds) {
    }
}
