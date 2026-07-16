package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fil.meta.TablespaceRegistry;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MtrSavepoint;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * External undo payload 页链的物理读写协作者。写路径只操作调用方已经预留容量的同一 undo segment；读路径逐页复制
 * chunk 后回滚到 MTR savepoint，避免长版本链读取同时 pin 全部 payload frame。
 */
final class UndoPayloadStorage {

    private final BufferPool pool;
    private final PageSize pageSize;
    private final TablespaceRegistry tablespaceRegistry;

    UndoPayloadStorage(BufferPool pool, PageSize pageSize, TablespaceRegistry tablespaceRegistry) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("undo payload storage pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.tablespaceRegistry = tablespaceRegistry;
    }

    /** 分配、格式化完整页链并返回尚未发布到普通 UNDO 页的根描述符。 */
    UndoPayloadDescriptor write(MiniTransaction mtr, UndoSpaceAllocator allocator, UndoSegmentHandle handle,
                                UndoRecordWritePlan plan) {
        if (mtr == null || allocator == null || handle == null || plan == null || !plan.external()) {
            throw new DatabaseValidationException("external undo payload write args/mode invalid");
        }
        byte[] payload = plan.encodedPayloadUnsafe();
        int capacity = UndoPayloadPageLayout.payloadCapacity(pageSize);
        List<PageId> pages = new ArrayList<>(plan.externalPageCount());
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "external undo allocation: fresh same-segment pages are unreachable before root descriptor")) {
            for (int i = 0; i < plan.externalPageCount(); i++) {
                pages.add(allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId()));
            }
            for (int i = 0; i < pages.size(); i++) {
                int offset = i * capacity;
                int length = Math.min(capacity, payload.length - offset);
                byte[] chunk = new byte[length];
                System.arraycopy(payload, offset, chunk, 0, length);
                PageId pageId = pages.get(i);
                requireOrdinaryAccess(mtr, pageId.spaceId());
                PageGuard guard = mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.UNDO_PAYLOAD);
                long previous = i == 0 ? FilePageHeader.FIL_NULL : pages.get(i - 1).pageNo().value();
                long next = i + 1 == pages.size() ? FilePageHeader.FIL_NULL : pages.get(i + 1).pageNo().value();
                UndoPayloadPage.format(guard, pageId, previous, next, i, handle,
                        plan.record().transactionId(), plan.record().undoNo(), payload.length,
                        pages.size(), plan.crc32(), chunk);
            }
        }
        return new UndoPayloadDescriptor(plan.record().type(), plan.record().transactionId(),
                plan.record().undoNo(), pages.get(0).pageNo(), payload.length, pages.size(), plan.crc32());
    }

    /**
     * prepared append 阶段固定全部 external payload 页并取得 X guard，但不写 placeholder body。新页此时仍不可达；
     * root descriptor 只会在 {@link #writePrepared(UndoSegmentHandle, UndoRecordWritePlan, List)} 完成真实 body 后发布。
     */
    List<PageId> preparePages(MiniTransaction mtr, UndoSpaceAllocator allocator, UndoSegmentHandle handle,
                              int pageCount) {
        if (mtr == null || allocator == null || handle == null || pageCount <= 0) {
            throw new DatabaseValidationException("prepared undo payload page arguments are invalid");
        }
        List<PageId> pages = new ArrayList<>(pageCount);
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "prepared external undo pages are fresh and unreachable; FSP never waits for undo page latches")) {
            for (int i = 0; i < pageCount; i++) {
                PageId pageId = allocator.allocatePage(mtr, handle.spaceId(), handle.inodeSlot(), handle.segmentId());
                requireOrdinaryAccess(mtr, pageId.spaceId());
                mtr.newPage(pool, pageId, PageLatchMode.EXCLUSIVE, PageType.UNDO_PAYLOAD);
                pages.add(pageId);
            }
        }
        return List.copyOf(pages);
    }

    /**
     * 把真实 undo payload 写入 prepare 阶段已经固定的页。页数与 retained X guard 必须精确匹配；任何偏差表示
     * prepared shape 被破坏，必须在 root descriptor/record slot 发布前拒绝。
     */
    UndoPayloadDescriptor writePrepared(MiniTransaction mtr, UndoSegmentHandle handle,
                                        UndoRecordWritePlan plan, List<PageId> pages) {
        if (mtr == null || handle == null || plan == null || pages == null || !plan.external()
                || pages.size() != plan.externalPageCount()) {
            throw new DatabaseValidationException("prepared undo payload shape mismatch");
        }
        byte[] payload = plan.encodedPayloadUnsafe();
        int capacity = UndoPayloadPageLayout.payloadCapacity(pageSize);
        for (int i = 0; i < pages.size(); i++) {
            PageId pageId = pages.get(i);
            PageGuard guard = mtr.retainedExclusivePage(pageId);
            if (guard == null) {
                throw new UndoLogFormatException("prepared undo payload page lost its X guard: " + pageId);
            }
            int offset = i * capacity;
            int length = Math.min(capacity, payload.length - offset);
            byte[] chunk = new byte[length];
            System.arraycopy(payload, offset, chunk, 0, length);
            long previous = i == 0 ? FilePageHeader.FIL_NULL : pages.get(i - 1).pageNo().value();
            long next = i + 1 == pages.size() ? FilePageHeader.FIL_NULL : pages.get(i + 1).pageNo().value();
            UndoPayloadPage.format(guard, pageId, previous, next, i, handle,
                    plan.record().transactionId(), plan.record().undoNo(), payload.length,
                    pages.size(), plan.crc32(), chunk);
        }
        return new UndoPayloadDescriptor(plan.record().type(), plan.record().transactionId(),
                plan.record().undoNo(), pages.getFirst().pageNo(), payload.length, pages.size(), plan.crc32());
    }


    /** 严格读取 descriptor 指向的页链；任何 link、owner、长度、页数或 CRC 不一致都拒绝返回部分记录。 */
    byte[] read(MiniTransaction mtr, SpaceId spaceId, SegmentIdentity owner,
                UndoPayloadDescriptor descriptor, int maxExternalPages) {
        if (mtr == null || spaceId == null || owner == null || descriptor == null) {
            throw new DatabaseValidationException("external undo payload read args must not be null");
        }
        if (descriptor.pageCount() > maxExternalPages) {
            throw new UndoPayloadTooLargeException("stored external undo payload uses " + descriptor.pageCount()
                    + " pages, configured maximum is " + maxExternalPages);
        }
        int capacity = UndoPayloadPageLayout.payloadCapacity(pageSize);
        int canonicalPages = (int) (((long) descriptor.totalLength() + capacity - 1L) / capacity);
        if (canonicalPages != descriptor.pageCount()) {
            throw new UndoLogFormatException("external undo descriptor non-canonical page count: expected="
                    + canonicalPages + " actual=" + descriptor.pageCount());
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(descriptor.totalLength());
        long pageNo = descriptor.firstPageNo().value();
        long expectedPrevious = FilePageHeader.FIL_NULL;
        Set<Long> visited = new HashSet<>(descriptor.pageCount());
        for (int index = 0; index < descriptor.pageCount(); index++) {
            if (pageNo >= FilePageHeader.FIL_NULL || !visited.add(pageNo)) {
                throw new UndoLogFormatException("external undo payload has premature tail/invalid/cyclic page at index "
                        + index + ": " + pageNo);
            }
            PageId pageId = PageId.of(spaceId, PageNo.of(pageNo));
            UndoPayloadPage.Snapshot page;
            PageGuard retained = mtr.retainedExclusivePage(pageId);
            if (retained != null) {
                page = UndoPayloadPage.read(retained, pageId, capacity);
            } else {
                MtrSavepoint savepoint = mtr.savepoint();
                try (var ignored = mtr.allowOutOfOrderPageLatch(
                        "external undo read follows validated logical links, independent of physical page order")) {
                    requireOrdinaryAccess(mtr, spaceId);
                    PageGuard guard = mtr.getPage(pool, pageId, PageLatchMode.SHARED);
                    page = UndoPayloadPage.read(guard, pageId, capacity);
                } finally {
                    mtr.rollbackToSavepoint(savepoint);
                }
            }
            validatePage(pageId, page, owner, descriptor, index, expectedPrevious);
            int expectedChunk = Math.min(capacity, descriptor.totalLength() - out.size());
            if (page.chunk().length != expectedChunk) {
                throw new UndoLogFormatException("external undo chunk length mismatch at " + pageId
                        + ": expected=" + expectedChunk + " actual=" + page.chunk().length);
            }
            out.writeBytes(page.chunk());
            expectedPrevious = pageNo;
            pageNo = page.nextPageNo();
        }
        if (pageNo != FilePageHeader.FIL_NULL || out.size() != descriptor.totalLength()) {
            throw new UndoLogFormatException("external undo payload chain tail/length mismatch");
        }
        byte[] payload = out.toByteArray();
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        if (crc32.getValue() != descriptor.crc32()) {
            throw new UndoLogFormatException("external undo payload CRC mismatch");
        }
        return payload;
    }

    private static void validatePage(PageId pageId, UndoPayloadPage.Snapshot page, SegmentIdentity owner,
                                     UndoPayloadDescriptor descriptor, int index, long expectedPrevious) {
        boolean tailLinkValid = index + 1 < descriptor.pageCount()
                ? page.nextPageNo() < FilePageHeader.FIL_NULL
                : page.nextPageNo() == FilePageHeader.FIL_NULL;
        if (page.previousPageNo() != expectedPrevious || !tailLinkValid || page.chunkIndex() != index
                || !page.segmentId().equals(owner.segmentId()) || page.inodeSlot() != owner.inodeSlot()
                || !page.transactionId().equals(descriptor.transactionId())
                || !page.undoNo().equals(descriptor.undoNo())
                || page.totalLength() != descriptor.totalLength() || page.pageCount() != descriptor.pageCount()
                || page.wholeCrc32() != descriptor.crc32()) {
            throw new UndoLogFormatException("external undo payload page identity/link mismatch at " + pageId);
        }
    }

    private void requireOrdinaryAccess(MiniTransaction mtr, SpaceId spaceId) {
        if (tablespaceRegistry != null) {
            mtr.acquireTablespaceLease(spaceId);
            tablespaceRegistry.require(spaceId);
        }
    }

    /** 根 UNDO 页提供的 segment 所有权，避免 direct RollPointer 读取依赖 first-page handle。 */
    record SegmentIdentity(cn.zhangyis.db.domain.SegmentId segmentId, int inodeSlot) {
        SegmentIdentity {
            if (segmentId == null || segmentId.value() <= 0 || inodeSlot < 0) {
                throw new DatabaseValidationException("invalid external undo segment identity");
            }
        }
    }
}
