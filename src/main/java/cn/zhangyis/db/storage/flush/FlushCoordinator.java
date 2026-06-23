package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.DirtyPageCandidate;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.fil.io.PageStore;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessController;
import cn.zhangyis.db.storage.fil.access.TablespaceAccessLease;
import cn.zhangyis.db.storage.flush.doublewrite.DoublewriteStrategy;
import cn.zhangyis.db.storage.page.PageImageChecksum;
import cn.zhangyis.db.storage.redo.RedoLogManager;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * F1 同步 Flush 协调器。它只编排 dirty page snapshot、redo durable gate、doublewrite、data file write 和
 * Buffer Pool clean/keep-dirty 回调，不维护 Buffer Pool 内部链表，也不直接操作 FileChannel。
 */
public final class FlushCoordinator {

    private final BufferPool bufferPool;
    private final PageStore pageStore;
    private final RedoLogManager redo;
    private final PageSize pageSize;
    private final DoublewriteStrategy doublewrite;
    private final Duration redoWaitTimeout;
    /** 与 MTR/truncate 共用的 operation lease；flush 持共享 lease 覆盖 snapshot 到 data-file force。 */
    private final TablespaceAccessController accessController;

    public FlushCoordinator(BufferPool bufferPool, PageStore pageStore, RedoLogManager redo, PageSize pageSize,
                            DoublewriteStrategy doublewrite, Duration redoWaitTimeout) {
        this(bufferPool, pageStore, redo, pageSize, doublewrite, redoWaitTimeout,
                new TablespaceAccessController());
    }

    /**
     * 创建与 lifecycle 服务共享准入控制器的 flush 协调器。截断线程持 X 时可在同线程重入 S 完成 marker flush；
     * 其它 flusher 会在 X 外等待，不能把尾页 snapshot 跨越物理 truncate。
     */
    public FlushCoordinator(BufferPool bufferPool, PageStore pageStore, RedoLogManager redo, PageSize pageSize,
                            DoublewriteStrategy doublewrite, Duration redoWaitTimeout,
                            TablespaceAccessController accessController) {
        if (bufferPool == null || pageStore == null || redo == null || pageSize == null
                || doublewrite == null || redoWaitTimeout == null || accessController == null) {
            throw new DatabaseValidationException("flush coordinator dependencies must not be null");
        }
        if (redoWaitTimeout.isNegative()) {
            throw new DatabaseValidationException("redo wait timeout must not be negative: " + redoWaitTimeout);
        }
        this.bufferPool = bufferPool;
        this.pageStore = pageStore;
        this.redo = redo;
        this.pageSize = pageSize;
        this.doublewrite = doublewrite;
        this.redoWaitTimeout = redoWaitTimeout;
        this.accessController = accessController;
    }

    /**
     * 按 Buffer Pool dirty view 选择 oldest <= targetLsn 的页，逐页同步 flush。
     *
     * @param targetLsn flush list 目标 LSN。
     * @param maxPages 最多刷页数。
     * @return 每个候选页的结果。
     */
    public List<FlushResult> flushList(Lsn targetLsn, int maxPages) {
        if (targetLsn == null) {
            throw new DatabaseValidationException("target LSN must not be null");
        }
        if (maxPages < 0) {
            throw new DatabaseValidationException("max pages must not be negative: " + maxPages);
        }
        List<FlushResult> results = new ArrayList<>();
        for (DirtyPageCandidate candidate : bufferPool.dirtyPageCandidates(targetLsn, maxPages)) {
            results.add(flushPage(candidate.pageId(), candidate.newestModificationLsn()));
        }
        return List.copyOf(results);
    }

    /**
     * 同步刷一个指定页。若页当前不脏或仍被 fixed，则返回 SKIPPED_NOT_DIRTY。
     *
     * @param pageId 目标页。
     * @return flush 结果。
     */
    public FlushResult singlePageFlush(PageId pageId) {
        if (pageId == null) {
            throw new DatabaseValidationException("page id must not be null");
        }
        return flushPage(pageId, Lsn.of(0));
    }

    private FlushResult flushPage(PageId pageId, Lsn observedPageLsn) {
        try (TablespaceAccessLease ignored = accessController.acquireShared(pageId.spaceId())) {
            return flushPageUnderLease(pageId, observedPageLsn);
        }
    }

    /** 调用方已持目标空间共享 operation lease；整个 snapshot/doublewrite/data force/clean 回调不可跨越 truncate X。 */
    private FlushResult flushPageUnderLease(PageId pageId, Lsn observedPageLsn) {
        Optional<FlushPageSnapshot> maybeSnapshot = bufferPool.snapshotForFlush(pageId);
        if (maybeSnapshot.isEmpty()) {
            return FlushResult.ok(pageId, observedPageLsn, FlushResultStatus.SKIPPED_NOT_DIRTY);
        }
        FlushPageSnapshot snapshot = maybeSnapshot.orElseThrow();
        if (snapshot.pageLsn().value() > redo.flushedToDiskLsn().value()
                && !redo.waitFlushed(snapshot.pageLsn(), redoWaitTimeout)) {
            return FlushResult.ok(pageId, snapshot.pageLsn(), FlushResultStatus.SKIPPED_REDO_NOT_DURABLE);
        }
        try {
            byte[] image = snapshot.pageImage();
            PageImageChecksum.stamp(image, pageSize);
            FlushPageSnapshot stamped = new FlushPageSnapshot(snapshot.pageId(), snapshot.pageLsn(),
                    snapshot.dirtyVersion(), image);
            doublewrite.beforeDataFileWrite(stamped);
            pageStore.writePage(stamped.pageId(), ByteBuffer.wrap(stamped.pageImage()));
            pageStore.force(stamped.pageId().spaceId());
            boolean clean = bufferPool.completeFlush(stamped);
            doublewrite.afterDataFileWrite(stamped);
            return FlushResult.ok(pageId, snapshot.pageLsn(),
                    clean ? FlushResultStatus.CLEAN : FlushResultStatus.KEPT_DIRTY);
        } catch (DatabaseRuntimeException e) {
            bufferPool.failFlush(pageId);
            return FlushResult.failed(pageId, snapshot.pageLsn(), e);
        }
    }
}
