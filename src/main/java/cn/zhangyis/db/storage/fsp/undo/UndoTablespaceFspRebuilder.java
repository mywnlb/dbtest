package cn.zhangyis.db.storage.fsp.undo;
import cn.zhangyis.db.storage.fsp.extent.ExtentDescriptorRepository;
import cn.zhangyis.db.storage.fsp.extent.FreeExtentService;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderRepository;
import cn.zhangyis.db.storage.fsp.header.SpaceHeaderSnapshot;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleHeader;


import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;

/**
 * 物理截断后的 UNDO FSP 基线重建器。它只重建 page0 与 page2：两页先经 PAGE_INIT 清零并写 FIL 信封，
 * 再写空 FSP header/extent0 XDES/空 inode array/同 epoch TRUNCATING marker。普通 extent 延续现有
 * lazy initialization 语义，{@code freeLimit=0}，后续分配按现有 FreeExtentService 纳入管理。
 */
public final class UndoTablespaceFspRebuilder {

    private final BufferPool pool;
    private final PageSize pageSize;
    private final SpaceHeaderRepository headerRepository;
    private final ExtentDescriptorRepository extentRepository;

    public UndoTablespaceFspRebuilder(BufferPool pool, PageSize pageSize) {
        if (pool == null || pageSize == null) {
            throw new DatabaseValidationException("undo FSP rebuilder pool/pageSize must not be null");
        }
        this.pool = pool;
        this.pageSize = pageSize;
        this.headerRepository = new SpaceHeaderRepository(pool);
        this.extentRepository = new ExtentDescriptorRepository(pool, pageSize);
    }

    /**
     * 在一个 MTR 内建立空空间基线。调用方必须已持 lifecycle X 且物理文件已缩到 target。
     *
     * @param mtr 维护 MTR。
     * @param previous 截断前/恢复时可读的 FSP 头，用于保留 flags/serverVersion 并推进 spaceVersion。
     * @param marker 同 epoch 的 TRUNCATING marker。
     */
    public void rebuild(MiniTransaction mtr, SpaceHeaderSnapshot previous, TablespaceLifecycleHeader marker) {
        if (mtr == null || previous == null || marker == null) {
            throw new DatabaseValidationException("undo FSP rebuild args must not be null");
        }
        SpaceId spaceId = previous.spaceId();
        PageNo target = marker.targetSizeInPages();
        // page0 先经 PAGE_INIT 清零并发 redo（截断后必须重置可能残留的旧 page0）；FSP_HDR 信封头不再在此显式写，
        // 而由下方 headerRepository.initialize 统一盖戳（同一 MTR 内 re-entrant 复用本 newPage 的 page0 X guard），
        // 避免两处各写一遍同一信封不变量。
        mtr.newPage(pool, PageId.of(spaceId, PageNo.of(0)),
                PageLatchMode.EXCLUSIVE, PageType.FSP_HDR);
        PageGuard page2 = mtr.newPage(pool, PageId.of(spaceId, PageNo.of(2)),
                PageLatchMode.EXCLUSIVE, PageType.INODE);
        PageEnvelope.writeHeader(page2, new FilePageHeader(spaceId, 2L,
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.INODE));

        SpaceHeaderSnapshot fresh = new SpaceHeaderSnapshot(spaceId, pageSize, previous.spaceFlags(),
                target, PageNo.of(0), 1L, FlstBase.EMPTY, FlstBase.EMPTY, FlstBase.EMPTY,
                PageNo.of(2), 0L, previous.serverVersion(), previous.spaceVersion() + 1);
        headerRepository.initialize(mtr, fresh);
        headerRepository.writeLifecycle(mtr, spaceId, marker);
        extentRepository.reserveSystemExtent(mtr, spaceId);
    }
}
