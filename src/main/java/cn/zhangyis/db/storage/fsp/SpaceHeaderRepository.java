package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * SpaceHeaderPage（page 0）仓储（设计 §6.2）。经 MTR 持 page 0 latch 读写 header 字段；写须 X latch。
 * 三个 extent list 头为 FLST base，由 {@link Flst} 维护——repo 只负责 initialize/read 整 base 与暴露 base 地址访问器。
 *
 * <p>简化点：本切片 no-redo，写页只标脏、不产 redo，不声明 crash-safe（设计 §15 redo 规则推迟满足）。
 */
public final class SpaceHeaderRepository {

    /** 受控页来源；本仓储只经 MTR.getPage 拿 page 0 的 PageGuard。 */
    private final BufferPool pool;

    public SpaceHeaderRepository(BufferPool pool) {
        if (pool == null) {
            throw new DatabaseValidationException("buffer pool must not be null");
        }
        this.pool = pool;
    }

    private static PageId page0(SpaceId spaceId) {
        return PageId.of(spaceId, PageNo.of(0));
    }

    /** 写入全部 header 字段（X）。三个 extent list base 按 snapshot 写入（新建表空间应为 EMPTY）。 */
    public void initialize(MiniTransaction mtr, SpaceHeaderSnapshot h) {
        requireMtr(mtr);
        if (h == null) {
            throw new DatabaseValidationException("space header snapshot must not be null");
        }
        PageGuard g = mtr.getPage(pool, page0(h.spaceId()), PageLatchMode.EXCLUSIVE);
        g.writeInt(SpaceHeaderLayout.SPACE_ID, h.spaceId().value());
        g.writeInt(SpaceHeaderLayout.PAGE_SIZE_BYTES, h.pageSize().bytes());
        g.writeInt(SpaceHeaderLayout.SPACE_FLAGS, h.spaceFlags());
        g.writeLong(SpaceHeaderLayout.CURRENT_SIZE, h.currentSizeInPages().value());
        g.writeLong(SpaceHeaderLayout.FREE_LIMIT, h.freeLimitPageNo().value());
        g.writeLong(SpaceHeaderLayout.NEXT_SEGMENT_ID, h.nextSegmentId());
        h.freeExtentList().writeTo(g, SpaceHeaderLayout.FREE_EXTENT_LIST_BASE);
        h.freeFragExtentList().writeTo(g, SpaceHeaderLayout.FREE_FRAG_LIST_BASE);
        h.fullFragExtentList().writeTo(g, SpaceHeaderLayout.FULL_FRAG_LIST_BASE);
        g.writeLong(SpaceHeaderLayout.FIRST_INODE_PAGE, h.firstInodePageNo().value());
        g.writeLong(SpaceHeaderLayout.SDI_ROOT, h.sdiRootPageNo());
        g.writeInt(SpaceHeaderLayout.SERVER_VERSION, h.serverVersion());
        g.writeLong(SpaceHeaderLayout.SPACE_VERSION, h.spaceVersion());
    }

    /** 读出全部 header 字段（S）；三个 list base 经 FlstBase.readFrom 解码（含空链一致性校验）。 */
    public SpaceHeaderSnapshot read(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.SHARED);
        return new SpaceHeaderSnapshot(
                SpaceId.of(g.readInt(SpaceHeaderLayout.SPACE_ID)),
                PageSize.ofBytes(g.readInt(SpaceHeaderLayout.PAGE_SIZE_BYTES)),
                g.readInt(SpaceHeaderLayout.SPACE_FLAGS),
                PageNo.of(g.readLong(SpaceHeaderLayout.CURRENT_SIZE)),
                PageNo.of(g.readLong(SpaceHeaderLayout.FREE_LIMIT)),
                g.readLong(SpaceHeaderLayout.NEXT_SEGMENT_ID),
                FlstBase.readFrom(g, SpaceHeaderLayout.FREE_EXTENT_LIST_BASE),
                FlstBase.readFrom(g, SpaceHeaderLayout.FREE_FRAG_LIST_BASE),
                FlstBase.readFrom(g, SpaceHeaderLayout.FULL_FRAG_LIST_BASE),
                PageNo.of(g.readLong(SpaceHeaderLayout.FIRST_INODE_PAGE)),
                g.readLong(SpaceHeaderLayout.SDI_ROOT),
                g.readInt(SpaceHeaderLayout.SERVER_VERSION),
                g.readLong(SpaceHeaderLayout.SPACE_VERSION));
    }

    public void setCurrentSizeInPages(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.CURRENT_SIZE, requireValue(value).value());
    }

    public void setFreeLimitPageNo(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.FREE_LIMIT, requireValue(value).value());
    }

    public void setFirstInodePageNo(MiniTransaction mtr, SpaceId spaceId, PageNo value) {
        writeLongField(mtr, spaceId, SpaceHeaderLayout.FIRST_INODE_PAGE, requireValue(value).value());
    }

    /** FSP_FREE 链 base 地址（page0 内固定偏移），供 Flst/2b 维护链。 */
    public FileAddress freeExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FREE_EXTENT_LIST_BASE);
    }

    /** FSP_FREE_FRAG 链 base 地址。 */
    public FileAddress freeFragExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FREE_FRAG_LIST_BASE);
    }

    /** FSP_FULL_FRAG 链 base 地址。 */
    public FileAddress fullFragExtentListBaseAddr(SpaceId spaceId) {
        requireSpace(spaceId);
        return FileAddress.of(PageNo.of(0), SpaceHeaderLayout.FULL_FRAG_LIST_BASE);
    }

    /** 读 nextSegmentId、写回 +1、返回旧值（segment id 分配，非幂等，调用方一个 MTR 内使用）。 */
    public long allocateNextSegmentId(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        long current = g.readLong(SpaceHeaderLayout.NEXT_SEGMENT_ID);
        if (current <= 0) {
            throw new FspMetadataException("invalid next segment id on disk: " + current);
        }
        g.writeLong(SpaceHeaderLayout.NEXT_SEGMENT_ID, current + 1);
        return current;
    }

    private void writeLongField(MiniTransaction mtr, SpaceId spaceId, int offset, long value) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeLong(offset, value);
    }

    private static void requireMtr(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("mini transaction must not be null");
        }
    }

    private static void requireSpace(SpaceId spaceId) {
        if (spaceId == null) {
            throw new DatabaseValidationException("space id must not be null");
        }
    }

    private static PageNo requireValue(PageNo value) {
        if (value == null) {
            throw new DatabaseValidationException("page no value must not be null");
        }
        return value;
    }
}
