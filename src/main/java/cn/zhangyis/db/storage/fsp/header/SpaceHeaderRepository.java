package cn.zhangyis.db.storage.fsp.header;
import cn.zhangyis.db.storage.fil.state.TablespaceState;

import cn.zhangyis.db.storage.fsp.exception.FspMetadataException;
import cn.zhangyis.db.storage.fsp.flst.FileAddress;
import cn.zhangyis.db.storage.fsp.flst.Flst;
import cn.zhangyis.db.storage.fsp.flst.FlstBase;
import cn.zhangyis.db.storage.fsp.lifecycle.TablespaceLifecycleFormat;
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

import java.util.Optional;

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
        // page0 物理信封：统一标记为 FSP_HDR 表空间头页（pageNo=0、无兄弟链）。这是 page0 与所有其它页型一致的
        // FilePageHeader 不变量——loader/recovery 打开时据此判定 page0 真为表空间头，拒绝绑定错误或损坏的物理页。
        // 信封头经 page guard 写入，作为 PAGE_BYTES 进入 MTR redo，replay 可重建；pageLSN 由 MTR commit 盖戳。
        // FSP 自描述字段（SPACE_ID@38 等）位于信封头之后，二者偏移不重叠。
        PageEnvelope.writeHeader(g, new FilePageHeader(h.spaceId(), 0L,
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, 0L, PageType.FSP_HDR));
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

    /**
     * 在同一 page-0 X latch 下写完整生命周期头。调用方应把状态转换与相关 FSP 修改放进同一 MTR，
     * 使 redo replay 不会观察到半个 marker。该方法不使用枚举 ordinal，磁盘兼容性由稳定状态码保证。
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 目标表空间。
     * @param header 要持久化的完整生命周期快照。
     */
    public void writeLifecycle(MiniTransaction mtr, SpaceId spaceId, TablespaceLifecycleHeader header) {
        requireMtr(mtr);
        requireSpace(spaceId);
        if (header == null) {
            throw new DatabaseValidationException("tablespace lifecycle header must not be null");
        }
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.EXCLUSIVE);
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_MAGIC, TablespaceLifecycleFormat.MAGIC);
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_FORMAT, TablespaceLifecycleFormat.VERSION);
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_STATE, header.state().persistentCode());
        g.writeLong(SpaceHeaderLayout.LIFECYCLE_INITIAL_SIZE, header.initialSizeInPages().value());
        g.writeLong(SpaceHeaderLayout.LIFECYCLE_EPOCH, header.truncateEpoch());
        g.writeLong(SpaceHeaderLayout.LIFECYCLE_TARGET_SIZE, header.targetSizeInPages().value());
        g.writeInt(SpaceHeaderLayout.LIFECYCLE_FINISH_STATE, header.finishState().persistentCode());
    }

    /**
     * 读取 page-0 生命周期头。magic 为 0 表示旧格式并返回 empty；其它未知 magic/format 属于元数据损坏，
     * 必须阻断截断，防止用猜测的 initial size 破坏文件。
     *
     * @param mtr 当前活动 MTR。
     * @param spaceId 目标表空间。
     * @return 已初始化的生命周期快照，或旧格式的 empty。
     */
    public Optional<TablespaceLifecycleHeader> readLifecycle(MiniTransaction mtr, SpaceId spaceId) {
        requireMtr(mtr);
        requireSpace(spaceId);
        PageGuard g = mtr.getPage(pool, page0(spaceId), PageLatchMode.SHARED);
        int magic = g.readInt(SpaceHeaderLayout.LIFECYCLE_MAGIC);
        if (magic == 0) {
            return Optional.empty();
        }
        if (magic != TablespaceLifecycleFormat.MAGIC) {
            throw new FspMetadataException("invalid tablespace lifecycle magic: " + Integer.toHexString(magic));
        }
        int format = g.readInt(SpaceHeaderLayout.LIFECYCLE_FORMAT);
        if (format != TablespaceLifecycleFormat.VERSION) {
            throw new FspMetadataException("unsupported tablespace lifecycle format: " + format);
        }
        return Optional.of(new TablespaceLifecycleHeader(
                TablespaceState.fromPersistentCode(g.readInt(SpaceHeaderLayout.LIFECYCLE_STATE)),
                PageNo.of(g.readLong(SpaceHeaderLayout.LIFECYCLE_INITIAL_SIZE)),
                g.readLong(SpaceHeaderLayout.LIFECYCLE_EPOCH),
                PageNo.of(g.readLong(SpaceHeaderLayout.LIFECYCLE_TARGET_SIZE)),
                TablespaceState.fromPersistentCode(g.readInt(SpaceHeaderLayout.LIFECYCLE_FINISH_STATE))));
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
