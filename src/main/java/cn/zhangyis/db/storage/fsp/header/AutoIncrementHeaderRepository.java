package cn.zhangyis.db.storage.fsp.header;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;

/**
 * 页 0 自增扩展仓储。所有读写都加入调用方 MTR memo；本类不持有事务锁，也不在页 latch 下等待 redo。
 */
public final class AutoIncrementHeaderRepository {

    /** 与其它 page0 仓储共享的 Buffer Pool。 */
    private final BufferPool pool;

    /**
     * @param pool 组合根共享 Buffer Pool
     */
    public AutoIncrementHeaderRepository(BufferPool pool) {
        if (pool == null) {
            throw new DatabaseValidationException(
                    "auto-increment header requires buffer pool");
        }
        this.pool = pool;
    }

    /**
     * 在未发布 CREATE 的页 0 上初始化 format 1。写入与 FSP/index root 属于同一建表 MTR。
     *
     * @param mtr 活动建表 MTR
     * @param spaceId 新建表空间
     * @param active 是否启用自增
     */
    public void initialize(
            MiniTransaction mtr, SpaceId spaceId, boolean active) {
        PageGuard page = pageForUpdate(mtr, spaceId);
        page.writeInt(SpaceHeaderLayout.AUTO_INCREMENT_FORMAT,
                AutoIncrementHeader.CURRENT_FORMAT);
        page.writeLong(SpaceHeaderLayout.AUTO_INCREMENT_HIGH_WATER, 0L);
        page.writeInt(SpaceHeaderLayout.AUTO_INCREMENT_FLAGS, active ? 1 : 0);
        page.writeBytes(
                SpaceHeaderLayout.AUTO_INCREMENT_RESERVED, new byte[2]);
    }

    /**
     * 取得页 0 X latch 并读取扩展；未知 flag/保留字节会阻止发号。
     *
     * @param mtr 活动短 MTR
     * @param spaceId 已打开表空间
     * @return format 1 快照
     */
    public AutoIncrementHeader readForUpdate(
            MiniTransaction mtr, SpaceId spaceId) {
        PageGuard page = pageForUpdate(mtr, spaceId);
        int format = page.readInt(SpaceHeaderLayout.AUTO_INCREMENT_FORMAT);
        int flags = page.readInt(SpaceHeaderLayout.AUTO_INCREMENT_FLAGS);
        byte[] reserved = page.readBytes(
                SpaceHeaderLayout.AUTO_INCREMENT_RESERVED, 2);
        if ((flags & ~1) != 0
                || reserved[0] != 0 || reserved[1] != 0) {
            throw new DatabaseValidationException(
                    "auto-increment page0 flags/reserved bytes are invalid");
        }
        return new AutoIncrementHeader(
                format,
                page.readLong(SpaceHeaderLayout.AUTO_INCREMENT_HIGH_WATER),
                (flags & 1) != 0);
    }

    /**
     * 覆盖当前 MTR 已持有的 high-water；调用者必须先用 readForUpdate 完成格式校验。
     *
     * @param mtr 活动短 MTR
     * @param spaceId 目标表空间
     * @param rawHighWater unsigned-long 原始位
     */
    public void writeHighWater(
            MiniTransaction mtr, SpaceId spaceId, long rawHighWater) {
        pageForUpdate(mtr, spaceId).writeLong(
                SpaceHeaderLayout.AUTO_INCREMENT_HIGH_WATER, rawHighWater);
    }

    private PageGuard pageForUpdate(
            MiniTransaction mtr, SpaceId spaceId) {
        if (mtr == null || spaceId == null) {
            throw new DatabaseValidationException(
                    "auto-increment page0 requires mtr/space identity");
        }
        return mtr.getPage(pool, PageId.of(spaceId, PageNo.of(0)),
                PageLatchMode.EXCLUSIVE);
    }
}
