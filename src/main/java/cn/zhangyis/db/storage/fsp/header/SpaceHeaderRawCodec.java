package cn.zhangyis.db.storage.fsp.header;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.ByteBuffer;

/**
 * 从 raw page-0 ByteBuffer 解出 {@link SpaceHeaderPhysical}。
 *
 * <p>该类封装 package-private {@link SpaceHeaderLayout} 偏移，使 api 层 loader 不必直接依赖页内布局常量。
 * 这里只解 loader 需要的物理字段，不解三个 FLST base：FLST 的一致性校验依赖 PageGuard 路径，raw loader 不复刻。
 *
 * <p>读写字节序沿用 {@link ByteBuffer} 默认 big-endian，与 {@link SpaceHeaderRepository#initialize} 经 PageGuard
 * 写入 int/long 的格式一致。
 */
public final class SpaceHeaderRawCodec {

    private SpaceHeaderRawCodec() {
    }

    /**
     * 解析 page-0 物理字段。使用绝对位置读取，不依赖调用方传入 buffer 的 position/limit 状态。
     *
     * @param page page-0 raw 字节缓冲，容量必须覆盖 SpaceHeaderLayout 当前最后一个字段。
     * @return page-0 物理字段快照。
     */
    public static SpaceHeaderPhysical readPhysical(ByteBuffer page) {
        if (page == null) {
            throw new DatabaseValidationException("space header page buffer must not be null");
        }
        if (page.capacity() < SpaceHeaderLayout.SPACE_VERSION + Long.BYTES) {
            throw new DatabaseValidationException("space header page buffer too small: " + page.capacity());
        }
        int spaceIdValue = page.getInt(SpaceHeaderLayout.SPACE_ID);
        int pageSizeBytes = page.getInt(SpaceHeaderLayout.PAGE_SIZE_BYTES);
        int spaceFlags = page.getInt(SpaceHeaderLayout.SPACE_FLAGS);
        long currentSize = page.getLong(SpaceHeaderLayout.CURRENT_SIZE);
        long freeLimit = page.getLong(SpaceHeaderLayout.FREE_LIMIT);
        long spaceVersion = page.getLong(SpaceHeaderLayout.SPACE_VERSION);
        return new SpaceHeaderPhysical(SpaceId.of(spaceIdValue), PageSize.ofBytes(pageSizeBytes), spaceFlags,
                PageNo.of(currentSize), PageNo.of(freeLimit), spaceVersion);
    }
}
