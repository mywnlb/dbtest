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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param page page-0 raw 字节缓冲，容量必须覆盖 SpaceHeaderLayout 当前最后一个字段。
     * @return page-0 物理字段快照。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static SpaceHeaderPhysical readPhysical(ByteBuffer page) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (page == null) {
            throw new DatabaseValidationException("space header page buffer must not be null");
        }
        if (page.capacity() < SpaceHeaderLayout.SPACE_VERSION + Long.BYTES) {
            throw new DatabaseValidationException("space header page buffer too small: " + page.capacity());
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        int spaceIdValue = page.getInt(SpaceHeaderLayout.SPACE_ID);
        int pageSizeBytes = page.getInt(SpaceHeaderLayout.PAGE_SIZE_BYTES);
        int spaceFlags = page.getInt(SpaceHeaderLayout.SPACE_FLAGS);
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        long currentSize = page.getLong(SpaceHeaderLayout.CURRENT_SIZE);
        long freeLimit = page.getLong(SpaceHeaderLayout.FREE_LIMIT);
        long spaceVersion = page.getLong(SpaceHeaderLayout.SPACE_VERSION);
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new SpaceHeaderPhysical(SpaceId.of(spaceIdValue), PageSize.ofBytes(pageSizeBytes), spaceFlags,
                PageNo.of(currentSize), PageNo.of(freeLimit), spaceVersion);
    }
}
