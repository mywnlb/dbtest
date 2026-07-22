package cn.zhangyis.db.dd.ddl;

import java.time.Duration;
import java.util.List;

/**
 * 通用INPLACE DROP与shadow source-space共享的table-level退休屏障。实现只协调storage history与
 * exact dictionary pin，不拥有descriptor、tablespace文件或DDL marker。
 */
public interface OnlineAlterRetirementBarrier {

    /**
     * final X/gate quiescence下冻结有序资源集合与transaction high-water。
     *
     * @param tableId source正table identity
     * @param sourceDictionaryVersion 被退休的source aggregate版本
     * @param descriptorGeneration sidecar或shadow generation
     * @param ownerDdlId marker/descriptor owner
     * @param resources canonical排序且去重的INDEX或TABLESPACE集合
     * @return 可一次安装到v4 marker的不可变fence
     */
    DdlRetirementFence captureFence(long tableId, long sourceDictionaryVersion,
                                    long descriptorGeneration, long ownerDdlId,
                                    List<DdlRetiredResource> resources);

    /**
     * 在单一绝对deadline内等待fence内history与source metadata pin安全。
     *
     * @param fence 已durable安装的table-level fence
     * @param timeout 正总预算
     * @throws OnlineDdlRetirementTimeoutException 未及时安全或等待中断时抛出；已发布target不得回滚
     */
    void awaitSafe(DdlRetirementFence fence, Duration timeout);
}
