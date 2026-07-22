package cn.zhangyis.db.dd.ddl;

import java.time.Duration;

/**
 * Online DROP在DD层使用的完整退休屏障。实现组合storage history高水位与exact source metadata pin，
 * 但不拥有descriptor、segment或DDL marker。
 */
public interface IndexRetirementBarrier {

    /**
     * 在final table X与gate quiescence下捕获不可变索引退休边界。
     *
     * @param tableId final X下重读的稳定正表 identity
     * @param sourceDictionaryVersion source aggregate 的正字典版本
     * @param indexId 待退休的稳定正二级索引 identity
     * @param descriptorGeneration page3 descriptor的正代际；v1单slot使用target dictionary version
     * @param ownerDdlId descriptor与marker共享的正DDL identity
     * @return 可直接一次性安装到marker的不可变fence
     */
    DdlRetirementFence captureIndexFence(long tableId, long sourceDictionaryVersion,
                                         long indexId, long descriptorGeneration, long ownerDdlId);

    /**
     * 有界等待fence保护的source history与metadata pin全部退出。
     *
     * @param fence 已由同一marker持久化、仅包含一个INDEX资源的退休边界
     * @param timeout history和cache pin共享的正有界时限
     * @throws OnlineDdlRetirementTimeoutException 未在预算内安全时抛出；target DD不得回滚且segment必须保留
     */
    void awaitIndexSafe(DdlRetirementFence fence, Duration timeout);
}
