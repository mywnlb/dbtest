package cn.zhangyis.db.storage.api;

import java.time.Duration;

/**
 * Online shadow ALTER等待旧schema ReadView退出的稳定storage API。DD只持有generation fence，
 * 不读取事务active table、ReadView集合或purge counter。
 */
public interface ReadViewRetentionBarrier {

    /**
     * 在final table X与source gate quiescence下捕获当前ReadView generation高水位。
     *
     * @return 非负generation；随后创建的ReadView严格晚于该值且不阻塞本次fence
     */
    long captureGeneration();

    /**
     * 有界等待所有generation不晚于fence的ReadView关闭。
     *
     * @param generationFence final X下捕获的非负generation
     * @param timeout 正总等待预算；超时或中断必须释放内部显式锁
     * @throws ReadViewRetentionTimeoutException 旧ReadView未在预算内退出时抛出
     * @throws ReadViewRetentionInterruptedException 等待线程被中断时抛出并恢复中断标志
     */
    void awaitClosedThrough(long generationFence, Duration timeout);
}
