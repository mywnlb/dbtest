package cn.zhangyis.db.storage.flush.cleaner;

/**
 * 创建 page cleaner worker 的小型工厂。
 *
 * <p>supervisor 通过该工厂重建 worker，不直接了解 {@code FlushService}、队列容量、周期 tick 等构造细节；测试也可
 * 注入不同 worker 组合验证失败重启策略。
 */
@FunctionalInterface
public interface PageCleanerWorkerFactory {

    /**
     * 创建一个尚未启动的新 worker。调用方负责随后调用 {@link PageCleanerWorker#start()}。
     *
     * @return NEW 状态的 worker 控制端口。
     */
    PageCleanerWorkerHandle create();
}
