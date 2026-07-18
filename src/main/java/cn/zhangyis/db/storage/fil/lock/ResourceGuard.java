package cn.zhangyis.db.storage.fil.lock;

/**
 * fil 物理资源的 RAII 风格释放守卫。
 *
 * <p>获取锁成功后，具体锁把对应 unlock/release 动作封装为该接口，调用方用
 * try-with-resources 把释放路径绑定到词法作用域。{@link #close()} 收窄为不声明受检异常，
 * 避免 IO 临界区出现手工 finally 分支。</p>
 *
 * <p>接口本身不承诺 guard 可跨线程转交或重复关闭；owner 约束和幂等性由具体获取方法说明。
 * 因此调用方应把 guard 视为一次性令牌，并按与获取相反的顺序关闭。</p>
 */
public interface ResourceGuard extends AutoCloseable {

    /**
     * 释放守卫代表的物理资源 ownership。
     *
     * <p>该契约不声明受检异常；若具体锁要求 owner 线程释放，违反约束仍可能产生项目校验异常
     * 或并发工具自身的未检查异常。</p>
     */
    @Override
    void close();
}
