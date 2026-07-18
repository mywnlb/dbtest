package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.domain.PageSize;

/**
 * 数据文件单次自动扩展增量的纯决策策略。
 *
 * <p>{@link DataFileHandle} 在持有 lifecycle shared 与 file-size 排他锁后，把锁内重新读取的
 * 当前物理页数和固定 {@link PageSize} 传入策略，再由 gateway 实际分配新增范围。策略不执行文件 IO、
 * 不更新 FSP free-limit，也不发布 registry/page0；返回值只表达物理增量页数。</p>
 *
 * <p>实现必须对相同输入给出稳定正数结果。调用方会拒绝非正增量和加法溢出；阈值的开闭区间属于
 * 持久行为，应由实现测试固定。</p>
 */
public interface AutoExtendPolicy {

    /**
     * 根据当前物理大小计算下一次文件尾扩展增量。
     *
     * @param currentSizeInPages 加锁后观察到的当前文件页数；必须非负
     * @param pageSize 数据文件创建时绑定的非空页大小，可用于推导 pages-per-extent
     * @return 本次应追加的正数页数；不得返回零或负数
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 输入不满足实现约束，或无法产生
     *         合法正增量时抛出；策略不得留下物理副作用
     */
    long nextIncrementPages(long currentSizeInPages, PageSize pageSize);
}
