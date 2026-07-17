package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.TablePurgeBarrier;

import java.time.Duration;

/** 基于 {@link HistoryList} 权威运行时投影实现的表级 purge barrier；自身不复制计数或持有第二把锁。 */
public final class HistoryTablePurgeBarrier implements TablePurgeBarrier {

    /** committed history 队列、引用计数和唤醒 Condition 的唯一 owner。 */
    private final HistoryList history;

    /**
     * 创建不复制状态的表级 barrier 适配器；commit、purge、recovery 与本对象必须共享同一 history owner。
     *
     * @param history 持有 committed 队列、affected-table 引用计数和唤醒 Condition 的权威运行时投影。
     * @throws DatabaseValidationException history 为空时抛出，防止 DDL 等待一个与 purge 脱节的伪 barrier。
     */
    public HistoryTablePurgeBarrier(HistoryList history) {
        if (history == null) {
            throw new DatabaseValidationException("table purge barrier history must not be null");
        }
        this.history = history;
    }

    /**
     * 委托权威 history 投影有界等待引用归零；方法不持有 DD MDL、页 latch、MTR 或文件锁。
     *
     * @param tableId DD 分配的稳定正表 id。
     * @param timeout 最大等待时间，必须为正值。
     * @throws cn.zhangyis.db.storage.api.TablePurgeBarrierTimeoutException 超时后引用仍非零时抛出，表应保持 ACTIVE。
     * @throws cn.zhangyis.db.storage.api.TablePurgeBarrierInterruptedException 等待中断时抛出，调用方应取消 DDL。
     */
    @Override
    public void awaitUnreferenced(long tableId, Duration timeout) {
        history.awaitTableUnreferenced(tableId, timeout);
    }

    /**
     * 读取当前运行时投影中的表引用数，仅供诊断与测试，不作为独立持久真相。
     *
     * @param tableId DD 分配的稳定正表 id。
     * @return committed history 中包含该表的 entry 数量；没有引用时返回零。
     */
    @Override
    public int referenceCount(long tableId) {
        return history.tableReferenceCount(tableId);
    }
}
