package cn.zhangyis.db.storage.trx;

import java.util.Set;
import java.util.TreeSet;

/**
 * 活跃读写事务 id 注册表（innodb-transaction-mvcc-design §5.3）。
 *
 * <p><b>非线程安全</b>：本对象**只由 {@link TransactionSystem} 在其 {@code ReentrantLock} 内调用**，
 * 不对外暴露为独立可变 owner，避免双重 owner。有序集合（{@link TreeSet}）供 ReadView 取最小活跃 id
 * 作 up-limit，也供 purge 在同一事务系统短锁内拒绝尚未完成终态发布的 creator。
 */
final class ActiveTransactionTable {

    /** 当前活跃读写事务 id 集合（升序）。 */
    private final TreeSet<Long> activeReadWriteIds = new TreeSet<>();

    /** 登记一个活跃读写事务（首次分配写 id 时）。
     *
     * @param txnId 参与 {@code register} 的原始数值身份 {@code txnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void register(long txnId) {
        activeReadWriteIds.add(txnId);
    }

    /** 移除（commit/rollback 读写事务）。
     *
     * @param txnId 参与 {@code remove} 的原始数值身份 {@code txnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     */
    void remove(long txnId) {
        activeReadWriteIds.remove(txnId);
    }

    /** 判断事务是否仍处于活跃写集合；调用方必须持有 TransactionSystem 短锁。
     *
     * @param txnId 参与 {@code contains} 的原始数值身份 {@code txnId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @return {@code contains} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    boolean contains(long txnId) {
        return activeReadWriteIds.contains(txnId);
    }

    /** 不可变拷贝快照。 */
    Set<Long> snapshot() {
        return Set.copyOf(activeReadWriteIds);
    }
}
