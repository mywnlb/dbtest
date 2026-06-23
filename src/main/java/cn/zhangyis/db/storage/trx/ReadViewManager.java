package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * ReadView 生命周期门面（设计 §8.1，T1.4）。按隔离级别决定一致性读快照的创建与复用；可见性算法与版本遍历
 * 不在此（见 {@link ReadView#isVisible}/{@link MvccReader}）。无状态——RR 的事务级缓存挂在 {@link Transaction}
 * 上，本类只持 {@link TransactionSystem} 供原子建快照，故多实例共享同一 {@code Transaction.readView}，不构成全局单例。
 *
 * <ul>
 *   <li><b>REPEATABLE_READ</b>：事务级 ReadView——首次一致性读经 {@link TransactionSystem#openReadViewSnapshot}
 *       建并缓存到 {@code Transaction.readView}，整事务复用；{@link #release} 清除。</li>
 *   <li><b>READ_COMMITTED</b>：每次一致性读新建 ReadView，不缓存（语句边界由未来 executor 负责）。</li>
 *   <li><b>READ_UNCOMMITTED / SERIALIZABLE</b>：本片未实现，显式抛 {@link TransactionStateException}，避免静默当 RR/RC。</li>
 * </ul>
 */
public final class ReadViewManager {

    /** 全局协调器；原子捕获活跃集合 + 水位、按需为可写事务分配 creator 写 id。 */
    private final TransactionSystem system;

    public ReadViewManager(TransactionSystem system) {
        if (system == null) {
            throw new DatabaseValidationException("transaction system must not be null");
        }
        this.system = system;
    }

    /**
     * 按隔离级别为 {@code txn} 取一致性读 ReadView。要求 {@code txn} ACTIVE。RR 复用事务级缓存、RC 每次新建、
     * RU/SERIALIZABLE 拒绝。可写事务首次建 ReadView 时由 {@link TransactionSystem#openReadViewSnapshot} 分配 creator 写 id。
     */
    public ReadView openReadView(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("openReadView txn must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("openReadView requires an ACTIVE transaction: " + txn.state());
        }
        return switch (txn.isolationLevel()) {
            case REPEATABLE_READ -> {
                ReadView cached = txn.readView();
                if (cached != null) {
                    yield cached;
                }
                ReadView created = system.openReadViewSnapshot(txn);
                txn.bindReadView(created);
                yield created;
            }
            case READ_COMMITTED -> system.openReadViewSnapshot(txn);
            case READ_UNCOMMITTED, SERIALIZABLE -> throw new TransactionStateException(
                    "ReadView not supported for isolation level " + txn.isolationLevel()
                            + " (T1.4 supports REPEATABLE_READ / READ_COMMITTED only)");
        };
    }

    /**
     * 释放事务级 ReadView（清 RR 缓存）。幂等，允许 ACTIVE/COMMITTING/ROLLING_BACK——由
     * {@code TransactionManager.commit}/{@code finishRollback} 在移出活跃表后、进入终态前调用。RC 无缓存，调用为 no-op。
     */
    public void release(Transaction txn) {
        if (txn == null) {
            throw new TransactionStateException("release txn must not be null");
        }
        // 先把事务级 RR 快照从存活集合注销（purge 低水位用），再清缓存。RC 无缓存→cached 为 null→只清缓存。
        ReadView cached = txn.readView();
        if (cached != null) {
            system.closeReadView(cached);
        }
        txn.clearReadView();
    }

    /**
     * 注销一条 READ_COMMITTED 语句级一致性读快照（语句边界）。RR 事务级快照由 {@link #release} 注销、不走本方法。
     * 调用方（未来 executor / 当前 purge 测试）在每条 RC 一致性读结束后调用，避免 RC 快照在 purge 低水位中无限存活
     * （评审 #2：RC openReadView 每次新建，不注销会让 purge 边界卡死）。幂等。
     *
     * @param view RC 一致性读快照，不能为 null。
     */
    public void closeReadView(ReadView view) {
        if (view == null) {
            throw new TransactionStateException("closeReadView view must not be null");
        }
        system.closeReadView(view);
    }
}
