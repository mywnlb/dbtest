package cn.zhangyis.db.storage.trx.lock;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 已授予事务锁的释放句柄。句柄采用 RAII 风格，调用 {@link #close()} 会释放这一把锁；
 * 事务结束时也可以使用 {@link LockManager#releaseAll(TransactionId)} 批量释放，批量释放后旧句柄再次 close 为 no-op。
 */
public final class LockHandle implements AutoCloseable {

    /** 创建该句柄的 LockManager，负责实际修改锁表。 */
    private final LockManager manager;

    /** 句柄对应的内部请求 id；仅用于在分片锁表中精确删除一把锁。 */
    private final long requestId;

    /** 持锁事务。 */
    private final TransactionId owner;

    /** 已授予锁资源。 */
    private final TransactionLockKey key;

    /** 已授予锁模式。 */
    private final TransactionLockMode mode;

    /** 防止 close 与 releaseAll 重复释放同一请求。 */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 创建 {@code LockHandle}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param manager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param requestId 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param owner 参与 {@code 构造} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param key 参与 {@code 构造} 的稳定领域标识 {@code TransactionLockKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    LockHandle(LockManager manager, long requestId, TransactionId owner, TransactionLockKey key, TransactionLockMode mode) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (manager == null) {
            throw new DatabaseValidationException("lock handle manager must not be null");
        }
        if (owner == null || owner.isNone()) {
            throw new DatabaseValidationException("lock handle owner must be a real transaction id");
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        if (key == null) {
            throw new DatabaseValidationException("lock handle key must not be null");
        }
        if (mode == null) {
            throw new DatabaseValidationException("lock handle mode must not be null");
        }
        this.manager = manager;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.requestId = requestId;
        this.owner = owner;
        this.key = key;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.mode = mode;
    }

    /** 返回持锁事务 id。 */
    public TransactionId owner() {
        return owner;
    }

    /** 返回锁资源 key。 */
    public TransactionLockKey key() {
        return key;
    }

    /** 返回锁模式。 */
    public TransactionLockMode mode() {
        return mode;
    }

    long requestId() {
        return requestId;
    }

    boolean markClosedByCaller() {
        return closed.compareAndSet(false, true);
    }

    /**
     * 更新 {@code markReleasedByManager} 指定的事务、MVCC 与锁局部状态；写入前校验身份和范围，成功后由所属对象维护一致性。
     */
    void markReleasedByManager() {
        closed.set(true);
    }

    /**
     * 释放这一把已授予事务锁。释放操作幂等；若事务级 releaseAll 已经清理该锁，本方法不会再次修改锁表。
     */
    @Override
    public void close() {
        manager.release(this);
    }
}
