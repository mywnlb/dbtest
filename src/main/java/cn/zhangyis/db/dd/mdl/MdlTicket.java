package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.dd.domain.MdlOwnerId;

import java.util.concurrent.atomic.AtomicBoolean;

/** 已授予 MDL 的 RAII ticket。close 幂等；upgrade 成功会在同一对象上发布更强 mode。 */
public final class MdlTicket implements AutoCloseable {

    /**
     * 记录 {@code requestId} 的稳定身份或单调版本；只由分配/发布路径更新，重复、回退或跨 owner 复用会破坏可见性。
     */
    private final long requestId;
    /**
     * 构造时冻结的 {@code owner} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final MdlOwnerId owner;
    /**
     * 构造时冻结的 {@code key} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final MdlKey key;
    /**
     * 本对象关联的 {@code duration} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
     */
    private final MdlDuration duration;
    /**
     * 本对象持有的 {@code manager} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final MetadataLockManager manager;
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private final AtomicBoolean closed = new AtomicBoolean();
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private volatile MdlMode mode;

    /**
     * 创建 {@code MdlTicket}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param requestId 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param owner 参与 {@code 构造} 的稳定领域标识 {@code MdlOwnerId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param key 参与 {@code 构造} 的稳定领域标识 {@code MdlKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param duration 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param manager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     */
    MdlTicket(long requestId, MdlOwnerId owner, MdlKey key, MdlMode mode, MdlDuration duration,
              MetadataLockManager manager) {
        this.requestId = requestId;
        this.owner = owner;
        this.key = key;
        this.mode = mode;
        this.duration = duration;
        this.manager = manager;
    }

    public MdlOwnerId owner() {
        return owner;
    }

    public MdlKey key() {
        return key;
    }

    public MdlMode mode() {
        return mode;
    }

    public MdlDuration duration() {
        return duration;
    }

    long requestId() {
        return requestId;
    }

    boolean isClosed() {
        return closed.get();
    }

    boolean closeByCaller() {
        return closed.compareAndSet(false, true);
    }

    /**
     * 返回 {@code closeByManager} 对应的数据字典受控对象；调用方获得使用权但不接管组合根或 owner 的生命周期。
     */
    void closeByManager() {
        closed.set(true);
    }

    /**
     * 推进 {@code publishMode} 对应的数据字典阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
     *
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     */
    void publishMode(MdlMode mode) {
        this.mode = mode;
    }

    /**
     * 释放本方法拥有的数据字典资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    public void close() {
        manager.release(this);
    }
}
