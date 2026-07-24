package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.sql.executor.storage.SqlTransactionHandle;
import cn.zhangyis.db.storage.trx.Transaction;

import java.util.concurrent.locks.ReentrantLock;

/** adapter 私有 handle；SQL 包只能看到 marker interface，不能取出真实 Transaction。 */
final class EngineSqlTransactionHandle implements SqlTransactionHandle {
    /**
     * 定义SQL 与存储引擎适配层的 {@code State} 状态或类别；枚举值用于显式分派领域行为，不得用声明顺序代替稳定编码。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code ACTIVE}：表示“ACTIVE”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code PREPARED}：storage phase one 已强持久，普通 SQL 操作必须拒绝</li>
     *     <li>{@code COMMIT_DECIDED}：registry 已持久选择提交，只允许同方向 phase-two 重试</li>
     *     <li>{@code ROLLBACK_DECIDED}：registry 已持久选择回滚，只允许同方向 phase-two 重试</li>
     *     <li>{@code COMMITTED}：表示“COMMITTED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code ROLLED_BACK}：表示“ROLLEDBACK”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code FAILED}：表示“FAILED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     * </ul>
     */
    enum State {
        ACTIVE,
        PREPARED,
        COMMIT_DECIDED,
        ROLLBACK_DECIDED,
        COMMITTED,
        ROLLED_BACK,
        FAILED
    }

    /** 防止同一不透明 handle 被两个调用线程同时用于 statement/commit/rollback。 */
    final ReentrantLock operationLock = new ReentrantLock(true);
    /**
     * 是否有 cursor 持有 operation lease；该字段只在 {@link #operationLock} 内读写，
     * 用于拒绝 ReentrantLock 对同一线程开放的重入，防止 cursor 存活期提交或回滚事务。
     */
    boolean cursorActive;
    /**
     * 本对象持有的 {@code owner} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    final DefaultSqlStorageGateway owner;
    /**
     * 本对象关联的 {@code transaction} 事务、会话或锁状态；owner 在生命周期内稳定，等待、可见性和释放路径均依赖该关联。
     */
    final Transaction transaction;
    /**
     * 本对象的权威状态机字段 {@code state}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
     */
    State state = State.ACTIVE;
    /**
     * 记录 {@code wrote} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
     */
    boolean wrote;

    /**
     * 创建 {@code EngineSqlTransactionHandle}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param owner 由组合根提供的 {@code DefaultSqlStorageGateway} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     */
    EngineSqlTransactionHandle(DefaultSqlStorageGateway owner, Transaction transaction) {
        this.owner = owner;
        this.transaction = transaction;
    }
}
