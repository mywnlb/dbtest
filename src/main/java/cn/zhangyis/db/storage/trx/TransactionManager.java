package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.UndoNo;

import java.util.Collection;

/**
 * 事务门面（innodb-transaction-mvcc-design §13.1）。begin/commit/rollback 为纯内存生命周期：读写事务惰性分配
 * 写 id（首次写入），commit 给读写事务分配 {@code transactionNo} 并移出活跃表。
 *
 * <p><b>本片无 undo</b>：commit/rollback 只翻转内存事务状态、维护活跃表，**不刷数据页、不撤销任何已写记录**——
 * 被 rollback 的事务插入的行仍物理留在页上、仍带它的 DB_TRX_ID。真正的数据回滚在 T1.3。
 * statement rollback 失败可把 ACTIVE 事务标记为 rollback-only：事务继续留在活跃表供 full rollback，但普通写入和
 * commit 会在任何提交副作用前被拒绝。
 *
 * <p>本片不提供 {@code current()}：不引入 ThreadLocal 绑定/嵌套 begin/线程切换语义，调用方显式持有并传递
 * {@link Transaction}，避免隐藏全局可变状态。
 */
public final class TransactionManager {

    /** 全局协调器（id/no 分配、活跃表）。 */
    private final TransactionSystem system;
    /**
     * 事务级 ReadView 门面（T1.4）。本管理器拥有它（由同一 {@code system} 构造），commit/finishRollback 经它释放
     * 事务级 ReadView。无状态（RR 缓存挂 Transaction），故按本管理器拥有即可，非全局单例；上层一致性读经
     * {@link #readViewManager()} 取同一实例。
     */
    private final ReadViewManager readViewManager;

    /**
     * 创建 {@code TransactionManager}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param system 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public TransactionManager(TransactionSystem system) {
        if (system == null) {
            throw new DatabaseValidationException("transaction system must not be null");
        }
        this.system = system;
        this.readViewManager = new ReadViewManager(system);
    }

    /** 暴露协调器供测试/上层查询活跃事务快照（不暴露内部可变表）。 */
    public TransactionSystem system() {
        return system;
    }

    /** 暴露本管理器拥有的 ReadView 门面，供一致性读（{@code MvccReader}）取/复用同一实例。 */
    public ReadViewManager readViewManager() {
        return readViewManager;
    }

    /** 开启事务，状态 ACTIVE；读写事务此时不分配写 id（惰性，§7.1）。
     *
     * @param options 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code begin} 创建或观察到的事务/锁状态；成功时不为 {@code null}，owner、可见性与生命周期来自当前会话
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Transaction begin(TransactionOptions options) {
        if (options == null) {
            throw new DatabaseValidationException("transaction options must not be null");
        }
        return new Transaction(options, System.currentTimeMillis());
    }

    /**
     * 在 recovery gate 关闭期间根据已核对的 PREPARED undo owners 重建运行时事务聚合。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 creator、rollback segment、binding 集合和全局 undo 高水位，拒绝空 participant。</li>
     *     <li>构造默认读写事务与 UndoContext，逐 kind 恢复稳定 slot/first-page/logical-head binding。</li>
     *     <li>把 creator 登记回 active table，使 phase-two commit/rollback 前继续阻挡 purge。</li>
     *     <li>最后发布 PREPARED；失败不会产生 COMMITTED/ROLLED_BACK 终态或释放任何持久 owner。</li>
     * </ol>
     *
     * @param transactionId undo/redo/page3 已交叉校验的正 creator id
     * @param rollbackSegmentId 所有恢复 binding 共同所属的 rollback segment
     * @param bindings 一或两条 INSERT/UPDATE undo binding，不得为空
     * @param globalHighWater 各物理 log LAST_UNDO_NO 的最大值，不得低于任一 logical head
     * @return 已登记 active membership、状态为 PREPARED 的运行时事务聚合
     * @throws TransactionStateException 恢复字段不完整、owner 重复或状态发布失败时抛出
     */
    public Transaction restorePrepared(TransactionId transactionId,
                                       RollbackSegmentId rollbackSegmentId,
                                       Collection<UndoLogBinding> bindings,
                                       UndoNo globalHighWater) {
        // 1、只有真实持久写 participant 才能进入 phase-two。
        if (transactionId == null || transactionId.isNone() || rollbackSegmentId == null
                || bindings == null || bindings.isEmpty() || globalHighWater == null) {
            throw new TransactionStateException("recovered prepared transaction fields are invalid");
        }
        // 2、UndoContext 自身验证 kind 唯一与 logical/global high-water 关系。
        Transaction transaction = new Transaction(TransactionOptions.defaults(), System.currentTimeMillis());
        transaction.setTransactionId(transactionId);
        UndoContext context = new UndoContext(rollbackSegmentId);
        for (UndoLogBinding binding : bindings) {
            context.restoreBinding(binding, globalHighWater);
        }
        transaction.setUndoContext(context);
        // 3、prepared 决议前仍属于 active writer；登记失败不发布 PREPARED。
        system.restorePreparedActive(transactionId);
        // 4、聚合完成后才发布状态，后续只允许显式 prepared phase-two。
        transaction.transitionTo(TransactionState.PREPARED);
        return transaction;
    }

    /**
     * 首次写入分配写 id（幂等：已分配则返回原 id）。只读事务调用直接拒绝。
     * 调用方在 B+Tree 聚簇写入前调用，再把 {@code txn.transactionId()} 传入写入路径。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @return {@code assignWriteId} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws TransactionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public TransactionId assignWriteId(Transaction txn) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireUsableActive(txn);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (txn.readOnly()) {
            throw new TransactionStateException("read-only transaction cannot assign a write id");
        }
        if (!txn.transactionId().isNone()) {
            return txn.transactionId();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        TransactionId id = system.allocateWriteId();
        txn.setTransactionId(id);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return id;
    }

    /**
     * 为提交预留 {@code TransactionNo}，但事务仍保持 ACTIVE 且留在活跃表。DML facade 需要先把
     * {@code UndoLogManager.onCommit} 的 undo header/slot 状态持久化，再真正移出 active table；否则
     * onCommit 失败后崩溃恢复会把已暴露为 COMMITTED 的事务误判为 ACTIVE undo 并回滚。
     *
     * <p>该方法只做提交序号的幂等预分配，不释放 ReadView、不释放事务锁、不进入 COMMITTING。调用方若后续
     * onCommit 失败，仍可按 ACTIVE 事务执行 rollback；若成功，再调用 {@link #commit(Transaction)} 完成状态转换。
     *
     * @param txn 仍处于 ACTIVE 的事务。
     */
    public void prepareCommit(Transaction txn) {
        requireUsableActive(txn);
        if (!txn.transactionId().isNone() && txn.transactionNo().isNone()) {
            txn.setTransactionNo(system.allocateTransactionNo());
        }
    }

    /**
     * 在 phase-one 物理 MTR 已经提交后发布运行时 PREPARED 状态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>要求事务仍是可提交 ACTIVE 写事务；NONE id 或 rollback-only 都不能伪装成 prepared 分支。</li>
     *     <li>释放事务级 ReadView，prepared 分支不再执行一致性读，但 write id 继续留在 active table 阻挡 purge。</li>
     *     <li>最后发布 ACTIVE→PREPARED；本方法不释放 row lock、不分配提交号，也不执行任何页或 redo IO。</li>
     * </ol>
     *
     * @param txn phase-one undo/redo 已持久化的 live 写事务；必须仍为 ACTIVE 且已分配 write id
     * @throws TransactionStateException 事务状态、提交资格或 write id 不满足 phase-one 发布条件时抛出；调用方不得开放普通 DML
     */
    public void finishPrepare(Transaction txn) {
        // 1、物理 prepare 只服务真实写分支；无 id 分支由上层按 read-only/one-phase 完成。
        requireUsableActive(txn);
        if (txn.transactionId().isNone()) {
            throw new TransactionStateException("prepared transaction requires an assigned write id");
        }
        // 2、PREPARED 不再消费旧版本；active membership 仍由 TransactionSystem 保留。
        readViewManager.release(txn);
        // 3、状态发布后普通 commit/rollback/DML 都会由既有 ACTIVE guard 拒绝。
        txn.transitionTo(TransactionState.PREPARED);
    }

    /**
     * 为 prepared commit 决议预留提交号。该步骤不改变 PREPARED 状态和 active membership，使物理 history
     * append 失败时分支仍能保留锁并重试 phase two。
     *
     * @param txn 已完成 phase one 的 PREPARED 写事务
     * @throws TransactionStateException 事务不是 PREPARED、缺少 write id 或提交号状态损坏时抛出
     */
    public void prepareCommitPrepared(Transaction txn) {
        requirePrepared(txn);
        if (txn.transactionNo().isNone()) {
            txn.setTransactionNo(system.allocateTransactionNo());
        }
    }

    /**
     * 在 prepared commit 的 undo/page3/redo MTR 成功后发布内存终态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 PREPARED、write id 与已预留提交号，防止内存终态领先物理 history。</li>
     *     <li>发布 COMMITTING并从 active table 移除 creator，使后续 ReadView 不再把它视为未提交。</li>
     *     <li>幂等释放 ReadView并进入 COMMITTED；row lock 由上层 prepared facade 在 durability 等待后释放。</li>
     * </ol>
     *
     * @param txn 已完成 prepared commit 物理终结的事务
     * @throws TransactionStateException 状态、write id 或提交号缺失时抛出；调用方必须保留锁并按 fail-stop 处理
     */
    public void commitPrepared(Transaction txn) {
        // 1、只有物理 phase-two commit 的调用方会到达本入口。
        requirePrepared(txn);
        if (txn.transactionNo().isNone()) {
            throw new TransactionStateException("prepared commit requires an assigned transaction number");
        }
        // 2、终态发布顺序与普通 commit 一致：先阻断新操作，再移出 active table。
        txn.transitionTo(TransactionState.COMMITTING);
        system.removeActive(txn.transactionId().value());
        // 3、ReadView 通常已在 prepare 释放；幂等调用保持恢复重建路径安全。
        readViewManager.release(txn);
        txn.transitionTo(TransactionState.COMMITTED);
    }

    /**
     * 提交：ACTIVE→COMMITTING→COMMITTED。读写事务分配提交序号并移出活跃表；只读事务（id 仍 NONE）
     * 不分配序号、无需移出。不刷数据页、不撤销记录。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     */
    public void commit(Transaction txn) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        requireUsableActive(txn);
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        txn.transitionTo(TransactionState.COMMITTING);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (!txn.transactionId().isNone()) {
            if (txn.transactionNo().isNone()) {
                txn.setTransactionNo(system.allocateTransactionNo());
            }
            system.removeActive(txn.transactionId().value());
        }
        // 移出活跃表后、进入终态前释放事务级 ReadView（T1.4；RC/未开 ReadView 时为 no-op）
        readViewManager.release(txn);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        txn.transitionTo(TransactionState.COMMITTED);
    }

    /**
     * 回滚：ACTIVE→ROLLING_BACK→ROLLED_BACK。读写事务移出活跃表。
     *
     * <p>T1.3c 之前无 undo，本方法只翻状态；T1.3d 起拆为 {@link #beginRollback}/{@link #finishRollback} 两阶段，
     * 本方法是「无 undo 链可走」（只读/未写事务）的便捷组合：{@code RollbackService} 对有 {@code UndoContext} 的
     * 事务改为先 {@code beginRollback}、反向走 undo 链、原子终结段并释放 slot，再 {@code finishRollback}，使撤销发生在真正的
     * {@code ROLLING_BACK} 状态内（设计 §7.6）。本组合行为与旧实现完全一致。
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     */
    public void rollback(Transaction txn) {
        beginRollback(txn);
        finishRollback(txn);
    }

    /**
     * 进入回滚：ACTIVE→ROLLING_BACK。供 {@code RollbackService} 在反向走 undo 链前调用，使整段撤销处于
     * {@code ROLLING_BACK} 状态。此阶段**不**移出活跃表——事务在撤销完成前仍是活跃读写事务（设计 §7.6 step 1）。
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     */
    void beginRollback(Transaction txn) {
        requireActive(txn);
        txn.transitionTo(TransactionState.ROLLING_BACK);
    }

    /**
     * 进入 prepared rollback 重试态，但不移出 active table。只有 {@link RollbackService} 的显式 phase-two
     * 路径可调用；普通 rollback 仍只接受 ACTIVE。
     *
     * @param txn 已持久化 PREPARED 的写事务
     * @throws TransactionStateException 状态或 write id 非法时抛出，事务状态不改变
     */
    void beginPreparedRollback(Transaction txn) {
        requirePrepared(txn);
        txn.transitionTo(TransactionState.PREPARED_ROLLING_BACK);
    }

    /**
     * 收尾回滚：ROLLING_BACK→ROLLED_BACK，读写事务移出活跃表。只有 undo 链**完整**走到 prev=NULL、持久终结并释放 slot 后
     * 才调用；单条 undo 失败不应到达此处（{@code RollbackService} 让异常传播、事务停在 {@code ROLLING_BACK} 可重试）。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @throws TransactionStateException 当前不在 {@code ROLLING_BACK}（未先 {@link #beginRollback}）。
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void finishRollback(Transaction txn) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null) {
            throw new DatabaseValidationException("transaction must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (txn.state() != TransactionState.ROLLING_BACK) {
            throw new TransactionStateException("finishRollback requires ROLLING_BACK: " + txn.state());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (!txn.transactionId().isNone()) {
            system.removeActive(txn.transactionId().value());
        }
        // 移出活跃表后、进入终态前释放事务级 ReadView（T1.4）
        readViewManager.release(txn);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        txn.transitionTo(TransactionState.ROLLED_BACK);
    }

    /**
     * prepared undo inverse、segment drop、page3 clear 与终态 redo 全部提交后完成回滚。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param txn 正处于 PREPARED_ROLLING_BACK 的事务
     * @throws TransactionStateException 物理终结前误调用或事务身份缺失时抛出；active membership 保持
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void finishPreparedRollback(Transaction txn) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (txn == null) {
            throw new DatabaseValidationException("prepared rollback transaction must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (txn.state() != TransactionState.PREPARED_ROLLING_BACK) {
            throw new TransactionStateException(
                    "finishPreparedRollback requires PREPARED_ROLLING_BACK: " + txn.state());
        }
        if (txn.transactionId().isNone()) {
            throw new TransactionStateException("prepared rollback requires an assigned write id");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        system.removeActive(txn.transactionId().value());
        readViewManager.release(txn);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        txn.transitionTo(TransactionState.ROLLED_BACK);
    }

    /**
     * statement rollback 失败后撤销事务提交资格。事务保持 ACTIVE 和 active-table 成员身份，保证调用方仍能执行
     * full rollback；后续写入、prepareCommit、commit 由 {@link #requireUsableActive} 统一拒绝。
     *
     * @param txn   发生不确定 statement rollback 的 ACTIVE 事务。
     * @param cause 触发失败的原始领域异常，用于保留首个诊断原因。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void markRollbackOnly(Transaction txn, RuntimeException cause) {
        requireActive(txn);
        if (cause == null) {
            throw new DatabaseValidationException("rollback-only cause must not be null");
        }
        String message = cause.getMessage();
        String reason = cause.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
        txn.markRollbackOnly(reason);
    }

    /** 校验事务可继续普通工作；rollback-only 虽保持 ACTIVE，但只能进入完整 rollback。 */
    private static void requireUsableActive(Transaction txn) {
        requireActive(txn);
        if (txn.rollbackOnly()) {
            throw new TransactionStateException(
                    "transaction is rollback-only and cannot continue or commit: " + txn.rollbackOnlyReason());
        }
    }

    private static void requireActive(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("transaction must not be null");
        }
        if (txn.state() != TransactionState.ACTIVE) {
            throw new TransactionStateException("transaction not ACTIVE: " + txn.state());
        }
    }

    /** 校验 phase-two 操作只消费带真实 write id 的 PREPARED 分支。 */
    private static void requirePrepared(Transaction txn) {
        if (txn == null) {
            throw new DatabaseValidationException("prepared transaction must not be null");
        }
        if (txn.state() != TransactionState.PREPARED) {
            throw new TransactionStateException("transaction not PREPARED: " + txn.state());
        }
        if (txn.transactionId().isNone()) {
            throw new TransactionStateException("prepared transaction requires an assigned write id");
        }
    }
}
