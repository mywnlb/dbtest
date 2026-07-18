package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;

import java.util.List;
import java.util.function.Function;

/**
 * 单一业务 MTR 拥有的 prepared undo append guard。prepare 已固定 slot owner、undo root 与 external payload 页，
 * 但尚未写入 record slot；调用者必须恰好一次发布真实 ownership。未发布即关闭表示跨过无 content-undo 的物理边界，
 * 因而释放资源后抛 fatal，禁止把半准备状态当成普通可重试失败。
 *
 * @param <T> actual ownership 元素类型；由对应 {@link DeferredUndoPlan} 决定并执行形状复核。
 */
public final class PreparedUndoAppend<T> implements AutoCloseable {

    /** 创建线程；MTR memo 与显式 latch 不允许跨线程转移。 */
    private final Thread ownerThread;
    /** 所属业务 MTR；append/close 前必须仍为 ACTIVE。 */
    private final MiniTransaction mtr;
    /** manager 提供的 actual plan 校验、物理 append 与事务 context 发布闭包。 */
    private final Function<List<T>, RollPointer> appender;
    /** 反序关闭 reservation/slot/reuse lease 的幂等闭包。 */
    private final Runnable releaser;
    /** 是否已经成功写入 actual undo 并发布逻辑头。 */
    private boolean published;
    /** guard 是否已经关闭。 */
    private boolean closed;

    /**
     * 创建 {@code PreparedUndoAppend}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param appender 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param releaser 生命周期回调；只在契约定义的成功或释放边界调用，且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    PreparedUndoAppend(MiniTransaction mtr,
                       Function<List<T>, RollPointer> appender,
                       Runnable releaser) {
        if (mtr == null || appender == null || releaser == null) {
            throw new DatabaseValidationException("prepared undo append fields must not be null");
        }
        this.ownerThread = Thread.currentThread();
        this.mtr = mtr;
        this.appender = appender;
        this.releaser = releaser;
    }

    /**
     * 校验并发布真实 INSERT/UPDATE undo。appender 先执行 ownership 与 codec shape 复核，再写 prepared slot 并
     * 发布事务 logical head；shape 校验失败不写 record，调用方仍可提供正确 ownership 重试一次。
     *
     * @param actualOwnerships 当前业务 MTR 已创建的真实 ownership；数量、顺序和非首页号字段必须与 placeholder 一致。
     * @return 新 undo record 的稳定 roll pointer，同时成为聚簇记录的新 DB_ROLL_PTR。
     * @throws UndoWriteStalePlanException guard 已发布/关闭或 actual ownership 改变冻结形状时抛出。
     * @throws UndoWriteFatalException MTR 已离开 ACTIVE 或 prepared 物理资源无法安全发布时抛出。
     */
    public RollPointer appendActual(List<T> actualOwnerships) {
        requireOpen("append actual undo");
        if (published) {
            throw new UndoWriteStalePlanException("prepared undo append was already published");
        }
        RollPointer pointer = appender.apply(actualOwnerships);
        published = true;
        return pointer;
    }

    /**
     * 结束 guard 并释放非页资源。published 路径正常返回；未 published 路径始终 fail-stop，因为 slot/page owner 或
     * prepared 页可能已经写入 buffer，MTR rollback 不能撤销这些 page content。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @throws UndoWriteFatalException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    @Override
    public void close() {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (closed) {
            return;
        }
        requireOwner();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        closed = true;
        RuntimeException releaseFailure = null;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        try {
            releaser.run();
        } catch (RuntimeException error) {
            releaseFailure = error;
        }
        if (!published) {
            throw new UndoWriteFatalException("prepared undo append closed before actual undo publication",
                    releaseFailure);
        }
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        if (releaseFailure != null) {
            throw new UndoWriteFatalException("prepared undo append resource release failed", releaseFailure);
        }
    }

    /**
     * 校验 {@code requireOpen} 涉及的事务、MVCC 与锁结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param operation 传给 {@code requireOpen} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws UndoWriteStalePlanException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     * @throws UndoWriteFatalException 日志或数据持久化协作失败时抛出；调用方不得确认提交、推进安全边界或清除未完成状态
     */
    private void requireOpen(String operation) {
        requireOwner();
        if (closed) {
            throw new UndoWriteStalePlanException(operation + " on closed prepared undo append");
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            throw new UndoWriteFatalException(operation + " requires ACTIVE MTR: " + mtr.state());
        }
    }

    private void requireOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw new UndoWriteFatalException("prepared undo append used by a non-owner thread");
        }
    }
}
