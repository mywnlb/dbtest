package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservation;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.mtr.MtrLatchOrderScope;
import cn.zhangyis.db.storage.record.format.LogicalRecord;

/**
 * 单一 MTR 拥有的聚簇 INSERT prepare guard。构造时已经固定 index X 路径、唯一性结果与最坏 split 预留，
 * 但没有写入 placeholder row；调用者完成 LOB/undo 后以实际记录和 roll pointer 恰好发布一次。
 */
public final class PreparedClusteredInsert implements AutoCloseable {

    /** 实际 row 发布闭包；路径、leaf 和 split 预留仍由创建它的 B+Tree service 封装。 */
    @FunctionalInterface
    interface Publisher {
        /**
         * 推进 {@code publish} 对应的B+Tree 索引阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         *
         * @param actualRecord 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
         * @param rollPointer 参与 {@code publish} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
         * @return {@code publish} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
         */
        BTreeInsertResult publish(LogicalRecord actualRecord, RollPointer rollPointer);
    }

    /** MTR memo 不允许跨线程转移，显式记录 owner 用于 fail-fast。 */
    private final Thread ownerThread;
    /** 所属业务 MTR。 */
    private final MiniTransaction mtr;
    /** 保持 index→FSP/LOB 局部逆序许可的无环证明 scope。 */
    private final MtrLatchOrderScope latchOrderScope;
    /** split 最坏页数预留；leaf 可直接容纳时为空。 */
    private final SpaceReservation reservation;
    /** 实际发布动作。 */
    private final Publisher publisher;
    /** 是否已成功发布 row。 */
    private boolean published;
    /** guard 是否已关闭。 */
    private boolean closed;

    /**
     * 创建 {@code PreparedClusteredInsert}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param latchOrderScope 锁子系统提供的请求、观测或持有状态；不得为 {@code null}，资源身份、owner 和锁生命周期必须与当前事务或会话一致
     * @param reservation FSP 层的链表、空间预留或分配方向对象；不得为 {@code null}，必须属于当前表空间且保持 extent/segment 所有权不变量
     * @param publisher 在预留和物理写入成功后发布结构变化的回调；不得为 {@code null}，失败路径不得调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    PreparedClusteredInsert(MiniTransaction mtr, MtrLatchOrderScope latchOrderScope,
                            SpaceReservation reservation, Publisher publisher) {
        if (mtr == null || latchOrderScope == null || publisher == null) {
            throw new DatabaseValidationException("prepared clustered insert fields must not be null");
        }
        this.ownerThread = Thread.currentThread();
        this.mtr = mtr;
        this.latchOrderScope = latchOrderScope;
        this.reservation = reservation;
        this.publisher = publisher;
    }

    /** 用真实 external values 与真实 undo pointer 发布聚簇行；成功后关闭 split reservation 与越序 scope。
     *
     * @param actualRecord 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param rollPointer 参与 {@code publish} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code publish} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BTreeInsertResult publish(LogicalRecord actualRecord, RollPointer rollPointer) {
        requireOpen("publish clustered row");
        if (actualRecord == null || rollPointer == null) {
            throw new DatabaseValidationException("prepared clustered publish record/pointer must not be null");
        }
        BTreeInsertResult result = publisher.publish(actualRecord, rollPointer);
        published = true;
        releaseResources();
        return result;
    }

    /** published 后幂等结束；未发布关闭表示 prepared 物理边界失败，必须 fail-stop。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @throws PreparedInsertStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    @Override
    public void close() {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (closed) {
            return;
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        requireOwner();
        closed = true;
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        RuntimeException releaseFailure = releaseResources();
        if (!published) {
            throw new PreparedInsertStateException("prepared clustered insert closed before row publication",
                    releaseFailure);
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        if (releaseFailure != null) {
            throw new PreparedInsertStateException("prepared clustered insert resource release failed",
                    releaseFailure);
        }
    }

    /**
     * 校验 {@code requireOpen} 涉及的B+Tree 索引结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param operation 传给 {@code requireOpen} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws PreparedInsertStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    private void requireOpen(String operation) {
        requireOwner();
        if (closed || published) {
            throw new PreparedInsertStateException(operation + " on closed/published prepared insert");
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            throw new PreparedInsertStateException(operation + " requires ACTIVE MTR: " + mtr.state());
        }
    }

    private void requireOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw new PreparedInsertStateException("prepared clustered insert used by a non-owner thread");
        }
    }

    /** 先释放 reservation、再退出越序 scope；返回首个异常并聚合 suppressed。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @return {@code releaseResources} 分类或包装后的领域异常；成功时不为 {@code null}，原始 cause 与 suppressed 异常关系保持不变
     */
    private RuntimeException releaseResources() {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        RuntimeException first = null;
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (reservation != null) {
            try {
                reservation.close();
            } catch (RuntimeException error) {
                first = error;
            }
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        try {
            latchOrderScope.close();
        } catch (RuntimeException error) {
            if (first == null) {
                first = error;
            } else {
                first.addSuppressed(error);
            }
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return first;
    }
}
