package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.mtr.MtrLatchOrderScope;
import cn.zhangyis.db.storage.record.format.LogicalRecord;

/**
 * 单一业务 MTR 拥有的聚簇 UPDATE prepare guard。构造时已经固定目标 leaf X latch、旧隐藏列 CAS 证据和
 * placeholder 的 key/encoded length，但没有修改记录；调用者写完 LOB 和 actual undo 后恰好发布一次真实行。
 */
public final class PreparedClusteredUpdate implements AutoCloseable {

    /** 实际行发布闭包；目标 leaf 和旧版本证据由创建它的 B+Tree service 封装。 */
    @FunctionalInterface
    interface Publisher {
        /**
         * 发布带真实 external envelope 和 roll pointer 的目标记录。
         *
         * @param actualRecord 逻辑列已经替换为真实 external envelope 的完整用户行。
         * @param rollPointer  actual UPDATE undo 的稳定地址。
         * @return 是否替换了仍符合旧隐藏列证据的目标版本。
         */
        BTreeUpdateResult publish(LogicalRecord actualRecord, RollPointer rollPointer);
    }

    /** 创建线程；MTR/page latch 不允许跨线程转移。 */
    private final Thread ownerThread;
    /** 所属 ACTIVE 业务 MTR。 */
    private final MiniTransaction mtr;
    /** index→LOB/FSP 单向锁序的显式例外 scope，发布或关闭时必须释放。 */
    private final MtrLatchOrderScope latchOrderScope;
    /** 已冻结目标 leaf/旧版本证据的实际发布动作。 */
    private final Publisher publisher;
    /** 是否已经成功执行 publish；只由 owner thread 修改。 */
    private boolean published;
    /** guard 是否已经关闭；关闭后不能再次发布。 */
    private boolean closed;

    /**
     * 创建 prepared UPDATE 生命周期 guard。
     *
     * @param mtr             已固定目标 leaf 的 ACTIVE MTR。
     * @param latchOrderScope 允许后续 index→LOB/FSP 的有界锁序 scope。
     * @param publisher       封装目标 leaf、key、旧隐藏列和 placeholder 形状的发布闭包。
     * @throws DatabaseValidationException 任一协作者缺失时抛出；失败不转移 guard ownership。
     */
    PreparedClusteredUpdate(MiniTransaction mtr, MtrLatchOrderScope latchOrderScope, Publisher publisher) {
        if (mtr == null || latchOrderScope == null || publisher == null) {
            throw new DatabaseValidationException("prepared clustered update fields must not be null");
        }
        this.ownerThread = Thread.currentThread();
        this.mtr = mtr;
        this.latchOrderScope = latchOrderScope;
        this.publisher = publisher;
    }

    /**
     * 用真实 LOB references 和 undo pointer 发布聚簇新版本，并结束 index→LOB/FSP 越序 scope。
     *
     * @param actualRecord 完整目标用户行；hidden columns 由 publisher 统一盖入，调用方不得预置恢复 identity。
     * @param rollPointer  actual UPDATE undo 的稳定 roll pointer。
     * @return 目标版本仍与 prepare 证据一致时 replaced=true；所有权不匹配时为幂等 false。
     * @throws DatabaseValidationException 参数缺失时抛出。
     * @throws PreparedUpdateStateException guard 状态、线程、MTR 或 actual key/encoded length 不满足冻结形状时抛出。
     */
    public BTreeUpdateResult publish(LogicalRecord actualRecord, RollPointer rollPointer) {
        requireOpen("publish clustered update");
        if (actualRecord == null || rollPointer == null) {
            throw new DatabaseValidationException("prepared clustered update record/pointer must not be null");
        }
        BTreeUpdateResult result = publisher.publish(actualRecord, rollPointer);
        published = true;
        RuntimeException releaseFailure = releaseScope();
        if (releaseFailure != null) {
            throw new PreparedUpdateStateException("prepared clustered update scope release failed", releaseFailure);
        }
        return result;
    }

    /**
     * 正常 published 后幂等结束；未发布即关闭表示业务 MTR 已越过 prepared 物理边界但没有形成可恢复行版本。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @throws PreparedUpdateStateException 未发布关闭、跨线程、MTR 非 ACTIVE 或 scope 释放失败时抛出。
     */
    @Override
    public void close() {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (closed) return;
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        requireOwner();
        closed = true;
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        RuntimeException releaseFailure = releaseScope();
        if (!published) {
            throw new PreparedUpdateStateException(
                    "prepared clustered update closed before row publication", releaseFailure);
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        if (releaseFailure != null) {
            throw new PreparedUpdateStateException(
                    "prepared clustered update resource release failed", releaseFailure);
        }
    }

    /** 校验 guard 仍由创建线程在 ACTIVE MTR 中独占且尚未发布。
     *
     * @param operation 传给 {@code requireOpen} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws PreparedUpdateStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    private void requireOpen(String operation) {
        requireOwner();
        if (closed || published) {
            throw new PreparedUpdateStateException(operation + " on closed/published prepared update");
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            throw new PreparedUpdateStateException(operation + " requires ACTIVE MTR: " + mtr.state());
        }
    }

    /** 拒绝跨线程使用 page-latch/MTR guard。 */
    private void requireOwner() {
        if (Thread.currentThread() != ownerThread) {
            throw new PreparedUpdateStateException("prepared clustered update used by non-owner thread");
        }
    }

    /** 退出 index→LOB/FSP 显式锁序 scope；返回异常供调用方保持主失败。
     *
     * @return {@code releaseScope} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code RuntimeException} 结果
     */
    private RuntimeException releaseScope() {
        try {
            latchOrderScope.close();
            return null;
        } catch (RuntimeException error) {
            return error;
        }
    }
}
