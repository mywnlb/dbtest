package cn.zhangyis.db.storage.api.lob;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * 当前 active MTR 新建 LOB 页链的 RAII ownership guard。创建线程在 actual undo/row publish 前持有它；未调用
 * {@link #transferOwnership()} 就关闭会反序回收全部新页。该对象不允许跨线程或跨 MTR 生命周期使用。
 */
public final class LobWriteAllocation implements AutoCloseable {

    /** guard 生命周期；仅创建线程读写，不需要 monitor/原子变量。
     *
     * <p>未单独声明 Javadoc 的枚举值语义：</p>
     * <ul>
     *     <li>{@code OWNED}：表示“OWNED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code TRANSFERRED}：表示“TRANSFERRED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code COMPENSATED}：表示“COMPENSATED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     *     <li>{@code COMPENSATION_FAILED}：表示“COMPENSATIONFAILED”状态；状态机只允许通过声明的领域分支进入或离开该状态</li>
     * </ul>
     */
    private enum State {
        OWNED,
        TRANSFERRED,
        COMPENSATED,
        COMPENSATION_FAILED
    }

    /** 执行分配的 storage facade；只用于同 MTR 补偿。 */
    private final LobStorage storage;

    /** 分配所属 MTR；ACTIVE 是合法补偿/转移的必要条件。 */
    private final MiniTransaction mtr;

    /** 页链所属 segment identity。 */
    private final SegmentRef segment;

    /** 按分配顺序冻结的新页集合；补偿时反序处理。 */
    private final List<PageId> allocatedPages;

    /** 可编码进 INSERT undo/clustered row 的 external envelope。 */
    private final ColumnValue.ExternalValue value;

    /** 单线程 owner；防止另一个线程在原线程发布 row 时并发补偿。 */
    private final Thread ownerThread;

    /** 当前权威 guard 状态，仅 owner thread 修改。 */
    private State state = State.OWNED;

    /** 首次补偿失败；重复 close 必须保留相同根因，不能重新释放部分已回收页。 */
    private LobAllocationStateException compensationFailure;

    /**
     * 创建 {@code LobWriteAllocation}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param storage 由组合根提供的 {@code LobStorage} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param segment 参与 {@code 构造} 的稳定领域标识 {@code SegmentRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param allocatedPages 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    LobWriteAllocation(LobStorage storage, MiniTransaction mtr, SegmentRef segment,
                       List<PageId> allocatedPages, ColumnValue.ExternalValue value) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (storage == null || mtr == null || segment == null || allocatedPages == null
                || allocatedPages.isEmpty() || value == null) {
            throw new DatabaseValidationException("LOB write allocation fields must not be null or empty");
        }
        this.storage = storage;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.mtr = mtr;
        this.segment = segment;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.allocatedPages = List.copyOf(allocatedPages);
        this.value = value;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.ownerThread = Thread.currentThread();
    }

    /** 返回已经完整格式化、但 ownership 尚可能由 guard 补偿的 external value。 */
    public ColumnValue.ExternalValue value() {
        return value;
    }

    /**
     * 把页链 ownership 转交给即将在同一 MTR 发布的 undo/row。转移后 close 为 no-op；调用方仍必须保证 MTR commit
     * 失败时按上层 fail-stop/rollback 规则处理，而不能再次用本 guard 释放可能已经可见的引用。
     *
     * @throws LobAllocationStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void transferOwnership() {
        requireOwnerThread();
        if (state == State.TRANSFERRED) {
            return;
        }
        if (state != State.OWNED || mtr.state() != MiniTransactionState.ACTIVE) {
            throw new LobAllocationStateException("cannot transfer LOB allocation in guard/MTR state "
                    + state + "/" + mtr.state());
        }
        state = State.TRANSFERRED;
    }

    /** 未转移时仅在原 active MTR 内反序补偿；终态、跨线程与既有补偿失败均 fail-closed。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     */
    @Override
    public void close() {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        requireOwnerThread();
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (state == State.TRANSFERRED || state == State.COMPENSATED) {
            return;
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (state == State.COMPENSATION_FAILED) {
            throw compensationFailure;
        }
        if (mtr.state() != MiniTransactionState.ACTIVE) {
            compensationFailure = new LobAllocationStateException(
                    "cannot compensate LOB allocation after MTR left ACTIVE: " + mtr.state());
            state = State.COMPENSATION_FAILED;
            throw compensationFailure;
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        try {
            storage.compensateAllocation(mtr, segment, allocatedPages);
            state = State.COMPENSATED;
        } catch (LobAllocationStateException failure) {
            compensationFailure = failure;
            state = State.COMPENSATION_FAILED;
            throw failure;
        }
    }

    private void requireOwnerThread() {
        if (Thread.currentThread() != ownerThread) {
            throw new LobAllocationStateException("LOB allocation guard used by non-owner thread: owner="
                    + ownerThread.getName() + ", current=" + Thread.currentThread().getName());
        }
    }
}
