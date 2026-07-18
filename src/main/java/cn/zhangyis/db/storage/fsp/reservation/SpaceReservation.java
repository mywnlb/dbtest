package cn.zhangyis.db.storage.fsp.reservation;

import cn.zhangyis.db.domain.SpaceId;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 表空间预留句柄。它是一次多页操作的 RAII guard：创建时占住内存态预留额度，操作成功、失败或 MTR 结束时
 * {@link #close()} 释放未消费额度。
 *
 * <p>该对象允许重复关闭。原因是推荐用法是 try-with-resources，同时 {@code MiniTransaction} memo 也会在
 * commit/rollback 时兜底关闭；双路径关闭不能把全局计数扣成负数。
 */
public final class SpaceReservation implements AutoCloseable {

    /** 归属服务；容量账本仍由服务锁保护，页额度消费只改本对象原子字段。 */
    private final SpaceReservationService owner;

    /** 创建该 reservation 的 MTR id，用于 allocatePage 消费时定位当前操作上下文。 */
    final long mtrId;

    /** 预留所属表空间。跨表空间分配必须分别预留，避免一个 space 的空闲额度被另一个 space 消费。 */
    final SpaceId spaceId;

    /** 预留类型。首版只用于诊断和未来限流策略。 */
    private final SpaceReservationKind kind;

    /** 调用方声明本操作最多会创建的数据页数，allocatePage 每成功进入一次会先消费一个额度。 */
    private final long requestedPages;

    /** 调用方声明本操作需要额外保底的完整 extent 数。 */
    private final long requestedExtents;

    /** 本 reservation 在全局容量账本中占用的完整 extent 数，直到 close 才整体释放。 */
    final long reservedCapacityExtents;

    /** 尚未被 allocatePage 消费的数据页额度；allocate 路径可能持有 B+Tree page latch，不能等待全局账本锁。 */
    private final AtomicLong remainingPages;

    /** 是否已释放；try-with-resources 与 MTR memo 兜底释放会并存，必须幂等。 */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * 创建 {@code SpaceReservation}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param owner 由组合根提供的 {@code SpaceReservationService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param mtrId 参与 {@code 构造} 的原始数值身份 {@code mtrId}；必须非负，零值仅用于对应格式明确声明的系统或空身份
     * @param spaceId 目标表空间的稳定标识；不得为 {@code null}，且必须已注册并满足当前生命周期准入条件
     * @param kind 选择 {@code 构造} 分支的 {@code SpaceReservationKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param requestedPages 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param requestedExtents 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param reservedCapacityExtents 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     */
    SpaceReservation(SpaceReservationService owner, long mtrId, SpaceId spaceId, SpaceReservationKind kind,
                     long requestedPages, long requestedExtents, long reservedCapacityExtents) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        this.owner = owner;
        this.mtrId = mtrId;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.spaceId = spaceId;
        this.kind = kind;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.requestedPages = requestedPages;
        this.requestedExtents = requestedExtents;
        this.reservedCapacityExtents = reservedCapacityExtents;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.remainingPages = new AtomicLong(requestedPages);
    }

    /**
     * 返回预留所属表空间。该值在 reservation 生命周期内不可变，可用于诊断当前操作是否跨 space 误用。
     */
    public SpaceId spaceId() {
        return spaceId;
    }

    /**
     * 返回预留类型。首版不按类型改变行为，但异常消息和后续 metrics 会依赖它区分来源。
     */
    public SpaceReservationKind kind() {
        return kind;
    }

    /**
     * 返回调用方声明的页额度。该值是原始声明，不随消费减少，方便测试和诊断确认调用方预算。
     */
    public long requestedPages() {
        return requestedPages;
    }

    /**
     * 返回调用方声明的额外 extent 额度。它与 page quota 一起转换成容量账本中的完整 extent 占用。
     */
    public long requestedExtents() {
        return requestedExtents;
    }

    /**
     * 释放未消费 reservation。方法幂等，允许 try-with-resources 和 MTR memo 兜底关闭同时存在。
     */
    @Override
    public void close() {
        owner.release(this);
    }

    /**
     * 尝试为当前 MTR 的一次 page allocation 消费一个页额度。该方法不获取全局 reservation 账本锁，避免
     * B+Tree split 在持有 index page latch 时与并发 reserve 的 page0 latch 等待形成环。
     *
     * @param targetSpace 待分配页所属表空间。
     * @return 消费结果：不匹配、已消费或匹配但额度耗尽。
     */
    ConsumeResult consumePageQuota(SpaceId targetSpace) {
        if (closed.get() || !spaceId.equals(targetSpace)) {
            return ConsumeResult.NOT_MATCHED;
        }
        while (true) {
            if (closed.get()) {
                return ConsumeResult.NOT_MATCHED;
            }
            long current = remainingPages.get();
            if (current <= 0L) {
                return ConsumeResult.EXHAUSTED;
            }
            if (remainingPages.compareAndSet(current, current - 1L)) {
                return ConsumeResult.CONSUMED;
            }
        }
    }

    /**
     * 标记 reservation 已关闭。返回 false 表示此前已经关闭，本次 close 不应再扣减容量计数。
     */
    boolean markClosed() {
        return closed.compareAndSet(false, true);
    }

    /** 页额度消费结果。 */
    enum ConsumeResult {
        /** 该 reservation 已关闭或属于其它表空间。 */
        NOT_MATCHED,
        /** 成功消费一个 page quota。 */
        CONSUMED,
        /** reservation 匹配该表空间，但调用方声明的页额度已经耗尽。 */
        EXHAUSTED
    }
}
