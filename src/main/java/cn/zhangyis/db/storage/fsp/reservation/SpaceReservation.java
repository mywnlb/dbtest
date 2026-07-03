package cn.zhangyis.db.storage.fsp.reservation;

import cn.zhangyis.db.domain.SpaceId;

/**
 * 表空间预留句柄。它是一次多页操作的 RAII guard：创建时占住内存态预留额度，操作成功、失败或 MTR 结束时
 * {@link #close()} 释放未消费额度。
 *
 * <p>该对象允许重复关闭。原因是推荐用法是 try-with-resources，同时 {@code MiniTransaction} memo 也会在
 * commit/rollback 时兜底关闭；双路径关闭不能把全局计数扣成负数。
 */
public final class SpaceReservation implements AutoCloseable {

    /** 归属服务；所有可变状态都在服务锁下读写。 */
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

    /** 尚未被 allocatePage 消费的数据页额度；只允许 owner 在服务锁下修改。 */
    long remainingPages;

    /** 是否已释放；只允许 owner 在服务锁下修改。 */
    boolean closed;

    SpaceReservation(SpaceReservationService owner, long mtrId, SpaceId spaceId, SpaceReservationKind kind,
                     long requestedPages, long requestedExtents, long reservedCapacityExtents) {
        this.owner = owner;
        this.mtrId = mtrId;
        this.spaceId = spaceId;
        this.kind = kind;
        this.requestedPages = requestedPages;
        this.requestedExtents = requestedExtents;
        this.reservedCapacityExtents = reservedCapacityExtents;
        this.remainingPages = requestedPages;
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
}
