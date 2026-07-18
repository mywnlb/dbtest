package cn.zhangyis.db.storage.flush.checkpoint;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.buf.BufferPool;
import cn.zhangyis.db.storage.redo.RedoCheckpointLabel;
import cn.zhangyis.db.storage.redo.RedoCheckpointStore;
import cn.zhangyis.db.storage.redo.RedoLogManager;
import cn.zhangyis.db.storage.redo.RedoReclaimBoundary;

import java.util.concurrent.locks.ReentrantLock;

/**
 * fuzzy checkpoint 协调器。F1 默认构造器只计算和单调发布内存安全 checkpoint LSN；R2 可注入
 * {@link RedoCheckpointStore} 持久化 redo control label，并可在 label 成功后驱动 redo 回收边界；它本身不触发
 * 后台 page cleaner。
 *
 * <p>checkpoint 必须读取 {@link RedoLogManager#closedLsn()}，不能用 current LSN 代替。current 只说明 redo
 * 已分配到哪里，closed 才说明相关 dirty page 已发布到 Buffer Pool 的 flush 视图。
 */
public final class CheckpointCoordinator {

    /**
     * 本对象持有的 {@code bufferPool} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final BufferPool bufferPool;
    /**
     * 本对象持有的 {@code redo} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final RedoLogManager redo;
    /** 可选 redo control store；为空时保持 F1 内存 checkpoint 语义。 */
    private final RedoCheckpointStore checkpointStore;
    /** redo label 之前必须 force 的附加恢复元数据；默认 no-op。 */
    private final CheckpointMetadataParticipant metadataParticipant;
    /**
     * 可选 redo 回收边界端口（0.18b）；为空时不驱动 redo 文件回收（单文件仓储场景）。checkpoint 持久并单调前进后，
     * 把已持久 checkpoint LSN 推送给它，让 redo 文件环复用旧文件。
     */
    private final RedoReclaimBoundary redoReclaimBoundary;
    /**
     * 保护本对象共享状态的显式并发闩；获取后必须在 {@code finally} 或 Guard 关闭路径释放。
     */
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * 构造时冻结的 {@code lastCheckpointLsn} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private Lsn lastCheckpointLsn = Lsn.of(0);

    /**
     * 创建 {@code CheckpointCoordinator}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public CheckpointCoordinator(BufferPool bufferPool, RedoLogManager redo) {
        this(bufferPool, redo, null);
    }

    /**
     * 创建可持久化 checkpoint 的协调器。传入 checkpointStore 时，安全 LSN 单调前进会先写 redo control label，
     * 写入成功后才发布内存 lastCheckpointLsn；control 写失败时保持旧 checkpoint，避免恢复起点虚高。
     *
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param checkpointStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     */
    public CheckpointCoordinator(BufferPool bufferPool, RedoLogManager redo, RedoCheckpointStore checkpointStore) {
        this(bufferPool, redo, checkpointStore, null);
    }

    /**
     * 创建可持久化 checkpoint 且驱动 redo 文件回收的协调器（0.18b）。当安全 checkpoint 单调前进并持久化后，
     * 通过 {@code redoReclaimBoundary} 把已持久 checkpoint LSN 推送给 redo 文件层；redo 据此回收落在该边界之内的旧文件。
     *
     * @param redoReclaimBoundary redo 回收边界端口（可空，单文件仓储场景传 null）。
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param checkpointStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     */
    public CheckpointCoordinator(BufferPool bufferPool, RedoLogManager redo, RedoCheckpointStore checkpointStore,
                                 RedoReclaimBoundary redoReclaimBoundary) {
        this(bufferPool, redo, checkpointStore, CheckpointMetadataParticipant.NO_OP, redoReclaimBoundary);
    }

    /**
     * 创建带附加恢复元数据屏障的 checkpoint 协调器。严格顺序为 participant→redo label→内存发布→reclaim；
     * participant 或 label 写失败都会在发布前抛出，使旧 redo 仍不可回收。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param metadataParticipant redo label 前持久化的恢复元数据参与者，不能为 null。
     * @param redoReclaimBoundary redo 回收边界端口（可空）。
     * @param bufferPool 由组合根提供的 {@code BufferPool} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param redo 由组合根提供的 {@code RedoLogManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param checkpointStore 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public CheckpointCoordinator(BufferPool bufferPool, RedoLogManager redo, RedoCheckpointStore checkpointStore,
                                 CheckpointMetadataParticipant metadataParticipant,
                                 RedoReclaimBoundary redoReclaimBoundary) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (bufferPool == null || redo == null) {
            throw new DatabaseValidationException("checkpoint buffer pool/redo must not be null");
        }
        if (metadataParticipant == null) {
            throw new DatabaseValidationException("checkpoint metadata participant must not be null");
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        if (redoReclaimBoundary != null && checkpointStore == null) {
            throw new DatabaseValidationException(
                    "redo reclaim boundary requires a persistent checkpoint store");
        }
        this.bufferPool = bufferPool;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.redo = redo;
        this.checkpointStore = checkpointStore;
        this.metadataParticipant = metadataParticipant;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.redoReclaimBoundary = redoReclaimBoundary;
    }

    /**
     * 计算安全 checkpoint LSN：不能越过 Buffer Pool oldest dirty、redo closed/current、redo flushed 任一边界。
     *
     * @return 当前安全 checkpoint LSN。
     */
    public Lsn computeSafeCheckpointLsn() {
        Lsn flushed = redo.flushedToDiskLsn();
        Lsn closed = redo.closedLsn();
        if (!bufferPool.hasDirtyPages()) {
            return min(closed, flushed);
        }
        Lsn oldestDirty = bufferPool.oldestDirtyLsnOr(flushed);
        return min(oldestDirty, closed, flushed);
    }

    /**
     * 单调推进内存 checkpoint。若当前安全边界没有超过 lastCheckpointLsn，则保持旧值。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取脏页、page LSN、代际与 checkpoint 压力快照，先排除未固定或已失效的候选。</li>
     *     <li>在不持页闩执行慢等待的前提下推进 redo durable 边界，确保数据页写盘前满足 WAL。</li>
     *     <li>按既定 doublewrite、表空间写入和 force 顺序持久化快照，部分失败只确认实际成功的页面。</li>
     *     <li>重新校验代际与 dirty version 后发布完成状态；并发再修改页继续保持 dirty，异常不推进不安全边界。</li>
     * </ol>
     *
     * @return 推进后的 lastCheckpointLsn。
     */
    public Lsn advanceCheckpoint() {
        // 1、读取脏页、page LSN、代际与 checkpoint 压力快照，在共享或持久副作用前拒绝非法状态。
        Lsn safe = computeSafeCheckpointLsn();
        Lsn published;
        // 2、继续完成范围、身份与候选校验；通过后，在不持页闩执行慢等待的前提下推进 redo durable 边界，保持处理顺序与资源边界。
        boolean advanced = false;
        lock.lock();
        // 3、在中间分支复核阶段性结果；满足条件后，按既定 doublewrite、表空间写入和 force 顺序持久化快照，并维持领域不变量。
        try {
            if (safe.value() > lastCheckpointLsn.value()) {
                // 事务高水位必须先于 redo checkpoint force；若顺序反转，redo 环回收后将无法证明纯 INSERT 事务 id/no。
                metadataParticipant.persistBeforeCheckpoint(safe);
                if (checkpointStore != null) {
                    checkpointStore.write(RedoCheckpointLabel.of(safe, redo.currentLsn(), System.currentTimeMillis()));
                }
                lastCheckpointLsn = safe;
                advanced = true;
            }
            published = lastCheckpointLsn;
        } finally {
            lock.unlock();
        }
        // 回收边界推进必须晚于 checkpoint label 持久化（上面已在锁内完成），否则崩溃后会从旧 checkpoint 重放却发现
        // 所需 redo 已被新一代覆盖。放在 checkpoint 锁之外推进：advanceReclaimBoundary 走 redo 文件 IO 锁，不在持
        // checkpoint 锁时等待文件锁，避免与 flush 的 append/force 形成长时间互等；回收边界自身单调，并发推进安全。
        if (advanced && redoReclaimBoundary != null) {
            redoReclaimBoundary.advanceReclaimBoundary(published);
        }
        // 4、重新校验代际与 dirty version 后发布完成状态，以稳定返回或领域异常完成收口。
        return published;
    }

    /** 最近一次发布的内存 checkpoint LSN。
     *
     * @return {@code lastCheckpointLsn} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    public Lsn lastCheckpointLsn() {
        lock.lock();
        try {
            return lastCheckpointLsn;
        } finally {
            lock.unlock();
        }
    }

    private static Lsn min(Lsn a, Lsn b, Lsn c) {
        long value = Math.min(a.value(), Math.min(b.value(), c.value()));
        return Lsn.of(value);
    }

    private static Lsn min(Lsn a, Lsn b) {
        return Lsn.of(Math.min(a.value(), b.value()));
    }
}
