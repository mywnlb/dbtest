package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单次 clustered mutation 的短 admission guard。guard 只覆盖 current-read 到物理 DML 完成的区间；关闭后
 * transaction/table 引用仍由 {@link OnlineDdlTableGate} 保留到 COMMIT、ROLLBACK 或 XA phase two。
 */
public final class OnlineDmlAdmission implements AutoCloseable {

    /** 创建 admission 的唯一 gate；负责 lease 计数和 candidate sequence 投影。 */
    private final OnlineDdlTableGate gate;
    /** 当前写事务的正 identity。 */
    private final TransactionId transactionId;
    /** 当前 clustered mutation 的表 identity。 */
    private final long tableId;
    /** admission 时冻结的可选 capture target；后续 phase 变化不会替换该引用。 */
    private final OnlineDdlCaptureTarget captureTarget;
    /** 防止异常补偿与 try-with-resources 重复递减 in-flight 计数。 */
    private final AtomicBoolean closed = new AtomicBoolean();

    OnlineDmlAdmission(OnlineDdlTableGate gate, TransactionId transactionId, long tableId,
                       OnlineDdlCaptureTarget captureTarget) {
        this.gate = gate;
        this.transactionId = transactionId;
        this.tableId = tableId;
        this.captureTarget = captureTarget;
    }

    /**
     * 返回 admission 时冻结的捕获定义。
     *
     * @return CAPTURING 时为唯一 target；ABSENT/ABORTING 时为空，调用方不得自行查询或打开日志
     */
    public Optional<OnlineIndexCaptureTarget> captureTarget() {
        return captureTarget instanceof OnlineIndexCaptureTarget indexTarget
                ? Optional.of(indexTarget) : Optional.empty();
    }

    /**
     * 返回任意Online DDL协议的冻结capture目标。DML路径应优先使用本入口，旧单索引调用仍可使用
     * {@link #captureTarget()}兼容视图。
     *
     * @return CAPTURING时为当前唯一通用target，否则为空
     */
    public Optional<OnlineDdlCaptureTarget> capture() {
        return Optional.ofNullable(captureTarget);
    }

    /**
     * 在业务 undo、MTR 和聚簇页修改前追加 opaque candidate。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证 guard 尚未关闭且 admission 确实携带 capture target。</li>
     *     <li>由 gate 取得短 I/O lease，随后在 gate 锁外执行日志 append。</li>
     *     <li>成功时把 sequence 投影到事务 high-water；容量 abort 返回 0 并令 build 进入 ABORTING。</li>
     * </ol>
     *
     * @param payload 已按 target row format 编码的 before/after physical entry；不得为 {@code null}
     * @return 正 candidate sequence；容量已触发 durable abort 时返回 0，当前业务 DML 可以继续
     * @throws DatabaseValidationException admission 无 target 或 payload 无效时抛出，调用方不得执行物理 DML
     * @throws DatabaseRuntimeException row-log I/O 失败且不能转为安全 abort 时抛出，事务应回滚或引擎 fail-stop
     */
    public long appendCandidate(byte[] payload) {
        // 1. guard 生命周期是 candidate 与 clustered mutation 顺序约束的一部分，关闭后不允许迟到 append。
        if (closed.get()) {
            throw new DatabaseRuntimeException("online DML admission is already closed");
        }
        if (captureTarget == null || payload == null) {
            throw new DatabaseValidationException("online DML admission has no capture target/payload");
        }
        // 2、3. gate 只在内存计数和发布 sequence 时持锁，FileChannel I/O 始终发生在锁外。
        return gate.appendCandidate(transactionId, tableId, captureTarget, payload);
    }

    /** 释放单次 clustered mutation 的 in-flight 计数；重复关闭为 no-op。 */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            gate.closeAdmission(tableId);
        }
    }
}
