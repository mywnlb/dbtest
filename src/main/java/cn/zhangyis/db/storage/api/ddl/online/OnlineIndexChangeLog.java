package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.domain.TransactionId;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * Online index row-log 的稳定端口。调用方只能使用 identity/sequence/opaque payload，不暴露FileChannel或文件锁。
 */
public interface OnlineIndexChangeLog extends OnlineDdlChangeLog {

    /**
     * terminal reserve 的格式下限。该空间同时覆盖一次尚未落盘 candidate 所需的
     * FORCE_WATERMARK 和最长 ABORT_REQUIRED frame，并保留格式演进余量；低于此值时，
     * “容量耗尽仍能持久终止”这一恢复不变量无法成立。
     */
    int MIN_TERMINAL_RESERVE_BYTES = 256;

    /** @return 文件 offset 0 已 force 的 immutable owner/manifest header；返回对象不暴露可变 manifest 数组。 */
    OnlineIndexLogHeader header();

    /** 旧build identity到通用capture identity的无损适配。 */
    @Override
    default OnlineDdlCaptureId captureId() {
        return OnlineDdlCaptureId.of(header().buildId().value());
    }

    /** @return 当前 build 唯一的绝对规范路径；调用方只能交回所属受控文件工厂。 */
    Path path();

    /**
     * 在 clustered 物理修改前追加 candidate，但不单独 force。
     *
     * @param transactionId 已分配 write id 的 ACTIVE 事务；不得为 NONE
     * @param payload 由 capture target codec 产生的完整 before/after entry 字节；不得为 {@code null}
     * @return 正 sequence；容量耗尽且 ABORT_REQUIRED 已 durable 时返回 0，业务 DML 可继续
     */
    long appendCandidate(TransactionId transactionId, byte[] payload);

    /**
     * 追加 coordinator 状态 frame。
     *
     * @param type 非 CANDIDATE、非 FORCE_WATERMARK、非 ABORT_REQUIRED 的协调器状态类型；abort 必须走强制接口
     * @param payload 类型相关完整 payload；空语义使用零长度数组而非 {@code null}
     * @return 新 frame 的正 sequence；是否 durable 由后续 force 或 ABORT_REQUIRED 类型语义决定
     */
    long appendState(OnlineIndexLogRecordType type, byte[] payload);

    /**
     * 有界等待并强制日志覆盖目标 candidate/state sequence。
     *
     * @param sequence 不大于 append high-water 的正目标
     * @param timeout 文件锁与 group-force follower 的正等待上界
     */
    void forceThrough(long sequence, Duration timeout);

    /**
     * 使用 terminal reserve 幂等追加并 force ABORT_REQUIRED。
     *
     * @param reason 触发回滚的稳定分类
     * @param timeout 文件锁/force 的正等待上界
     */
    void markAbortRequired(OnlineDdlAbortReason reason, Duration timeout);

    /** @return 当前 generation 是否已经持久观察到 ABORT_REQUIRED。 */
    boolean abortRequired();

    /** @return 当前 generation 最后成功追加的 frame sequence；仅有 header 时为 0。 */
    long highestAppendedSequence();

    /** @return 已由成功 force 覆盖的 frame high-water；尚未 force 状态 frame 时为 0。 */
    long highestForcedSequence();

    /** @return 当前 header 与所有 frame 的文件总字节数。 */
    long sizeBytes();

    /**
     * 在一次文件状态锁内复制 generation、candidate、容量、sequence 和终止状态。
     *
     * @return 不持有 channel/frame 引用的不可变诊断快照
     */
    OnlineChangeLogSnapshot snapshot();

    /** @return 当前 generation 已完成 CRC/sequence 校验的不可变 frame 快照。 */
    List<OnlineIndexLogRecord> readAll();

    /**
     * 恢复期截断全部 frame并 force，只保留 immutable manifest；后续 append 使用更高 generation。
     *
     * @param timeout 正恢复等待上界；当前 FileChannel force 失败时实例进入 fail-stop
     */
    void resetToManifest(Duration timeout);

    /** 关闭本实例拥有的文件句柄；不删除 durable 文件，重复调用幂等。 */
    @Override
    void close();
}
