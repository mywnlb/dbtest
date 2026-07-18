package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单进程内存 recovery progress journal。它以显式锁保护事件列表和序号水位，供 recovery 主线程写入、
 * 诊断线程读取快照；可选 sink 只把同一事件复制到诊断介质，不参与 crash 后的恢复决策。
 */
public final class RecoveryProgressJournal {

    /** 保护事件列表与下一个序号；不与 page latch、redo IO lock 或事务锁嵌套。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** progress 事件观察端口；默认 no-op，生产 engine 使用文件 sink 追加 JSONL。 */
    private final RecoveryProgressSink sink;
    /** 当前 JVM 生命周期内的事件序列，只能在 lock 下追加或复制。 */
    private final List<RecoveryProgressEvent> events = new ArrayList<>();
    /** 下一个事件序号；从 1 开始，便于诊断输出按自然序排序。 */
    private long nextSequence = 1;

    /**
     * 创建仅保留内存事件的 journal。适用于单元测试或调用方暂不需要持久诊断文件的场景。
     */
    public RecoveryProgressJournal() {
        this(RecoveryProgressSink.noop());
    }

    /**
     * 创建带自定义输出端口的 journal。事件会先进入内存快照，再写入 sink；sink 失败会抛出项目异常，
     * 由 recovery 总控决定是否 fail closed。
     *
     * @param sink progress 观察端口，不能为 null。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RecoveryProgressJournal(RecoveryProgressSink sink) {
        if (sink == null) {
            throw new DatabaseValidationException("recovery progress sink must not be null");
        }
        this.sink = sink;
    }

    /**
     * 创建同时保留内存快照、又追加 JSONL 文件的 progress journal。持久文件仅用于诊断，不会在下次启动时读取。
     *
     * @param path JSONL 文件路径。
     * @return 文件持久化 journal。
     */
    public static RecoveryProgressJournal persistent(Path path) {
        return new RecoveryProgressJournal(new FileRecoveryProgressSink(path));
    }

    /**
     * 记录阶段开始。开始事件只说明阶段已进入，redo 边界尚未知，用 0 作为诊断占位。
     *
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param stageName 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     */
    public void stageStarted(RecoveryMode mode, RecoveryStageName stageName) {
        append(mode, stageName, RecoveryProgressEventKind.STARTED,
                RecoveryState.RECOVERING, Lsn.of(0), "");
    }

    /**
     * 记录阶段成功完成。
     *
     * @param mode recovery 模式。
     * @param stageName 完成的阶段。
     * @param state 完成时的 recovery 状态。
     * @param recoveredToLsn 当前已知 redo 恢复边界。
     */
    public void stageCompleted(RecoveryMode mode, RecoveryStageName stageName,
                               RecoveryState state, Lsn recoveredToLsn) {
        stageCompleted(mode, stageName, state, recoveredToLsn, "");
    }

    /**
     * 记录带诊断明细的阶段成功完成。force-skip 会在 OPEN_TRAFFIC 阶段写入 skipped space 和跳过计数，
     * 便于启动日志说明本次恢复为何没有触达某些表空间。
     *
     * @param mode recovery 模式。
     * @param stageName 完成的阶段。
     * @param state 完成时的 recovery 状态。
     * @param recoveredToLsn 当前已知 redo 恢复边界。
     * @param detail 诊断明细；null 按空串记录。
     */
    public void stageCompleted(RecoveryMode mode, RecoveryStageName stageName,
                               RecoveryState state, Lsn recoveredToLsn, String detail) {
        append(mode, stageName, RecoveryProgressEventKind.COMPLETED, state, recoveredToLsn,
                detail == null ? "" : detail);
    }

    /**
     * 记录阶段失败。异常只转换为简短诊断文本，原始 cause 仍由 {@link CrashRecoveryService#lastError()} 保存。
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param stageName 恢复、checkpoint、doublewrite 或刷脏阶段的协作状态；不得为 {@code null}，阶段和持久化边界必须与当前实例的恢复状态机一致
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void stageFailed(RecoveryMode mode, RecoveryStageName stageName, Throwable cause) {
        if (cause == null) {
            throw new DatabaseValidationException("recovery progress failure cause must not be null");
        }
        String message = cause.getClass().getSimpleName()
                + (cause.getMessage() == null || cause.getMessage().isBlank() ? "" : ": " + cause.getMessage());
        append(mode, stageName, RecoveryProgressEventKind.FAILED,
                RecoveryState.FAILED, Lsn.of(0), message);
    }

    /**
     * 返回不可变事件快照。调用方拿到的是列表拷贝，无法修改 journal 内部状态，也不持有 journal 锁。
     *
     * @return 调用时刻的不可变状态集合或映射；没有已发布条目时返回空集合，调用方修改不会影响权威状态
     */
    public List<RecoveryProgressEvent> snapshot() {
        lock.lock();
        try {
            return List.copyOf(events);
        } finally {
            lock.unlock();
        }
    }

    private void append(RecoveryMode mode, RecoveryStageName stageName, RecoveryProgressEventKind kind,
                        RecoveryState state, Lsn recoveredToLsn, String detail) {
        lock.lock();
        try {
            RecoveryProgressEvent event = new RecoveryProgressEvent(nextSequence++, mode, stageName, kind,
                    state, recoveredToLsn, detail);
            events.add(event);
            sink.append(event);
        } finally {
            lock.unlock();
        }
    }
}
