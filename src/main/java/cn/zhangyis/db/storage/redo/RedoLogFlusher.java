package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * 同步 redo flusher（R1）：对 redo 文件执行 force/fsync，并在成功后发布 flushedToDiskLsn。
 */
public final class RedoLogFlusher {

    /** redo 文件仓储，负责 force。 */
    private final RedoLogFileRepository repository;
    /** 已 fsync 的最高 LSN；flush 模块 WAL gate 以后只读该边界。 */
    private Lsn flushedToDiskLsn = Lsn.of(0);

    /**
     * 创建 {@code RedoLogFlusher}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param repository 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoLogFlusher(RedoLogFileRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("redo log repository must not be null");
        }
        this.repository = repository;
    }

    /**
     * force redo 文件，并把 durable 边界推进到 target。
     *
     * @param target writer 已经完整写出的目标 LSN。
     * @return 已 durable 的最高 LSN。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Lsn flushTo(Lsn target) {
        if (target == null) {
            throw new DatabaseValidationException("redo flush target must not be null");
        }
        repository.force();
        if (target.value() > flushedToDiskLsn.value()) {
            flushedToDiskLsn = target;
        }
        return flushedToDiskLsn;
    }

    /** 已 fsync 的最高 LSN。 */
    public Lsn flushedToDiskLsn() {
        return flushedToDiskLsn;
    }
}
