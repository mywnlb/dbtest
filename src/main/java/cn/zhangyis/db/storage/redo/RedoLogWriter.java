package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * 同步 redo writer（R1）：把 {@link RedoLogBatch} 追加到 redo 文件并推进 written LSN。
 *
 * <p>简化点：本批不引入后台线程和 ready-for-write tracker；调用方在 {@link RedoLogManager#flush()} 中同步驱动。
 */
public final class RedoLogWriter {

    /** redo 文件仓储，负责实际 channel append。 */
    private final RedoLogFileRepository repository;
    /** 已写入 OS/page cache 的最高 LSN；未 fsync，不代表 durable。 */
    private Lsn writtenToDiskLsn = Lsn.of(0);

    public RedoLogWriter(RedoLogFileRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("redo log repository must not be null");
        }
        this.repository = repository;
    }

    /**
     * 写出一个批次并推进 written LSN。调用方必须按 LSN 顺序调用。
     *
     * @param batch 待写批次。
     * @return 写出后的最高 written LSN。
     */
    public Lsn write(RedoLogBatch batch) {
        if (batch == null) {
            throw new DatabaseValidationException("redo log batch must not be null");
        }
        repository.append(batch);
        writtenToDiskLsn = batch.range().end();
        return writtenToDiskLsn;
    }

    /** 已写入但未必 fsync 的最高 LSN。 */
    public Lsn writtenToDiskLsn() {
        return writtenToDiskLsn;
    }
}
