package cn.zhangyis.db.storage.flush;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;

/**
 * 可恢复 doublewrite 策略：data file write 前先把完整页副本写入 doublewrite 文件并 force，
 * 崩溃后可用该副本修复 torn/corrupt data page。
 */
public final class RecoverableDoublewriteStrategy implements DoublewriteStrategy {

    private final DoublewriteFileRepository repository;

    public RecoverableDoublewriteStrategy(DoublewriteFileRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("doublewrite repository must not be null");
        }
        this.repository = repository;
    }

    @Override
    public DoublewriteMode mode() {
        return DoublewriteMode.DETECT_AND_RECOVER;
    }

    @Override
    public void beforeDataFileWrite(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        repository.append(snapshot);
        repository.force();
    }
}
