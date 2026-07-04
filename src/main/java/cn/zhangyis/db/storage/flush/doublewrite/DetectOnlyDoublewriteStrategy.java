package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.buf.FlushPageSnapshot;

/**
 * detect-only doublewrite 策略：data file write 前只持久化页定位、pageLSN 与校验元数据。
 *
 * <p>该策略保留 doublewrite-before-data-file 的顺序约束，因此仍在 before 阶段 force doublewrite 文件；但它不保存
 * 完整页镜像，恢复期只能发现并报告 torn/corrupt page，不能用该 slot 修复 data file。
 */
public final class DetectOnlyDoublewriteStrategy implements DoublewriteStrategy {

    private final DoublewriteFileRepository repository;

    public DetectOnlyDoublewriteStrategy(DoublewriteFileRepository repository) {
        if (repository == null) {
            throw new DatabaseValidationException("doublewrite repository must not be null");
        }
        this.repository = repository;
    }

    @Override
    public DoublewriteMode mode() {
        return DoublewriteMode.DETECT_ONLY;
    }

    @Override
    public void beforeDataFileWrite(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        boolean appended = false;
        try {
            repository.appendDetectOnly(snapshot);
            appended = true;
            repository.force();
        } catch (DatabaseRuntimeException e) {
            if (appended) {
                repository.releaseSlot(snapshot);
            }
            throw e;
        }
    }

    @Override
    public void afterDataFileWrite(FlushPageSnapshot snapshot) {
        if (snapshot == null) {
            throw new DatabaseValidationException("flush page snapshot must not be null");
        }
        repository.releaseSlot(snapshot);
    }
}
