package cn.zhangyis.db.storage.engine;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;

import java.nio.file.Path;
import java.time.Duration;

/**
 * {@link StorageEngine} 启动配置（E1）。固定文件布局在 {@code baseDir} 下：redo 日志 {@code redo.log}、
 * redo control（checkpoint label）{@code redo-control}、系统 undo 表空间 {@code undo_<undoSpaceId>.ibu}；
 * 数据表空间文件路径由调用方建/开（本片无 data dictionary）。
 *
 * @param baseDir                  引擎数据目录（redo/control/undo 文件所在）。
 * @param pageSize                 实例页大小。
 * @param bufferPoolCapacityFrames buffer pool 帧数；E1 假设 &gt; 工作集（不发生脏页淘汰，见 slice WAL 限制）。
 * @param undoSpaceId              系统 undo 表空间编号（引擎 open 时建/开）。
 * @param undoSpaceInitialPages    undo 表空间初始页数。
 * @param slotCapacity             rollback segment slot 容量（{@code RollbackSegmentSlotManager}）。
 * @param maxVersionHops           MVCC 版本链最大跳数（{@code MvccReader} 防环）。
 * @param flushTimeout             {@code FlushService.flushThrough} 的等待超时。
 * @param redoCapacityBytes        redo 容量策略字节数（{@code RedoCapacityPolicy.fixed}）。
 */
public record EngineConfig(Path baseDir, PageSize pageSize, int bufferPoolCapacityFrames,
                           SpaceId undoSpaceId, PageNo undoSpaceInitialPages, int slotCapacity,
                           int maxVersionHops, Duration flushTimeout, long redoCapacityBytes) {

    public EngineConfig {
        if (baseDir == null || pageSize == null || undoSpaceId == null
                || undoSpaceInitialPages == null || flushTimeout == null) {
            throw new DatabaseValidationException("engine config object fields must not be null");
        }
        if (bufferPoolCapacityFrames <= 0) {
            throw new DatabaseValidationException("bufferPoolCapacityFrames must be positive: " + bufferPoolCapacityFrames);
        }
        if (undoSpaceInitialPages.value() <= 0) {
            throw new DatabaseValidationException("undoSpaceInitialPages must be positive: " + undoSpaceInitialPages.value());
        }
        if (slotCapacity <= 0) {
            throw new DatabaseValidationException("slotCapacity must be positive: " + slotCapacity);
        }
        if (maxVersionHops <= 0) {
            throw new DatabaseValidationException("maxVersionHops must be positive: " + maxVersionHops);
        }
        if (flushTimeout.isZero() || flushTimeout.isNegative()) {
            throw new DatabaseValidationException("flushTimeout must be positive: " + flushTimeout);
        }
        if (redoCapacityBytes <= 0) {
            throw new DatabaseValidationException("redoCapacityBytes must be positive: " + redoCapacityBytes);
        }
    }

    /** redo 日志文件路径。 */
    public Path redoFile() {
        return baseDir.resolve("redo.log");
    }

    /** redo control（checkpoint label）文件路径。 */
    public Path redoControlFile() {
        return baseDir.resolve("redo-control");
    }

    /** 系统 undo 表空间数据文件路径。 */
    public Path undoFile() {
        return baseDir.resolve("undo_" + undoSpaceId.value() + ".ibu");
    }
}
