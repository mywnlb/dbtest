package cn.zhangyis.db.storage.flush.doublewrite;

import cn.zhangyis.db.storage.buf.FlushPageSnapshot;
import cn.zhangyis.db.storage.flush.FlushBatchSource;

import java.util.List;

/**
 * data file 写入前后的 doublewrite 策略。FlushCoordinator 必须先调用 {@link #beforeDataFileWrite}
 * 并等待其持久化成功，再写 tablespace data file，以满足 doublewrite 防 torn page 的顺序约束。
 */
public interface DoublewriteStrategy {

    /** 当前策略模式。
     *
     * @return {@code mode} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     */
    DoublewriteMode mode();

    /**
     * data file write 前调用；可写入并 fsync doublewrite 副本。
     *
     * @param snapshot 即将写 data file 的页镜像。
     */
    void beforeDataFileWrite(FlushPageSnapshot snapshot);

    /** 批量 data-file 写入前钩子；默认逐页实现保持旧策略兼容。
     *
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    default void beforeDataFileWriteBatch(List<FlushPageSnapshot> snapshots) {
        if (snapshots == null) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "doublewrite batch snapshots must not be null");
        }
        for (FlushPageSnapshot snapshot : snapshots) {
            beforeDataFileWrite(snapshot);
        }
    }

    /** source-aware 批量入口；旧实现默认沿用单文件兼容路径。
     *
     * @param source 选择 {@code beforeDataFileWriteBatch} 分支的 {@code FlushBatchSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    default void beforeDataFileWriteBatch(FlushBatchSource source, List<FlushPageSnapshot> snapshots) {
        beforeDataFileWriteBatch(snapshots);
    }

    /**
     * data file write 成功后调用。F1 无 slot 回收语义，默认 no-op。
     *
     * @param snapshot 已写 data file 的页镜像。
     */
    default void afterDataFileWrite(FlushPageSnapshot snapshot) {
    }

    /** 批量 data-file 写入完成钩子；默认逐页释放。
     *
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws cn.zhangyis.db.common.exception.DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    default void afterDataFileWriteBatch(List<FlushPageSnapshot> snapshots) {
        if (snapshots == null) {
            throw new cn.zhangyis.db.common.exception.DatabaseValidationException(
                    "doublewrite batch snapshots must not be null");
        }
        for (FlushPageSnapshot snapshot : snapshots) {
            afterDataFileWrite(snapshot);
        }
    }

    /** source-aware 完成入口。
     *
     * @param source 选择 {@code afterDataFileWriteBatch} 分支的 {@code FlushBatchSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    default void afterDataFileWriteBatch(FlushBatchSource source, List<FlushPageSnapshot> snapshots) {
        afterDataFileWriteBatch(snapshots);
    }

    /**
     * data file 尚未完成时释放 reservation；磁盘副本保留给 crash recovery，避免 slot 被永久占用。
     *
     * @param source 选择 {@code abortDataFileWriteBatch} 分支的 {@code FlushBatchSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param snapshots 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     */
    default void abortDataFileWriteBatch(FlushBatchSource source, List<FlushPageSnapshot> snapshots) {
        afterDataFileWriteBatch(source, snapshots);
    }
}
