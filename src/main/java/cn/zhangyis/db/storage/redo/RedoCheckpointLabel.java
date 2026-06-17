package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

/**
 * redo control 文件中的 checkpoint label。
 *
 * <p>该对象表达 fuzzy checkpoint 的恢复起点：恢复阶段可以跳过 {@code endLsn <= checkpointLsn} 的完整 redo
 * 批次。{@code currentLsnAtCheckpoint} 用于诊断 checkpoint 写出时 redo 生成边界，后续 capacity pressure 与
 * control 文件扩展会依赖它判断 checkpoint age。
 *
 * @param checkpointLsn 已持久化发布的恢复起点 LSN。
 * @param currentLsnAtCheckpoint 写 label 时 redo manager 的当前 LSN。
 * @param createdAtMillis label 创建时间，使用调用方注入的 epoch millis，便于测试稳定断言。
 */
public record RedoCheckpointLabel(Lsn checkpointLsn, Lsn currentLsnAtCheckpoint, long createdAtMillis) {

    public RedoCheckpointLabel {
        if (checkpointLsn == null || currentLsnAtCheckpoint == null) {
            throw new DatabaseValidationException("checkpoint label LSN fields must not be null");
        }
        if (createdAtMillis < 0) {
            throw new DatabaseValidationException("checkpoint label createdAtMillis must not be negative: "
                    + createdAtMillis);
        }
        if (currentLsnAtCheckpoint.value() < checkpointLsn.value()) {
            throw new DatabaseValidationException("checkpoint current LSN must cover checkpoint LSN: checkpoint="
                    + checkpointLsn + ", current=" + currentLsnAtCheckpoint);
        }
    }

    /**
     * 创建普通 checkpoint label。使用静态工厂让测试和生产代码表达字段语义，避免三个 long/Lsn 参数混淆。
     */
    public static RedoCheckpointLabel of(Lsn checkpointLsn, Lsn currentLsnAtCheckpoint, long createdAtMillis) {
        return new RedoCheckpointLabel(checkpointLsn, currentLsnAtCheckpoint, createdAtMillis);
    }

    /**
     * 空 control 文件或两个 slot 均无效时的安全初始 label。恢复从 redo 文件头开始扫描。
     */
    public static RedoCheckpointLabel initial() {
        return new RedoCheckpointLabel(Lsn.of(0), Lsn.of(0), 0);
    }
}
