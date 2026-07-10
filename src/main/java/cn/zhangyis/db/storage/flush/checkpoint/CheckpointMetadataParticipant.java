package cn.zhangyis.db.storage.flush.checkpoint;

import cn.zhangyis.db.domain.Lsn;

/**
 * redo checkpoint 之外、但必须在同一回收边界之前持久化的恢复元数据端口。
 *
 * <p>实现不得持有 page latch、行锁或长生命周期事务锁执行 IO。允许仅为复制 counter 快照短暂获取
 * {@code TransactionSystem} 的专用 counter lock，但必须在文件 force 前释放。调用发生在 checkpoint 协调器
 * 短锁内并可能执行一次有界文件 force；抛出异常会阻止 redo label、内存 checkpoint 和 reclaim boundary 前进。
 */
@FunctionalInterface
public interface CheckpointMetadataParticipant {

    /** 无附加元数据的兼容参与者。 */
    CheckpointMetadataParticipant NO_OP = checkpointLsn -> { };

    /**
     * 持久化覆盖 {@code checkpointLsn} 的恢复基线。
     *
     * @param checkpointLsn 即将发布的安全 redo checkpoint LSN。
     */
    void persistBeforeCheckpoint(Lsn checkpointLsn);
}
