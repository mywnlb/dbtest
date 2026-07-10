package cn.zhangyis.db.storage.redo;

/**
 * 事务状态 logical redo 的顺序消费端口。
 *
 * <p>redo handler 只负责把已解码 record 连同原始 batch LSN 边界交付给该端口；它不依赖 recovery/trx 包，
 * 也不执行事务提交、回滚、MVCC 或锁等待。
 */
@FunctionalInterface
public interface TransactionStateDeltaSink {

    /** 不收集事务证据的兼容 sink。 */
    TransactionStateDeltaSink NO_OP = (range, record) -> { };

    /**
     * 按 redo 文件顺序接收一条事务状态变化。
     *
     * @param range record 所属原始 MTR batch LSN 范围。
     * @param record 已通过 frame codec 校验的事务状态 record。
     */
    void accept(LogRange range, TransactionStateDeltaRecord record);
}
