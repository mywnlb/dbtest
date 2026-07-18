package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.storage.redo.LogRange;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaRecord;
import cn.zhangyis.db.storage.redo.TransactionStateDeltaSink;
import cn.zhangyis.db.storage.redo.RedoLogBatch;
import cn.zhangyis.db.storage.redo.RedoRecord;

import java.util.List;

/**
 * 单次 crash recovery 的事务表上下文。
 *
 * <p>对象在 request 构造时同时提供 redo sink 和 sidecar 来源；真正的表在 RecoveryService 读出 redo checkpoint 后
 * 初始化，随后仅由同一 recovery 线程写入。它不加全局锁，也不拥有/关闭外部 store 生命周期。
 */
public final class TransactionRecoveryContext {

    /** 事务高水位 sidecar，由 StorageEngine 生命周期持有。 */
    private final TransactionRecoveryCheckpointSource checkpointSource;
    /** 当前恢复尝试的线程独占表；每次 initialize 都重建，避免失败重试混入旧记录。 */
    private RecoveredTransactionTable table;
    /** 稳定 sink 身份；RecoveryRequest 用同一实例校验 dispatcher 绑定，避免每次方法引用产生不同对象。 */
    private final TransactionStateDeltaSink deltaSink = this::accept;

    private TransactionRecoveryContext(TransactionRecoveryCheckpointSource checkpointSource) {
        this.checkpointSource = checkpointSource;
    }

    /** 创建引用指定 sidecar 的恢复上下文；不会提前读取文件或发布 counter。
     *
     * @param checkpointSource 选择 {@code using} 分支的 {@code TransactionRecoveryCheckpointSource} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @return {@code using} 产生的恢复或持久化阶段对象；成功时不为 {@code null}，其中的 durable 边界不超过已安全完成的工作
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static TransactionRecoveryContext using(TransactionRecoveryCheckpointSource checkpointSource) {
        if (checkpointSource == null) {
            throw new DatabaseValidationException("transaction recovery checkpoint source must not be null");
        }
        return new TransactionRecoveryContext(checkpointSource);
    }

    /**
     * 返回可在 request 构造时注入 dispatcher 的 sink。调用在 initialize 之后才合法，由 RecoveryService 阶段顺序保证。
     */
    public TransactionStateDeltaSink deltaSink() {
        return deltaSink;
    }

    /** 在 redo replay 前按已持久 redo checkpoint 创建新的恢复表。
     *
     * @param redoCheckpointLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void initialize(Lsn redoCheckpointLsn) {
        if (redoCheckpointLsn == null) {
            throw new DatabaseValidationException("transaction recovery redo checkpoint must not be null");
        }
        table = RecoveredTransactionTable.open(redoCheckpointLsn, checkpointSource.readLatest());
    }

    /** redo replay 完成后复制不可变 snapshot；未初始化表示 recovery 编排顺序错误。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    RecoveredTransactionSnapshot snapshot() {
        if (table == null) {
            throw new TransactionRecoveryException("transaction recovery context was not initialized");
        }
        return table.snapshot();
    }

    /**
     * 校验实际扫描到的最后完整 redo 至少覆盖 sidecar LSN。sidecar 领先 redo label 是合法 crash window，
     * 但若 redo 文件尾又被截断到 sidecar 之前，就不能仅凭高估 counter 继续启动。
     *
     * @param recoveredToLsn redo 日志边界；不得为 {@code null}，必须单调且与调用方已发布的页或事务状态一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws TransactionRecoveryException 恢复证据、阶段顺序或事务重建无法继续时抛出；owner 应停止恢复并保持普通流量关闭
     */
    void verifyRedoCoverage(Lsn recoveredToLsn) {
        if (recoveredToLsn == null) {
            throw new DatabaseValidationException("transaction recovery covered redo LSN must not be null");
        }
        RecoveredTransactionSnapshot snapshot = snapshot();
        if (recoveredToLsn.value() < snapshot.baselineCheckpointLsn().value()) {
            throw new TransactionRecoveryException(
                    "redo stream does not cover transaction recovery sidecar: recoveredTo="
                            + recoveredToLsn.value() + ", sidecarCheckpoint="
                            + snapshot.baselineCheckpointLsn().value());
        }
    }

    /**
     * READ_ONLY_VALIDATE 的 scan-only trx 校验：只提取 non-page transaction delta 顺序送表，不调用 page handler。
     *
     * @param batches 参与 {@code validateTransactionDeltas} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void validateTransactionDeltas(List<RedoLogBatch> batches) {
        if (batches == null) {
            throw new DatabaseValidationException("read-only transaction redo batches must not be null");
        }
        for (RedoLogBatch batch : batches) {
            if (batch == null) {
                throw new DatabaseValidationException("read-only transaction redo batch must not be null");
            }
            for (RedoRecord record : batch.records()) {
                if (record instanceof TransactionStateDeltaRecord delta) {
                    accept(batch.range(), delta);
                }
            }
        }
    }

    private void accept(LogRange range, TransactionStateDeltaRecord record) {
        if (table == null) {
            throw new TransactionRecoveryException(
                    "transaction state redo arrived before recovery baseline initialization");
        }
        table.accept(range, record);
    }
}
