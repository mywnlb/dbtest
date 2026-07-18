package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.List;

/**
 * 事务状态 logical redo 的恢复 handler。non-page record 按 batch 顺序交给可注入的
 * {@link TransactionStateDeltaSink}；默认 sink 为 no-op，正式 StorageEngine recovery 注入线程独占事务恢复表。
 */
public final class TransactionStateRedoHandler implements RedoApplyHandler {

    /** 顺序消费端口；默认 no-op 维持通用 page replay 的兼容行为。 */
    private final TransactionStateDeltaSink sink;

    /** 创建不收集事务恢复证据的兼容 handler。 */
    public TransactionStateRedoHandler() {
        this(TransactionStateDeltaSink.NO_OP);
    }

    /** 创建把已解码 record 顺序交给指定 sink 的 handler。
     *
     * @param sink 由组合根提供的 {@code TransactionStateDeltaSink} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public TransactionStateRedoHandler(TransactionStateDeltaSink sink) {
        if (sink == null) {
            throw new DatabaseValidationException("transaction state delta sink must not be null");
        }
        this.sink = sink;
    }

    /**
     * 判断 {@code supports} 所表达的Redo/WAL条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code supports} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    @Override
    public boolean supports(RedoRecord record) {
        return record instanceof TransactionStateDeltaRecord;
    }

    /**
     * 事务状态 redo 不修改物理页，因此不触发表空间 skip predicate，也不访问 PageStore。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public List<PageId> affectedPages(RedoRecord record) {
        if (!(record instanceof TransactionStateDeltaRecord)) {
            throw new DatabaseValidationException("transaction state handler received unsupported record: "
                    + record.getClass().getName());
        }
        return List.of();
    }

    /**
     * 定位并读取Redo/WAL领域对象；先校验标识与准入状态，返回值只暴露稳定视图或受控句柄。
     *
     * @param range redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param context redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code openBatch} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public RedoApplyBatchHandler openBatch(LogRange range, RedoApplyContext context) {
        if (range == null || context == null) {
            throw new DatabaseValidationException("transaction state redo apply range/context must not be null");
        }
        return new Batch(range, sink);
    }

    /** 单个 redo batch 的交付会话；只持不可变 range/sink，不访问页或事务系统。 */
    private static final class Batch implements RedoApplyBatchHandler {

        /**
         * 本次物理修改持有的 {@code range} redo 状态；LSN、预算和批次边界必须单调，刷页与恢复路径依赖它维护 WAL。
         */
        private final LogRange range;
        /**
         * 本对象的权威状态机字段 {@code sink}；只有合法转换方法可以更新，更新受显式锁、原子发布或单一 owner 线程保护，下游据此决定可执行阶段。
         */
        private final TransactionStateDeltaSink sink;

        private Batch(LogRange range, TransactionStateDeltaSink sink) {
            this.range = range;
            this.sink = sink;
        }

        /**
         * 执行Redo/WAL恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
         *
         * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
         * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
         */
        @Override
        public void apply(RedoRecord record) {
            if (!(record instanceof TransactionStateDeltaRecord delta)) {
                throw new DatabaseValidationException("transaction state handler received unsupported record: "
                        + record.getClass().getName());
            }
            sink.accept(range, delta);
        }

        /**
         * 推进 {@code finish} 对应的Redo/WAL阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override
        public void finish() {
        }
    }
}
