package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;

import java.util.List;

/**
 * FSP page allocation/free 逻辑 redo handler。0.19b 只把 allocation intent 用于恢复期物理容量对齐：
 * 若崩溃发生在 autoextend 尚未 durable 但 redo 已 durable 的窗口，先把数据文件扩到能容纳目标页，再让后续
 * {@link PageRedoApplyHandler} 重放 {@link PageInitRecord}/{@link PageBytesRecord}。
 *
 * <p>0.19c 起 free intent 也由本 handler 接住，但 apply 为 no-op：它只给 FORCE_SKIP 和诊断提供 affected page，
 * 不重新执行 free-list 或 segment 状态机。FSP 账本字段由同一 batch 的 metadata delta 恢复，未迁移的页信封
 * 或生命周期字节仍可与 {@link PageBytesRecord} 混合回放。
 */
public final class FspPageAllocationRedoHandler implements RedoApplyHandler {

    /**
     * 判断 {@code supports} 所表达的Redo/WAL条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code supports} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    @Override
    public boolean supports(RedoRecord record) {
        return record instanceof FspPageAllocationRecord || record instanceof FspPageFreeRecord;
    }

    /**
     * 计算 {@code affectedPages} 所表达的Redo/WAL数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    @Override
    public List<PageId> affectedPages(RedoRecord record) {
        if (record instanceof FspPageAllocationRecord allocation) {
            return List.of(allocation.allocatedPageId());
        }
        if (record instanceof FspPageFreeRecord free) {
            return List.of(free.freedPageId());
        }
        throw unsupported(record);
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
            throw new DatabaseValidationException("FSP allocation redo range/context must not be null");
        }
        return new Batch(context);
    }

    /**
     * 校验 {@code requireAllocationRecord} 涉及的Redo/WAL结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code requireAllocationRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    private static FspPageAllocationRecord requireAllocationRecord(RedoRecord record) {
        if (record instanceof FspPageAllocationRecord allocation) {
            return allocation;
        }
        throw unsupported(record);
    }

    private static RedoLogCorruptedException unsupported(RedoRecord record) {
        return new RedoLogCorruptedException("unsupported FSP intent redo record: "
                + (record == null ? "null" : record.getClass().getName()));
    }

    /**
     * Redo/WAL的 {@code Batch} 批处理上下文；它聚合同一批次的输入与阶段结果，批次结束后不得跨请求复用。
     */
    private static final class Batch implements RedoApplyBatchHandler {

        /** recovery 物理页上下文；本 handler 只调用 PageStore.ensureCapacity，不读取或写回页内容。 */
        private final RedoApplyContext context;

        private Batch(RedoApplyContext context) {
            this.context = context;
        }

        /**
         * 执行Redo/WAL恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
         *
         * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
         */
        @Override
        public void apply(RedoRecord record) {
            if (record instanceof FspPageAllocationRecord allocation) {
                PageId pageId = allocation.allocatedPageId();
                context.pageStore().ensureCapacity(pageId.spaceId(), PageNo.of(pageId.pageNo().value() + 1));
            } else if (!(record instanceof FspPageFreeRecord)) {
                throw unsupported(record);
            }
        }

        /**
         * 推进 {@code finish} 对应的Redo/WAL阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override
        public void finish() {
            // FSP allocation intent 没有批末 pageLSN 写回；pageLSN 仍由 page handler 处理物理页。
        }
    }
}
