package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Redo apply 分发器。0.19a 起支持多 handler registry；0.19b 起默认生产入口同时注册
 * FSP allocation intent handler 与 PAGE_INIT/PAGE_BYTES 物理页 handler。
 *
 * <p>分发器必须保持 batch 内 record 的原始顺序：page handler 依赖批末统一 pageLSN，未来 FSP/BTree
 * handler 也可能依赖同一 MTR 内的先后关系。因此这里按 record 顺序解析 handler 并调用 batch session，
 * 只在 batch 末尾调用各 handler 的 {@link RedoApplyBatchHandler#finish()}。
 */
public final class RedoApplyDispatcher {

    /** 已注册 handler，顺序用于诊断与批末 finish 的首次出现顺序；构造后不可变。 */
    private final List<RedoApplyHandler> handlers;
    /** 标准 page dispatcher 绑定的 trx sink；自定义 registry 为 null，不能伪装成 formal recovery wiring。 */
    private final TransactionStateDeltaSink transactionStateSink;

    /**
     * 创建 {@code RedoApplyDispatcher}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param handlers 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param transactionStateSink 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private RedoApplyDispatcher(List<RedoApplyHandler> handlers,
                                TransactionStateDeltaSink transactionStateSink) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (handlers == null) {
            throw new DatabaseValidationException("redo apply handlers must not be null");
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        for (RedoApplyHandler handler : handlers) {
            if (handler == null) {
                throw new DatabaseValidationException("redo apply handler must not be null");
            }
        }
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.handlers = List.copyOf(handlers);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.transactionStateSink = transactionStateSink;
    }

    /** 创建生产恢复分发器。保留历史命名，当前默认包含 FSP allocation、page handler 与 non-page trx handler。 */
    public static RedoApplyDispatcher pageDispatcher() {
        return pageDispatcher(TransactionStateDeltaSink.NO_OP);
    }

    /**
     * 创建带事务状态顺序消费端口的生产恢复分发器。页/FSP handler 集合不变，只有 non-page trx handler
     * 把稳定 record 交给 sink。
     *
     * @param transactionStateSink 事务状态 redo 消费端口。
     * @return dispatcher。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RedoApplyDispatcher pageDispatcher(TransactionStateDeltaSink transactionStateSink) {
        if (transactionStateSink == null) {
            throw new DatabaseValidationException("transaction state delta sink must not be null");
        }
        return new RedoApplyDispatcher(List.of(
                new FspPageAllocationRedoHandler(),
                new PageRedoApplyHandler(),
                new TransactionStateRedoHandler(transactionStateSink)), transactionStateSink);
    }

    /**
     * 创建自定义 handler registry 的恢复分发器。每个 record 在 apply 时必须恰好匹配一个 handler。
     *
     * @param handlers handler 列表；允许为空，但实际 apply 会因没有匹配 handler 而失败。
     * @return dispatcher。
     */
    public static RedoApplyDispatcher withHandlers(List<RedoApplyHandler> handlers) {
        return new RedoApplyDispatcher(handlers, null);
    }

    /**
     * 判断该标准 dispatcher 是否绑定到同一个 trx sink 实例。RecoveryRequest 用身份校验阻止 formal context
     * 与 no-op/custom dispatcher 误组合；sink 本身不从此方法泄漏。
     *
     * @param sink 由组合根提供的 {@code TransactionStateDeltaSink} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code isBoundToTransactionStateSink} 调用
     * @return {@code isBoundToTransactionStateSink} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    public boolean isBoundToTransactionStateSink(TransactionStateDeltaSink sink) {
        return sink != null && transactionStateSink == sink;
    }

    /**
     * 应用一个 redo 批次。返回摘要是为了让 crash recovery 在最终报告中暴露诊断信息；
     * 既有调用方可以忽略返回值，默认路径不做任何 skip 过滤。
     *
     * @param batch 原始 MTR redo batch。
     * @param context 页重放上下文。
     * @return 本批次的扫描/应用摘要。
     */
    public RedoApplySummary apply(RedoLogBatch batch, RedoApplyContext context) {
        return apply(batch, context, pageId -> false);
    }

    /**
     * 应用一个 redo 批次，并在触碰 PageStore 前过滤不允许访问的 PageId。
     *
     * <p>过滤后仍保留原始 batch range：page handler 必须用原始 end LSN 盖 pageLSN，不能把过滤后的
     * 子集当成新的 redo batch，否则恢复幂等判断会低估已重放边界。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param batch 原始 MTR redo batch。
     * @param context 页重放上下文。
     * @param shouldSkipPage 返回 true 的页不会被 read/write/ensureCapacity/force。
     * @return 本批次的扫描/应用/跳过记录摘要。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoApplySummary apply(RedoLogBatch batch, RedoApplyContext context, Predicate<PageId> shouldSkipPage) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (batch == null || context == null) {
            throw new DatabaseValidationException("redo apply batch/context must not be null");
        }
        if (shouldSkipPage == null) {
            throw new DatabaseValidationException("redo apply skip predicate must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        Map<RedoApplyHandler, RedoApplyBatchHandler> sessions = new LinkedHashMap<>();
        int skipped = 0;
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        int appliedRecords = 0;
        for (RedoRecord record : batch.records()) {
            RedoApplyHandler handler = resolveHandler(record);
            if (shouldSkip(record, handler, shouldSkipPage)) {
                skipped++;
                continue;
            }
            RedoApplyBatchHandler session = sessions.computeIfAbsent(handler,
                    h -> openSession(h, batch.range(), context));
            session.apply(record);
            appliedRecords++;
        }
        for (RedoApplyBatchHandler session : sessions.values()) {
            session.finish();
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return new RedoApplySummary(1, appliedRecords > 0 ? 1 : 0, skipped);
    }

    /**
     * 按文件顺序应用多个 redo 批次。默认路径不跳过任何表空间。
     *
     * @param batches 从 redo 文件顺序读出的 batch 列表。
     * @param context 页重放上下文。
     * @return redo apply 摘要。
     */
    public RedoApplySummary applyAll(List<RedoLogBatch> batches, RedoApplyContext context) {
        return applyAll(batches, context, pageId -> false);
    }

    /**
     * 按文件顺序应用多个 redo 批次，并在每个 batch 内执行页级 skip 过滤。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param batches 从 redo 文件顺序读出的 batch 列表。
     * @param context 页重放上下文。
     * @param shouldSkipPage 返回 true 的页不会被 read/write/ensureCapacity/force。
     * @return redo apply 摘要。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public RedoApplySummary applyAll(List<RedoLogBatch> batches, RedoApplyContext context,
                                     Predicate<PageId> shouldSkipPage) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (batches == null || context == null) {
            throw new DatabaseValidationException("redo apply batches/context must not be null");
        }
        if (shouldSkipPage == null) {
            throw new DatabaseValidationException("redo apply skip predicate must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        int scanned = 0;
        int applied = 0;
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        int skipped = 0;
        for (RedoLogBatch batch : batches) {
            RedoApplySummary summary = apply(batch, context, shouldSkipPage);
            scanned += summary.scannedBatchCount();
            applied += summary.appliedBatchCount();
            skipped += summary.skippedRecordCount();
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return new RedoApplySummary(scanned, applied, skipped);
    }

    private RedoApplyHandler resolveHandler(RedoRecord record) {
        RedoApplyHandler matched = null;
        for (RedoApplyHandler handler : handlers) {
            if (handler.supports(record)) {
                if (matched != null) {
                    throw new DatabaseValidationException("multiple redo apply handlers support record: "
                            + record.getClass().getName());
                }
                matched = handler;
            }
        }
        if (matched == null) {
            throw new DatabaseValidationException("no redo apply handler supports record: "
                    + record.getClass().getName());
        }
        return matched;
    }

    private static boolean shouldSkip(RedoRecord record,
                                      RedoApplyHandler handler,
                                      Predicate<PageId> shouldSkipPage) {
        List<PageId> affectedPages = handler.affectedPages(record);
        if (affectedPages == null) {
            throw new DatabaseValidationException("redo apply handler affected pages must not be null");
        }
        for (PageId pageId : affectedPages) {
            if (pageId == null) {
                throw new DatabaseValidationException("redo apply handler affected page must not be null");
            }
            if (shouldSkipPage.test(pageId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 根据调用参数创建或转换 {@code openSession} 返回的 {@code RedoApplyBatchHandler}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * @param handler 调用方持有的 {@code RedoApplyHandler} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param range redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param context redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @return {@code openSession} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    private static RedoApplyBatchHandler openSession(RedoApplyHandler handler,
                                                     LogRange range,
                                                     RedoApplyContext context) {
        RedoApplyBatchHandler session = handler.openBatch(range, context);
        if (session == null) {
            throw new DatabaseValidationException("redo apply handler opened null batch session: "
                    + handler.getClass().getName());
        }
        return session;
    }
}
