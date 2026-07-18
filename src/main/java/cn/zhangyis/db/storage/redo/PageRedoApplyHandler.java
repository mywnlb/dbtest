package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PAGE_INIT/PAGE_BYTES/FSP metadata delta/undo metadata delta/undo payload/B+Tree page delta 的物理页回放 handler。
 * 它按批次缓存同页修改，
 * 先应用批内所有记录，再把 pageLSN 盖到 batch endLsn。
 *
 * <p>这个顺序很关键：D3/D4 的 MTR commit 语义是“批内所有修改共享 endLsn”。若 PAGE_INIT 后立即盖 pageLSN，
 * 同批后续 PAGE_BYTES 会被 pageLSN 幂等判断误跳过，导致恢复页半成品。
 */
public final class PageRedoApplyHandler implements RedoApplyHandler {

    /**
     * 判断 {@code supports} 所表达的Redo/WAL条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code supports} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    @Override
    public boolean supports(RedoRecord record) {
        return record instanceof PageInitRecord
                || record instanceof PageBytesRecord
                || record instanceof FspMetadataDeltaRecord
                || record instanceof UndoMetadataDeltaRecord
                || record instanceof UndoRecordPayloadRecord
                || record instanceof BTreePageDeltaRecord;
    }

    /**
     * 计算 {@code affectedPages} 所表达的Redo/WAL数量、容量或物理位置；计算只读取输入，溢出或越界以领域异常报告。
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return 按当前快照筛出的候选页、脏页或阻塞关系；保持方法声明的稳定顺序，无候选时返回空集合而非 {@code null}
     */
    @Override
    public List<PageId> affectedPages(RedoRecord record) {
        return List.of(pageIdOf(record));
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
            throw new DatabaseValidationException("page redo apply range/context must not be null");
        }
        return new Batch(range, context);
    }

    /** 应用一个完整 redo 批次；每个页只在批次末尾写回一次。
     *
     * @param batch redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param context redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void apply(RedoLogBatch batch, RedoApplyContext context) {
        if (batch == null || context == null) {
            throw new DatabaseValidationException("page redo apply batch/context must not be null");
        }
        apply(new RedoApplyBatchView(batch.range(), batch.records()), context);
    }

    /**
     * 应用一个 redo apply view；每个页只在批次末尾写回一次。
     *
     * <p>view 可能只包含原 batch 的部分记录（例如 recovery force-skip 坏表空间），但 {@code range.end()}
     * 必须保持原始 batch end LSN。该 LSN 是 MTR 的幂等边界，写低会导致下一次 recovery 重放已处理记录。
     *
     * @param batch 原始 range 加过滤后记录的内部视图。
     * @param context 页重放上下文。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void apply(RedoApplyBatchView batch, RedoApplyContext context) {
        if (batch == null || context == null) {
            throw new DatabaseValidationException("page redo apply batch/context must not be null");
        }
        RedoApplyBatchHandler session = openBatch(batch.range(), context);
        for (RedoRecord record : batch.records()) {
            session.apply(record);
        }
        session.finish();
    }

    /**
     * Redo/WAL的 {@code Batch} 批处理上下文；它聚合同一批次的输入与阶段结果，批次结束后不得跨请求复用。
     */
    private static final class Batch implements RedoApplyBatchHandler {

        /** 原始 batch range，finish 时作为所有 touched 页的 pageLSN 幂等边界。 */
        private final LogRange range;

        /** recovery apply 上下文；page handler 只通过 PageStore 读写物理页，不依赖 BufferPool/MTR。 */
        private final RedoApplyContext context;

        /** 本 batch 已装入并可能修改的页，保持首次触达顺序，finish 时逐页写回。 */
        private final Map<PageId, ReplayPage> pages = new LinkedHashMap<>();

        /** pageLSN 已覆盖本 batch 的页；同 batch 后续同页 record 必须整体跳过。 */
        private final Set<PageId> skippedPages = new HashSet<>();

        private Batch(LogRange range, RedoApplyContext context) {
            this.range = range;
            this.context = context;
        }

        /**
         * 执行Redo/WAL恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
         *
         * <p>数据流：</p>
         * <ol>
         *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
         *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
         *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
         *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
         * </ol>
         *
         * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
         * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
         */
        @Override
        public void apply(RedoRecord record) {
            // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
            PageId pageId = pageIdOf(record);
            // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
            if (skippedPages.contains(pageId)) {
                return;
            }
            // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
            ReplayPage page = pages.get(pageId);
            if (page == null) {
                if (record instanceof PageInitRecord) {
                    // PAGE_INIT 是唯一的建页记录：崩溃后物理文件可能被截短到该页之前，按需扩容使其可写。
                    ensureReplayCapacity(context, pageId);
                } else if (isBeyondFileEnd(context, pageId)) {
                    // 首触是页 patch 却越过物理文件尾：该页从未经 PAGE_INIT 建立，凭空造页属于 redo 损坏，
                    // 不得静默扩容补一个半成品页。正确的 fuzzy checkpoint 下不会出现该情形（未刷的建页 redo 不会被
                    // checkpoint 越过），故判损坏而非容忍。
                    throw new RedoLogCorruptedException(
                            "redo page patch targets uninitialized page beyond data file size: " + pageId);
                }
                byte[] current = readPage(context, pageId);
                if (pageLsn(current).value() >= range.end().value()) {
                    skippedPages.add(pageId);
                    return;
                }
                page = new ReplayPage(pageId, current);
                pages.put(pageId, page);
            }
            // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
            if (record instanceof PageInitRecord pir) {
                page.bytes = initializedPage(context, pir);
                page.touched = true;
            } else if (record instanceof PageBytesRecord pbr) {
                applyBytes(context, page, pbr);
            } else if (record instanceof FspMetadataDeltaRecord fmd) {
                applyMetadataDelta(context, page, fmd);
            } else if (record instanceof UndoMetadataDeltaRecord umd) {
                applyUndoMetadataDelta(context, page, umd);
            } else if (record instanceof UndoRecordPayloadRecord urp) {
                applyUndoRecordPayload(context, page, urp);
            } else if (record instanceof BTreePageDeltaRecord btd) {
                applyBTreePageDelta(context, page, btd);
            } else {
                throw new RedoLogCorruptedException("unsupported redo record type: " + record.getClass().getName());
            }
        }

        /**
         * 推进 {@code finish} 对应的Redo/WAL阶段转换；成功只发布一次目标状态，失败路径保留可取消或可诊断的原状态。
         */
        @Override
        public void finish() {
            for (ReplayPage page : pages.values()) {
                if (page.touched) {
                    stampPageLsn(page.bytes, range.end());
                    context.pageStore().writePage(page.pageId, ByteBuffer.wrap(page.bytes));
                }
            }
        }
    }

    private static void applyBytes(RedoApplyContext context, ReplayPage page, PageBytesRecord record) {
        applyPatch(context, page, record.offset(), record.bytes(), "redo PAGE_BYTES");
    }

    /**
     * FSP metadata delta 与 PAGE_BYTES 共用同一个 batch page cache。这样同一 MTR 内 metadata delta 和仍需物理
     * 保护的 PAGE_BYTES 指向同一 page0/page2 时，恢复只会读一次、批末写一次，避免两个 handler 各自缓存并覆盖彼此修改。
     */
    private static void applyMetadataDelta(RedoApplyContext context, ReplayPage page, FspMetadataDeltaRecord record) {
        applyPatch(context, page, record.offset(), record.afterImage(), "redo FSP metadata delta");
    }

    /**
     * Undo/rseg metadata delta 同样只做页内 after-image patch。恢复期不能重新执行事务提交、slot release 或 undo append
     * 状态机，否则会把 crash recovery 变成普通运行时逻辑并破坏幂等边界。
     */
    private static void applyUndoMetadataDelta(RedoApplyContext context, ReplayPage page,
                                               UndoMetadataDeltaRecord record) {
        applyPatch(context, page, record.offset(), record.afterImage(), "redo undo metadata delta");
    }

    /**
     * 完整 undo record payload 只做页内槽 after-image patch。恢复期不能重新执行 appendRecord，否则会重复推进
     * undo page header 和 first-page log header。
     */
    private static void applyUndoRecordPayload(RedoApplyContext context, ReplayPage page,
                                               UndoRecordPayloadRecord record) {
        applyPatch(context, page, record.recordOffset(), record.slotImage(), "redo undo record payload");
    }

    /**
     * B+Tree 结构 delta 同样只做页内 after-image patch。恢复期不重新运行 split/merge/root shrink 算法，
     * 避免把 redo replay 变成普通运行时结构调整。
     *
     * @param context redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static void applyBTreePageDelta(RedoApplyContext context, ReplayPage page,
                                            BTreePageDeltaRecord record) {
        if (record.kind() == BTreePageDeltaKind.NODE_POINTER_AREA) {
            long end = (long) record.offset() + record.afterImage().length;
            long trailerStart = context.pageSize().bytes() - PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;
            if (end > trailerStart) {
                throw new RedoLogCorruptedException("redo B+Tree node pointer delta crosses file trailer: offset="
                        + record.offset() + " length=" + record.afterImage().length
                        + " trailerStart=" + trailerStart);
            }
        }
        applyPatch(context, page, record.offset(), record.afterImage(), "redo B+Tree page delta");
    }

    /**
     * 执行Redo/WAL恢复或重放步骤；按持久证据校验并幂等推进状态，不执行普通 SQL 业务语义。
     *
     * @param context redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param patch 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param what 传给 {@code applyPatch} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private static void applyPatch(RedoApplyContext context, ReplayPage page, int offset, byte[] patch, String what) {
        long end = (long) offset + patch.length;
        if (offset < 0 || end > context.pageSize().bytes()) {
            throw new RedoLogCorruptedException(what + " out of page bounds: offset="
                    + offset + " length=" + patch.length + " pageSize=" + context.pageSize().bytes());
        }
        System.arraycopy(patch, 0, page.bytes, offset, patch.length);
        page.touched = true;
    }

    /**
     * extend-on-demand（仅 PAGE_INIT 触发）：崩溃后物理文件可能因 autoExtend 未 fsync 而短于 redo 写过的页号。
     * 重放一个建页记录前先把物理文件扩到能容纳该页（幂等），避免随后的 readPage/writePage 越界。
     * SPACE_FILE_RECONCILE 阶段会在 replay 之后再按 page0 权威大小补齐 extent 内无 redo 描述的尾部零页。
     */
    private static void ensureReplayCapacity(RedoApplyContext context, PageId pageId) {
        context.pageStore().ensureCapacity(pageId.spaceId(), PageNo.of(pageId.pageNo().value() + 1));
    }

    /** 目标页号是否越过当前物理文件尾（用于判定首触 PAGE_BYTES 是否指向未建立的页）。 */
    private static boolean isBeyondFileEnd(RedoApplyContext context, PageId pageId) {
        return pageId.pageNo().value() >= context.pageStore().currentSizeInPages(pageId.spaceId()).value();
    }

    private static byte[] readPage(RedoApplyContext context, PageId pageId) {
        byte[] bytes = new byte[context.pageSize().bytes()];
        context.pageStore().readPage(pageId, ByteBuffer.wrap(bytes));
        return bytes;
    }

    /**
     * 根据调用参数构造 {@code initializedPage} 对应的Redo/WAL领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param context redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code initializedPage} 生成的非空字节表示；调用方获得独立结果或受控视图，格式失败通过领域异常报告
     */
    private static byte[] initializedPage(RedoApplyContext context, PageInitRecord record) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        byte[] bytes = new byte[context.pageSize().bytes()];
        ByteBuffer page = ByteBuffer.wrap(bytes);
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        page.putInt(PageEnvelopeLayout.SPACE_ID, record.pageId().spaceId().value());
        page.putInt(PageEnvelopeLayout.PAGE_NO, (int) record.pageId().pageNo().value());
        page.putInt(PageEnvelopeLayout.PREV_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        page.putInt(PageEnvelopeLayout.NEXT_PAGE_NO, (int) FilePageHeader.FIL_NULL);
        page.putLong(PageEnvelopeLayout.PAGE_LSN, 0L);
        page.putInt(PageEnvelopeLayout.PAGE_TYPE, record.pageType().code());
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        return bytes;
    }

    private static Lsn pageLsn(byte[] page) {
        return Lsn.of(ByteBuffer.wrap(page).getLong(PageEnvelopeLayout.PAGE_LSN));
    }

    private static void stampPageLsn(byte[] page, Lsn lsn) {
        ByteBuffer.wrap(page).putLong(PageEnvelopeLayout.PAGE_LSN, lsn.value());
    }

    /**
     * 根据调用参数创建或转换 {@code pageIdOf} 返回的 {@code PageId}；输入先完成领域校验，成功结果不为 {@code null}。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 checkpoint、redo、doublewrite 或事务持久证据，并校验阶段、范围与文件身份。</li>
     *     <li>依据 page LSN、恢复进度和稳定标识判断跳过或续作，保证重复启动不会重复产生副作用。</li>
     *     <li>按恢复阶段应用物理页或事务状态变化，并在每个可恢复边界记录已完成进度。</li>
     *     <li>发布恢复结果并释放恢复专用资源；失败保持 fail-closed，不能提前开放普通 SQL 流量。</li>
     * </ol>
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code pageIdOf} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    static PageId pageIdOf(RedoRecord record) {
        // 1、读取 checkpoint、redo、doublewrite 或事务持久证据，在共享或持久副作用前拒绝非法状态。
        if (record instanceof PageInitRecord pir) {
            return pir.pageId();
        }
        if (record instanceof PageBytesRecord pbr) {
            return pbr.pageId();
        }
        // 2、继续完成范围、身份与候选校验；通过后，依据 page LSN、恢复进度和稳定标识判断跳过或续作，保持处理顺序与资源边界。
        if (record instanceof FspMetadataDeltaRecord fmd) {
            return fmd.pageId();
        }
        if (record instanceof UndoMetadataDeltaRecord umd) {
            return umd.pageId();
        }
        // 3、在中间分支复核阶段性结果；满足条件后，按恢复阶段应用物理页或事务状态变化，并维持领域不变量。
        if (record instanceof UndoRecordPayloadRecord urp) {
            return urp.pageId();
        }
        if (record instanceof BTreePageDeltaRecord btd) {
            return btd.pageId();
        }
        // 4、发布恢复结果并释放恢复专用资源，以稳定返回或领域异常完成收口。
        throw new RedoLogCorruptedException("unsupported redo record type: " + record.getClass().getName());
    }

    /**
     * 单页 redo replay 的批内工作副本；pageId 固定，bytes 只由当前批次替换，touched 标识是否需要写回 PageStore。
     */
    private static final class ReplayPage {
        /**
         * 构造时冻结的 {@code pageId} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
         */
        private final PageId pageId;
        /**
         * 本对象独占的 {@code bytes} 数据缓冲；构造和访问路径必须遵守防御性复制或受控视图约束，不能泄漏可变数组。
         */
        private byte[] bytes;
        /**
         * 记录 {@code touched} 生命周期事实是否成立；只由本类状态转换更新，共享访问受所属显式锁、原子发布或单一 owner 线程保护。
         */
        private boolean touched;

        private ReplayPage(PageId pageId, byte[] bytes) {
            this.pageId = pageId;
            this.bytes = bytes;
        }
    }
}
