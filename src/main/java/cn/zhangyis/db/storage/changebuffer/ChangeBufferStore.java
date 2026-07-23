package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeInsertResult;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeScanRange;
import cn.zhangyis.db.storage.btree.BTreeSecondaryRemovalResult;
import cn.zhangyis.db.storage.btree.SecondaryEntryRemovalStatus;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * 全局 Change Buffer B+Tree 的事务性存取门面。page 3 header 与树记录在同一 MTR 更新，因此
 * {@code pendingOperations}、{@code nextSequence} 和 root level 不会与已提交树内容分叉。
 */
public final class ChangeBufferStore {

    /** page 3 权威状态仓储。 */
    private final ChangeBufferHeaderRepository headerRepository;
    /** 复用既有 split/merge/redo/FSP 能力的内部 B+Tree 服务。 */
    private final SplitCapableBTreeIndexService btree;
    /** mutation payload 自描述编码器。 */
    private final ChangeBufferMutationCodec mutationCodec;
    /** descriptor schema 与持久格式上界使用的实例页大小。 */
    private final PageSize pageSize;

    /**
     * @param headerRepository 固定 page 3 仓储
     * @param btree 全局树读写使用的 split-capable 服务
     * @param pageSize 实例页大小
     */
    public ChangeBufferStore(ChangeBufferHeaderRepository headerRepository,
                             SplitCapableBTreeIndexService btree, PageSize pageSize) {
        if (headerRepository == null || btree == null || pageSize == null) {
            throw new DatabaseValidationException("change buffer store dependencies must not be null");
        }
        this.headerRepository = headerRepository;
        this.btree = btree;
        this.pageSize = pageSize;
        this.mutationCodec = new ChangeBufferMutationCodec(pageSize);
    }

    /**
     * 分配全局 sequence、插入一条 mutation 并同步推进 header；本方法不提交调用方 MTR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 X latch 读取 page 3，验证 ACTIVE 状态并取得唯一 nextSequence。</li>
     *     <li>构造含目标页、DD identity 与完整 entry bytes 的 mutation，再编码为内部非聚簇记录。</li>
     *     <li>向全局 IBUF_INDEX 插入记录；split/FSP/redo 全部复用 B+Tree 下游，获得最新 root level。</li>
     *     <li>在同一 MTR 写回 nextSequence+1、pending+1 与 root level；失败时调用方回滚且不得置 bitmap buffered 位。</li>
     * </ol>
     *
     * @param mtr 活动写 MTR；不得为 {@code null}
     * @param targetPageId 未驻留目标二级 leaf 的物理页；不得为 {@code null}
     * @param tableId DD 稳定正表 id
     * @param schemaVersion 目标 entry exact-version 正 schema 版本
     * @param indexId 目标二级索引正 id
     * @param operation 允许缓冲的物理动作；不得为 {@code null}
     * @param entryBytes 目标二级 entry 的完整编码；非空且须落在单条 mutation 上限内
     * @return 包含已分配 sequence 和结构后置快照的结果
     * @throws ChangeBufferStateException header 未处于 ACTIVE 时抛出，调用方必须回退直写或停止服务
     */
    public ChangeBufferAppendResult append(MiniTransaction mtr, PageId targetPageId,
                                           long tableId, long schemaVersion, long indexId,
                                           ChangeBufferOperation operation, byte[] entryBytes) {
        // 1、page3 X latch 串行化 sequence 和 pending 计数；禁止读取后再升级。
        requireAppendArguments(mtr, targetPageId, operation, entryBytes);
        if (targetPageId.spaceId().equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)
                || targetPageId.pageNo().value() < 3L
                || ChangeBufferBitmapLayout.isBitmapPage(pageSize, targetPageId.pageNo())) {
            throw new DatabaseValidationException(
                    "change buffer append target is outside the managed user-leaf range: " + targetPageId);
        }
        ChangeBufferHeaderSnapshot header = headerRepository.readForUpdate(mtr);
        if (header.state() != ChangeBufferHeaderState.ACTIVE) {
            throw new ChangeBufferStateException("change buffer is not ACTIVE: " + header.state());
        }
        if (header.nextSequence() == Long.MAX_VALUE || header.pendingOperations() == Long.MAX_VALUE) {
            throw new ChangeBufferStateException(
                    "change buffer append counter is exhausted before tree mutation: next="
                            + header.nextSequence() + ", pending=" + header.pendingOperations());
        }
        // 2、sequence 在 X latch 内分配，mutation key 在崩溃重放后仍保持全局唯一。
        ChangeBufferMutation mutation = new ChangeBufferMutation(targetPageId, header.nextSequence(),
                tableId, schemaVersion, indexId, operation, entryBytes);
        LogicalRecord record = toRecord(mutation);
        // 3、全局树结构变化由现有 B+Tree/MTR/FSP 管线完成。
        BTreeIndex before = ChangeBufferBootstrap.index(header, pageSize);
        BTreeInsertResult inserted = btree.insert(mtr, before, record);
        // 4、树记录与计数共享提交边界，避免 bitmap/header 宣称不存在的 mutation。
        headerRepository.write(mtr, header.afterAppend(inserted.indexAfterInsert().rootLevel()));
        return new ChangeBufferAppendResult(mutation, inserted.indexAfterInsert());
    }

    /**
     * 在 page 3 的同一 X-latch 临界区验证全局容量，并把该 latch 保留到调用方随后 append/commit。
     * 不同目标页使用不同 page gate，只有把最终判断放在权威 header latch 下，才能阻止并发 eligibility
     * 快照同时通过后把 pending 推过配置上限。本方法不修改 header；容量不足时当前 MTR 仍由调用方回滚。
     *
     * @param mtr 随后执行 {@link #append} 的活动写 MTR；检查后必须在同一 MTR 内继续或回滚
     * @param maxPendingOperations 配置允许的正 pending 页等价量上限
     * @throws ChangeBufferCapacityExceededException 当前 pending 已达到上限时抛出，调用方应回退直写
     * @throws ChangeBufferStateException header 非 ACTIVE 时抛出，调用方不得把持久状态损坏当作容量回退
     */
    public void requireAppendCapacity(MiniTransaction mtr, long maxPendingOperations) {
        if (mtr == null || maxPendingOperations <= 0L) {
            throw new DatabaseValidationException("change buffer append capacity arguments are invalid");
        }
        ChangeBufferHeaderSnapshot header = headerRepository.readForUpdate(mtr);
        if (header.state() != ChangeBufferHeaderState.ACTIVE) {
            throw new ChangeBufferStateException("change buffer is not ACTIVE: " + header.state());
        }
        if (header.pendingOperations() >= maxPendingOperations) {
            throw new ChangeBufferCapacityExceededException(
                    "change buffer capacity reached: pending=" + header.pendingOperations()
                            + ", limit=" + maxPendingOperations);
        }
    }

    /**
     * 按目标页前缀扫描 mutation，结果严格按全局 sequence 升序并校验 record key 与 payload identity 一致。
     *
     * @param mtr 活动读 MTR；不得为 {@code null}
     * @param targetPageId 需要合并的稳定目标页
     * @param limit 本次最大物化条数；必须为正
     * @return 不引用页 cursor/guard 的不可变 mutation 列表
     */
    public List<ChangeBufferMutation> scanPage(MiniTransaction mtr, PageId targetPageId, int limit) {
        if (mtr == null || targetPageId == null || limit <= 0) {
            throw new DatabaseValidationException("change buffer scan MTR/target/limit is invalid");
        }
        ChangeBufferHeaderSnapshot header = headerRepository.read(mtr);
        BTreeIndex index = ChangeBufferBootstrap.index(header, pageSize);
        SearchKey prefix = pagePrefix(targetPageId);
        List<BTreeLookupResult> rows = btree.scan(mtr, index,
                new BTreeScanRange(prefix, true, prefix, true, limit));
        List<ChangeBufferMutation> result = new ArrayList<>(rows.size());
        long previous = 0L;
        for (BTreeLookupResult row : rows) {
            ChangeBufferMutation mutation = fromRecord(row.record());
            if (!mutation.targetPageId().equals(targetPageId) || mutation.sequence() <= previous) {
                throw new ChangeBufferFormatException("change buffer page prefix/sequence mismatch for "
                        + targetPageId);
            }
            previous = mutation.sequence();
            result.add(mutation);
        }
        return List.copyOf(result);
    }

    /**
     * 按全局物理 key 顺序扫描一批 mutation。返回对象已经完成 payload CRC 与 key/payload identity 校验，
     * 不引用 B+Tree cursor 或 page guard；后台 merge、DDL barrier 和恢复校验共用该入口。
     *
     * @param mtr 活动只读 MTR；不得为 {@code null}
     * @param limit 最多物化的正记录数
     * @return 按 target space/page/sequence 升序的不可变 mutation 列表
     */
    public List<ChangeBufferMutation> scanAll(MiniTransaction mtr, int limit) {
        if (mtr == null || limit <= 0) {
            throw new DatabaseValidationException("change buffer full scan MTR/limit is invalid");
        }
        ChangeBufferHeaderSnapshot header = headerRepository.read(mtr);
        BTreeIndex index = ChangeBufferBootstrap.index(header, pageSize);
        List<BTreeLookupResult> rows = btree.scan(mtr, index, BTreeScanRange.unbounded(limit));
        List<ChangeBufferMutation> result = new ArrayList<>(rows.size());
        ChangeBufferMutation previous = null;
        for (BTreeLookupResult row : rows) {
            ChangeBufferMutation mutation = fromRecord(row.record());
            if (previous != null && compareIdentity(previous, mutation) >= 0) {
                throw new ChangeBufferFormatException("change buffer global key order is not strictly increasing");
            }
            result.add(mutation);
            previous = mutation;
        }
        return List.copyOf(result);
    }

    /**
     * 为后台 worker 选取全局 key 前缀中的不同目标页。热点页最多占用 64 条记录，因此扫描窗口按该上界展开；
     * 返回顺序稳定且去重，不触发目标页 IO。
     *
     * @param mtr 活动只读 MTR
     * @param distinctLimit 本轮最多返回的不同目标页数
     * @return 全局 key 顺序中的首批不同目标页
     */
    public List<PageId> firstTargetPages(MiniTransaction mtr, int distinctLimit) {
        if (mtr == null || distinctLimit <= 0) {
            throw new DatabaseValidationException("change buffer target scan arguments are invalid");
        }
        long expanded = Math.multiplyExact((long) distinctLimit,
                SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE);
        int scanLimit = expanded > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) expanded;
        LinkedHashSet<PageId> pages = new LinkedHashSet<>();
        for (ChangeBufferMutation mutation : scanAll(mtr, scanLimit)) {
            pages.add(mutation.targetPageId());
            if (pages.size() == distinctLimit) {
                break;
            }
        }
        return List.copyOf(pages);
    }

    /**
     * 物理删除一条已成功应用到目标 leaf 的 mutation，并在同一 MTR 精确扣减 pending。
     * ABSENT 是幂等 no-op，不重复扣减；STATE_CONFLICT 表示内部树出现不应存在的 delete-mark。
     *
     * @param mtr 活动写 MTR；不得为 {@code null}
     * @param mutation 已应用成功、待 consume 的完整 mutation
     * @return 本次是否实际从全局树移除记录
     * @throws ChangeBufferStateException header 计数矛盾或内部记录状态冲突时抛出
     */
    public boolean consume(MiniTransaction mtr, ChangeBufferMutation mutation) {
        if (mtr == null || mutation == null) {
            throw new DatabaseValidationException("change buffer consume MTR/mutation must not be null");
        }
        ChangeBufferHeaderSnapshot header = headerRepository.readForUpdate(mtr);
        BTreeIndex index = ChangeBufferBootstrap.index(header, pageSize);
        BTreeSecondaryRemovalResult removed = btree.deletePublishedSecondary(mtr, index, fullKey(mutation));
        if (removed.status() == SecondaryEntryRemovalStatus.ABSENT) {
            return false;
        }
        if (removed.status() != SecondaryEntryRemovalStatus.REMOVED) {
            throw new ChangeBufferStateException("change buffer internal record state conflict: "
                    + mutation.targetPageId() + "/" + mutation.sequence());
        }
        if (header.pendingOperations() == 0) {
            throw new ChangeBufferStateException("change buffer header pending count underflow");
        }
        headerRepository.write(mtr, header.afterConsume(removed.indexAfter().rootLevel(), 1L));
        return true;
    }

    /**
     * @param mtr 活动读 MTR
     * @return page 3 中尚未 consume 的全局记录计数
     */
    public long pendingOperations(MiniTransaction mtr) {
        if (mtr == null) {
            throw new DatabaseValidationException("change buffer pending count MTR must not be null");
        }
        return headerRepository.read(mtr).pendingOperations();
    }

    private LogicalRecord toRecord(ChangeBufferMutation mutation) {
        return new LogicalRecord(ChangeBufferRecordSchema.SCHEMA_VERSION, List.of(
                new ColumnValue.IntValue(mutation.targetPageId().spaceId().value()),
                new ColumnValue.IntValue(mutation.targetPageId().pageNo().value()),
                new ColumnValue.IntValue(mutation.sequence()),
                new ColumnValue.BinaryValue(mutationCodec.encode(mutation))),
                false, RecordType.CONVENTIONAL);
    }

    private ChangeBufferMutation fromRecord(LogicalRecord record) {
        if (record == null || record.deleted() || record.recordType() != RecordType.CONVENTIONAL
                || record.schemaVersion() != ChangeBufferRecordSchema.SCHEMA_VERSION
                || record.columnValues().size() != 4
                || !(record.columnValues().get(0) instanceof ColumnValue.IntValue space)
                || !(record.columnValues().get(1) instanceof ColumnValue.IntValue page)
                || !(record.columnValues().get(2) instanceof ColumnValue.IntValue sequence)
                || !(record.columnValues().get(3) instanceof ColumnValue.BinaryValue payload)) {
            throw new ChangeBufferFormatException("change buffer internal record shape mismatch");
        }
        ChangeBufferMutation mutation = mutationCodec.decode(payload.value());
        if (space.value() != mutation.targetPageId().spaceId().value()
                || page.value() != mutation.targetPageId().pageNo().value()
                || sequence.value() != mutation.sequence()) {
            throw new ChangeBufferFormatException("change buffer record key/payload identity mismatch");
        }
        return mutation;
    }

    private static SearchKey pagePrefix(PageId pageId) {
        return new SearchKey(List.of(new ColumnValue.IntValue(pageId.spaceId().value()),
                new ColumnValue.IntValue(pageId.pageNo().value())));
    }

    private static SearchKey fullKey(ChangeBufferMutation mutation) {
        return new SearchKey(List.of(
                new ColumnValue.IntValue(mutation.targetPageId().spaceId().value()),
                new ColumnValue.IntValue(mutation.targetPageId().pageNo().value()),
                new ColumnValue.IntValue(mutation.sequence())));
    }

    private static int compareIdentity(ChangeBufferMutation left, ChangeBufferMutation right) {
        int space = Integer.compare(left.targetPageId().spaceId().value(),
                right.targetPageId().spaceId().value());
        if (space != 0) {
            return space;
        }
        int page = Long.compare(left.targetPageId().pageNo().value(),
                right.targetPageId().pageNo().value());
        return page != 0 ? page : Long.compare(left.sequence(), right.sequence());
    }

    private static void requireAppendArguments(MiniTransaction mtr, PageId targetPageId,
                                               ChangeBufferOperation operation, byte[] entryBytes) {
        if (mtr == null || targetPageId == null || operation == null || entryBytes == null
                || entryBytes.length == 0) {
            throw new DatabaseValidationException("change buffer append arguments are invalid");
        }
    }
}
