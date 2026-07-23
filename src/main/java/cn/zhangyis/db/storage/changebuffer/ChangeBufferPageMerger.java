package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.buf.PageGuard;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.page.PageEnvelope;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordDecoder;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageDeleter;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
import cn.zhangyis.db.storage.record.page.RecordPagePurger;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.RecordPageStructureValidator;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

/**
 * 已 adopt 的单个 INDEX leaf 上按 sequence 局部应用 mutation。它不做 B+Tree 导航、split/merge/FSP 分配，
 * 因而可在页面仍处于 Buffer Pool LOADING 时运行；全局记录 consume 与 bitmap 更新由上层同一 MTR 编排。
 */
public final class ChangeBufferPageMerger {

    /** exact-version DD metadata 解析入口。 */
    private final ChangeBufferMetadataResolver metadataResolver;
    /** entry bytes 解码器。 */
    private final RecordDecoder decoder;
    /** 页内 key 定位器。 */
    private final RecordPageSearch search;
    /** 页内有序插入器；溢出前零页修改。 */
    private final RecordPageInserter inserter;
    /** RecordCursor 解析 delete 位与字段布局时共享的 codec registry。 */
    private final TypeCodecRegistry typeRegistry;
    /** 等长 delete-mark 操作。 */
    private final RecordPageDeleter deleter = new RecordPageDeleter();
    /** 已标记记录的局部物理摘除器；不触发 leaf merge。 */
    private final RecordPagePurger purger = new RecordPagePurger();
    /** page body/trailer 与 bitmap free class 计算使用的实例页大小。 */
    private final PageSize pageSize;

    /**
     * @param metadataResolver exact-version 二级索引解析器
     * @param typeRegistry 与 B+Tree/record 层共享的类型 codec registry
     * @param pageSize 实例固定页大小
     */
    public ChangeBufferPageMerger(ChangeBufferMetadataResolver metadataResolver,
                                  TypeCodecRegistry typeRegistry, PageSize pageSize) {
        if (metadataResolver == null || typeRegistry == null || pageSize == null) {
            throw new DatabaseValidationException("change buffer page merger dependencies must not be null");
        }
        this.metadataResolver = metadataResolver;
        this.decoder = new RecordDecoder(typeRegistry);
        this.search = new RecordPageSearch(typeRegistry);
        this.inserter = new RecordPageInserter(typeRegistry);
        this.typeRegistry = typeRegistry;
        this.pageSize = pageSize;
    }

    /**
     * 校验整个批次后，在目标 leaf 内按 sequence 依次达到 INSERT/live、DELETE_MARK/marked、DELETE/absent 终态。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验批次同页、严格递增 sequence 与一致 DD identity，解析 exact-version metadata 和全部 entry bytes。</li>
     *     <li>复核 FIL PageId/INDEX 类型、RecordPage level/index id/结构，并以全部 INSERT 的最坏字节和校验连续空间。</li>
     *     <li>逐条重新定位物理 key：缺失/已在目标状态按幂等完成处理，实际变化只调用单页 record 算子。</li>
     *     <li>再次验证页结构，计算真实 free bytes/class；上层据此 consume 全部记录并清 bitmap pending 位。</li>
     * </ol>
     *
     * @param targetGuard 已由 merge MTR adopt、X-latched 且仍未对普通 fixer 发布的目标页
     * @param mutations 同一目标页按 sequence 严格升序的非空批次
     * @return 可写回 bitmap 与统计快照的 merge 结果
     * @throws ChangeBufferMergeCapacityException 保守 dry-run 证明 INSERT 不能完整装入时抛出，实例必须 fail-stop
     * @throws ChangeBufferFormatException 页/metadata/payload identity 不一致时抛出
     */
    public ChangeBufferMergeResult apply(PageGuard targetGuard, List<ChangeBufferMutation> mutations) {
        // 1、批次和 DD identity 全部在第一处页写之前冻结。
        if (targetGuard == null || mutations == null || mutations.isEmpty()) {
            throw new DatabaseValidationException("change buffer merge target/batch must not be empty");
        }
        PageId targetPageId = targetGuard.pageId();
        BatchPlan plan = plan(targetPageId, mutations);
        // 2、专用 envelope、leaf identity 与连续空间上界必须全部通过才允许执行第一条 mutation。
        validateTargetPage(targetGuard, targetPageId, plan.metadata());
        RecordPage page = new RecordPage(targetGuard, pageSize);
        RecordPageStructureValidator.validate(page);
        if (plan.worstCaseInsertBytes() > page.freeSpace()) {
            throw new ChangeBufferMergeCapacityException("change buffer merge requires "
                    + plan.worstCaseInsertBytes() + " bytes but leaf " + targetPageId
                    + " has only " + page.freeSpace());
        }
        // 3、每条操作都以当前页状态重定位，保证 INSERT→DELETE_MARK→DELETE 序列和 crash 重试均幂等。
        int changed = 0;
        for (PlannedMutation planned : plan.mutations()) {
            OptionalInt found = search.findEqual(page, planned.key(), plan.metadata().index().keyDef(),
                    plan.metadata().index().schema());
            changed += applyOne(page, targetPageId, plan.metadata(), planned, found) ? 1 : 0;
        }
        // 4、后验结构校验在 consume global 记录之前完成；任何损坏都保留全局证据并 fail-closed。
        RecordPageStructureValidator.validate(page);
        int freeBytes = page.freeSpace();
        return new ChangeBufferMergeResult(plan.mutations().size(), changed, freeBytes,
                ChangeBufferBitmapLayout.freeSpaceClass(pageSize, freeBytes));
    }

    /**
     * 在目标页第一处写入前冻结并验证完整 merge plan。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>用首条 stable identity 解析 exact-version metadata，并拒绝 unique/clustered/DESC binding。</li>
     *     <li>逐条校验同 target、同 binding 与严格 sequence，解码 entry 并派生完整 physical key。</li>
     *     <li>累计所有 INSERT 的保守页空间上界并返回不引用 payload 输入数组的不可变计划。</li>
     * </ol>
     *
     * @param targetPageId 已 adopt LOADING frame 的稳定 identity
     * @param mutations 同目标、sequence 递增的非空批次
     * @return 已解码 metadata/entry/key 与保守 INSERT 字节数
     * @throws ChangeBufferFormatException metadata 或任一 mutation identity/entry 不一致时抛出
     */
    private BatchPlan plan(PageId targetPageId, List<ChangeBufferMutation> mutations) {
        // 1、metadata identity 只由持久 mutation 决定，不能用“当前最新版”近似解析。
        ChangeBufferMutation first = mutations.getFirst();
        SecondaryIndexMetadata metadata;
        try {
            metadata = metadataResolver.resolve(
                    first.tableId(), first.schemaVersion(), first.indexId());
        } catch (ChangeBufferFormatException format) {
            throw format;
        } catch (RuntimeException unresolved) {
            throw new ChangeBufferFormatException(
                    "change buffer exact-version metadata cannot be resolved: table="
                            + first.tableId() + ", schema=" + first.schemaVersion()
                            + ", index=" + first.indexId(), unresolved);
        }
        if (metadata == null || metadata.index().indexId() != first.indexId()
                || metadata.index().schema().schemaVersion() != first.schemaVersion()
                || metadata.logicalUnique() || metadata.index().clustered()
                || metadata.index().keyDef().parts().stream().anyMatch(part -> part.order() != KeyOrder.ASC)) {
            throw new ChangeBufferFormatException("change buffer mutation resolved to ineligible index metadata");
        }
        // 2、完整批次先解码/校验，第一条 record page 写入只能发生在 plan 返回之后。
        List<PlannedMutation> planned = new ArrayList<>(mutations.size());
        long previousSequence = 0L;
        int insertBytes = 0;
        for (ChangeBufferMutation mutation : mutations) {
            if (mutation == null || !targetPageId.equals(mutation.targetPageId())
                    || mutation.sequence() <= previousSequence
                    || mutation.tableId() != first.tableId()
                    || mutation.schemaVersion() != first.schemaVersion()
                    || mutation.indexId() != first.indexId()) {
                throw new ChangeBufferFormatException("change buffer merge batch identity/order mismatch");
            }
            LogicalRecord entry;
            SearchKey key;
            try {
                entry = decoder.decode(mutation.entryBytes(), metadata.index().schema());
                if (entry.deleted()) {
                    throw new ChangeBufferFormatException(
                            "change buffer entry payload must encode an unmarked row");
                }
                key = physicalKey(entry, metadata);
            } catch (ChangeBufferFormatException format) {
                throw format;
            } catch (RuntimeException invalidEntry) {
                throw new ChangeBufferFormatException(
                        "change buffer entry cannot be decoded by exact metadata: sequence="
                                + mutation.sequence(), invalidEntry);
            }
            planned.add(new PlannedMutation(mutation, entry, key));
            if (mutation.operation() == ChangeBufferOperation.INSERT) {
                // 每条 INSERT 最坏再预留一个 directory slot；忽略可复用 garbage 和同批 DELETE，保持严格保守。
                insertBytes = Math.addExact(insertBytes, Math.addExact(mutation.entryBytes().length, 2));
            }
            previousSequence = mutation.sequence();
        }
        // 3、保守上界故意不抵扣同批 DELETE/garbage；误差只会 fail-stop 或前台提前 direct，不会越界写页。
        return new BatchPlan(metadata, List.copyOf(planned), insertBytes);
    }

    private boolean applyOne(RecordPage page, PageId pageId, SecondaryIndexMetadata metadata,
                             PlannedMutation planned, OptionalInt found) {
        return switch (planned.mutation().operation()) {
            case INSERT -> {
                if (found.isEmpty()) {
                    inserter.insert(page, pageId, planned.entry(), metadata.index().keyDef(),
                            metadata.index().schema());
                    yield true;
                }
                RecordCursor cursor = new RecordCursor(page, found.getAsInt(), metadata.index().schema(),
                        typeRegistry);
                if (cursor.isDeleted()) {
                    page.setDeleted(found.getAsInt(), false);
                    yield true;
                }
                yield false;
            }
            case DELETE_MARK -> {
                if (found.isEmpty()) {
                    yield false;
                }
                RecordCursor cursor = new RecordCursor(page, found.getAsInt(), metadata.index().schema(),
                        typeRegistry);
                if (cursor.isDeleted()) {
                    yield false;
                }
                deleter.deleteMark(page, found.getAsInt());
                yield true;
            }
            case DELETE -> {
                if (found.isEmpty()) {
                    yield false;
                }
                RecordCursor cursor = new RecordCursor(page, found.getAsInt(), metadata.index().schema(),
                        typeRegistry);
                if (!cursor.isDeleted()) {
                    deleter.deleteMark(page, found.getAsInt());
                }
                purger.purge(page, found.getAsInt());
                yield true;
            }
        };
    }

    /**
     * 交叉校验 FIL、descriptor 与 RecordPage 三层 target identity。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>解析 FIL envelope并要求 PageId/INDEX type 与待发布 frame 一致。</li>
     *     <li>要求 exact metadata 指向同一用户空间且 descriptor 自身为普通 INDEX。</li>
     *     <li>解析 record header并要求 level 0/index id 精确匹配，防止把 mutation 应用到旧 incarnation。</li>
     * </ol>
     *
     * @param guard 尚未发布且持 X latch 的目标 guard
     * @param expected global mutation 指定的物理 target
     * @param metadata exact-version resolver 返回的二级 binding
     * @throws ChangeBufferFormatException 任一 identity/type/level 不一致时抛出，target 和 global evidence 不得发布
     */
    private void validateTargetPage(PageGuard guard, PageId expected,
                                    SecondaryIndexMetadata metadata) {
        // 1、FIL identity 是读取任何 record body 前的最外层损坏边界。
        FilePageHeader envelope;
        try {
            envelope = PageEnvelope.readHeader(guard);
        } catch (RuntimeException invalid) {
            throw new ChangeBufferFormatException("change buffer target FIL envelope is invalid", invalid);
        }
        if (!envelope.spaceId().equals(expected.spaceId()) || envelope.pageNo() != expected.pageNo().value()
                || envelope.pageType() != PageType.INDEX) {
            throw new ChangeBufferFormatException("change buffer target page identity/type mismatch: " + envelope);
        }
        // 2、metadata 必须仍表示同一用户 tablespace 的普通二级树。
        if (metadata.index().pageType() != PageType.INDEX) {
            throw new ChangeBufferFormatException("change buffer target descriptor is not ordinary INDEX");
        }
        if (!metadata.index().rootPageId().spaceId().equals(expected.spaceId())) {
            throw new ChangeBufferFormatException("change buffer target space does not match descriptor root: target="
                    + expected + ", root=" + metadata.index().rootPageId());
        }
        // 3、页内 level/index id 防止 space/page 复用后把旧 mutation 写入新对象。
        RecordPage page = new RecordPage(guard, pageSize);
        if (page.header().level() != 0 || page.header().indexId() != metadata.index().indexId()) {
            throw new ChangeBufferFormatException("change buffer target leaf/index identity mismatch at "
                    + expected + ": level=" + page.header().level() + ", index=" + page.header().indexId());
        }
    }

    private static SearchKey physicalKey(LogicalRecord entry, SecondaryIndexMetadata metadata) {
        List<ColumnValue> values = new ArrayList<>(metadata.index().keyDef().parts().size());
        for (KeyPartDef part : metadata.index().keyDef().parts()) {
            values.add(entry.columnValues().get(part.columnId().value()));
        }
        return new SearchKey(values);
    }

    private record PlannedMutation(ChangeBufferMutation mutation, LogicalRecord entry, SearchKey key) {
    }

    private record BatchPlan(SecondaryIndexMetadata metadata, List<PlannedMutation> mutations,
                             int worstCaseInsertBytes) {
    }
}
