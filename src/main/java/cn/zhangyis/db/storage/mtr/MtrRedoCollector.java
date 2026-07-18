package cn.zhangyis.db.storage.mtr;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.storage.buf.PageWriteListener;
import cn.zhangyis.db.storage.redo.BTreePageDeltaRecord;
import cn.zhangyis.db.storage.page.PageType;
import cn.zhangyis.db.storage.redo.FspMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.PageBytesRecord;
import cn.zhangyis.db.storage.redo.PageInitRecord;
import cn.zhangyis.db.storage.redo.RedoRecord;
import cn.zhangyis.db.storage.redo.UndoMetadataDeltaRecord;
import cn.zhangyis.db.storage.redo.UndoRecordPayloadRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 本 MTR 的 redo 收集器：实现 buf 的 {@link PageWriteListener}，把页写译成 {@link PageBytesRecord} 累积；
 * {@code newPage} 经 {@link #recordInit} 产 {@link PageInitRecord}，FSP 等模块可显式追加逻辑 intent record。
 * {@code touchedPages} 只由物理页写和页初始化维护，commit 据此决定给哪些页盖 pageLSN（不读 PageGuard.wrote 私有状态）。
 *
 * <p>0.23b 起额外维护 {@link MtrRedoEntry} 诊断列表：record 是实际提交给 redo manager 的持久命令，
 * category 只表达本 MTR 内这次记录来自 record、btree、fsp 还是 undo。它不改变 redo 编码和恢复行为。
 *
 * <p>{@code enabled} 开关：commit 盖戳前关闭，使 pageLSN 写不入 redo。单线程拥有（随 MiniTransaction），无需加锁。
 */
final class MtrRedoCollector implements PageWriteListener {

    /** 实际提交给 redo manager 的 redo record，顺序即本 MTR 内收集顺序。 */
    private final List<RedoRecord> records = new ArrayList<>();

    /** 与 {@link #records} 一一对应的本地诊断条目；分类不进入持久 redo 文件，只用于审计语义来源。 */
    private final List<MtrRedoEntry> entries = new ArrayList<>();

    /** 已被任一 redo record 触碰的页；commit 依赖它定位需要盖同一 batch end LSN 的 page guard。 */
    private final Set<PageId> touchedPages = new LinkedHashSet<>();

    /** 当前打开的 redo 分类作用域栈；只由 MTR owner 线程访问，关闭时必须按 LIFO 恢复分类。 */
    private final Deque<MtrRedoCategoryScope> categoryScopes = new ArrayDeque<>();

    /** 当前普通 {@code PAGE_BYTES} 写入使用的分类；没有 scope 时保持 generic。 */
    private MtrRedoCategory currentCategory = MtrRedoCategory.PAGE_BYTES_GENERIC;

    /** commit 盖 pageLSN 前关闭 collector，避免 pageLSN stamp 自身被回收成 redo。 */
    private boolean enabled = true;

    /**
     * 接收 {@code onWrite} 对应的Mini Transaction生命周期事件；只更新本策略拥有的统计或顺序状态，不接管事件来源资源。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param offset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @param newBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     */
    @Override
    public void onWrite(PageId pageId, int offset, byte[] newBytes) {
        if (!enabled) {
            return;
        }
        PageBytesRecord record = new PageBytesRecord(pageId, offset, newBytes);
        records.add(record);
        entries.add(new MtrRedoEntry(record, currentCategory));
        touchedPages.add(pageId);
    }

    /** 记录一次页初始化（newPage）。
     *
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param pageType 选择 {@code recordInit} 分支的 {@code PageType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     */
    void recordInit(PageId pageId, PageType pageType) {
        if (!enabled) {
            return;
        }
        PageInitRecord record = new PageInitRecord(pageId, pageType);
        records.add(record);
        entries.add(new MtrRedoEntry(record, MtrRedoCategory.PAGE_INIT));
        touchedPages.add(pageId);
    }

    /**
     * 追加一条显式逻辑 redo record。它不来自 PageGuard 字节写监听，因此不会自动增加 touched page；
     * 调用方必须确保同一 MTR 内存在对应的物理页修改或恢复期安全副作用来承载该逻辑意图。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param record  将被持久化到 redo 文件的逻辑 record。
     * @param category 本地诊断分类，说明 record 来源模块；不进入 redo 文件。
     * @param reason  追加原因，必须说明数据库语义，便于 review 追踪。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void recordLogical(RedoRecord record, MtrRedoCategory category, String reason) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (!enabled) {
            return;
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (record == null || category == null) {
            throw new DatabaseValidationException("logical redo record/category must not be null");
        }
        if (category == MtrRedoCategory.PAGE_INIT) {
            throw new DatabaseValidationException("PAGE_INIT category is reserved for MiniTransaction.newPage");
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("logical redo reason must not be blank");
        }
        records.add(record);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        entries.add(new MtrRedoEntry(record, category));
    }

    /**
     * 进入一个 redo 分类作用域。普通 {@code PAGE_BYTES} 写入使用当前分类；{@code PAGE_INIT} 永远保持自己的分类，
     * 以便恢复语义上继续作为唯一建页记录被审计。
     * @param category redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param reason 传给 {@code enterCategory} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code enterCategory} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    MtrRedoCategoryScope enterCategory(MtrRedoCategory category, String reason) {
        if (category == null) {
            throw new DatabaseValidationException("MTR redo category must not be null");
        }
        if (category == MtrRedoCategory.PAGE_INIT) {
            throw new DatabaseValidationException("PAGE_INIT category is reserved for MiniTransaction.newPage");
        }
        if (reason == null || reason.isBlank()) {
            throw new DatabaseValidationException("MTR redo category reason must not be blank");
        }
        MtrRedoCategoryScope scope = new MtrRedoCategoryScope(this, currentCategory, category, reason);
        currentCategory = category;
        categoryScopes.push(scope);
        return scope;
    }

    /** 按 LIFO 关闭分类作用域，防止嵌套分类被乱序恢复。
     *
     * @param scope redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws MtrStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    void closeCategoryScope(MtrRedoCategoryScope scope) {
        if (categoryScopes.peek() != scope) {
            throw new MtrStateException("MTR redo category scope closed out of order: " + scope.reason());
        }
        categoryScopes.pop();
        currentCategory = scope.previousCategory();
    }

    /** 关闭收集（commit 盖 pageLSN 前调用，排除 LSN 写进 redo）。 */
    void disable() {
        enabled = false;
    }

    /**
     * 返回真正交给 redo manager 的持久 record 视图。0.19d 起 FSP metadata 写点已经有
     * {@link FspMetadataDeltaRecord} 表达 after-image；如果同一 MTR 内仍由 PageGuard listener 捕获到完全相同的
     * FSP 分类 {@link PageBytesRecord}，这里在提交边界做精确去重。去重只影响 redo 文件内容，不影响
     * {@link #touchedPages}：物理写仍会让 commit 给页面盖 batch end LSN，并在 guard 释放时发布 dirty page。
     *
     * <p>不能按分类粗暴删除所有 FSP {@code PAGE_BYTES}：page0 FSP_HDR 信封、生命周期 truncate marker 等路径仍可能
     * 只由物理字节 redo 保护。只有被某条逻辑 delta 的 pageId、offset 和 after-image 完整覆盖的物理字节才可删除。
     * 0.19f 起 undo/rseg metadata 也走同一精确规则；0.19g 起完整 undo record payload 可以替代对应槽字节；
     * 0.19h 起 B+Tree structure delta v1 可以替代 sibling-link 等结构字段字节。B+Tree 新页初始化会先写
     * FIL prev/next 初值再写最终 sibling link；只要字节被 delta 精确覆盖，同一 MTR 内的 generic header 字节也可删除，
     * 因为 PAGE_INIT 与 B+Tree delta 已携带恢复所需的最终页头状态。
     */
    List<RedoRecord> records() {
        return persistedEntries().stream().map(MtrRedoEntry::record).toList();
    }

    List<MtrRedoEntry> entries() {
        return List.copyOf(persistedEntries());
    }

    Set<PageId> touchedPages() {
        return touchedPages;
    }

    /**
     * 校验输入与当前状态后修改Mini Transaction领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @return {@code persistedEntries} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     */
    private List<MtrRedoEntry> persistedEntries() {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        List<FspMetadataDeltaRecord> fspMetadataDeltas = entries.stream()
                .map(MtrRedoEntry::record)
                .filter(FspMetadataDeltaRecord.class::isInstance)
                .map(FspMetadataDeltaRecord.class::cast)
                .toList();
        List<UndoMetadataDeltaRecord> undoMetadataDeltas = entries.stream()
                .map(MtrRedoEntry::record)
                .filter(UndoMetadataDeltaRecord.class::isInstance)
                .map(UndoMetadataDeltaRecord.class::cast)
                .toList();
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        List<UndoRecordPayloadRecord> undoPayloadDeltas = entries.stream()
                .map(MtrRedoEntry::record)
                .filter(UndoRecordPayloadRecord.class::isInstance)
                .map(UndoRecordPayloadRecord.class::cast)
                .toList();
        List<BTreePageDeltaRecord> btreePageDeltas = entries.stream()
                .map(MtrRedoEntry::record)
                .filter(BTreePageDeltaRecord.class::isInstance)
                .map(BTreePageDeltaRecord.class::cast)
                .toList();
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (fspMetadataDeltas.isEmpty() && undoMetadataDeltas.isEmpty()
                && undoPayloadDeltas.isEmpty() && btreePageDeltas.isEmpty()) {
            return List.copyOf(entries);
        }
        List<MtrRedoEntry> retained = new ArrayList<>(entries.size());
        for (MtrRedoEntry entry : entries) {
            if (!isCoveredFspMetadataBytes(entry, fspMetadataDeltas)
                    && !isCoveredUndoMetadataBytes(entry, undoMetadataDeltas)
                    && !isCoveredUndoRecordPayloadBytes(entry, undoPayloadDeltas)
                    && !isCoveredBTreeStructureBytes(entry, btreePageDeltas)) {
                retained.add(entry);
            }
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return retained;
    }

    private static boolean isCoveredFspMetadataBytes(MtrRedoEntry entry,
                                                     List<FspMetadataDeltaRecord> metadataDeltas) {
        if (entry.category() != MtrRedoCategory.FSP_METADATA_BYTES
                || !(entry.record() instanceof PageBytesRecord pageBytes)) {
            return false;
        }
        for (FspMetadataDeltaRecord delta : metadataDeltas) {
            if (covers(delta, pageBytes)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断 {@code isCoveredUndoMetadataBytes} 所表达的Mini Transaction条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @param entry redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param metadataDeltas 参与 {@code isCoveredUndoMetadataBytes} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code isCoveredUndoMetadataBytes} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    private static boolean isCoveredUndoMetadataBytes(MtrRedoEntry entry,
                                                      List<UndoMetadataDeltaRecord> metadataDeltas) {
        if (entry.category() != MtrRedoCategory.UNDO_PAGE_BYTES
                || !(entry.record() instanceof PageBytesRecord pageBytes)) {
            return false;
        }
        for (UndoMetadataDeltaRecord delta : metadataDeltas) {
            if (covers(delta, pageBytes)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断 {@code isCoveredUndoRecordPayloadBytes} 所表达的Mini Transaction条件；方法只读取稳定状态，并用返回值报告是否满足条件。
     *
     * @param entry redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @param payloadDeltas 参与 {@code isCoveredUndoRecordPayloadBytes} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code isCoveredUndoRecordPayloadBytes} 命名的领域事实成立时为 {@code true}，否则为 {@code false}；查询本身不改变权威状态
     */
    private static boolean isCoveredUndoRecordPayloadBytes(MtrRedoEntry entry,
                                                           List<UndoRecordPayloadRecord> payloadDeltas) {
        if (entry.category() != MtrRedoCategory.UNDO_PAGE_BYTES
                || !(entry.record() instanceof PageBytesRecord pageBytes)) {
            return false;
        }
        for (UndoRecordPayloadRecord delta : payloadDeltas) {
            if (covers(delta, pageBytes)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCoveredBTreeStructureBytes(MtrRedoEntry entry,
                                                        List<BTreePageDeltaRecord> pageDeltas) {
        if ((entry.category() != MtrRedoCategory.BTREE_STRUCTURE_BYTES
                && entry.category() != MtrRedoCategory.PAGE_BYTES_GENERIC)
                || !(entry.record() instanceof PageBytesRecord pageBytes)) {
            return false;
        }
        for (BTreePageDeltaRecord delta : pageDeltas) {
            if (covers(delta, pageBytes)) {
                return true;
            }
        }
        return false;
    }

    private static boolean covers(FspMetadataDeltaRecord delta, PageBytesRecord pageBytes) {
        return covers(delta.pageId(), delta.offset(), delta.afterImage(), pageBytes);
    }

    private static boolean covers(UndoMetadataDeltaRecord delta, PageBytesRecord pageBytes) {
        return covers(delta.pageId(), delta.offset(), delta.afterImage(), pageBytes);
    }

    private static boolean covers(UndoRecordPayloadRecord delta, PageBytesRecord pageBytes) {
        return covers(delta.pageId(), delta.recordOffset(), delta.slotImage(), pageBytes);
    }

    private static boolean covers(BTreePageDeltaRecord delta, PageBytesRecord pageBytes) {
        return covers(delta.pageId(), delta.offset(), delta.afterImage(), pageBytes);
    }

    private static boolean covers(PageId logicalPageId, int logicalOffset, byte[] logicalBytes,
                                  PageBytesRecord pageBytes) {
        if (!logicalPageId.equals(pageBytes.pageId())) {
            return false;
        }
        byte[] physicalBytes = pageBytes.bytes();
        long physicalStart = pageBytes.offset();
        long physicalEnd = physicalStart + physicalBytes.length;
        long logicalStart = logicalOffset;
        long logicalEnd = logicalStart + logicalBytes.length;
        if (physicalStart < logicalStart || physicalEnd > logicalEnd) {
            return false;
        }
        int deltaOffset = (int) (physicalStart - logicalStart);
        for (int i = 0; i < physicalBytes.length; i++) {
            if (physicalBytes[i] != logicalBytes[deltaOffset + i]) {
                return false;
            }
        }
        return true;
    }
}
