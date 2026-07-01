package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.index.IndexPageHandle;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.page.IndexPageHeader;
import cn.zhangyis.db.storage.record.page.RecordComparator;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageDeleter;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
import cn.zhangyis.db.storage.record.page.RecordPageOverflowException;
import cn.zhangyis.db.storage.record.page.RecordPagePurger;
import cn.zhangyis.db.storage.record.page.RecordPageReorganizer;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.RecordPageUpdater;
import cn.zhangyis.db.storage.record.page.RecordRef;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.page.UpdateOutcome;
import cn.zhangyis.db.storage.record.page.UpdateResult;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.LongAdder;

/**
 * B3 split-capable B+Tree 实现。范围严格限制为 root leaf split、level-1 root-to-leaf 路由、
 * leaf sibling range scan，以及 level-1 leaf split；root 高度超过 1 或 parent split 均显式拒绝。
 *
 * <p>锁顺序：root → 目标 leaf → 分配新 leaf → 原右邻居。当前没有事务锁/MVCC，所有页 latch 均由调用方 MTR memo
 * 统一持有并在 commit/rollback 释放。页内容修改全部通过 MTR-owned {@link IndexPageAccess} 或 {@link IndexPageHandle}
 * 完成，依赖 D3/D4 的物理 PAGE_BYTES/PAGE_INIT redo 与 commit pageLSN。
 */
public final class SplitCapableBTreeIndexService implements BTreeIndexService {

    /** INDEX 页访问 facade；不暴露 BufferFrame，只返回 MTR-owned 页视图。 */
    private final IndexPageAccess pageAccess;
    /** 空间管理 facade；split 分配新 leaf 页必须走 segment/page 分配路径。 */
    private final DiskSpaceManager disk;
    /** 类型 codec 注册表；record 比较、编码、物化共享同一套类型规则。 */
    private final TypeCodecRegistry registry;
    /** 页内查找器；root node pointer 选择与 leaf duplicate 检查复用其目录二分逻辑。 */
    private final RecordPageSearch search;
    /** 页内插入器；负责 key 有序链入与 PageDirectory 维护。 */
    private final RecordPageInserter inserter;
    /** leaf record 与 scan 边界的比较器。 */
    private final RecordComparator recordComparator;
    /** 内存中 split 列表排序用比较器，复用类型 codec 保序编码。 */
    private final SearchKeyComparator keyComparator;
    /** node pointer 逻辑模型与 LogicalRecord 的转换器。 */
    private final BTreeNodePointerCodec pointerCodec;
    /** 预估父页是否容纳新 node pointer 时使用的编码器。 */
    private final RecordEncoder recordEncoder;
    /** 页内 delete-mark 算子；{@code deleteClustered} 物理删除前先逻辑标记（已标记则跳过）。 */
    private final RecordPageDeleter deleter;
    /** 页内 purge 算子；{@code deleteClustered} 把 delete-marked 记录物理摘链 + 回收空间。 */
    private final RecordPagePurger purger;
    /** 页内 update 算子；{@code replaceClustered} 整记录替换（原地/搬迁），key 变化返回 REQUIRES_REINSERT。 */
    private final RecordPageUpdater updater;
    /** 页内重组算子；merge 前压实 survivor，把 garbage（已 purge 空洞）回收为连续空闲，使 victim 条目可放入。 */
    private final RecordPageReorganizer reorganizer;
    /** 页容量（来自 {@link IndexPageAccess}）；merge 的 underflow 阈值（MERGE_THRESHOLD≈50%）与 fit 判定按它折算。 */
    private final PageSize pageSize;

    // ---- 写路径 latch coupling 诊断计数（0.13a）----
    // 非权威状态、不影响正确性，仅统计乐观 vs 悲观路径命中，便于测试/观测确认 crab 生效（对齐 InnoDB 的 Innodb_* 计数思路）。
    // LongAdder：并发写者各自累加、读时求和，避免热点 CAS。
    /** 乐观 insert 命中数：leaf-only 放得下、仅在 leaf 持 X 完成。 */
    private final LongAdder optimisticInsertHits = new LongAdder();
    /** 乐观 insert 回退数：leaf 溢出需 split，释放 leaf X 后改走悲观全 X。 */
    private final LongAdder pessimisticInsertFallbacks = new LongAdder();
    /** 乐观 delete 命中数：删后不欠载、仅在 leaf 持 X 完成（跳过 merge）。 */
    private final LongAdder optimisticDeleteHits = new LongAdder();
    /** 乐观 delete 回退数：删后欠载需 merge，释放 leaf X 后改走悲观全 X。 */
    private final LongAdder pessimisticDeleteFallbacks = new LongAdder();
    /** 乐观 replace 命中数（0.13b）：replace 永不结构变更，恒 leaf-only；仅 root 即 leaf 时交悲观。 */
    private final LongAdder optimisticReplaceHits = new LongAdder();
    /** 乐观 delete-mark 命中数（0.13b）：等长纯写、永不结构变更，恒 leaf-only；仅 root 即 leaf 时交悲观。 */
    private final LongAdder optimisticMarkHits = new LongAdder();
    /** 乐观 purge 命中数（0.13b；含 stale no-op 与不欠载物理移除）。 */
    private final LongAdder optimisticPurgeHits = new LongAdder();
    /** 乐观 purge 回退数（0.13b）：移除后欠载需 merge，释放 leaf X 后改走悲观全 X。 */
    private final LongAdder pessimisticPurgeFallbacks = new LongAdder();

    public SplitCapableBTreeIndexService(IndexPageAccess pageAccess, DiskSpaceManager disk,
                                         TypeCodecRegistry registry) {
        if (pageAccess == null || disk == null || registry == null) {
            throw new DatabaseValidationException("split btree pageAccess/disk/registry must not be null");
        }
        this.pageAccess = pageAccess;
        this.disk = disk;
        this.registry = registry;
        this.pageSize = pageAccess.pageSize();
        this.search = new RecordPageSearch(registry);
        this.inserter = new RecordPageInserter(registry);
        this.recordComparator = new RecordComparator(registry);
        this.keyComparator = new SearchKeyComparator(registry);
        this.pointerCodec = new BTreeNodePointerCodec();
        this.recordEncoder = new RecordEncoder(registry);
        this.deleter = new RecordPageDeleter();
        this.purger = new RecordPagePurger();
        this.updater = new RecordPageUpdater(registry);
        this.reorganizer = new RecordPageReorganizer();
    }

    /**
     * 聚簇 insert：用调用方事务 id 与 undo roll pointer 盖戳隐藏列（DB_TRX_ID=transactionId、
     * DB_ROLL_PTR=rollPointer），再走通用 {@link #insert}。
     *
     * <p>T1.3c 起不再恒写 {@link RollPointer#NULL}：roll pointer 由上层 orchestration
     * （{@code assignWriteId → UndoLogManager.beforeInsert → insertClustered}）传入，指向本事务刚追加的
     * INSERT undo record。本方法不 import trx/undo，只收一个 {@link RollPointer} 值对象，保持 B+Tree 与
     * 事务 undo 模块解耦；{@code RollPointer#NULL} 仍合法（表「无 undo」，用于不接 undo 的路径或测试），
     * 但 Java null 引用必须拒绝。
     *
     * <p>split 保留不变量：若插入触发 split，{@code materializeLeafRecords} 经 {@code RecordCursor.materialize()}
     * 重物化既有记录（已带隐藏列），再 {@code RecordPageInserter} 重编码；因 {@code index.schema().clustered()}
     * 为真，encoder 自动带住隐藏区，故 split 不丢 DB_TRX_ID/DB_ROLL_PTR。
     *
     * <p>事务 id 来源：调用方须先 {@code TransactionManager.assignWriteId(txn)}，此处只验非 NONE，
     * 不依赖 TransactionManager，保持 B+Tree 与事务管理解耦。
     *
     * @param rollPointer 本事务 INSERT undo record 的 roll pointer；可为 {@link RollPointer#NULL}，不能为 Java null。
     */
    public BTreeInsertResult insertClustered(MiniTransaction mtr, BTreeIndex index,
                                             LogicalRecord record, TransactionId transactionId,
                                             RollPointer rollPointer) {
        if (record == null || transactionId == null || rollPointer == null) {
            throw new DatabaseValidationException(
                    "clustered insert record/transactionId/rollPointer must not be null");
        }
        if (!index.clustered()) {
            throw new DatabaseValidationException(
                    "insertClustered requires a clustered index: " + index.indexId());
        }
        if (transactionId.isNone()) {
            throw new DatabaseValidationException("clustered insert requires a non-NONE transaction id");
        }
        LogicalRecord stamped = new LogicalRecord(record.schemaVersion(), record.columnValues(),
                record.deleted(), record.recordType(),
                new HiddenColumns(transactionId, rollPointer));
        return insert(mtr, index, stamped);
    }

    /**
     * 聚簇记录物理删除（T1.3d，rollback 反向走链消费 INSERT undo 的删除原语）。数据流：导航到目标 leaf（level 0=root
     * leaf / level 1=chooseChild，X）→ {@code findEqual} 定位 → **所有权校验**（命中记录的 {@code DB_TRX_ID}/
     * {@code DB_ROLL_PTR} 必须同时等于调用方期望值）→ 未标记则 {@code deleteMark} 后 {@code purge}，已标记则跳过
     * deleteMark 直接 purge（幂等/可重试）。同 MTR 产 PAGE_BYTES redo。
     *
     * <p><b>所有权校验的不变量</b>（设计 §7.6/§14.4）：rollback 只删「本 undo 插入的那一行」。{@code insertClustered}
     * 当初把该 undo record 的 roll pointer 盖进记录的 {@code DB_ROLL_PTR}，故 {@code expectedRollPtr} = 正在应用的
     * undo roll pointer 是「就是这一行」的最强判据。未命中、或命中但隐藏列不匹配（例如 orphan undo 场景下该 key 已被
     * 另一事务重新插入）都返回 {@code removed=false} 且**不做任何修改**，绝不误删同 key 记录。
     *
     * <p>本方法不 import trx/undo，只收 {@link SearchKey} 与 domain 的 {@link TransactionId}/{@link RollPointer}，
     * 保持 B+Tree 与事务/undo 模块解耦（与 {@code insertClustered} 对称）。不做 merge / node-pointer 维护 / 空页回收：
     * 删后空 leaf 留页、root node pointer 的 lowKey 作为保守下界仍能正确路由（misroute 只会落到 findEqual 空）。
     *
     * @param key             目标聚簇 key。
     * @param expectedTrxId   期望的 DB_TRX_ID（本 undo 的写事务 id），不能为 null。
     * @param expectedRollPtr 期望的 DB_ROLL_PTR（正在应用的 undo roll pointer），不能为 Java null（可为 NULL 指针）。
     * @return {@link BTreeDeleteResult#removed()} 表示是否真正摘除了一条匹配记录。
     */
    public BTreeDeleteResult deleteClustered(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                             TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        if (key == null || expectedTrxId == null || expectedRollPtr == null) {
            throw new DatabaseValidationException(
                    "deleteClustered key/expectedTrxId/expectedRollPtr must not be null");
        }
        if (!index.clustered()) {
            throw new DatabaseValidationException(
                    "deleteClustered requires a clustered index: " + index.indexId());
        }
        // 0.13a：乐观优先——多层树 S-crab 到 leaf(X)，删后不欠载则仅改 leaf 即成（跳过 merge）；
        // 未命中/所有权不符=幂等 no-op（纯读，safe）。删后欠载需 merge（unsafe）或单 root leaf 时返回 null，回退悲观全 X。
        BTreeDeleteResult optimistic = tryOptimisticDelete(mtr, index, key, expectedTrxId, expectedRollPtr);
        if (optimistic != null) {
            return optimistic;
        }
        List<IndexPageHandle> path = descendPath(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return deleteInLeaf(mtr, index, path, key, expectedTrxId, expectedRollPtr);
    }

    /**
     * 乐观删除尝试（0.13a）：{@link #descendOptimistic} S-crab 到 leaf(X)。
     *
     * <p>数据流与 safe/unsafe 判据：{@code findEqual} 未命中、或命中但所有权（DB_TRX_ID/DB_ROLL_PTR）不符 → 幂等 no-op
     * （纯读，safe）；命中且归属本 undo → {@link #deleteWouldUnderflow} 预判物理 purge 后是否欠载：
     * <ul>
     *   <li><b>不欠载（safe）</b>：未标记则 {@code deleteMark}、再 {@code purge}，**跳过** {@link #reclaimAfterRemoval}——
     *       乐观路径已 crab 释放父页、拿不到 merge 所需全路径，而预判保证无需 merge，故仅改 leaf 即安全完成；</li>
     *   <li><b>欠载（unsafe）</b>：**写页前**提前释放 leaf X（零页修改，可干净重启），返回 null 交悲观全 X 重启（带 merge）。</li>
     * </ul>
     * root 即 leaf（单页树）时 {@code descendOptimistic} 返回 null，本方法同样返回 null 交悲观。
     */
    private BTreeDeleteResult tryOptimisticDelete(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                  TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        if (leafHandle == null) {
            return null; // root 即 leaf：交悲观
        }
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            optimisticDeleteHits.increment();
            return BTreeDeleteResult.noChange(index); // 幂等：未命中，纯读（safe）
        }
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        if (!expectedTrxId.equals(cursor.dbTrxId()) || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            optimisticDeleteHits.increment();
            return BTreeDeleteResult.noChange(index); // 幂等：所有权不符，纯读（safe）
        }
        if (deleteWouldUnderflow(leaf, cursor, index)) {
            // unsafe：删后欠载需 merge（须全路径 X）→ 写页前提前放 leaf X → 交悲观重启。
            pageAccess.releaseHandle(mtr, leafHandle);
            pessimisticDeleteFallbacks.increment();
            return null;
        }
        // safe：删后不欠载，仅改 leaf，跳过欠载回收（无 merge）。
        if (!cursor.isDeleted()) {
            deleter.deleteMark(leaf, offset);
        }
        purger.purge(leaf, offset);
        optimisticDeleteHits.increment();
        return BTreeDeleteResult.removed(index, List.of());
    }

    /**
     * 预判：物理 purge 掉 {@code cursor} 指向的记录后 leaf 是否会欠载（须 merge，走悲观）。用与 {@link #isUnderfull}
     * 完全一致的阈值公式（{@code reclaimable*2 > pageSize}），但对「删后可回收空闲」取**上界**：freed 估为编码长度 +
     * {@link #MERGE_ENTRY_MARGIN}，实际释放 ≥ 估值 → 投影 reclaimable 偏大 → 更易判欠载 → 偏向悲观。
     *
     * <p><b>为何偏保守</b>：漏判（欠载判成不欠载）会让乐观路径删后不 merge，留下本该回收的欠载页——破坏 0.12 空间回收契约
     * 与既有 merge 测试；误判（不欠载判成欠载）只是多走一次悲观全 X（悲观 {@code considerMerge} 再按真实占用判定，
     * 不欠载则 no-op merge，结果正确）。故宁可误判、不可漏判。
     */
    private boolean deleteWouldUnderflow(RecordPage leaf, RecordCursor cursor, BTreeIndex index) {
        int freedUpperBound = recordEncoder.encode(cursor.materialize(), index.schema()).length + MERGE_ENTRY_MARGIN;
        int reclaimableAfter = leaf.freeSpace() + leaf.header().garbage() + freedUpperBound;
        return reclaimableAfter * 2 > pageSize.bytes();
    }

    /** 乐观 delete 命中计数（诊断/观测；含幂等 no-op 与不欠载删除）。 */
    long optimisticDeleteHitCount() {
        return optimisticDeleteHits.sum();
    }

    /** 乐观 delete 回退悲观计数（诊断/观测；删后欠载需 merge）。 */
    long pessimisticDeleteFallbackCount() {
        return pessimisticDeleteFallbacks.sum();
    }

    /**
     * 在已定位的 leaf（path 末项）上执行所有权校验 + delete-mark + purge，物理删除成功后触发欠载回收
     * （merge + 原地 root shrink，见 {@link #reclaimAfterRemoval}）。{@code findEqual} 返回的是物理命中（含 delete-marked），
     * 因此这里要先按隐藏列确认归属、再判断是否已标记，避免对非本 undo 的行或已标记的行误操作。
     */
    private BTreeDeleteResult deleteInLeaf(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path,
                                          SearchKey key, TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        IndexPageHandle leafHandle = path.get(path.size() - 1);
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return BTreeDeleteResult.noChange(index);
        }
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        // 所有权校验：dbTrxId+dbRollPtr 同时匹配才是本 undo 插入的行；否则不删（幂等收敛）
        if (!expectedTrxId.equals(cursor.dbTrxId()) || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            return BTreeDeleteResult.noChange(index);
        }
        // 已 delete-marked（半失败/重试）则跳过 deleteMark 直接 purge；否则先逻辑标记再物理摘除
        if (!cursor.isDeleted()) {
            deleter.deleteMark(leaf, offset);
        }
        purger.purge(leaf, offset);
        MergeOutcome outcome = reclaimAfterRemoval(mtr, index, path);
        return BTreeDeleteResult.removed(outcome.indexAfter(), outcome.freedPages());
    }

    /**
     * Purge 物理移除一条**已经 delete-marked** 的聚簇记录（purge 专用，与 {@link #deleteClustered} 的关键区别：
     * 绝不主动 delete-mark）。purge worker 处理某已提交事务的 DELETE_MARK undo 时调用：re-locate {@code key} →
     * 必须命中、且记录**当前仍为 delete-marked**、且 {@code DB_TRX_ID==expectedTrxId}（删除该行的事务）、
     * 且 {@code DB_ROLL_PTR==expectedRollPtr}（= 该 DELETE_MARK undo record 自身地址）→ 才 {@link RecordPagePurger#purge}
     * 物理摘链回收。任一不符（未命中/未标记/隐藏列不符）一律确认 stale，返回 {@code removed=false} 且**不做任何修改**。
     *
     * <p>为什么必须严格：purge 在 boundary 之外异步运行，若像 {@code deleteClustered} 那样对未标记行也先 deleteMark，
     * 会把一行**存活**记录误删（例如该 key 已被新事务重新插入为 live 行）。purge 只能回收确属本 undo 删除的死行。
     *
     * @param key             目标聚簇 key。
     * @param expectedTrxId   删除该行的事务 id（undo 的 DB_TRX_ID），不能为 null。
     * @param expectedRollPtr 该 DELETE_MARK undo record 自身的 roll pointer（= 记录应有的 DB_ROLL_PTR），不能为 Java null。
     * @return {@link BTreeDeleteResult#removed()} 是否物理移除；false 表示确认 stale、未改任何内容。
     */
    public BTreeDeleteResult purgeDeleteMarkedClustered(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                        TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        if (key == null || expectedTrxId == null || expectedRollPtr == null) {
            throw new DatabaseValidationException(
                    "purgeDeleteMarkedClustered key/expectedTrxId/expectedRollPtr must not be null");
        }
        if (!index.clustered()) {
            throw new DatabaseValidationException(
                    "purgeDeleteMarkedClustered requires a clustered index: " + index.indexId());
        }
        List<IndexPageHandle> path = descendPath(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return purgeInLeaf(mtr, index, path, key, expectedTrxId, expectedRollPtr);
    }

    /**
     * 在已定位 leaf（path 末项）上执行 purge 严格校验：命中 + 已 delete-marked + 隐藏列(DB_TRX_ID/DB_ROLL_PTR)匹配才物理摘除；
     * 否则不改任何内容（stale 收敛）。与 {@link #deleteInLeaf} 的差别：未标记记录在此**不**会被 deleteMark。
     * 物理摘除成功后同样触发欠载回收（{@link #reclaimAfterRemoval}）。
     */
    private BTreeDeleteResult purgeInLeaf(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path,
                                          SearchKey key, TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        IndexPageHandle leafHandle = path.get(path.size() - 1);
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return BTreeDeleteResult.noChange(index);
        }
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        if (!cursor.isDeleted()
                || !expectedTrxId.equals(cursor.dbTrxId())
                || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            return BTreeDeleteResult.noChange(index);
        }
        purger.purge(leaf, offset);
        MergeOutcome outcome = reclaimAfterRemoval(mtr, index, path);
        return BTreeDeleteResult.removed(outcome.indexAfter(), outcome.freedPages());
    }

    /**
     * 聚簇记录整记录替换（T1.3e，前向 UPDATE 与 rollback 恢复共用）。数据流：导航到目标 leaf（level 0/1，X）→
     * {@code findEqual} 定位 → **所有权校验**（当前记录 {@code DB_TRX_ID}/{@code DB_ROLL_PTR} 必须同时等于
     * {@code expectedTrxId}/{@code expectedRollPtr}）→ {@code RecordPageUpdater.update} 整记录替换为
     * {@code newRecord}（含其 {@link HiddenColumns}）。
     *
     * <p><b>两种调用</b>：前向 UPDATE 传新列 + 新隐藏列 {@code (txnId,newRollPtr)}，expected=旧 {@code (txnId,rollPtr)}；
     * rollback 恢复传旧 image（旧列 + 旧隐藏列），expected=被回滚 update 写入的 {@code (txnId,updateRollPtr)}。两者都靠
     * 所有权校验确保只改"本事务/本版本"的那一行；未命中或不匹配返回 {@code replaced=false} 且**不做任何修改**（幂等）。
     *
     * <p>改聚簇 key（{@code RecordPageUpdater} 返回 {@link UpdateOutcome#REQUIRES_REINSERT}）抛
     * {@link BTreeUnsupportedStructureException}（T1.3e 不支持改 PK）。本方法不 import trx/undo，只收
     * {@link LogicalRecord} 与 domain 的 {@link TransactionId}/{@link RollPointer}（与 insert/delete 对称）。
     *
     * @param newRecord       替换后的完整聚簇记录（必须携带 {@link HiddenColumns}）。
     * @param expectedTrxId   期望的当前 DB_TRX_ID，不能为 null。
     * @param expectedRollPtr 期望的当前 DB_ROLL_PTR，不能为 Java null。
     * @return {@link BTreeUpdateResult#replaced()} 表示是否真正替换了一条匹配记录。
     */
    public BTreeUpdateResult replaceClustered(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                              LogicalRecord newRecord, TransactionId expectedTrxId,
                                              RollPointer expectedRollPtr) {
        if (key == null || newRecord == null || expectedTrxId == null || expectedRollPtr == null) {
            throw new DatabaseValidationException(
                    "replaceClustered key/newRecord/expectedTrxId/expectedRollPtr must not be null");
        }
        if (!index.clustered()) {
            throw new DatabaseValidationException("replaceClustered requires a clustered index: " + index.indexId());
        }
        if (newRecord.hiddenColumns() == null) {
            throw new DatabaseValidationException("replaceClustered newRecord must carry hidden columns (clustered)");
        }
        // 0.13b：乐观优先。replace 永不 split/merge（原地/页内搬迁），故恒 safe——只有 root 即 leaf（单页无 crab 收益）
        // 才交悲观 findLeaf(X)。改 PK/搬迁页满都是与路径无关的异常，leaf 未改直接上抛（悲观也一样抛）。
        BTreeUpdateResult optimistic = tryOptimisticReplace(mtr, index, key, newRecord, expectedTrxId, expectedRollPtr);
        if (optimistic != null) {
            return optimistic;
        }
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return replaceInLeaf(leaf.page(), leaf.pageId(), index, key, newRecord, expectedTrxId, expectedRollPtr);
    }

    /**
     * 乐观替换尝试（0.13b）：{@link #descendOptimistic} S-crab 到 leaf(X) → {@link #replaceInLeaf}。replace 是纯 leaf-only
     * （原地或页内搬迁），**从不 split/merge** → 恒 safe，无 unsafe 回退分支。{@code descendOptimistic} 返回 null（root 即 leaf）
     * 时返回 null 交悲观。命中计数在 {@code replaceInLeaf} 正常返回后自增——改 PK（{@link BTreeUnsupportedStructureException}）
     * 或搬迁页满（{@link RecordPageOverflowException}，updater 溢出前零改动、leaf 未改）抛出时不计数、直接上抛（与悲观同）。
     */
    private BTreeUpdateResult tryOptimisticReplace(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                   LogicalRecord newRecord, TransactionId expectedTrxId,
                                                   RollPointer expectedRollPtr) {
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        if (leafHandle == null) {
            return null; // root 即 leaf：交悲观
        }
        BTreeUpdateResult result = replaceInLeaf(leafHandle.recordPage(), leafHandle.pageId(), index, key,
                newRecord, expectedTrxId, expectedRollPtr);
        optimisticReplaceHits.increment();
        return result;
    }

    /** 乐观 replace 命中计数（诊断/观测；含所有权不符的 no-op）。 */
    long optimisticReplaceHitCount() {
        return optimisticReplaceHits.sum();
    }

    /**
     * 在已定位 leaf 上执行所有权校验 + 整记录替换。所有权不匹配/未命中即 no-op（{@code replaced=false}）；改聚簇 key
     * 抛 {@link BTreeUnsupportedStructureException}。
     */
    private BTreeUpdateResult replaceInLeaf(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key,
                                           LogicalRecord newRecord, TransactionId expectedTrxId,
                                           RollPointer expectedRollPtr) {
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return new BTreeUpdateResult(false);
        }
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        if (!expectedTrxId.equals(cursor.dbTrxId()) || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            return new BTreeUpdateResult(false);
        }
        UpdateResult res = updater.update(leaf, leafId, offset, newRecord, index.keyDef(), index.schema());
        if (res.outcome() == UpdateOutcome.REQUIRES_REINSERT) {
            throw new BTreeUnsupportedStructureException(
                    "replaceClustered does not support changing the clustered key (T1.3e), index " + index.indexId());
        }
        return new BTreeUpdateResult(true);
    }

    /**
     * 翻转聚簇记录 delete 位并盖新隐藏列（T1.3f，前向删除与回滚取消标记共用）。**plan-then-execute**：plan 阶段
     * 导航 leaf(X)→{@code findEqual}（含已标记）→**所有权校验**（当前 dbTrxId/dbRollPtr==expected，不符=changed=false
     * 幂等）→**翻转合法校验**（当前 delete 位必须 != {@code deleted}，否则抛——非法重复/损坏，旧 delete flag 隐含 false）；
     * execute 阶段连续两步纯写 {@code setDeleted} + {@code writeHiddenColumns}（列与记录长度不变，杜绝半状态）。
     *
     * <p>前向删除：{@code deleted=true}、{@code newHidden=(txnId, delMarkRollPtr)}、{@code expected}=存活版本隐藏列；
     * 回滚取消标记：{@code deleted=false}、{@code newHidden}=删除前旧隐藏列、{@code expected}=(删除事务 id, delMarkRollPtr)。
     * 物理移除归 purge（本片不做）。不 import trx/undo。
     *
     * @param deleted         目标 delete 位。
     * @param newHidden       目标隐藏列（DB_TRX_ID/DB_ROLL_PTR），不能为 null。
     * @param expectedTrxId   期望的当前 DB_TRX_ID，不能为 null。
     * @param expectedRollPtr 期望的当前 DB_ROLL_PTR，不能为 Java null。
     */
    public BTreeDeleteMarkResult setClusteredDeleteMark(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                        boolean deleted, HiddenColumns newHidden,
                                                        TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        if (key == null || newHidden == null || expectedTrxId == null || expectedRollPtr == null) {
            throw new DatabaseValidationException(
                    "setClusteredDeleteMark key/newHidden/expectedTrxId/expectedRollPtr must not be null");
        }
        if (!index.clustered()) {
            throw new DatabaseValidationException("setClusteredDeleteMark requires a clustered index: " + index.indexId());
        }
        // 0.13b：乐观优先。delete-mark 是等长纯写（setDeleted+writeHiddenColumns），无 size 变化/无 overflow/无结构变更 → 恒 safe。
        // 只有 root 即 leaf 交悲观 findLeaf(X)；非法翻转与路径无关，抛出即上抛。
        BTreeDeleteMarkResult optimistic = tryOptimisticMark(mtr, index, key, deleted, newHidden,
                expectedTrxId, expectedRollPtr);
        if (optimistic != null) {
            return optimistic;
        }
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return markInLeaf(leaf.page(), leaf.pageId(), index, key, deleted, newHidden, expectedTrxId, expectedRollPtr);
    }

    /**
     * 乐观 delete-mark 尝试（0.13b）：{@link #descendOptimistic} S-crab 到 leaf(X) → {@link #markInLeaf}。翻转 delete 位 +
     * 改隐藏列均**等长纯写**，不改记录长度、不 split/merge → 恒 safe，无 unsafe 回退。root 即 leaf 返回 null 交悲观；
     * 非法翻转（{@link DatabaseValidationException}）抛出时不计数、直接上抛（与悲观同）。
     */
    private BTreeDeleteMarkResult tryOptimisticMark(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                    boolean deleted, HiddenColumns newHidden,
                                                    TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        if (leafHandle == null) {
            return null; // root 即 leaf：交悲观
        }
        BTreeDeleteMarkResult result = markInLeaf(leafHandle.recordPage(), leafHandle.pageId(), index, key,
                deleted, newHidden, expectedTrxId, expectedRollPtr);
        optimisticMarkHits.increment();
        return result;
    }

    /** 乐观 delete-mark 命中计数（诊断/观测；含所有权不符的 no-op）。 */
    long optimisticMarkHitCount() {
        return optimisticMarkHits.sum();
    }

    /**
     * 在已定位 leaf 上 plan-then-execute 翻转 delete 位 + 改隐藏列。所有权不匹配/未命中=changed=false（幂等）；
     * 非法翻转（当前 delete 位已等于目标）抛 {@link DatabaseValidationException}。
     */
    private BTreeDeleteMarkResult markInLeaf(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key,
                                            boolean deleted, HiddenColumns newHidden,
                                            TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        validateLeafPage(leaf, index, leafId);
        // ---- plan：定位 + 校验（任何失败在写页前）----
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return new BTreeDeleteMarkResult(false);
        }
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        if (!expectedTrxId.equals(cursor.dbTrxId()) || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            return new BTreeDeleteMarkResult(false);
        }
        if (cursor.isDeleted() == deleted) {
            throw new DatabaseValidationException("illegal delete-mark flip: record delete flag already "
                    + deleted + " at index " + index.indexId());
        }
        // ---- execute：两步纯写（不抛），保持列与记录长度不变 ----
        leaf.setDeleted(offset, deleted);
        leaf.writeHiddenColumns(offset, newHidden);
        return new BTreeDeleteMarkResult(true);
    }

    /**
     * 点查索引。数据流：打开 root 并校验 header → level=0 直接查 leaf；
     * level=1 用 root node pointer 选择 leaf，再在 leaf 内执行页内等值查找。
     */
    @Override
    public Optional<BTreeLookupResult> lookup(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        return doLookup(mtr, index, key, false);
    }

    /**
     * 点查但**不过滤** delete-marked 当前版本（T1.3f，供 MVCC）。普通 {@link #lookup} 把 delete-marked 当作"消失"
     * 返回空；一致性读必须看到 delete-marked 当前版本（其 {@code DB_TRX_ID}/{@code DB_ROLL_PTR}）才能按 ReadView
     * 判可见性（可见删除→行消失；不可见删除→沿版本链取删除前版本）。返回的 {@code LogicalRecord.deleted()} 携带删除位。
     */
    public Optional<BTreeLookupResult> lookupIncludingDeleted(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        return doLookup(mtr, index, key, true);
    }

    /**
     * 点查共用导航。数据流：打开 root 并校验 header → level=0 直接查 leaf；level=1 用 root node pointer 选择 leaf，
     * 再在 leaf 内执行页内等值查找。{@code includeDeleted=false} 过滤 delete-marked（普通读），{@code true} 保留（MVCC）。
     */
    private Optional<BTreeLookupResult> doLookup(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                boolean includeDeleted) {
        if (key == null) {
            throw new DatabaseValidationException("btree lookup key must not be null");
        }
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.SHARED);
        return lookupInLeaf(leaf.page(), leaf.pageId(), index, key, includeDeleted);
    }

    /**
     * 历史 scanLeaf 方法在 B3 后委托为真正 scan。leaf-only 语义仍由 LeafOnlyBTreeIndexService 保持。
     */
    @Override
    public List<BTreeLookupResult> scanLeaf(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        return scan(mtr, index, range);
    }

    /**
     * 有界范围扫描。level=0 扫 root leaf；level=1 从 lowerKey 对应 leaf 开始，沿 FIL sibling next 链顺序扫。
     */
    @Override
    public List<BTreeLookupResult> scan(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        if (range == null) {
            throw new DatabaseValidationException("btree scan range must not be null");
        }
        if (range.limit() == 0) {
            return List.of();
        }
        // 任意树高：先 findLeaf 定位 lowerKey 所在起始 leaf，再沿 FIL sibling next 链顺序扫（level 0 时起始 leaf 即 root，无 next）。
        PageId leafId = findLeaf(mtr, index, range.lowerKey(), PageLatchMode.SHARED).pageId();
        ScanAccumulator acc = new ScanAccumulator(range.limit());
        while (true) {
            IndexPageHandle leafHandle = pageAccess.openIndexPageHandle(mtr, leafId, PageLatchMode.SHARED);
            RecordPage leaf = leafHandle.recordPage();
            validateLeafPage(leaf, index, leafId);
            boolean stop = scanLeafPage(leaf, leafId, index, range, acc);
            if (stop || acc.full()) {
                break;
            }
            long next = leafHandle.fileHeader().nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                break;
            }
            leafId = PageId.of(index.rootPageId().spaceId(), PageNo.of(next));
        }
        return acc.results();
    }

    /**
     * 插入逻辑记录（0.13a：写路径 latch coupling）。<b>乐观优先</b>：多层树先 {@link #tryOptimisticInsert} 走
     * S-crab 下降 + leaf X，放得下即成（仅 leaf 持 X，放开 root 处写并发）；leaf 溢出需 split（unsafe）或树只有单 root leaf
     * 时返回 null，回退 {@link #pessimisticInsert} 悲观全路径 X（现有 split 引擎不变）。
     */
    @Override
    public BTreeInsertResult insert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("btree insert record must not be null");
        }
        SearchKey key = keyOf(record, index);
        BTreeInsertResult optimistic = tryOptimisticInsert(mtr, index, record, key);
        if (optimistic != null) {
            return optimistic;
        }
        return pessimisticInsert(mtr, index, record, key);
    }

    /**
     * 乐观插入尝试：{@link #descendOptimistic} S-crab 到 leaf(X)；leaf 放得下则仅改 leaf 即成（safe，caller commit）。
     *
     * <p>safe 判据靠**试错**而非预测：{@link RecordPageInserter#insert} 在页满时于 heap 分配处、任何页字节写入之前抛
     * {@link RecordPageOverflowException}（leaf 完全未改），故此处直接 try——成功即 leaf-only 安全插入；溢出即 unsafe，
     * **提前释放 leaf X**（零页修改，可干净重启）后返回 null，交悲观全 X 处理 split。root 即 leaf（单页树）无 crab 收益，
     * {@code descendOptimistic} 返回 null，本方法同样返回 null 交悲观。唯一性冲突（{@link #ensureUniqueAbsent} 抛
     * {@link BTreeDuplicateKeyException}）与路径无关，直接上抛（悲观也会抛，语义一致）。
     */
    private BTreeInsertResult tryOptimisticInsert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record,
                                                  SearchKey key) {
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        if (leafHandle == null) {
            return null; // root 即 leaf：交悲观（单页无并发收益）
        }
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        ensureUniqueAbsent(leaf, leafId, index, key);
        try {
            RecordRef ref = inserter.insert(leaf, leafId, record, index.keyDef(), index.schema());
            optimisticInsertHits.increment();
            return new BTreeInsertResult(index, ref); // safe：仅 leaf 变更
        } catch (RecordPageOverflowException overflow) {
            // unsafe：leaf 未改（inserter 溢出前零改动）→ 提前放掉 leaf X → 交悲观全 X 重启做 split。
            pageAccess.releaseHandle(mtr, leafHandle);
            pessimisticInsertFallbacks.increment();
            return null;
        }
    }

    /**
     * 悲观插入：全路径 X 下降（root→leaf 全部 latch 持到 commit），放得下直接插、溢出自底向上 split 传播。
     * 既是单 root leaf 树的基础路径，也是乐观 unsafe（split）后的重启路径；split 引擎与本次改动无关，行为不变。
     */
    private BTreeInsertResult pessimisticInsert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record,
                                                SearchKey key) {
        List<IndexPageHandle> path = descendPath(mtr, index, key, PageLatchMode.EXCLUSIVE);
        IndexPageHandle leafHandle = path.get(path.size() - 1);
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        ensureUniqueAbsent(leaf, leafId, index, key);
        try {
            RecordRef ref = inserter.insert(leaf, leafId, record, index.keyDef(), index.schema());
            return new BTreeInsertResult(index, ref);
        } catch (RecordPageOverflowException overflow) {
            requireLeafSegment(index);
            return splitLeafAndPropagate(mtr, index, path, record);
        }
    }

    /** 乐观 insert 命中计数（诊断/观测）。 */
    long optimisticInsertHitCount() {
        return optimisticInsertHits.sum();
    }

    /** 乐观 insert 回退悲观计数（诊断/观测）。 */
    long pessimisticInsertFallbackCount() {
        return pessimisticInsertFallbacks.sum();
    }

    /** root leaf 页满时的稳定 root split：旧 root 内容物化到内存，root 重建为 level-1，两个新 leaf 保存旧/新记录。 */
    private BTreeInsertResult splitRootLeaf(MiniTransaction mtr, BTreeIndex index, IndexPageHandle rootHandle,
                                           LogicalRecord inserted) {
        RecordPage oldRootLeaf = rootHandle.recordPage();
        List<LogicalRecord> records = sortedWithInserted(materializeLeafRecords(oldRootLeaf, index), inserted, index);
        SplitRows split = splitRows(records);

        PageId leftId = disk.allocatePage(mtr, index.leafSegment());
        RecordPage leftPage = pageAccess.createIndexPage(mtr, leftId, index.indexId(), 0);
        PageId rightId = disk.allocatePage(mtr, index.leafSegment());
        RecordPage rightPage = pageAccess.createIndexPage(mtr, rightId, index.indexId(), 0);
        IndexPageHandle leftHandle = pageAccess.openIndexPageHandle(mtr, leftId, PageLatchMode.EXCLUSIVE);
        IndexPageHandle rightHandle = pageAccess.openIndexPageHandle(mtr, rightId, PageLatchMode.EXCLUSIVE);
        leftHandle.writeSiblingLinks(FilePageHeader.FIL_NULL, rightId.pageNo().value());
        rightHandle.writeSiblingLinks(leftId.pageNo().value(), FilePageHeader.FIL_NULL);

        RecordRef insertedRef = insertAll(leftPage, leftId, split.left(), index, inserted);
        RecordRef rightInsertedRef = insertAll(rightPage, rightId, split.right(), index, inserted);
        if (insertedRef == null) {
            insertedRef = rightInsertedRef;
        }

        RecordPage root = rootHandle.recordPage();
        root.format(index.indexId(), 1);
        rootHandle.writeSiblingLinks(FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL);
        BTreeIndex after = index.withRootLevel(1);
        insertPointer(root, index.rootPageId(), after, new BTreeNodePointer(lowKey(split.left(), index), leftId));
        insertPointer(root, index.rootPageId(), after, new BTreeNodePointer(lowKey(split.right(), index), rightId));
        return new BTreeInsertResult(index, insertedRef, after, true, List.of(leftId, rightId));
    }

    /**
     * leaf 溢出后的 split 传播入口（任意树高）。leaf 即 root（path 仅 root）→ 原地长高（level 0→1，复用
     * {@link #splitRootLeaf}）；否则 leaf=左半、新右兄弟=右半，separator 经 {@link #insertSeparator} 上插父页，
     * 父满则递归内部 split，直至某层放下或根原地长高。
     */
    private BTreeInsertResult splitLeafAndPropagate(MiniTransaction mtr, BTreeIndex index,
                                                    List<IndexPageHandle> path, LogicalRecord inserted) {
        int depth = path.size() - 1;
        IndexPageHandle leafHandle = path.get(depth);
        if (depth == 0) {
            return splitRootLeaf(mtr, index, leafHandle, inserted);
        }
        List<PageId> allocated = new ArrayList<>();
        LeafSplitResult ls = splitNonRootLeaf(mtr, index, leafHandle, inserted, allocated);
        BTreeNodePointer separator = new BTreeNodePointer(ls.rightLowKey(), ls.newRightId());
        BTreeIndex after = insertSeparator(mtr, index, path, depth - 1, separator, allocated);
        return new BTreeInsertResult(index, ls.insertedRef(), after, true, List.copyOf(allocated));
    }

    /** 非 root leaf split：旧 leaf 改写为左半、分配新右兄弟存右半、维护双向 sibling 链；返回新插入 ref + 右半 lowKey + 新右兄弟页号。 */
    private LeafSplitResult splitNonRootLeaf(MiniTransaction mtr, BTreeIndex index, IndexPageHandle oldLeafHandle,
                                             LogicalRecord inserted, List<PageId> allocated) {
        RecordPage oldLeaf = oldLeafHandle.recordPage();
        PageId oldLeafId = oldLeafHandle.pageId();
        FilePageHeader oldLeafHeader = oldLeafHandle.fileHeader();
        List<LogicalRecord> records = sortedWithInserted(materializeLeafRecords(oldLeaf, index), inserted, index);
        SplitRows split = splitRows(records);

        PageId newLeafId = disk.allocatePage(mtr, index.leafSegment());
        allocated.add(newLeafId);
        RecordPage newLeaf = pageAccess.createIndexPage(mtr, newLeafId, index.indexId(), 0);
        IndexPageHandle newLeafHandle = pageAccess.openIndexPageHandle(mtr, newLeafId, PageLatchMode.EXCLUSIVE);

        oldLeaf.format(index.indexId(), 0);
        oldLeafHandle.writeSiblingLinks(oldLeafHeader.prevPageNo(), newLeafId.pageNo().value());
        newLeafHandle.writeSiblingLinks(oldLeafId.pageNo().value(), oldLeafHeader.nextPageNo());
        if (oldLeafHeader.nextPageNo() != FilePageHeader.FIL_NULL) {
            PageId rightSiblingId = PageId.of(oldLeafId.spaceId(), PageNo.of(oldLeafHeader.nextPageNo()));
            IndexPageHandle rightSibling = pageAccess.openIndexPageHandle(mtr, rightSiblingId, PageLatchMode.EXCLUSIVE);
            rightSibling.writeSiblingLinks(newLeafId.pageNo().value(), rightSibling.fileHeader().nextPageNo());
        }

        RecordRef insertedRef = insertAll(oldLeaf, oldLeafId, split.left(), index, inserted);
        RecordRef newInsertedRef = insertAll(newLeaf, newLeafId, split.right(), index, inserted);
        if (insertedRef == null) {
            insertedRef = newInsertedRef;
        }
        return new LeafSplitResult(insertedRef, lowKey(split.right(), index), newLeafId);
    }

    /**
     * 把 separator 插入 {@code path[depth]}（非叶页）。放得下直接插（树高不变）；放不下则对该非叶页做内部 split：
     * 它若是 root 则原地长高（level L→L+1），否则改写为左半 + 新右兄弟存右半，新 separator 继续向上递归。
     * 返回插入后调用方应使用的索引快照（仅原地长高会改 rootLevel）。
     */
    private BTreeIndex insertSeparator(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path,
                                       int depth, BTreeNodePointer separator, List<PageId> allocated) {
        IndexPageHandle parentHandle = path.get(depth);
        RecordPage parent = parentHandle.recordPage();
        if (pointerFits(parent, index, separator)) {
            insertPointer(parent, parentHandle.pageId(), index, separator);
            return index;
        }
        requireNonLeafSegment(index);
        List<BTreeNodePointer> combined = materializePointers(parent, index);
        combined.add(separator);
        combined.sort((a, b) -> keyComparator.compare(a.lowKey(), b.lowKey(), index.keyDef(), index.schema()));
        SplitPointers split = splitPointers(combined);
        if (depth == 0) {
            return growRootWithInternal(mtr, index, parentHandle, split, allocated);
        }
        PageId newSiblingId = splitNonRootInternal(mtr, index, parentHandle, split, allocated);
        BTreeNodePointer upSeparator = new BTreeNodePointer(split.right().get(0).lowKey(), newSiblingId);
        return insertSeparator(mtr, index, path, depth - 1, upSeparator, allocated);
    }

    /** root（非叶，level L）原地长高：分配两个 level-L 新子页存左右半 pointer，root 页号不变重建为 level L+1 两 pointer。 */
    private BTreeIndex growRootWithInternal(MiniTransaction mtr, BTreeIndex index, IndexPageHandle rootHandle,
                                            SplitPointers split, List<PageId> allocated) {
        int oldLevel = index.rootLevel();
        PageId leftId = disk.allocatePage(mtr, index.nonLeafSegment());
        allocated.add(leftId);
        RecordPage leftPage = pageAccess.createIndexPage(mtr, leftId, index.indexId(), oldLevel);
        PageId rightId = disk.allocatePage(mtr, index.nonLeafSegment());
        allocated.add(rightId);
        RecordPage rightPage = pageAccess.createIndexPage(mtr, rightId, index.indexId(), oldLevel);
        writePointers(leftPage, leftId, split.left(), index);
        writePointers(rightPage, rightId, split.right(), index);

        RecordPage root = rootHandle.recordPage();
        root.format(index.indexId(), oldLevel + 1);
        rootHandle.writeSiblingLinks(FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL);
        BTreeIndex after = index.withRootLevel(oldLevel + 1);
        insertPointer(root, index.rootPageId(), index, new BTreeNodePointer(split.left().get(0).lowKey(), leftId));
        insertPointer(root, index.rootPageId(), index, new BTreeNodePointer(split.right().get(0).lowKey(), rightId));
        return after;
    }

    /** 非 root 内部页 split：该页改写为左半 pointer、分配新右兄弟存右半 pointer（非叶页不参与 leaf sibling 链）；返回新右兄弟页号。 */
    private PageId splitNonRootInternal(MiniTransaction mtr, BTreeIndex index, IndexPageHandle nodeHandle,
                                        SplitPointers split, List<PageId> allocated) {
        RecordPage node = nodeHandle.recordPage();
        int level = node.header().level();
        PageId newSiblingId = disk.allocatePage(mtr, index.nonLeafSegment());
        allocated.add(newSiblingId);
        RecordPage newSibling = pageAccess.createIndexPage(mtr, newSiblingId, index.indexId(), level);
        node.format(index.indexId(), level);
        writePointers(node, nodeHandle.pageId(), split.left(), index);
        writePointers(newSibling, newSiblingId, split.right(), index);
        return newSiblingId;
    }

    // =====================================================================
    // 删除 / purge 后的欠载回收（0.12）：merge 同父相邻兄弟 + 摘 parent pointer + free page + 自底向上传播 + 原地 root shrink。
    // 设计 §8.3/§5.6。本切片不做 redistribute（fit 不下即留欠载/留退化页 → 0.12b）。
    //
    // min-key-pointer 约定（每个 node pointer 携子树 lowKey、split 右半整体保留）使 merge 对 leaf/内部页统一：
    // survivor 恒取相邻对的【左】者、victim 取【右】者，把 victim 条目并入 survivor、从 parent 摘 victim 的 pointer —— survivor
    // 的父 pointer key 不变，故【无 separator 下拉/更新】。叶页额外修 FIL prev/next；内部页不参与 FIL 链。
    //
    // 锁：悲观全路径 X（descend 已持）+ 额外取 sibling/远兄弟/child 的 X，均入 MTR memo，commit 统一释放（无 latch coupling，0.13）。
    // redo：复用物理 PAGE_BYTES/PAGE_INIT（move 条目 = inserter 写、摘 pointer = purger 写、root.format = PAGE_INIT），无 btree 专用逻辑 redo。
    // =====================================================================

    /** 一条记录被物理摘除后，survivor 容纳 victim 所需的每条目额外余量（目录槽 + 对齐安全），与 {@link #pointerFits} 的 +8 一致。 */
    private static final int MERGE_ENTRY_MARGIN = 8;

    /**
     * 欠载回收入口：从刚删过记录的 leaf（path 末项）向上评估 merge。返回操作后的索引快照（root shrink 会降 rootLevel，
     * root 页号稳定）与本次回收的页集合。无 segment 元数据（leaf-only 风格）无法 freePage，保守不回收。
     */
    private MergeOutcome reclaimAfterRemoval(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path) {
        if (index.leafSegment() == null || index.nonLeafSegment() == null) {
            return new MergeOutcome(index, List.of());
        }
        List<PageId> freed = new ArrayList<>();
        BTreeIndex after = considerMerge(mtr, index, path, path.size() - 1, freed);
        return new MergeOutcome(after, List.copyOf(freed));
    }

    /**
     * 评估 {@code path[depth]} 是否欠载并 merge，自底向上传播。{@code depth==0}（节点即 root）不 merge（root 无兄弟）。
     * 否则：欠载 → 经 parent 的 pointer 顺序选同父相邻兄弟（survivor=左/victim=右）→ fit 则 merge、摘 victim 的 parent
     * pointer、free victim 页 → 传播：parent 非 root 且欠载则递归到 {@code depth-1}；parent 是 root 且剩 1 pointer 则 shrinkRoot。
     *
     * <p>fit 不下（survivor 容不下 victim 全部条目）即返回原 index：留欠载页（甚至退化的 1-pointer 内部页），由 0.12b
     * 的 redistribute 再平衡，导航仍正确（chooseChild 对单 pointer 也返回该子）。
     */
    private BTreeIndex considerMerge(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path,
                                    int depth, List<PageId> freed) {
        if (depth == 0) {
            return index; // 节点即 root：root 不与兄弟 merge（root 允许欠载）
        }
        IndexPageHandle nodeHandle = path.get(depth);
        RecordPage node = nodeHandle.recordPage();
        if (!isUnderfull(node)) {
            return index;
        }
        IndexPageHandle parentHandle = path.get(depth - 1);
        RecordPage parent = parentHandle.recordPage();
        boolean parentIsRoot = (depth - 1 == 0);

        MergePair pair = chooseMergePair(mtr, index, parent, nodeHandle);
        if (pair == null) {
            // 无同父兄弟（parent 仅 1 child）：parent 是 root 即 shrink；非 root 退化页留待 0.12b。
            if (parentIsRoot && userRecordCount(parent) == 1) {
                return shrinkRoot(mtr, index, parentHandle, freed);
            }
            return index;
        }
        boolean leaf = node.header().level() == 0;
        if (!mergeFits(pair, leaf, index)) {
            // merge 放不下（相邻对合计 > 一页）→ redistribute 对半再平衡（0.12b）：双方脱离欠载、不删页/不传播/不改树高。
            redistribute(index, pair, leaf, parent, parentHandle.pageId());
            return index;
        }
        if (leaf) {
            mergeLeaf(mtr, index, pair);
        } else {
            mergeInternal(index, pair);
        }
        removePointerFromParent(parent, parentHandle.pageId(), index, pair.victimId());
        freed.add(pair.victimId());
        disk.freePage(mtr, leaf ? index.leafSegment() : index.nonLeafSegment(), pair.victimId());

        if (parentIsRoot) {
            // 摘 pointer 后 root 剩 1 pointer → 原地 shrink；否则 root 允许任意 ≥2 pointer（含欠载），收工。
            if (userRecordCount(parent) == 1) {
                return shrinkRoot(mtr, index, parentHandle, freed);
            }
            return index;
        }
        // parent 非 root：可能因少一 pointer 而欠载，向上递归（parent 在 path 中仍持 X，内容已更新）。
        return considerMerge(mtr, index, path, depth - 1, freed);
    }

    /**
     * MERGE_THRESHOLD≈50%：页内**活记录占用**低于页一半即视为欠载（空页恒欠载）。
     *
     * <p>关键：必须用「可回收空闲」= {@code freeSpace()（连续）+ garbage()（已删记录字节，purge 挂入 GarbageList 但不降 heapTop）}
     * 来判断填充率，而非仅连续 {@code freeSpace()}。否则刚 purge 出的空洞计入 garbage 而非连续空闲，会把实际很空的页误判为「满」。
     * 页头/目录开销相对 16KB 页可忽略，故按 {@code pageSize/2} 折算（与 InnoDB 基于 PAGE_GARBAGE 的精确阈值相比为教学简化）。
     */
    private boolean isUnderfull(RecordPage page) {
        int reclaimableFree = page.freeSpace() + page.header().garbage();
        return reclaimableFree * 2 > pageSize.bytes();
    }

    /** 非叶页的 node pointer 数 / 叶页的用户记录数（= header.nRecs，含 delete-marked）。shrink 用它判 root 是否只剩 1 child。 */
    private int userRecordCount(RecordPage page) {
        return page.header().nRecs();
    }

    /**
     * 经 parent 的 pointer 顺序选同父相邻 merge 对（保证同父，无需依赖 FIL 链）：node 在 parent 有序 pointer 中下标 i，
     * 有左兄弟取 (i-1, i)、否则取 (i, i+1)；survivor 恒为对的左者（其父 pointer key 不变）。仅 1 child 返回 null。
     * 兄弟页不在 descend path 上，故在此取 X 入 MTR memo。
     */
    private MergePair chooseMergePair(MiniTransaction mtr, BTreeIndex index, RecordPage parent,
                                      IndexPageHandle nodeHandle) {
        List<BTreeNodePointer> pointers = materializePointers(parent, index);
        PageId nodeId = nodeHandle.pageId();
        int i = -1;
        for (int k = 0; k < pointers.size(); k++) {
            if (pointers.get(k).childPageId().equals(nodeId)) {
                i = k;
                break;
            }
        }
        if (i < 0) {
            throw new BTreeStructureCorruptedException("child " + nodeId
                    + " not found in parent pointers for index " + index.indexId());
        }
        if (i > 0) {
            PageId leftId = pointers.get(i - 1).childPageId();
            IndexPageHandle left = pageAccess.openIndexPageHandle(mtr, leftId, PageLatchMode.EXCLUSIVE);
            return new MergePair(left, nodeHandle, leftId, nodeId);
        }
        if (i + 1 < pointers.size()) {
            PageId rightId = pointers.get(i + 1).childPageId();
            IndexPageHandle right = pageAccess.openIndexPageHandle(mtr, rightId, PageLatchMode.EXCLUSIVE);
            return new MergePair(nodeHandle, right, nodeId, rightId);
        }
        return null; // parent 仅 1 child，无同父兄弟
    }

    /**
     * survivor 是否容得下 victim 全部条目（leaf 行或内部 pointer），每条目计 encode+{@link #MERGE_ENTRY_MARGIN}。
     * 比较基准是 survivor 的**可回收空闲**（{@code freeSpace()+garbage()}）而非仅连续空闲：merge 执行时会先 reorganize
     * survivor 压实 garbage（见 {@link #mergeLeaf}/{@link #mergeInternal}），届时连续空闲 == 可回收空闲。
     */
    private boolean mergeFits(MergePair pair, boolean leaf, BTreeIndex index) {
        RecordPage survivor = pair.survivor().recordPage();
        RecordPage victim = pair.victim().recordPage();
        int required = 0;
        if (leaf) {
            for (LogicalRecord row : materializeLeafRecords(victim, index)) {
                required += recordEncoder.encode(row, index.schema()).length + MERGE_ENTRY_MARGIN;
            }
        } else {
            BTreeNodePointerSchema ps = BTreeNodePointerSchema.from(index);
            for (BTreeNodePointer p : materializePointers(victim, index)) {
                required += recordEncoder.encode(pointerCodec.toRecord(p, ps), ps.schema()).length + MERGE_ENTRY_MARGIN;
            }
        }
        return required <= survivor.freeSpace() + survivor.header().garbage();
    }

    /**
     * 叶页 merge：先 reorganize survivor 压实 garbage（把历次 purge 留下的空洞回收为连续空闲），再把 victim 全部行并入
     * survivor（victim 所有 key &gt; survivor，按页内顺序插入即追加保持有序），最后修 FIL 链：survivor 是相邻对左者，
     * 原 {@code survivor.next==victim}，改为 {@code victim.next}，并令远兄弟 {@code victim.next.prev=survivor}。
     */
    private void mergeLeaf(MiniTransaction mtr, BTreeIndex index, MergePair pair) {
        IndexPageHandle survivorHandle = pair.survivor();
        IndexPageHandle victimHandle = pair.victim();
        RecordPage survivor = survivorHandle.recordPage();
        RecordPage victim = victimHandle.recordPage();
        reorganizer.reorganize(survivor);
        for (LogicalRecord row : materializeLeafRecords(victim, index)) {
            inserter.insert(survivor, pair.survivorId(), row, index.keyDef(), index.schema());
        }
        long victimNext = victimHandle.fileHeader().nextPageNo();
        survivorHandle.writeSiblingLinks(survivorHandle.fileHeader().prevPageNo(), victimNext);
        if (victimNext != FilePageHeader.FIL_NULL) {
            PageId farId = PageId.of(pair.victimId().spaceId(), PageNo.of(victimNext));
            IndexPageHandle far = pageAccess.openIndexPageHandle(mtr, farId, PageLatchMode.EXCLUSIVE);
            far.writeSiblingLinks(pair.survivorId().pageNo().value(), far.fileHeader().nextPageNo());
        }
    }

    /** 内部页 merge：先 reorganize survivor 压实 garbage，再把 victim 全部 node pointer 并入 survivor（victim key 段整体 &gt; survivor，按序追加）；内部页不参与 FIL 链。 */
    private void mergeInternal(BTreeIndex index, MergePair pair) {
        RecordPage survivor = pair.survivor().recordPage();
        RecordPage victim = pair.victim().recordPage();
        reorganizer.reorganize(survivor);
        writePointers(survivor, pair.survivorId(), materializePointers(victim, index), index);
    }

    /**
     * redistribute 对半再平衡（0.12b，merge 放不下时的 fallback）：把相邻同父对（left=survivor / right=victim）的全部条目
     * 合并后**对半重分**到两页，使双方都脱离欠载。只更新 parent 中 **right 成员** 的 lowKey——min-key-pointer 约定下
     * left 的 lowKey = 左半首条 = 原 left 首条（不变），仅 right 的最小 key 变。**不删页、不动 FIL 链
     * （format 只碰页体不碰信封区）、不向上传播、不改树高**，故相对 merge 少改父页、避免页空洞。leaf 与内部页统一。
     *
     * <p>数据流：先在任何 format 前 materialize left+right 全部条目（左全 &lt; 右、各自有序）→ {@link #splitRows}/
     * {@link #splitPointers} 对半 → 两页各 {@code format} 后重灌左/右半 → 删 parent 中 right 旧 pointer（按 childId）
     * + 插 {@code (newRightLowKey → rightId)} 新 pointer（parent pointer 数不变，不会令 parent 欠载）。
     *
     * <p>进入前提（{@code mergeFits=false} 且选到相邻兄弟）保证两页合计 &gt; 一页 ⟹ 条目数 ≥ 2 且对半后每页 &lt; 一页、
     * &gt; 半页（脱离欠载）；防御：万一合计 &lt; 2 直接返回（留原状，不抛）。
     */
    private void redistribute(BTreeIndex index, MergePair pair, boolean leaf, RecordPage parent, PageId parentId) {
        RecordPage left = pair.survivor().recordPage();
        RecordPage right = pair.victim().recordPage();
        PageId leftId = pair.survivorId();
        PageId rightId = pair.victimId();
        SearchKey newRightLowKey;
        if (leaf) {
            List<LogicalRecord> all = new ArrayList<>(materializeLeafRecords(left, index));
            all.addAll(materializeLeafRecords(right, index));
            if (all.size() < 2) {
                return;
            }
            SplitRows s = splitRows(all);
            left.format(index.indexId(), 0);
            for (LogicalRecord row : s.left()) {
                inserter.insert(left, leftId, row, index.keyDef(), index.schema());
            }
            right.format(index.indexId(), 0);
            for (LogicalRecord row : s.right()) {
                inserter.insert(right, rightId, row, index.keyDef(), index.schema());
            }
            newRightLowKey = keyOf(s.right().get(0), index);
        } else {
            int level = left.header().level();
            List<BTreeNodePointer> all = new ArrayList<>(materializePointers(left, index));
            all.addAll(materializePointers(right, index));
            if (all.size() < 2) {
                return;
            }
            SplitPointers s = splitPointers(all);
            left.format(index.indexId(), level);
            writePointers(left, leftId, s.left(), index);
            right.format(index.indexId(), level);
            writePointers(right, rightId, s.right(), index);
            newRightLowKey = s.right().get(0).lowKey();
        }
        removePointerFromParent(parent, parentId, index, rightId);
        insertPointer(parent, parentId, index, new BTreeNodePointer(newRightLowKey, rightId));
    }

    /** 从 parent 摘除指向 {@code victimId} 的 node pointer（deleteMark + purge；node pointer 非系统记录可标记后摘）。 */
    private void removePointerFromParent(RecordPage parent, PageId parentId, BTreeIndex index, PageId victimId) {
        BTreeNodePointerSchema ps = BTreeNodePointerSchema.from(index);
        for (int off : parent.recordOffsetsInOrder()) {
            BTreeNodePointer p = pointerCodec.fromRecord(
                    new RecordCursor(parent, off, ps.schema(), registry).materialize(), ps);
            if (p.childPageId().equals(victimId)) {
                deleter.deleteMark(parent, off);
                purger.purge(parent, off);
                return;
            }
        }
        throw new BTreeStructureCorruptedException("victim pointer " + victimId
                + " not found in parent " + parentId + " for index " + index.indexId());
    }

    /**
     * 原地 root shrink：root 恰剩 1 个 node pointer 时，把唯一 child 内容吸收进 root（root 页号稳定）、树高 -1、free child。
     * child 是 leaf → root 退回 level-0 leaf（FIL 链清 NULL）；child 是内部页 → root 降到 child level 并吸收其 pointer，
     * 若吸收后 root 仍只剩 1 pointer 则级联再 shrink。返回降级后的索引快照。
     */
    private BTreeIndex shrinkRoot(MiniTransaction mtr, BTreeIndex index, IndexPageHandle rootHandle, List<PageId> freed) {
        RecordPage root = rootHandle.recordPage();
        BTreeNodePointerSchema ps = BTreeNodePointerSchema.from(index);
        int off = root.recordOffsetsInOrder().get(0);
        PageId childId = pointerCodec.fromRecord(
                new RecordCursor(root, off, ps.schema(), registry).materialize(), ps).childPageId();
        int childLevel = index.rootLevel() - 1;
        IndexPageHandle childHandle = pageAccess.openIndexPageHandle(mtr, childId, PageLatchMode.EXCLUSIVE);
        RecordPage child = childHandle.recordPage();

        if (childLevel == 0) {
            // 先物化 child 行，再 format(0) 清空 root 重灌（format 重置 nRecs/infimum/supremum）。
            List<LogicalRecord> rows = materializeLeafRecords(child, index);
            root.format(index.indexId(), 0);
            rootHandle.writeSiblingLinks(FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL);
            for (LogicalRecord row : rows) {
                inserter.insert(root, index.rootPageId(), row, index.keyDef(), index.schema());
            }
            freed.add(childId);
            disk.freePage(mtr, index.leafSegment(), childId);
            return index.withRootLevel(0);
        }
        List<BTreeNodePointer> pointers = materializePointers(child, index);
        root.format(index.indexId(), childLevel);
        rootHandle.writeSiblingLinks(FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL);
        writePointers(root, index.rootPageId(), pointers, index);
        freed.add(childId);
        disk.freePage(mtr, index.nonLeafSegment(), childId);
        BTreeIndex after = index.withRootLevel(childLevel);
        if (userRecordCount(root) == 1) {
            return shrinkRoot(mtr, after, rootHandle, freed);
        }
        return after;
    }

    /** 欠载回收结果：操作后索引快照（root shrink 会降 rootLevel）+ 回收页集合。 */
    private record MergeOutcome(BTreeIndex indexAfter, List<PageId> freedPages) {
    }

    /** merge 的相邻同父页对：survivor 恒为左者（保留、父 pointer key 不变），victim 为右者（并入 survivor 后被摘 pointer + free）。 */
    private record MergePair(IndexPageHandle survivor, IndexPageHandle victim, PageId survivorId, PageId victimId) {
    }

    /**
     * 打开 root 页。<b>导航高度权威（0.12）</b>：descend 始终按 root 页**实际 level** 下降（见 {@link #descendPath}），
     * 故这里只校验 {@code indexId}（拿错页/元数据损坏的硬判据），不再断言 {@code header.level()==index.rootLevel()}。
     *
     * <p>原因：root shrink 会降低实际 level，而批量 rollback/purge 持同一 {@link BTreeIndex} 快照跨多条记录
     * （每条独立 MTR）调用，shrink 后快照 {@code rootLevel} 必然陈旧——严格相等断言会把合法的 shrink 误判为
     * 「root changed」。页才是树高的权威；快照 level 仅作 {@code indexAfter} 观测/回填用。并发场景下的真正
     * 重定位/重启协议留 0.13/2.7，届时再以 latch coupling + 版本校验替代（{@code BTreeRootChangedException} 保留备用）。
     */
    private IndexPageHandle openRoot(MiniTransaction mtr, BTreeIndex index, PageLatchMode mode) {
        if (mtr == null || index == null || mode == null) {
            throw new DatabaseValidationException("btree mtr/index/mode must not be null");
        }
        IndexPageHandle root = pageAccess.openIndexPageHandle(mtr, index.rootPageId(), mode);
        IndexPageHeader header = root.recordPage().header();
        if (header.indexId() != index.indexId()) {
            throw new BTreeStructureCorruptedException("root page index id mismatch: page="
                    + header.indexId() + " expected=" + index.indexId());
        }
        return root;
    }

    /**
     * 从 root 逐层下降到 leaf（level 0），支持任意树高（解锁 height>1）。非叶层用 {@link #chooseChild}（node-pointer
     * schema 与层级无关）选子页并逐层校验 header.indexId；读用 SHARED、写用 EXCLUSIVE，全路径页 latch 由 MTR memo
     * 持有至 commit（悲观全路径，latch coupling / 乐观→悲观下降留 0.13）。返回定位到的 leaf 句柄/页视图/页号。
     */
    private LeafLocation findLeaf(MiniTransaction mtr, BTreeIndex index, SearchKey key, PageLatchMode mode) {
        List<IndexPageHandle> path = descendPath(mtr, index, key, mode);
        IndexPageHandle leaf = path.get(path.size() - 1);
        return new LeafLocation(leaf, leaf.recordPage(), leaf.pageId());
    }

    /**
     * 从 root 逐层下降到 leaf，返回完整路径（root 在 0、leaf 在末尾）。供 insert split 自底向上传播取祖先页；
     * 读路径只取末项即可（见 {@link #findLeaf}）。每层 latch 由 MTR memo 持有至 commit。
     */
    private List<IndexPageHandle> descendPath(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                              PageLatchMode mode) {
        if (key == null) {
            throw new DatabaseValidationException("btree descend key must not be null");
        }
        List<IndexPageHandle> path = new ArrayList<>();
        IndexPageHandle handle = openRoot(mtr, index, mode);
        path.add(handle);
        RecordPage page = handle.recordPage();
        PageId pageId = index.rootPageId();
        while (page.header().level() > 0) {
            PageId childId = chooseChild(page, index, key);
            handle = pageAccess.openIndexPageHandle(mtr, childId, mode);
            page = handle.recordPage();
            pageId = childId;
            if (page.header().indexId() != index.indexId()) {
                throw new BTreeStructureCorruptedException("child page index id mismatch at " + pageId
                        + ": page=" + page.header().indexId() + " expected=" + index.indexId());
            }
            path.add(handle);
        }
        validateLeafPage(page, index, pageId);
        return path;
    }

    /**
     * 乐观下降（0.13a，latch coupling / crab，设计 §10.2）：内部层 S-crab——持父页 S → latch 子页 → 释放父页 S；
     * 最后一层（leaf）取 X 返回。至多同时持「1 内部父 + 1 子」，越过的祖先立即释放，放开 root 处写并发。
     * 仅供 {@link #tryOptimisticInsert}/{@link #tryOptimisticDelete}；unsafe（split/merge）时调用方释放 leaf X 后改走
     * {@link #descendPath} 悲观全 X 重启。
     *
     * <p><b>root 即 leaf</b>（level 0，单页树）无 crab 收益：释放 S root 返回 {@code null}，交悲观（悲观重取 root X）。
     *
     * <p><b>并发正确性</b>（与悲观路径协作）：结构变更只走悲观全路径 X 且**持到 commit**（含 root X），故任一乐观下降者的
     * root S 获取会与并发结构变更者的 root X 冲突而串行——不会带陈旧指针穿过正在变更的子树。crab 的 hand-over-hand
     * 保证「被释放的祖先即使随后被并发 split/merge/free，本线程也已先 latch 到仍有效的子页」（re-parent 只移动指针不移动子页；
     * 移动 key 的 split/merge 必然冲突在该页 latch 上）。故本设计当前**并发正确**，代价是结构变更 tree-wide 串行于 root
     * （0.13b 用 SX latch 放松）。释放的内部页均为 S（从不写→非 touched），满足 {@link MiniTransaction#releaseLatch} 防护。
     */
    private IndexPageHandle descendOptimistic(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        if (key == null) {
            throw new DatabaseValidationException("btree descend key must not be null");
        }
        IndexPageHandle node = openRoot(mtr, index, PageLatchMode.SHARED);
        RecordPage page = node.recordPage();
        if (page.header().level() == 0) {
            // root 即 leaf：乐观无 crab 收益，放掉 S root 交悲观全 X（悲观会重取 root X，不构成 S→X 升级）。
            pageAccess.releaseHandle(mtr, node);
            return null;
        }
        while (page.header().level() > 0) {
            PageId childId = chooseChild(page, index, key);
            // 下一层是否 leaf：当前层 level==1 时子页即 leaf，取 X（将写）；更高层取 S（只导航）。
            boolean childIsLeaf = page.header().level() == 1;
            IndexPageHandle child = pageAccess.openIndexPageHandle(mtr, childId,
                    childIsLeaf ? PageLatchMode.EXCLUSIVE : PageLatchMode.SHARED);
            // crab：latch 到子页后立即释放父页（父页为内部 S，未写→非 touched，可提前释放）。
            pageAccess.releaseHandle(mtr, node);
            page = child.recordPage();
            if (page.header().indexId() != index.indexId()) {
                throw new BTreeStructureCorruptedException("child page index id mismatch at " + child.pageId()
                        + ": page=" + page.header().indexId() + " expected=" + index.indexId());
            }
            node = child;
        }
        validateLeafPage(page, index, node.pageId());
        return node;
    }

    /** {@link #findLeaf} 定位结果：leaf 句柄（scan 取 sibling 链/header）、leaf 页视图、leaf 物理页号。 */
    private record LeafLocation(IndexPageHandle handle, RecordPage page, PageId pageId) {
    }

    private void validateLeafPage(RecordPage page, BTreeIndex index, PageId pageId) {
        IndexPageHeader header = page.header();
        if (header.indexId() != index.indexId()) {
            throw new BTreeStructureCorruptedException("leaf page index id mismatch at " + pageId
                    + ": page=" + header.indexId() + " expected=" + index.indexId());
        }
        if (header.level() != 0) {
            throw new BTreeStructureCorruptedException("expected leaf level 0 at " + pageId
                    + " but got " + header.level());
        }
    }

    private Optional<BTreeLookupResult> lookupInLeaf(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key,
                                                     boolean includeDeleted) {
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return Optional.empty();
        }
        RecordCursor cursor = new RecordCursor(leaf, found.getAsInt(), index.schema(), registry);
        if (!includeDeleted && cursor.isDeleted()) {
            return Optional.empty();
        }
        return Optional.of(materialize(index, leafId, cursor));
    }

    private PageId chooseChild(RecordPage root, BTreeIndex index, SearchKey key) {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        int prev = search.findInsertPosition(root, key, pointerSchema.keyDef(), pointerSchema.schema());
        int pointerOffset = prev == root.infimumOffset() ? root.nextRecord(prev) : prev;
        if (pointerOffset == root.supremumOffset()) {
            throw new BTreeStructureCorruptedException("non-leaf root has no child pointer for index "
                    + index.indexId());
        }
        RecordCursor cursor = new RecordCursor(root, pointerOffset, pointerSchema.schema(), registry);
        return pointerCodec.fromRecord(cursor.materialize(), pointerSchema).childPageId();
    }

    private void ensureUniqueAbsent(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key) {
        if (!index.unique()) {
            return;
        }
        if (search.findEqual(leaf, key, index.keyDef(), index.schema()).isPresent()) {
            throw new BTreeDuplicateKeyException("duplicate key in unique btree index " + index.indexId()
                    + " at leaf " + leafId);
        }
    }

    private boolean scanLeafPage(RecordPage leaf, PageId leafId, BTreeIndex index, BTreeScanRange range,
                                 ScanAccumulator acc) {
        for (int off : leaf.recordOffsetsInOrder()) {
            RecordCursor cursor = new RecordCursor(leaf, off, index.schema(), registry);
            if (cursor.isDeleted()) {
                continue;
            }
            if (belowLower(cursor, index, range)) {
                continue;
            }
            if (aboveUpper(cursor, index, range)) {
                return true;
            }
            acc.add(materialize(index, leafId, cursor));
            if (acc.full()) {
                return true;
            }
        }
        return false;
    }

    private List<LogicalRecord> materializeLeafRecords(RecordPage page, BTreeIndex index) {
        List<LogicalRecord> records = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            records.add(new RecordCursor(page, off, index.schema(), registry).materialize());
        }
        return records;
    }

    private List<LogicalRecord> sortedWithInserted(List<LogicalRecord> records, LogicalRecord inserted,
                                                   BTreeIndex index) {
        List<LogicalRecord> all = new ArrayList<>(records.size() + 1);
        all.addAll(records);
        all.add(inserted);
        all.sort(Comparator.comparing(rec -> keyOf(rec, index),
                (a, b) -> keyComparator.compare(a, b, index.keyDef(), index.schema())));
        return all;
    }

    private SplitRows splitRows(List<LogicalRecord> records) {
        if (records.size() < 2) {
            throw new BTreeStructureCorruptedException("cannot split fewer than two records");
        }
        int mid = records.size() >>> 1;
        return new SplitRows(List.copyOf(records.subList(0, mid)), List.copyOf(records.subList(mid, records.size())));
    }

    private RecordRef insertAll(RecordPage page, PageId pageId, List<LogicalRecord> rows, BTreeIndex index,
                                LogicalRecord inserted) {
        RecordRef insertedRef = null;
        for (LogicalRecord row : rows) {
            RecordRef ref = inserter.insert(page, pageId, row, index.keyDef(), index.schema());
            if (row == inserted) {
                insertedRef = ref;
            }
        }
        return insertedRef;
    }

    /** 向非叶页插入一个 node pointer（root 或任意内部页；node-pointer schema 与层级无关，故只需 index 派生 schema）。 */
    private void insertPointer(RecordPage page, PageId pageId, BTreeIndex index, BTreeNodePointer pointer) {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        LogicalRecord record = pointerCodec.toRecord(pointer, pointerSchema);
        inserter.insert(page, pageId, record, pointerSchema.keyDef(), pointerSchema.schema());
    }

    /** 批量写入 node pointer 到已格式化的非叶页（调用方负责先 format/createIndexPage 到目标 level）。 */
    private void writePointers(RecordPage page, PageId pageId, List<BTreeNodePointer> pointers, BTreeIndex index) {
        for (BTreeNodePointer pointer : pointers) {
            insertPointer(page, pageId, index, pointer);
        }
    }

    /** 物化非叶页的全部 node pointer（按页内顺序），供内部 split 在内存对半切分。 */
    private List<BTreeNodePointer> materializePointers(RecordPage page, BTreeIndex index) {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        List<BTreeNodePointer> pointers = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            LogicalRecord record = new RecordCursor(page, off, pointerSchema.schema(), registry).materialize();
            pointers.add(pointerCodec.fromRecord(record, pointerSchema));
        }
        return pointers;
    }

    /** 非叶页是否还容得下一个新 node pointer（= 旧 ensureRootHasRoomForPointer 的判定，返回布尔而非抛出）。 */
    private boolean pointerFits(RecordPage page, BTreeIndex index, BTreeNodePointer pointer) {
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        LogicalRecord record = pointerCodec.toRecord(pointer, pointerSchema);
        int required = recordEncoder.encode(record, pointerSchema.schema()).length + 8;
        return required <= page.freeSpace();
    }

    /** 内部页 pointer 列表对半切（min-key-pointer 约定，右半保留全部，separator=右半首 pointer 的 lowKey）。 */
    private SplitPointers splitPointers(List<BTreeNodePointer> pointers) {
        if (pointers.size() < 2) {
            throw new BTreeStructureCorruptedException("cannot split fewer than two node pointers");
        }
        int mid = pointers.size() >>> 1;
        return new SplitPointers(List.copyOf(pointers.subList(0, mid)),
                List.copyOf(pointers.subList(mid, pointers.size())));
    }

    private void requireNonLeafSegment(BTreeIndex index) {
        if (index.nonLeafSegment() == null) {
            throw new BTreeUnsupportedStructureException(
                    "btree parent/root split requires non-leaf segment metadata for index " + index.indexId());
        }
    }

    private SearchKey lowKey(List<LogicalRecord> rows, BTreeIndex index) {
        if (rows.isEmpty()) {
            throw new BTreeStructureCorruptedException("split child must not be empty");
        }
        return keyOf(rows.get(0), index);
    }

    private BTreeLookupResult materialize(BTreeIndex index, PageId pageId, RecordCursor cursor) {
        return new BTreeLookupResult(index, cursor.recordRef(pageId, index.indexId()), cursor.materialize());
    }

    private boolean belowLower(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        int c = recordComparator.compare(cursor, range.lowerKey(), index.keyDef(), index.schema());
        return c < 0 || (c == 0 && !range.lowerInclusive());
    }

    private boolean aboveUpper(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        int c = recordComparator.compare(cursor, range.upperKey(), index.keyDef(), index.schema());
        return c > 0 || (c == 0 && !range.upperInclusive());
    }

    private SearchKey keyOf(LogicalRecord record, BTreeIndex index) {
        List<ColumnValue> values = new ArrayList<>(index.keyDef().parts().size());
        for (var part : index.keyDef().parts()) {
            values.add(record.columnValues().get(part.columnId().value()));
        }
        return new SearchKey(values);
    }

    private void requireLeafSegment(BTreeIndex index) {
        if (index.leafSegment() == null) {
            throw new BTreeUnsupportedStructureException("split-capable insert requires leaf segment metadata");
        }
    }

    /** 分裂后的左右记录集；两侧都必须非空。 */
    private record SplitRows(List<LogicalRecord> left, List<LogicalRecord> right) {
    }

    /** 非叶页内部 split 的左右 pointer 集；两侧都必须非空。 */
    private record SplitPointers(List<BTreeNodePointer> left, List<BTreeNodePointer> right) {
    }

    /** 非 root leaf split 结果：新插入记录页内短期 ref、右半最小 key（separator）、新右兄弟页号。 */
    private record LeafSplitResult(RecordRef insertedRef, SearchKey rightLowKey, PageId newRightId) {
    }

    /** scan 结果累积器，集中管理 limit 与不可变输出。 */
    private static final class ScanAccumulator {
        private final int limit;
        private final List<BTreeLookupResult> rows = new ArrayList<>();

        private ScanAccumulator(int limit) {
            this.limit = limit;
        }

        private void add(BTreeLookupResult row) {
            rows.add(row);
        }

        private boolean full() {
            return rows.size() >= limit;
        }

        private List<BTreeLookupResult> results() {
            return List.copyOf(rows);
        }
    }
}
