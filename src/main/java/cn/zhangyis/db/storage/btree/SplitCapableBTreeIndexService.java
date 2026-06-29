package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
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

    public SplitCapableBTreeIndexService(IndexPageAccess pageAccess, DiskSpaceManager disk,
                                         TypeCodecRegistry registry) {
        if (pageAccess == null || disk == null || registry == null) {
            throw new DatabaseValidationException("split btree pageAccess/disk/registry must not be null");
        }
        this.pageAccess = pageAccess;
        this.disk = disk;
        this.registry = registry;
        this.search = new RecordPageSearch(registry);
        this.inserter = new RecordPageInserter(registry);
        this.recordComparator = new RecordComparator(registry);
        this.keyComparator = new SearchKeyComparator(registry);
        this.pointerCodec = new BTreeNodePointerCodec();
        this.recordEncoder = new RecordEncoder(registry);
        this.deleter = new RecordPageDeleter();
        this.purger = new RecordPagePurger();
        this.updater = new RecordPageUpdater(registry);
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
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return deleteInLeaf(leaf.page(), leaf.pageId(), index, key, expectedTrxId, expectedRollPtr);
    }

    /**
     * 在已定位的 leaf 上执行所有权校验 + delete-mark + purge。{@code findEqual} 返回的是物理命中（含 delete-marked），
     * 因此这里要先按隐藏列确认归属、再判断是否已标记，避免对非本 undo 的行或已标记的行误操作。
     */
    private BTreeDeleteResult deleteInLeaf(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key,
                                          TransactionId expectedTrxId, RollPointer expectedRollPtr) {
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
        return BTreeDeleteResult.removed(index, List.of());
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
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return purgeInLeaf(leaf.page(), leaf.pageId(), index, key, expectedTrxId, expectedRollPtr);
    }

    /**
     * 在已定位 leaf 上执行 purge 严格校验：命中 + 已 delete-marked + 隐藏列(DB_TRX_ID/DB_ROLL_PTR)匹配才物理摘除；
     * 否则不改任何内容（stale 收敛）。与 {@link #deleteInLeaf} 的差别：未标记记录在此**不**会被 deleteMark。
     */
    private BTreeDeleteResult purgeInLeaf(RecordPage leaf, PageId leafId, BTreeIndex index, SearchKey key,
                                          TransactionId expectedTrxId, RollPointer expectedRollPtr) {
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
        return BTreeDeleteResult.removed(index, List.of());
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
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return replaceInLeaf(leaf.page(), leaf.pageId(), index, key, newRecord, expectedTrxId, expectedRollPtr);
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
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        return markInLeaf(leaf.page(), leaf.pageId(), index, key, deleted, newHidden, expectedTrxId, expectedRollPtr);
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
     * 插入逻辑记录。level=0 先尝试 root leaf 直接插入，页满则执行 root split；
     * level=1 先路由到目标 leaf，页满则执行 leaf split 并向 root 插入新 node pointer。
     */
    @Override
    public BTreeInsertResult insert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record) {
        if (record == null) {
            throw new DatabaseValidationException("btree insert record must not be null");
        }
        SearchKey key = keyOf(record, index);
        // 任意树高：X 下降记录 path（root→leaf），叶页放得下直接插，溢出则自底向上 split 传播（必要时原地长高）。
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
