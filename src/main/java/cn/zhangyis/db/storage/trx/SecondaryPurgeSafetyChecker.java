package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SearchKeyComparator;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * secondary purge 的聚簇版本链证明器。它不修改索引：从当前聚簇版本向后逐跳构造 old image，检查目标 undo 之前
 * 的较新 live 版本是否仍映射到待删 physical identity；到达目标 pointer 即停止，不把目标 old image 当作保留理由。
 * 每次 clustered/undo 读取使用独立短 MTR，调用方必须在外层持有同一 table+cluster-key row guard。
 */
public final class SecondaryPurgeSafetyChecker {

    /** clustered/undo 独立短读 MTR 的唯一工厂；任何一次读取返回前都释放 latch/fix。 */
    private final MiniTransactionManager mtrManager;
    /** 包含 delete-marked 当前聚簇版本的 B+Tree 点读入口。 */
    private final SplitCapableBTreeIndexService btree;
    /** 按 RollPointer 读取历史 undo record 的稳定访问端口。 */
    private final UndoLogSegmentAccess undoAccess;
    /** 所有 target roll pointer 所属的系统 undo 表空间。 */
    private final SpaceId undoSpace;
    /** 版本链最大跳数；防止损坏链在 purge 线程中无界遍历。 */
    private final int maxVersionHops;
    /** 与二级树共享 type/prefix/collation/order 语义的完整 physical key 比较器。 */
    private final SearchKeyComparator keyComparator;

    /**
     * 创建与 StorageEngine 共享页访问、undo 表空间和 comparator 语义的安全检查器。
     *
     * @param mtrManager    clustered 与 undo 每跳短读 MTR 的工厂。
     * @param btree         读取当前聚簇物理版本的 B+Tree 服务。
     * @param undoAccess    按 target/历史 roll pointer 读取 undo record 的访问端口。
     * @param undoSpace     target pointer 所属的系统 undo 表空间 identity。
     * @param maxVersionHops 单次证明允许遍历的正版本跳数上限，超限按链损坏 fail-closed。
     * @param registry      与 record/B+Tree 使用相同 codec、prefix 与 collation 配置的注册表。
     * @throws DatabaseValidationException 任一协作者缺失或跳数上限非正时抛出。
     */
    public SecondaryPurgeSafetyChecker(MiniTransactionManager mtrManager,
                                       SplitCapableBTreeIndexService btree,
                                       UndoLogSegmentAccess undoAccess,
                                       SpaceId undoSpace,
                                       int maxVersionHops,
                                       TypeCodecRegistry registry) {
        if (mtrManager == null || btree == null || undoAccess == null || undoSpace == null || registry == null
                || maxVersionHops <= 0) {
            throw new DatabaseValidationException("secondary purge safety checker collaborators are invalid");
        }
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.undoAccess = undoAccess;
        this.undoSpace = undoSpace;
        this.maxVersionHops = maxVersionHops;
        this.keyComparator = new SearchKeyComparator(registry);
    }

    /**
     * 判断 target undo 的旧 secondary entry 是否仍被较新 live 聚簇版本需要。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 UPDATE/DELETE target、exact-version metadata 与 pointer，使用 undo old image重建目标 physical key。</li>
     *     <li>短读当前聚簇物理版本并释放 clustered latch；缺失或隐藏列缺失均不能证明安全。</li>
     *     <li>若当前版本 pointer 已等于 target，直接判 REMOVE；否则仅当当前版本 live 且映射目标 key 时判 RETAIN。</li>
     *     <li>按当前 DB_ROLL_PTR 独立短读 undo，校验 owner/table/index/cluster key，再构造上一 live 版本。</li>
     *     <li>重复 3、4 直到到达 target；环、INSERT/NULL 终点或跳数上限前未到达均 fail-closed。</li>
     * </ol>
     *
     * @param targetUndo    当前 history task 对应的 UPDATE_ROW 或 DELETE_MARK undo。
     * @param targetPointer 该 undo record 自身 pointer；到达它时停止，不读取其 old image。
     * @param table         exact-version 表级索引聚合。
     * @param secondary     mutation 指向的 exact-version 二级 metadata。
     * @return REMOVE 或 RETAIN；链不完整时不返回猜测结果。
     * @throws DatabaseValidationException target 类型、pointer 或 metadata 归属不合法时抛出，且不会访问页面。
     * @throws SecondaryPurgeVersionChainException 当前行/隐藏列/undo old image 缺失、链成环/超限/不到 target，
     *                                             或 owner/table/index/key 身份错配时抛出；调用方必须保留 history head。
     */
    public SecondaryPurgeDecision evaluate(UndoRecord targetUndo, RollPointer targetPointer,
                                           TableIndexMetadata table, SecondaryIndexMetadata secondary) {
        // 1. target old image 是待物理删除 identity 的唯一来源；INSERT/history 或 metadata 错配不允许进入遍历。
        validate(targetUndo, targetPointer, table, secondary);
        LogicalRecord targetOld = oldRecord(targetUndo, table);
        SearchKey targetPhysicalKey = secondary.layout().physicalKey(
                secondary.layout().toEntry(targetOld, false));
        SearchKey clusterKey = new SearchKey(targetUndo.clusterKey());

        // 2. 当前物理版本先完全物化；后续 undo 读取不会与 clustered latch 重叠。
        LogicalRecord version = readCurrent(table, clusterKey).orElseThrow(() ->
                new SecondaryPurgeVersionChainException("clustered row is absent before secondary purge reaches target: table="
                        + table.tableId() + " key=" + clusterKey + " target=" + targetPointer));
        Set<RollPointer> visited = new HashSet<>();
        int hops = 0;
        while (true) {
            HiddenColumns hidden = version.hiddenColumns();
            if (hidden == null) {
                throw new SecondaryPurgeVersionChainException(
                        "clustered version missing hidden columns during secondary purge");
            }
            RollPointer pointer = hidden.dbRollPtr();

            // 3. pointer==target 表示当前版本正是 target 前向操作结果；DELETE 的 marked 版本和 UPDATE 新 key 都不保留旧 entry。
            if (pointer.equals(targetPointer)) {
                return SecondaryPurgeDecision.REMOVE;
            }
            if (!version.deleted() && mapsToTarget(version, secondary, targetPhysicalKey)) {
                return SecondaryPurgeDecision.RETAIN;
            }

            // 4、5. target 之前遇到终点/环即说明 history task 与当前版本链脱节，必须保留 history head。
            if (pointer.isNull() || pointer.insert() || !visited.add(pointer) || ++hops > maxVersionHops) {
                throw new SecondaryPurgeVersionChainException(
                        "secondary purge version chain cannot reach target " + targetPointer
                                + " from table=" + table.tableId() + " key=" + clusterKey);
            }
            UndoRecord undo = readUndo(table, pointer);
            if (!undo.transactionId().equals(hidden.dbTrxId())
                    || undo.tableId() != targetUndo.tableId()
                    || undo.indexId() != targetUndo.indexId()
                    || !undo.clusterKey().equals(targetUndo.clusterKey())
                    || undo.type() != UndoRecordType.UPDATE_ROW && undo.type() != UndoRecordType.DELETE_MARK) {
                throw new SecondaryPurgeVersionChainException(
                        "secondary purge version-chain identity mismatch at " + pointer);
            }
            version = oldRecord(undo, table);
        }
    }

    /**
     * 判断一个较新 live 完整行是否仍按 exact layout 映射到待删除的 secondary physical identity。
     *
     * @param row       当前或历史重建出的完整聚簇行，必须匹配 layout 表版本。
     * @param secondary 目标二级 exact-version metadata。
     * @param target    从 target undo old image 投影出的完整 physical key。
     * @return comparator 按类型、prefix、collation、方向判等时返回 {@code true}，此时本轮 purge 必须 RETAIN。
     * @throws DatabaseValidationException 行/layout/key 形状或类型不匹配时抛出。
     */
    private boolean mapsToTarget(LogicalRecord row, SecondaryIndexMetadata secondary, SearchKey target) {
        SearchKey candidate = secondary.layout().physicalKey(secondary.layout().toEntry(row, false));
        return keyComparator.compare(candidate, target, secondary.index().keyDef(),
                secondary.index().schema()) == 0;
    }

    /**
     * 独立短读当前聚簇物理版本，包含 delete-marked 状态；返回前提交读 MTR 并释放 clustered latch/fix。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建独立只读 MTR并按完整聚簇主键执行 including-deleted 点查。</li>
     *     <li>只保留完全物化 LogicalRecord，提交 MTR释放 clustered 页资源后返回。</li>
     *     <li>失败时终止仍 ACTIVE 的 MTR，释放异常附加到原始失败。</li>
     * </ol>
     *
     * @param table      提供目标聚簇 descriptor 的 exact-version 聚合。
     * @param clusterKey target undo 固定前缀中的完整物化聚簇主键。
     * @return 当前物理记录的完全物化快照；记录已被提前物理移除时为空。
     * @throws RuntimeException 页读取、记录解码或 MTR 提交失败时抛出，释放失败作为 suppressed 保留。
     */
    private Optional<LogicalRecord> readCurrent(TableIndexMetadata table, SearchKey clusterKey) {
        // 1. 当前物理版本独占一个短读 MTR，不与任何 undo read 重叠。
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            // 2. including-deleted 保留 DELETE_MARK 当前版本；返回前提交并释放 clustered latch/fix。
            Optional<LogicalRecord> result = btree.lookupIncludingDeleted(
                    read, table.clusteredIndex(), clusterKey).map(BTreeLookupResult::record);
            mtrManager.commit(read);
            return result;
        } catch (RuntimeException error) {
            // 3. 释放失败不得覆盖页读取/提交首因。
            rollback(read, error);
            throw error;
        }
    }

    /**
     * 按 roll pointer 独立短读一条历史 undo；返回前释放 undo first/record page latch 与 buffer fix。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建独立只读 MTR，以 exact-version 聚簇 key/schema 解码 pointer 指向的 undo。</li>
     *     <li>提交 MTR释放 undo 页资源后返回完全物化 record。</li>
     *     <li>失败时终止仍 ACTIVE 的 MTR并保留原始异常。</li>
     * </ol>
     *
     * @param table   提供聚簇 key definition/schema 的 exact-version 聚合，用于解码 old image。
     * @param pointer 当前版本 DB_ROLL_PTR 指向的非空、非 INSERT undo 地址。
     * @return 解码并完成格式校验的 UPDATE_ROW 或 DELETE_MARK undo record。
     * @throws RuntimeException undo 页读取、格式校验或 MTR 提交失败时抛出，释放失败作为 suppressed 保留。
     */
    private UndoRecord readUndo(TableIndexMetadata table, RollPointer pointer) {
        // 1. 每个版本跳转独占一个短读 MTR，避免沿链累计固定 undo 页。
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            UndoRecord record = undoAccess.readRecordByRollPointer(read, undoSpace, pointer,
                    table.clusteredIndex().keyDef(), table.clusteredIndex().schema());
            // 2. record 已完全物化；提交释放 first/record 页后才交给版本重建。
            mtrManager.commit(read);
            return record;
        } catch (RuntimeException error) {
            // 3. 只终止 ACTIVE MTR，释放失败作为 suppressed 保留。
            rollback(read, error);
            throw error;
        }
    }

    /**
     * 从 UPDATE/DELETE undo 的全量 old image 构造上一 live 聚簇版本；delete 状态按前向操作前语义恢复为 false。
     *
     * @param undo  必须携带 oldColumnValues 与 oldHiddenColumns 的 UPDATE_ROW/DELETE_MARK 记录。
     * @param table 提供 old image schema version 的 exact-version 聚合。
     * @return 可继续沿 DB_ROLL_PTR 向后遍历的上一聚簇版本值对象。
     * @throws SecondaryPurgeVersionChainException old image 不完整时抛出，禁止用猜测版本证明可删。
     */
    private static LogicalRecord oldRecord(UndoRecord undo, TableIndexMetadata table) {
        if (undo.oldColumnValues() == null || undo.oldHiddenColumns() == null) {
            throw new SecondaryPurgeVersionChainException("secondary purge undo has no old image: " + undo.type());
        }
        return new LogicalRecord(table.schemaVersion(), undo.oldColumnValues(), false,
                RecordType.CONVENTIONAL, undo.oldHiddenColumns());
    }

    /**
     * 在任何页访问前校验 target undo、pointer 与 exact-version metadata 的身份不变量。
     *
     * @param undo      当前 history task 的 UPDATE_ROW/DELETE_MARK undo。
     * @param pointer   该 undo record 自身的稳定 roll pointer，不能为 {@code null}。
     * @param table     table id 与聚簇 index id 必须精确匹配 undo 固定前缀的表级聚合。
     * @param secondary 必须是 {@code table} 中按 index id 精确解析出的二级 metadata。
     * @throws DatabaseValidationException 字段缺失、传入 INSERT undo 或任一 identity/metadata 归属错配时抛出。
     */
    private static void validate(UndoRecord undo, RollPointer pointer, TableIndexMetadata table,
                                 SecondaryIndexMetadata secondary) {
        if (undo == null || pointer == null || table == null || secondary == null
                || undo.type() == UndoRecordType.INSERT_ROW
                || table.tableId() != undo.tableId()
                || table.clusteredIndex().indexId() != undo.indexId()
                || !table.requireSecondary(secondary.index().indexId()).equals(secondary)) {
            throw new DatabaseValidationException("secondary purge safety target/metadata is invalid");
        }
    }

    /**
     * 只终止仍 ACTIVE 的短读 MTR；若资源释放也失败，则把释放异常附加到原始失败，避免覆盖首因。
     *
     * @param read     当前短读 MTR；已提交/已终止时不重复处理。
     * @param original 页读取、解码或提交阶段抛出的原始运行时异常。
     */
    private void rollback(MiniTransaction read, RuntimeException original) {
        if (read.state() != MiniTransactionState.ACTIVE) {
            return;
        }
        try {
            mtrManager.rollbackUncommitted(read);
        } catch (RuntimeException releaseFailure) {
            original.addSuppressed(releaseFailure);
        }
    }
}
