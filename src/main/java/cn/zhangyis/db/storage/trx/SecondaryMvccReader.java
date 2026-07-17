package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SearchKeyComparator;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

import java.util.List;
import java.util.Optional;

/**
 * logical unique 二级索引的一致性点读服务。secondary entry 不含 DB_TRX_ID/DB_ROLL_PTR，因此本类只把它当作候选：
 * 先在独立短 MTR 中物化并释放二级页，再逐候选调用 {@link MvccReader} 回到聚簇版本链，最后从可见完整行重算
 * logical key。任何路径都不会同时持有 secondary、clustered 或 undo page latch。
 */
public final class SecondaryMvccReader {

    /** 单次 unique 点查允许物化的最大历史候选；多一个用于显式发现异常膨胀，避免损坏树导致无界内存。 */
    private static final int MAX_UNIQUE_CANDIDATES = 1024;

    /** secondary scan 短 MTR 来源。 */
    private final MiniTransactionManager mtrManager;
    /** 包含 delete-marked 候选的二级 prefix scan 入口。 */
    private final SplitCapableBTreeIndexService btree;
    /** 聚簇当前版本与 undo 版本链可见性判定入口。 */
    private final MvccReader clusteredReader;
    /** 与 B+Tree 共用的类型、prefix、collation 与方向比较器。 */
    private final SearchKeyComparator keyComparator;

    /**
     * 创建回表 MVCC 读服务；全部协作者必须来自同一 StorageEngine 组合根。
     *
     * @param mtrManager     secondary 短读 MTR 工厂。
     * @param btree          二级 prefix scan 服务。
     * @param clusteredReader 聚簇 MVCC 版本选择服务。
     * @param registry       与 record/B+Tree 相同的稳定 codec/collation 注册表。
     * @throws DatabaseValidationException 任一协作者为空时抛出。
     */
    public SecondaryMvccReader(MiniTransactionManager mtrManager,
                               SplitCapableBTreeIndexService btree,
                               MvccReader clusteredReader,
                               TypeCodecRegistry registry) {
        if (mtrManager == null || btree == null || clusteredReader == null || registry == null) {
            throw new DatabaseValidationException("secondary MVCC reader collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.clusteredReader = clusteredReader;
        this.keyComparator = new SearchKeyComparator(registry);
    }

    /**
     * 按完整 logical unique key 返回对给定 ReadView 可见的完整聚簇行。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 table/secondary exact-version 归属与 logical key 形状；SQL 等值 NULL 直接返回空，不访问页。</li>
     *     <li>在单个只读 MTR 中扫描包含 delete-marked 的二级 prefix 候选，提交后释放全部 secondary fix/latch。</li>
     *     <li>逐候选提取完整聚簇主键，调用聚簇 {@link MvccReader}；它分别使用 clustered/undo 短 MTR 选择可见版本。</li>
     *     <li>从可见完整行重算 logical key，并用相同类型、prefix、collation、方向规则复核查询谓词。</li>
     *     <li>按聚簇 key 语义去重；若同一非 NULL unique key 得到两个不同可见行，fail-closed 报告唯一性损坏。</li>
     * </ol>
     *
     * @param readView  覆盖 secondary scan、聚簇版本遍历以及调用方后续 LOB hydration/投影的存活快照。
     * @param table     exact-version 全索引聚合，提供唯一聚簇 descriptor。
     * @param secondary 属于 {@code table} 的 logical unique 二级 metadata。
     * @param logicalKey 覆盖全部声明 logical parts 的等值 key；含 NULL 时按 SQL 等值语义返回空。
     * @return 唯一可见完整聚簇行；无候选或候选对该快照/谓词不可见时为空。
     * @throws DatabaseValidationException 参数、table/index 归属、unique 属性或 key part 数无效时抛出。
     * @throws SecondaryUniqueVisibilityCorruptionException 候选异常膨胀或不同聚簇主键同时可见时抛出。
     */
    public Optional<LogicalRecord> readUnique(ReadView readView, TableIndexMetadata table,
                                              SecondaryIndexMetadata secondary, SearchKey logicalKey) {
        // 1. exact-version metadata 与输入形状先于页访问校验；NULL 等值不会误走“NULL-safe equality”。
        validate(readView, table, secondary, logicalKey);
        if (logicalKey.values().stream().anyMatch(ColumnValue.NullValue.class::isInstance)) {
            return Optional.empty();
        }

        // 2. 候选只保存已物化值对象；read MTR 提交后才开始任何聚簇/undo 访问。
        List<BTreeLookupResult> candidates = scanCandidates(secondary, logicalKey);
        if (candidates.size() > MAX_UNIQUE_CANDIDATES) {
            throw new SecondaryUniqueVisibilityCorruptionException(
                    "secondary unique candidate limit exceeded: table=" + table.tableId()
                            + " index=" + secondary.index().indexId() + " key=" + logicalKey
                            + " limit=" + MAX_UNIQUE_CANDIDATES);
        }

        IndexKeyDef logicalKeyDef = logicalKeyDefinition(secondary);
        Optional<LogicalRecord> visible = Optional.empty();
        SearchKey visibleClusterKey = null;
        for (BTreeLookupResult candidate : candidates) {
            // 3. 每个回表调用自身先结束 clustered MTR，再逐跳读取 undo；不继承第 2 阶段 secondary latch。
            SearchKey clusterKey = secondary.layout().clusterKey(candidate.record());
            Optional<LogicalRecord> row = clusteredReader.read(
                    readView, table.clusteredIndex(), clusterKey);
            if (row.isEmpty()) {
                continue;
            }

            // 4. secondary entry 可能属于不可见的新版本或已标删旧版本，只有可见完整行重投影后仍匹配才算命中。
            SearchKey visibleLogicalKey = secondary.layout().logicalKey(
                    secondary.layout().toEntry(row.orElseThrow(), false));
            if (keyComparator.compare(visibleLogicalKey, logicalKey, logicalKeyDef,
                    secondary.index().schema()) != 0) {
                continue;
            }

            // 5. 同一聚簇 identity 的重复物化保守去重；不同 identity 同时可见违反 logical unique 不变量。
            if (visible.isEmpty()) {
                visible = row;
                visibleClusterKey = clusterKey;
            } else if (keyComparator.compare(visibleClusterKey, clusterKey,
                    table.clusteredIndex().keyDef(), table.clusteredIndex().schema()) != 0) {
                throw new SecondaryUniqueVisibilityCorruptionException(
                        "multiple visible rows for secondary unique key: table=" + table.tableId()
                                + " index=" + secondary.index().indexId() + " key=" + logicalKey
                                + " firstClusterKey=" + visibleClusterKey + " secondClusterKey=" + clusterKey);
            }
        }
        return visible;
    }

    /**
     * 在独立短 MTR 中物化包括 delete-marked 的 logical-prefix 候选，并在返回前释放全部 secondary 页资源。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建只读 MTR并把候选上限固定为“合法上限 + 1”，用于发现异常膨胀。</li>
     *     <li>扫描并物化候选，不把 page handle、cursor 或 latch 暴露给回表阶段。</li>
     *     <li>提交 MTR释放全部 secondary 资源后返回列表；失败时仅终止仍 ACTIVE 的 MTR。</li>
     * </ol>
     *
     * @param secondary exact-version logical unique 二级索引 metadata，提供物理树与紧凑 layout。
     * @param logicalKey 覆盖全部声明 logical parts 的非 NULL 等值键。
     * @return 最多 {@code MAX_UNIQUE_CANDIDATES + 1} 个不携带页句柄的候选快照；顺序保持 B+Tree 物理 key 顺序。
     * @throws RuntimeException 页读取、记录解码、MTR 提交或资源释放失败时抛出；原始异常会保留。
     */
    private List<BTreeLookupResult> scanCandidates(SecondaryIndexMetadata secondary, SearchKey logicalKey) {
        // 1. 候选扫描拥有独立只读 MTR，与后续 clustered/undo 版本遍历完全分离。
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            // 2. 多取一个候选让调用方显式发现 unique tree 异常膨胀，且返回值只含物化记录。
            List<BTreeLookupResult> candidates = btree.scanSecondaryPrefixIncludingDeleted(
                    read, secondary, logicalKey, MAX_UNIQUE_CANDIDATES + 1);
            // 3. 提交释放所有 secondary fix/latch 后，调用方才允许开始回表。
            mtrManager.commit(read);
            return candidates;
        } catch (RuntimeException error) {
            if (read.state() == MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(read);
            }
            throw error;
        }
    }

    /**
     * 在任何页访问前校验 logical unique reader 的 ReadView、exact-version table/index 归属与完整 key 形状。
     *
     * @param readView  调用方仍持有的可见性快照，不能为 {@code null}。
     * @param table     包含目标聚簇与二级索引的 exact-version 聚合。
     * @param secondary 必须由 {@code table} 按稳定 id 精确解析出的 logical unique metadata。
     * @param logicalKey part 数必须等于 layout 声明 logical key part 数的搜索键。
     * @throws DatabaseValidationException 字段缺失、metadata 版本/归属错配、索引非 logical unique 或 key 形状错误时抛出。
     */
    private static void validate(ReadView readView, TableIndexMetadata table,
                                 SecondaryIndexMetadata secondary, SearchKey logicalKey) {
        if (readView == null || table == null || secondary == null || logicalKey == null) {
            throw new DatabaseValidationException("secondary MVCC read args must not be null");
        }
        SecondaryIndexMetadata mapped = table.requireSecondary(secondary.index().indexId());
        if (!mapped.equals(secondary) || !secondary.logicalUnique()) {
            throw new DatabaseValidationException(
                    "secondary MVCC unique read requires exact table-owned logical unique metadata");
        }
        if (logicalKey.size() != secondary.layout().logicalKeyPartCount()) {
            throw new DatabaseValidationException("secondary MVCC logical key part count mismatch: expected="
                    + secondary.layout().logicalKeyPartCount() + " actual=" + logicalKey.size());
        }
    }

    /**
     * 从完整 physical key definition 截取声明的 logical parts，供回表后的谓词复核复用同一 prefix/order 语义。
     *
     * @param secondary exact-version 二级 metadata；其 key definition 前缀必须与 layout logical part 数一致。
     * @return 复用相同 index id、仅包含 logical key parts 的不可变比较定义。
     * @throws DatabaseValidationException key definition 无 logical part 或 layout/descriptor 错配时由领域对象拒绝。
     */
    private static IndexKeyDef logicalKeyDefinition(SecondaryIndexMetadata secondary) {
        int parts = secondary.layout().logicalKeyPartCount();
        return new IndexKeyDef(secondary.index().indexId(),
                secondary.index().keyDef().parts().subList(0, parts));
    }
}
