package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadMode;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadService;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SearchKeyComparator;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SecondaryLogicalKeyLockTokenFactory;
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
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.trx.lock.LockWaitTimeoutException;
import cn.zhangyis.db.storage.trx.lock.SecondaryLogicalKeyLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * non-unique secondary logical-prefix locking current read。它先申请稳定 predicate S/X，再用短 MTR 物化
 * secondary 候选，最后逐行取得聚簇 record S/X 并重算谓词；任何事务锁等待都不持 page latch/buffer fix。
 */
public final class SecondaryCurrentReadService {

    /** 与一致性 range 相同的批量安全上限；多取一个只用于检测溢出，不能静默截断 SQL 结果。 */
    private static final int MAX_RANGE_CANDIDATES = 4096;
    /** 聚簇 point current-read 等待后最多重定位次数。 */
    private static final int MAX_RELOCATION_RETRIES = 3;

    /** 为 locking read 分配真实事务 id，并验证 ACTIVE/read-only 状态。 */
    private final TransactionManager transactionManager;
    /** secondary candidate 短 MTR 工厂。 */
    private final MiniTransactionManager mtrManager;
    /** including-deleted logical-prefix scan。
     *
     * 本对象持有的 {@code btree} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final SplitCapableBTreeIndexService btree;
    /** 聚簇 current-read record lock 与重定位入口。 */
    private final BTreeCurrentReadService currentRead;
    /** logical-prefix S/X 与聚簇 record lock 共用的事务锁目录。 */
    private final LockManager lockManager;
    /** logical key 的 type/prefix/collation 稳定 token 工厂。 */
    private final SecondaryLogicalKeyLockTokenFactory tokenFactory;
    /** visible/current row 谓词与聚簇 identity 比较器。 */
    private final SearchKeyComparator keyComparator;

    /**
     * 创建 secondary current-read 服务；所有协作者必须来自同一 StorageEngine 组合根。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @throws DatabaseValidationException 任一协作者为空时抛出。
     * @param transactionManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param mtrManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param currentRead 由组合根提供的 {@code BTreeCurrentReadService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param lockManager 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     */
    public SecondaryCurrentReadService(TransactionManager transactionManager,
                                       MiniTransactionManager mtrManager,
                                       SplitCapableBTreeIndexService btree,
                                       BTreeCurrentReadService currentRead,
                                       LockManager lockManager,
                                       TypeCodecRegistry registry) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (transactionManager == null || mtrManager == null || btree == null || currentRead == null
                || lockManager == null || registry == null) {
            throw new DatabaseValidationException("secondary current-read collaborators must not be null");
        }
        this.transactionManager = transactionManager;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.mtrManager = mtrManager;
        this.btree = btree;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.currentRead = currentRead;
        this.lockManager = lockManager;
        this.tokenFactory = new SecondaryLogicalKeyLockTokenFactory(registry);
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.keyComparator = new SearchKeyComparator(registry);
    }

    /**
     * 对完整 non-unique logical key 执行共享或排他锁定读，返回当前版本的完整聚簇行。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务、exact metadata、non-unique key 与模式；NULL equality 直接返回空。</li>
     *     <li>分配真实 write id，并在不持 page 资源时申请 normalized logical-prefix REC_S/REC_X。</li>
     *     <li>授锁后用独立短 MTR 重扫 including-deleted candidates，提交后释放全部 secondary 页资源。</li>
     *     <li>逐候选取得聚簇 REC_S/REC_X；point 服务在等待前释放页资源并在授锁后重定位当前记录。</li>
     *     <li>从当前完整行重算 logical key，过滤 marked history 假命中并按聚簇 identity 去重。</li>
     * </ol>
     *
     * @param transaction ACTIVE 且非 read-only 的事务；锁保持到其 commit/rollback。
     * @param table       exact-version 全索引聚合。
     * @param secondary   table 拥有的普通 non-unique secondary。
     * @param logicalKey  完整 logical equality key。
     * @param mode        FOR_SHARE 或 FOR_UPDATE。
     * @param timeout     整个 predicate/clustered 锁等待共享的正绝对预算。
     * @return 不可变、按 secondary physical suffix 稳定排列且聚簇 identity 去重的当前行。
     * @throws DatabaseValidationException 参数、metadata、模式或 key 形状错误时抛出。
     * @throws LockWaitTimeoutException 共享预算耗尽或任一锁等待超时时抛出。
     * @throws SecondaryRangeCapacityException candidate 超过批量安全上限时抛出。
     */
    public List<LogicalRecord> readRange(Transaction transaction, TableIndexMetadata table,
                                         SecondaryIndexMetadata secondary, SearchKey logicalKey,
                                         BTreeCurrentReadMode mode, Duration timeout) {
        // 1. fail-fast 早于 transaction id、锁和页访问；NULL '=' 不形成可锁 predicate range。
        validate(transaction, table, secondary, logicalKey, mode, timeout);
        if (logicalKey.values().stream().anyMatch(ColumnValue.NullValue.class::isInstance)) {
            return List.of();
        }
        WaitDeadline deadline = WaitDeadline.after(timeout);

        // 2. locking read 必须有真实 owner；logical-prefix 稳定锁使同 key DML 发布/标删与本次读取串行。
        TransactionId owner = transactionManager.assignWriteId(transaction);
        String token = tokenFactory.create(secondary, logicalKey);
        lockManager.acquire(owner, new SecondaryLogicalKeyLockKey(secondary.index().indexId(), token),
                mode == BTreeCurrentReadMode.FOR_SHARE
                        ? TransactionLockMode.REC_S : TransactionLockMode.REC_X,
                deadline.remaining("secondary logical-prefix lock"));

        // 3. prefix 锁授予后重扫；candidate 仅为物化值对象，返回前 MTR 已提交并释放全部页资源。
        List<BTreeLookupResult> candidates = scanCandidates(secondary, logicalKey);
        if (candidates.size() > MAX_RANGE_CANDIDATES) {
            throw new SecondaryRangeCapacityException(
                    "secondary current-read candidate limit exceeded: table=" + table.tableId()
                            + " index=" + secondary.index().indexId() + " key=" + logicalKey
                            + " limit=" + MAX_RANGE_CANDIDATES);
        }

        IndexKeyDef logicalKeyDef = logicalKeyDefinition(secondary);
        List<SearchKey> identities = new ArrayList<>();
        List<LogicalRecord> rows = new ArrayList<>();
        for (BTreeLookupResult candidate : candidates) {
            SearchKey clusterKey = secondary.layout().clusterKey(candidate.record());
            if (containsIdentity(identities, clusterKey, table)) {
                continue;
            }
            // 4. 每次 point request 消费同一绝对 budget；等待期间不存在 secondary MTR/page latch。
            BTreeCurrentReadRequest request = new BTreeCurrentReadRequest(owner, transaction.isolationLevel(),
                    deadline.remaining("clustered row lock"), MAX_RELOCATION_RETRIES);
            Optional<BTreeLookupResult> locked = currentRead.lockPoint(
                    table.clusteredIndex(), clusterKey, request, mode);
            if (locked.isEmpty()) {
                continue;
            }

            // 5. secondary marked history 可能仍指向已换 key 的当前行；只返回重投影后仍满足 predicate 的版本。
            LogicalRecord row = locked.orElseThrow().record();
            SearchKey currentLogicalKey = secondary.layout().logicalKey(
                    secondary.layout().toEntry(row, false));
            if (keyComparator.compare(currentLogicalKey, logicalKey, logicalKeyDef,
                    secondary.index().schema()) == 0) {
                identities.add(clusterKey);
                rows.add(row);
            }
        }
        return List.copyOf(rows);
    }

    /**
     * 在独立短 MTR 中物化 including-deleted candidates；异常时只终止仍 ACTIVE 的读 MTR。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建只读 MTR，把 secondary page fix/latch 的 owner 限定在本方法。</li>
     *     <li>在 logical-prefix 锁已由调用方持有的前提下，多取一个候选用于显式检测容量溢出，
     *         且只物化不携带页资源的 lookup result。</li>
     *     <li>提交后释放全部 secondary 资源再返回；扫描或提交失败时回滚仍 ACTIVE 的 MTR，
     *         已授予的事务锁由外层事务终态统一释放。</li>
     * </ol>
     *
     * @param secondary exact-version 普通二级 metadata，必须与调用方已锁 predicate 相同。
     * @param logicalKey 完整且非 NULL 的 logical equality key。
     * @return 按物理 secondary key 排序的候选快照，最多为安全上限加一。
     * @throws RuntimeException 页访问、记录解码或 MTR 提交失败时抛出并保留原始领域异常。
     */
    private List<BTreeLookupResult> scanCandidates(SecondaryIndexMetadata secondary, SearchKey logicalKey) {
        // 1. 本方法不复用 clustered current-read MTR，避免跨树 latch/fix 重叠。
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            // 2. 多取一个只用于 fail-closed 检测；不能把截断结果冒充完整 SQL range。
            List<BTreeLookupResult> candidates = btree.scanSecondaryPrefixIncludingDeleted(
                    read, secondary, logicalKey, MAX_RANGE_CANDIDATES + 1);
            // 3. 页资源全部释放后，调用方才会进入 clustered record lock/current-read。
            mtrManager.commit(read);
            return candidates;
        } catch (RuntimeException error) {
            if (read.state() == MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(read);
            }
            throw error;
        }
    }

    private boolean containsIdentity(List<SearchKey> identities, SearchKey candidate,
                                     TableIndexMetadata table) {
        return identities.stream().anyMatch(existing -> keyComparator.compare(
                existing, candidate, table.clusteredIndex().keyDef(),
                table.clusteredIndex().schema()) == 0);
    }

    private static IndexKeyDef logicalKeyDefinition(SecondaryIndexMetadata secondary) {
        int parts = secondary.layout().logicalKeyPartCount();
        return new IndexKeyDef(secondary.index().indexId(),
                secondary.index().keyDef().parts().subList(0, parts));
    }

    private static void validate(Transaction transaction, TableIndexMetadata table,
                                 SecondaryIndexMetadata secondary, SearchKey logicalKey,
                                 BTreeCurrentReadMode mode, Duration timeout) {
        if (transaction == null || table == null || secondary == null || logicalKey == null
                || mode == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("secondary current-read arguments are invalid");
        }
        if (transaction.state() != TransactionState.ACTIVE || transaction.readOnly()) {
            throw new DatabaseValidationException("secondary locking read requires ACTIVE read-write transaction");
        }
        SecondaryIndexMetadata mapped = table.requireSecondary(secondary.index().indexId());
        if (!mapped.equals(secondary) || secondary.logicalUnique()) {
            throw new DatabaseValidationException(
                    "secondary locking range requires exact table-owned non-unique metadata");
        }
        if (logicalKey.size() != secondary.layout().logicalKeyPartCount()) {
            throw new DatabaseValidationException("secondary locking range logical key part count mismatch");
        }
    }

    /**
     * 单次 current-read 的 monotonic 绝对等待预算；每一阶段只能消费剩余值，不能重新获得完整 timeout。
     *
     * @param startedNanos 参与 {@code 构造} 的时间量 {@code startedNanos}；必须非负，零表示立即检查或尚未累计等待
     * @param budgetNanos 参与 {@code 构造} 的时间量 {@code budgetNanos}；必须非负，零表示立即检查或尚未累计等待
     */
    private record WaitDeadline(long startedNanos, long budgetNanos) {
        private static WaitDeadline after(Duration timeout) {
            long nanos;
            try {
                nanos = timeout.toNanos();
            } catch (ArithmeticException overflow) {
                nanos = Long.MAX_VALUE;
            }
            return new WaitDeadline(System.nanoTime(), nanos);
        }

        private Duration remaining(String stage) {
            long elapsed = System.nanoTime() - startedNanos;
            long remaining = budgetNanos == Long.MAX_VALUE ? Long.MAX_VALUE : budgetNanos - elapsed;
            if (remaining <= 0) {
                throw new LockWaitTimeoutException("secondary current-read deadline exhausted at " + stage);
            }
            return Duration.ofNanos(remaining);
        }
    }
}
