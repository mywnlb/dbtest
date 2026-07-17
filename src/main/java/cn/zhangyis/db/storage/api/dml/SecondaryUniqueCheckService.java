package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SearchKeyComparator;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SecondaryLogicalKeyLockTokenFactory;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.trx.lock.SecondaryUniqueKeyLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;

import java.util.List;

/**
 * 二级 entry 发布前的事务锁与物理候选检查。logical unique 非 NULL key 先取得 collation/prefix 归一化的
 * {@link SecondaryUniqueKeyLockKey} X 锁并持有到事务终态，再扫描包括 delete-marked 的 logical prefix；
 * 非唯一或含 NULL 的 key 只检查同一完整物理 identity，以支持 A->B->A 的 revive。
 *
 * <p>该 logical-key 锁是教学实现对 InnoDB unique-prefix next-key/gap 锁的稳定等价抽象：它不依赖瞬时 page/heapNo，
 * 但仍进入同一个 LockManager/Wait-For Graph，并由事务终态统一释放。</p>
 */
public final class SecondaryUniqueCheckService {

    /** 短只读 MTR 工厂；物理扫描结束并释放 page latch 后才返回 DML。 */
    private final MiniTransactionManager mtrManager;
    /** 二级 B+Tree including-deleted lookup/prefix scan。 */
    private final SplitCapableBTreeIndexService btree;
    /** 事务 unique-key 锁入口；锁归事务 owner，服务不提前关闭成功 handle。 */
    private final LockManager lockManager;
    /** logical key -> collation/prefix 等价 token。 */
    private final SecondaryLogicalKeyLockTokenFactory tokenFactory;
    /** 完整物理 key 比较器；用于区分同主键 revive 与其它主键冲突。 */
    private final SearchKeyComparator keyComparator;

    /**
     * 创建二级唯一检查服务；所有 collaborator 必须来自同一 {@code StorageEngine} 组合根。
     *
     * @param mtrManager 短只读 MTR 的唯一工厂，负责页 fix/latch memo 的提交或异常释放。
     * @param btree      执行 including-deleted exact lookup 与 logical-prefix scan 的 B+Tree 服务。
     * @param lockManager 保存 logical unique X 锁直到事务终态的统一锁目录。
     * @param registry   与目标 B+Tree comparator 一致的类型 codec/collation 注册表。
     * @throws DatabaseValidationException 任一协作者为空时抛出，防止锁判等和页比较使用不同配置。
     */
    public SecondaryUniqueCheckService(MiniTransactionManager mtrManager,
                                       SplitCapableBTreeIndexService btree,
                                       LockManager lockManager,
                                       TypeCodecRegistry registry) {
        if (mtrManager == null || btree == null || lockManager == null || registry == null) {
            throw new DatabaseValidationException("secondary unique check collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.lockManager = lockManager;
        this.tokenFactory = new SecondaryLogicalKeyLockTokenFactory(registry);
        this.keyComparator = new SearchKeyComparator(registry);
    }

    /**
     * 检查完整行对应的二级 entry 是否可发布。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>由 exact-version layout 投影 entry/logical/physical key，校验请求与行 schema。</li>
     *     <li>logical unique 且所有 part 非 NULL 时，先取得归一化 logical-key REC_X；等待期间不持 page latch/MTR。</li>
     *     <li>用短只读 MTR 扫描 logical prefix（unique）或 exact physical key（非 unique/NULL），包含 marked 候选。</li>
     *     <li>其它主键或同物理 key live 均返回 duplicate；同物理 key marked 返回 DELETE_MARKED，否则 ABSENT。</li>
     * </ol>
     *
     * @param metadata 二级索引 exact-version metadata。
     * @param row      待发布的新完整聚簇行（可带或不带隐藏列）。
     * @param request  写事务 owner、隔离级别与有界锁等待参数。
     * @return 冲突结果，或供发布阶段选择 insert/revive 的目标完整物理 identity 前态。
     * @throws DatabaseValidationException 参数缺失、行/layout 错配或类型归一化失败时抛出。
     * @throws cn.zhangyis.db.storage.trx.lock.LockWaitTimeoutException logical unique X 锁等待超时时抛出；
     *                                                                  此时没有创建页 MTR。
     */
    public SecondaryUniqueCheckResult check(SecondaryIndexMetadata metadata, LogicalRecord row,
                                            cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest request) {
        // 1. 所有 key 都从归一化完整行投影，禁止用户字面量绕过 collation/type 语义。
        if (metadata == null || row == null || request == null) {
            throw new DatabaseValidationException("secondary unique check metadata/row/request must not be null");
        }
        LogicalRecord targetEntry = metadata.layout().toEntry(row, false);
        SearchKey logicalKey = metadata.layout().logicalKey(targetEntry);
        SearchKey physicalKey = metadata.layout().physicalKey(targetEntry);
        boolean containsNull = logicalKey.values().stream().anyMatch(ColumnValue.NullValue.class::isInstance);

        // 2. 非 NULL logical unique key 先取事务级等价锁；成功 handle 留在 LockManager，随事务 commit/rollback 释放。
        if (metadata.logicalUnique() && !containsNull) {
            String token = tokenFactory.create(metadata, logicalKey);
            lockManager.acquire(request.owner(),
                    new SecondaryUniqueKeyLockKey(metadata.index().indexId(), token),
                    TransactionLockMode.REC_X, request.lockWaitTimeout());
        }

        // 3. 页读取只存在于独立只读 MTR；异常时显式 rollback 释放 fix/latch。
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            List<BTreeLookupResult> candidates;
            if (metadata.logicalUnique() && !containsNull) {
                candidates = btree.scanSecondaryPrefixIncludingDeleted(read, metadata, logicalKey, 2);
            } else {
                candidates = btree.lookupIncludingDeleted(read, metadata.index(), physicalKey)
                        .map(List::of).orElseGet(List::of);
            }
            mtrManager.commit(read);

            // 4. prefix 中任何其它 physical key 都是其它主键冲突；精确 marked identity 才允许 revive。
            SecondaryPublishState state = SecondaryPublishState.ABSENT;
            for (BTreeLookupResult candidate : candidates) {
                SearchKey candidatePhysical = metadata.layout().physicalKey(candidate.record());
                boolean sameIdentity = keyComparator.compare(candidatePhysical, physicalKey,
                        metadata.index().keyDef(), metadata.index().schema()) == 0;
                if (!sameIdentity || !candidate.record().deleted()) {
                    return SecondaryUniqueCheckResult.conflict();
                }
                state = SecondaryPublishState.DELETE_MARKED;
            }
            return SecondaryUniqueCheckResult.available(state);
        } catch (RuntimeException error) {
            if (read.state() == cn.zhangyis.db.storage.mtr.MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(read);
            }
            throw error;
        }
    }
}
