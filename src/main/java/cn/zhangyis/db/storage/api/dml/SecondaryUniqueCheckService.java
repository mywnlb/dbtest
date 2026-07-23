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
import cn.zhangyis.db.storage.trx.lock.SecondaryLogicalKeyLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;

import java.util.List;

/**
 * 二级 entry 发布前的事务锁与物理候选检查。所有非 NULL logical key 先取得 collation/prefix 归一化的
 * {@link SecondaryLogicalKeyLockKey} X 锁并持有到事务终态，使 non-unique locking range 与 DML 使用同一稳定资源；
 * logical unique 再扫描完整 prefix，UPDATE/revive 的非唯一或含 NULL key 检查同一完整物理 identity。聚簇层已经
 * 证明主键全新的 INSERT 可通过 {@link #checkFreshInsert} 跳过非唯一 leaf 读取，为 Change Buffer 保留冷页条件。
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
     * @param lockManager 保存 logical-prefix S/X 锁直到事务终态的统一锁目录。
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
     *     <li>所有非 NULL logical key 先取得归一化 REC_X；等待期间不持 page latch/MTR，并与 locking range 共用资源。</li>
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

        // 2. 所有非 NULL logical key 都与 range locking read 共用稳定 X 资源；NULL 不参与 SQL '=' 范围。
        lockLogicalKey(metadata, logicalKey, request);

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

    /**
     * 检查聚簇唯一性已经证明主键全新的 INSERT 所对应的二级发布前态。逻辑唯一索引仍调用完整 prefix 检查；
     * 非唯一索引的 physical key 由“logical key + 全部聚簇主键”组成，而聚簇层已经在同一事务下锁定并确认该
     * 主键不存在，因此其发布前态必为 ABSENT，无需把二级目标 leaf 提前载入 Buffer Pool。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 exact-version metadata、完整行和事务请求；逻辑唯一索引委托完整检查，绝不推迟唯一冲突。</li>
     *     <li>从非唯一 layout 投影 logical key，复用同一归一化 REC_X 事务锁，保持与 locking range 的并发边界。</li>
     *     <li>依赖调用方已完成的聚簇主键唯一检查，把完整 physical identity 冻结为 ABSENT，不访问二级页。</li>
     *     <li>返回无页副作用的发布前态；后续直写仍由 B+Tree 校验页内 identity，冷页则允许 Change Buffer 接管。</li>
     * </ol>
     *
     * @param metadata 目标 exact-version 二级索引；非唯一分支的 physical key 必须包含完整聚簇主键后缀
     * @param row 已通过聚簇主键唯一检查、即将写入的新完整行
     * @param request 当前写事务 owner、隔离级别和有界锁等待配置
     * @return 逻辑唯一索引的真实扫描结果；非唯一索引固定返回可发布且前态为 ABSENT
     * @throws DatabaseValidationException 参数缺失、行/layout 错配或 key 归一化失败时抛出
     * @throws cn.zhangyis.db.storage.trx.lock.LockWaitTimeoutException logical key 锁等待超时时抛出；此时未访问二级页
     */
    public SecondaryUniqueCheckResult checkFreshInsert(
            SecondaryIndexMetadata metadata, LogicalRecord row,
            cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest request) {
        // 1、唯一约束必须读取完整 prefix 候选，Change Buffer 不能改变冲突判定时点。
        if (metadata == null || row == null || request == null) {
            throw new DatabaseValidationException("secondary fresh-insert check metadata/row/request must not be null");
        }
        if (metadata.logicalUnique()) {
            return check(metadata, row, request);
        }

        // 2、非唯一索引仍取得 logical-key X 锁，保持现有 range-lock 与 DML 串行语义。
        LogicalRecord targetEntry = metadata.layout().toEntry(row, false);
        SearchKey logicalKey = metadata.layout().logicalKey(targetEntry);
        lockLogicalKey(metadata, logicalKey, request);

        // 3、聚簇主键已被同事务确认并锁定为全新，故完整二级 identity 不可能有合法旧版本。
        // 4、不打开目标 leaf；后续 coordinator 才能按 residency/bitmap 决定 buffer 或直写。
        return SecondaryUniqueCheckResult.available(SecondaryPublishState.ABSENT);
    }

    /**
     * 为 DML 即将发布或标记的 non-NULL logical secondary key 申请事务级 X 锁。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 metadata、logical key 与 current-read request，且 part 数与 layout 完全一致。</li>
     *     <li>若任一 part 为 SQL NULL，直接返回；NULL equality 不形成可读取 prefix range。</li>
     *     <li>按 type/prefix/collation 生成稳定 token，在不持 page latch/MTR 时向 LockManager 申请 REC_X。</li>
     *     <li>成功锁保持到事务终态；timeout/deadlock 由 LockManager 清理等待状态并抛出领域异常。</li>
     * </ol>
     *
     * @param metadata   exact-version secondary metadata。
     * @param logicalKey 从完整行按 layout 投影出的 logical key。
     * @param request    真实事务 owner 与有界等待参数。
     * @throws DatabaseValidationException 参数缺失或 key part 数与 metadata 不一致时抛出。
     */
    public void lockLogicalKey(SecondaryIndexMetadata metadata, SearchKey logicalKey,
                               cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest request) {
        // 1. identity 必须来自同一 exact layout，不能让部分 key 获得错误的粗粒度资源。
        if (metadata == null || logicalKey == null || request == null
                || logicalKey.size() != metadata.layout().logicalKeyPartCount()) {
            throw new DatabaseValidationException("secondary logical lock metadata/key/request mismatch");
        }
        // 2. SQL NULL equality 为空；DML 对含 NULL unique key 仍允许多行，不制造错误冲突。
        if (logicalKey.values().stream().anyMatch(ColumnValue.NullValue.class::isInstance)) {
            return;
        }
        // 3. token 与 B+Tree comparator 共享 registry，大小写/前缀等价值必定落到同一锁 identity。
        String token = tokenFactory.create(metadata, logicalKey);
        lockManager.acquire(request.owner(),
                new SecondaryLogicalKeyLockKey(metadata.index().indexId(), token),
                TransactionLockMode.REC_X, request.lockWaitTimeout());
        // 4. 不关闭 handle；事务 commit/rollback 的 releaseAll 是唯一成功释放边界。
    }
}
