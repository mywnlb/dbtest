package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadMode;
import cn.zhangyis.db.storage.btree.BTreeCurrentReadRequest;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.BTreeRedoBudgetEstimator;
import cn.zhangyis.db.storage.btree.BTreeRootSnapshotService;
import cn.zhangyis.db.storage.btree.SecondaryIndexMetadata;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.btree.TableIndexMetadata;
import cn.zhangyis.db.storage.changebuffer.SecondaryIndexMutationCoordinator;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.redo.RedoBudgetPurpose;
import cn.zhangyis.db.storage.trx.PurgeDmlRowGuard;
import cn.zhangyis.db.storage.trx.PurgeDmlRowGuardManager;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionManager;
import cn.zhangyis.db.storage.trx.TransactionState;
import cn.zhangyis.db.storage.undo.SecondaryEntryBeforeState;
import cn.zhangyis.db.storage.undo.SecondaryUndoMutation;
import cn.zhangyis.db.storage.trx.lock.LockManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 表级多索引 DML 编排器。它以聚簇索引为逻辑 undo anchor，先固定全部 secondary mutation 列表，随后按
 * index id 顺序使用独立短 MTR 发布二级 entry；rollback/purge 可据同一列表幂等收敛。
 *
 * <p>物理锁顺序：事务 current-read/unique lock -> row guard -> 单棵索引 MTR/page latch；任何可能等待事务锁的
 * current-read 都发生在 row guard 之前。聚簇首 MTR 与各二级 MTR 不跨树持 latch，避免 buffer pin 和锁序放大。</p>
 */
public final class TableDmlService {

    /** 聚簇 DML 内核；负责 undo owner、LOB、cluster current-read 与事务终态。 */
    private final ClusteredDmlService clustered;
    /** 写 id 与事务状态来源。 */
    private final TransactionManager transactionManager;
    /** 二级短 MTR 工厂。 */
    private final MiniTransactionManager mtrManager;
    /** 二级页结构原语。 */
    private final SplitCapableBTreeIndexService btree;
    /** 聚簇 current-read；二级唯一检查使用独立服务。 */
    private final cn.zhangyis.db.storage.btree.BTreeCurrentReadService currentRead;
    /** exact logical unique 检查和事务锁服务。 */
    private final SecondaryUniqueCheckService uniqueCheck;
    /** root page header 快照刷新入口。 */
    private final BTreeRootSnapshotService rootSnapshots;
    /** purge/DML 短物理行协调器。 */
    private final PurgeDmlRowGuardManager rowGuards;
    /** 与 B+Tree comparator 相同的 registry，供逻辑 key 相等比较。 */
    private final TypeCodecRegistry registry;
    /** redo end LSN 诊断；聚簇内核结果仍是本条逻辑 DML 的公开 anchor。 */
    private final cn.zhangyis.db.storage.redo.RedoLogManager redo;
    /** 可选 Change Buffer 决策点；legacy/低层测试为空时保持原来的真实 B+Tree 直写语义。 */
    private final SecondaryIndexMutationCoordinator secondaryMutations;
    /** 仅包内测试可替换的 secondary MTR 稳定边界故障接缝；生产始终为 no-op。 */
    private TableDmlProgressFaultInjector faultInjector = TableDmlProgressFaultInjector.NO_OP;

    /**
     * 创建表级 DML 服务；所有 collaborator 必须来自同一 StorageEngine 组合根，不能另建旁路事务/锁目录。
     *
     * @param clustered         聚簇 DML 内核，负责聚簇页、逻辑 undo、LOB 与事务级 commit/rollback。
     * @param transactionManager 分配 write id 并发布事务终态的权威事务目录。
     * @param mtrManager        所有二级短物理 MTR 的创建、redo admission 与资源释放入口。
     * @param btree             执行二级 insert、delete-mark/revive 和结构变化的 B+Tree 服务。
     * @param currentRead       聚簇 current-read 与事务 record/unique lock 的入口。
     * @param lockManager       保存 logical secondary unique X 锁直到事务终态的统一锁目录。
     * @param rootSnapshots     在结构写前从 root 页头刷新权威 level 的短读服务。
     * @param registry          与 B+Tree comparator 一致的类型 codec/collation 注册表。
     * @param rowGuards         前台 DML 与 purge 共用的聚簇行短物理协调器。
     * @param redo              提供逻辑 DML 完成后的 redo end LSN 诊断快照。
     * @throws DatabaseValidationException 任一协作者为空时抛出；失败时服务不会形成部分可用状态。
     */
    public TableDmlService(ClusteredDmlService clustered, TransactionManager transactionManager,
                           MiniTransactionManager mtrManager, SplitCapableBTreeIndexService btree,
                           cn.zhangyis.db.storage.btree.BTreeCurrentReadService currentRead,
                           LockManager lockManager, BTreeRootSnapshotService rootSnapshots,
                           TypeCodecRegistry registry, PurgeDmlRowGuardManager rowGuards,
                           cn.zhangyis.db.storage.redo.RedoLogManager redo) {
        this(clustered, transactionManager, mtrManager, btree, currentRead, lockManager,
                rootSnapshots, registry, rowGuards, redo, null);
    }

    /**
     * 创建接入 Change Buffer 的生产表级 DML 服务。原构造器继续服务不含 system.ibd 的 legacy 测试组合根。
     *
     * @param clustered 聚簇 DML 与逻辑 undo anchor
     * @param transactionManager 事务 write id/终态目录
     * @param mtrManager 二级短 MTR 工厂
     * @param btree 二级 B+Tree 原语
     * @param currentRead 聚簇 current-read 服务
     * @param lockManager logical unique/record 锁目录
     * @param rootSnapshots root level 快照协作者
     * @param registry 索引 comparator/codec 注册表
     * @param rowGuards DML/purge 物理行协调器
     * @param redo 公开结果的 redo 边界来源
     * @param secondaryMutations 可选统一 buffer-or-direct 决策点；为空保持 legacy 直写
     */
    public TableDmlService(ClusteredDmlService clustered, TransactionManager transactionManager,
                           MiniTransactionManager mtrManager, SplitCapableBTreeIndexService btree,
                           cn.zhangyis.db.storage.btree.BTreeCurrentReadService currentRead,
                           LockManager lockManager, BTreeRootSnapshotService rootSnapshots,
                           TypeCodecRegistry registry, PurgeDmlRowGuardManager rowGuards,
                           cn.zhangyis.db.storage.redo.RedoLogManager redo,
                           SecondaryIndexMutationCoordinator secondaryMutations) {
        if (clustered == null || transactionManager == null || mtrManager == null || btree == null
                || currentRead == null || lockManager == null || rootSnapshots == null || registry == null
                || rowGuards == null || redo == null) {
            throw new DatabaseValidationException("table DML collaborators must not be null");
        }
        this.clustered = clustered;
        this.transactionManager = transactionManager;
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.currentRead = currentRead;
        this.uniqueCheck = new SecondaryUniqueCheckService(mtrManager, btree, lockManager, registry);
        this.rootSnapshots = rootSnapshots;
        this.registry = registry;
        this.rowGuards = rowGuards;
        this.redo = redo;
        this.secondaryMutations = secondaryMutations;
    }

    /**
     * 向表的聚簇索引和全部二级索引发布一行。任一 secondary 中断留下的 active undo 由同一 mutation tail
     * 在 statement/full rollback 中反向收敛，不能只回滚聚簇行。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 ACTIVE 事务，分配稳定 write id，并从完整输入行投影聚簇主键和 current-read 请求。</li>
     *     <li>在不持 row guard/page latch 时检查聚簇唯一性及全部 logical secondary unique key，
     *         同时按 index id 冻结 secondary entry 与 undo mutation。</li>
     *     <li>以“表 id + 物化聚簇键”有界取得 row guard，再由聚簇首 MTR 写 logical undo 和聚簇记录；
     *         聚簇层失败时 guard 自动释放，且不会开始二级发布。</li>
     *     <li>按稳定 index id 逐棵树使用独立短 MTR 发布 secondary entry，最后返回聚簇影响行数与当前 redo LSN；
     *         任一二级失败时已提交物理动作由 active undo 支持幂等 rollback。</li>
     * </ol>
     *
     * @param command 事务、exact-version 表索引聚合、完整用户行、可选 LOB segment 和有界等待参数。
     * @return 是否写入、影响行数、逻辑 DML 结束时 redo LSN 与事务 write id。
     * @throws DatabaseValidationException 命令缺失、事务非 ACTIVE 或 metadata/行/key 形状不一致时抛出。
     * @throws DmlDuplicateKeyException 聚簇 key、logical unique secondary key 或完整二级 identity 冲突时抛出。
     * @throws DatabaseRuntimeException 锁等待、row guard、MTR、undo、B+Tree 或 redo 发布失败时抛出；
     *                                  调用方必须 rollback 当前 statement/transaction。
     */
    public DmlWriteResult insert(TableInsertCommand command) {
        // 1. write id 与主键都来自当前 ACTIVE 事务和完整表行；禁止用未经类型物化的用户字面量作为锁 identity。
        requireActive(command == null ? null : command.transaction());
        TableIndexMetadata metadata = command.metadata();
        registerChangeBufferMetadata(metadata);
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        SearchKey clusterKey = keyFromRow(command.record(), metadata.clusteredIndex());
        BTreeCurrentReadRequest request = request(txnId, txn, command.lockWaitTimeout());

        // 2. 所有可能等待的聚簇/unique 事务锁先于 row guard；同时冻结与 exact-version layout 对应的 undo 证据。
        if (currentRead.checkUniqueForInsert(metadata.clusteredIndex(), clusterKey, request).duplicate()) {
            throw new DmlDuplicateKeyException("duplicate clustered key for table " + metadata.tableId());
        }
        List<SecondaryPlan> plans = new ArrayList<>();
        List<SecondaryUndoMutation> mutations = new ArrayList<>();
        for (SecondaryIndexMetadata secondary : metadata.secondaryIndexes()) {
            SecondaryUniqueCheckResult checked = uniqueCheck.checkFreshInsert(
                    secondary, command.record(), request);
            if (checked.duplicate() || checked.publishState() != SecondaryPublishState.ABSENT) {
                throw new DmlDuplicateKeyException("duplicate secondary key for index "
                        + secondary.index().indexId());
            }
            plans.add(new SecondaryPlan(secondary, secondary.layout().toEntry(command.record(), false), null,
                    SecondaryPublishState.ABSENT));
            mutations.add(SecondaryUndoMutation.insertEntry(secondary.index().indexId()));
        }

        // 3. guard identity 使用物化聚簇 key；聚簇首 MTR 是逻辑 undo anchor，失败时 try-with-resources 释放 guard。
        try (PurgeDmlRowGuard ignored = rowGuards.acquireForDml(metadata.tableId(), clusterKey,
                command.lockWaitTimeout())) {
            ClusteredInsertCommand clusterCommand = new ClusteredInsertCommand(txn, metadata.clusteredIndex(),
                    clusterKey, command.record(), metadata.tableId(), command.lobSegment(),
                    command.lockWaitTimeout());
            DmlWriteResult result = clustered.insert(clusterCommand, mutations);

            // 4. 每个 secondary 使用独立短 MTR；异常不会跨树遗留 page latch，active undo 保留已完成动作的反向证据。
            for (SecondaryPlan plan : plans) {
                publishSecondary(metadata.tableId(), metadata.schemaVersion(), plan, false);
            }
            return new DmlWriteResult(result.changed(), result.affectedRows(), redo.currentLsn(), txnId);
        }
    }

    /**
     * 更新一条聚簇行，并仅维护 logical key 真正变化的二级索引；二级前向顺序固定为“发布新 entry，再标记旧 entry”。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 ACTIVE 事务、分配 write id，并通过聚簇 FOR_UPDATE current-read 取得事务行锁和旧完整行；
     *         目标不存在时不创建 undo、不修改页，直接返回零影响。</li>
     *     <li>从旧行重建实际聚簇键并核对命令定位；逐个二级 layout 比较 logical key，只为变键索引执行
     *         unique 检查并冻结 old/new entry、发布前态与 CHANGE_KEY undo mutation。</li>
     *     <li>在所有事务锁等待结束后取得 row guard，由聚簇首 MTR 写 UPDATE undo 和新聚簇版本；
     *         聚簇层同时拒绝 v1 不支持的主键变化。</li>
     *     <li>按 index id 对每棵变键二级树先 insert/revive 新 entry、再 delete-mark 旧 entry；最后返回影响行数和 redo LSN，
     *         中途失败由 undo 中的 new-before-state 决定 remove 还是 re-mark。</li>
     * </ol>
     *
     * @param command 事务、exact-version metadata、旧聚簇定位键、新完整用户行和有界等待参数。
     * @return 目标不存在时返回零影响；成功时返回一行更新及事务 write id/redo LSN。
     * @throws DatabaseValidationException 命令缺失、事务非 ACTIVE、主键定位错配或新行不符合目标 schema 时抛出。
     * @throws DmlDuplicateKeyException 新 logical secondary key 与其它 live row 冲突时抛出。
     * @throws DatabaseRuntimeException 锁等待、row guard、undo、MTR、B+Tree 或 redo 发布失败时抛出。
     */
    public DmlWriteResult update(TableUpdateCommand command) {
        requireActive(command == null ? null : command.transaction());
        return updateInternal(command.transaction(), command.metadata(), command.clusterKey(),
                ignored -> command.newRecord(), command.lobSegment(), command.lockWaitTimeout());
    }

    /**
     * 在同一次 FOR_UPDATE current-read 物化的旧行上应用列 patch。未赋值列直接复制锁定版本，
     * 因而 external LOB 引用不会被 gateway hydrate 后重写，且读取与更新之间不存在竞态窗口。
     *
     * @param command 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @return {@code update} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    public DmlWriteResult update(TableUpdatePatchCommand command) {
        requireActive(command == null ? null : command.transaction());
        return updateInternal(command.transaction(), command.metadata(), command.clusterKey(), oldRow -> {
            ArrayList<cn.zhangyis.db.storage.record.type.ColumnValue> values =
                    new ArrayList<>(oldRow.columnValues());
            for (TableColumnAssignment assignment : command.assignments()) {
                values.set(assignment.ordinal(), assignment.value());
            }
            return new LogicalRecord(oldRow.schemaVersion(), values, false,
                    cn.zhangyis.db.storage.record.format.RecordType.CONVENTIONAL);
        }, command.lobSegment(), command.lockWaitTimeout());
    }

    /**
     * 校验输入与当前状态后修改存储引擎稳定 API领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param txn 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param metadata 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param clusterKey 参与 {@code updateInternal} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param rowFactory 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param lobSegment 可选的 {@code lobSegment}；参数本身不得为 {@code null}，空 {@code Optional} 明确表示调用方未提供该领域值
     * @param lockWaitTimeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @return {@code updateInternal} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DmlDuplicateKeyException 目标身份或唯一键已被占用时抛出；调用方应回滚本次变更或改用其他合法身份
     */
    private DmlWriteResult updateInternal(Transaction txn, TableIndexMetadata metadata, SearchKey clusterKey,
                                          java.util.function.Function<LogicalRecord, LogicalRecord> rowFactory,
                                          Optional<cn.zhangyis.db.storage.api.SegmentRef> lobSegment,
                                          Duration lockWaitTimeout) {
        // 1. 聚簇 FOR_UPDATE 在 row guard 之前完成可能阻塞的事务锁等待，并物化旧行作为后续 exact-version 投影来源。
        registerChangeBufferMetadata(metadata);
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = request(txnId, txn, lockWaitTimeout);
        Optional<BTreeLookupResult> locked = currentRead.lockPoint(metadata.clusteredIndex(), clusterKey,
                request, BTreeCurrentReadMode.FOR_UPDATE);
        if (locked.isEmpty()) {
            return new DmlWriteResult(false, 0, redo.currentLsn(), txnId);
        }

        // 2. 命令主键必须与锁定行一致；只记录 comparator 判断发生 logical key 变化的二级 mutation。
        LogicalRecord oldRow = locked.orElseThrow().record();
        SearchKey actualClusterKey = keyFromRow(oldRow, metadata.clusteredIndex());
        if (!actualClusterKey.equals(clusterKey)) {
            throw new DatabaseValidationException("table update cluster key must match materialized row");
        }
        LogicalRecord newRecord = rowFactory.apply(oldRow);
        if (newRecord == null || newRecord.schemaVersion() != metadata.schemaVersion()
                || newRecord.columnValues().size() != metadata.clusteredIndex().schema().columnCount()
                || newRecord.hiddenColumns() != null) {
            throw new DatabaseValidationException("materialized table update row does not match metadata");
        }

        List<SecondaryPlan> changed = new ArrayList<>();
        List<SecondaryUndoMutation> mutations = new ArrayList<>();
        for (SecondaryIndexMetadata secondary : metadata.secondaryIndexes()) {
            LogicalRecord oldEntry = secondary.layout().toEntry(oldRow, false);
            LogicalRecord newEntry = secondary.layout().toEntry(newRecord, false);
            if (sameLogicalKey(secondary, oldEntry, newEntry)) {
                continue;
            }
            // locking range 先保护旧 prefix，防止本事务在读者持 S/X 时把仍属于范围的 entry 标删。
            uniqueCheck.lockLogicalKey(secondary, secondary.layout().logicalKey(oldEntry), request);
            SecondaryUniqueCheckResult checked = uniqueCheck.check(secondary, newRecord, request);
            if (checked.duplicate()) {
                throw new DmlDuplicateKeyException("duplicate secondary key for index "
                        + secondary.index().indexId());
            }
            SecondaryEntryBeforeState before = checked.publishState() == SecondaryPublishState.DELETE_MARKED
                    ? SecondaryEntryBeforeState.DELETE_MARKED : SecondaryEntryBeforeState.ABSENT;
            changed.add(new SecondaryPlan(secondary, newEntry, oldEntry, checked.publishState()));
            mutations.add(SecondaryUndoMutation.changeKey(secondary.index().indexId(), before));
        }

        // 3. 所有 unique/record lock 等待已经结束；row guard 只覆盖聚簇和二级的若干短物理 MTR。
        try (PurgeDmlRowGuard ignored = rowGuards.acquireForDml(metadata.tableId(), actualClusterKey,
                lockWaitTimeout)) {
            ClusteredUpdateCommand clusterCommand = new ClusteredUpdateCommand(txn, metadata.clusteredIndex(),
                    clusterKey, newRecord, metadata.tableId(), lobSegment, lockWaitTimeout);
            DmlWriteResult result = clustered.update(clusterCommand, mutations);

            // 4. new-before-old 顺序保证回表不会因先删旧 entry 而出现瞬时断链；undo 前态使异常后 inverse 可判定。
            for (SecondaryPlan plan : changed) {
                publishSecondary(metadata.tableId(), metadata.schemaVersion(), plan, true);
                markSecondary(metadata.tableId(), metadata.schemaVersion(),
                        plan.secondary(), plan.oldEntry(), true);
            }
            return new DmlWriteResult(result.changed(), result.affectedRows(), redo.currentLsn(), txnId);
        }
    }

    /**
     * 对一条聚簇行及其全部二级 entry 发布 delete mark；物理摘除由满足 MVCC horizon 的 purge 完成。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 ACTIVE 事务、分配 write id，并通过聚簇 FOR_UPDATE current-read 取得行锁和旧完整行；
     *         目标不存在时不写 undo/redo，返回零影响。</li>
     *     <li>从锁定旧行重建实际聚簇键，并按 exact-version layout 冻结全部二级 entry 与 DELETE_MARK mutation。</li>
     *     <li>在事务锁等待结束后取得 row guard，由聚簇首 MTR 写 DELETE_MARK undo 并标记聚簇记录。</li>
     *     <li>按稳定 index id 用独立短 MTR 标记全部二级 entry，返回影响行数与 redo LSN；
     *         任一中断由 undo mutation 支持 rollback revive，物理页不会在本方法回收。</li>
     * </ol>
     *
     * @param command 事务、exact-version metadata、完整聚簇定位键和有界等待参数。
     * @return 目标不存在时返回零影响；成功时返回一行逻辑删除及事务 write id/redo LSN。
     * @throws DatabaseValidationException 命令缺失、事务非 ACTIVE 或 materialized key/metadata 错配时抛出。
     * @throws DatabaseRuntimeException 行锁等待、row guard、undo、MTR、B+Tree 或 redo 发布失败时抛出。
     */
    public DmlWriteResult delete(TableDeleteCommand command) {
        // 1. 聚簇 FOR_UPDATE 在 row guard 之前取得事务行锁；不存在分支不会生成二级计划或物理副作用。
        requireActive(command == null ? null : command.transaction());
        TableIndexMetadata metadata = command.metadata();
        registerChangeBufferMetadata(metadata);
        Transaction txn = command.transaction();
        TransactionId txnId = transactionManager.assignWriteId(txn);
        BTreeCurrentReadRequest request = request(txnId, txn, command.lockWaitTimeout());
        Optional<BTreeLookupResult> locked = currentRead.lockPoint(metadata.clusteredIndex(), command.clusterKey(),
                request, BTreeCurrentReadMode.FOR_UPDATE);
        if (locked.isEmpty()) {
            return new DmlWriteResult(false, 0, redo.currentLsn(), txnId);
        }

        // 2. 二级 entry 只能从锁定的完整旧行投影，不能从 DELETE 谓词或用户字面量猜测。
        LogicalRecord oldRow = locked.orElseThrow().record();
        SearchKey actualClusterKey = keyFromRow(oldRow, metadata.clusteredIndex());
        List<SecondaryPlan> plans = metadata.secondaryIndexes().stream()
                .map(secondary -> new SecondaryPlan(secondary,
                        secondary.layout().toEntry(oldRow, false), null, SecondaryPublishState.DELETE_MARKED))
                .toList();
        // 所有 logical-prefix X 等待必须发生在 row guard/page MTR 之前；锁保持到 DELETE 事务终态。
        for (SecondaryPlan plan : plans) {
            uniqueCheck.lockLogicalKey(plan.secondary(),
                    plan.secondary().layout().logicalKey(plan.entry()), request);
        }
        List<SecondaryUndoMutation> mutations = metadata.secondaryIndexes().stream()
                .map(secondary -> SecondaryUndoMutation.deleteMarkEntry(secondary.index().indexId()))
                .toList();

        // 3. guard 内先由聚簇首 MTR 固化 undo anchor 与聚簇 delete mark。
        try (PurgeDmlRowGuard ignored = rowGuards.acquireForDml(metadata.tableId(), actualClusterKey,
                command.lockWaitTimeout())) {
            ClusteredDeleteCommand clusterCommand = new ClusteredDeleteCommand(txn, metadata.clusteredIndex(),
                    command.clusterKey(), metadata.tableId(), command.lobSegment(), command.lockWaitTimeout());
            DmlWriteResult result = clustered.delete(clusterCommand, mutations);

            // 4. 二级仅翻转 delete 位，不做结构删除；purge horizon 未满足前必须保留旧版本导航入口。
            for (SecondaryPlan plan : plans) {
                markSecondary(metadata.tableId(), metadata.schemaVersion(),
                        plan.secondary(), plan.entry(), true);
            }
            return new DmlWriteResult(result.changed(), result.affectedRows(), redo.currentLsn(), txnId);
        }
    }

    /**
     * 提交表级 DML 事务；当前实现复用聚簇事务终态编排，并由它完成 undo finalize、redo durability 与锁释放。
     *
     * @param command ACTIVE 写事务、durability policy 与有界等待参数。
     * @return 提交序号、是否满足指定 durability，以及事务终态释放的锁数量。
     * @throws DatabaseValidationException 命令或事务状态无效时抛出。
     * @throws DmlOperationException undo finalize、事务终态发布或 redo durability 失败时抛出。
     */
    public DmlCommitResult commit(DmlCommitCommand command) {
        return clustered.commit(command);
    }

    /**
     * 兼容低层单聚簇调用方的回滚入口。命令只携带一个聚簇索引，因此只适用于未写 secondary tail 的事务；
     * 由本服务执行过多索引写入的事务必须调用 {@link #rollback(ResolvedDmlRollbackCommand)}，让 undo identity
     * 通过 exact-version resolver 恢复全部 secondary 与聚簇状态。
     *
     * @param command ACTIVE/ROLLBACK_ONLY 写事务及其唯一聚簇索引快照。
     * @return 单聚簇 rollback 执行摘要与事务终态释放的锁数量。
     * @throws DatabaseValidationException 命令或事务状态无效时抛出。
     * @throws DmlOperationException undo 遍历、补偿 redo 或事务终态发布失败时抛出。
     */
    public DmlRollbackResult rollback(DmlRollbackCommand command) {
        return clustered.rollback(command);
    }

    /**
     * 通过 undo 固定 table/index identity 和 DD exact-version resolver 回滚表级多索引事务。
     *
     * @param command 仅携带待回滚事务；每条 undo 的表、索引、layout 与 LOB binding 均由 resolver 解析。
     * @return 多索引 inverse 完成后的 rollback 摘要与释放锁数量。
     * @throws DatabaseRuntimeException target 解析、任一二级/聚簇 inverse、marker 或终态发布失败时抛出。
     */
    public DmlRollbackResult rollback(ResolvedDmlRollbackCommand command) {
        return clustered.rollback(command);
    }

    /**
     * 安装包内测试故障接缝。调用方必须在单线程、无在途 DML 时设置，并在 finally 中恢复
     * {@link TableDmlProgressFaultInjector#NO_OP}；该入口不属于 storage 公共 API。
     * @param faultInjector 由当前模块组合根提供的领域协作者；不得为 {@code null}，其状态和生命周期必须覆盖本次调用且不能绕过模块边界
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void installFaultInjectorForTest(TableDmlProgressFaultInjector faultInjector) {
        if (faultInjector == null) {
            throw new DatabaseValidationException("table DML fault injector must not be null");
        }
        this.faultInjector = faultInjector;
    }

    /**
     * 发布一个 secondary entry。完整物理 identity 的检查前态决定执行新插入还是 revive；每棵树使用独立短 MTR，
     * 任何异常先释放 page latch，外层 active undo mutation 保留供后续 rollback 重试。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取 root 页头刷新索引 level，避免使用 DDL binding 中可能过期的结构提示计算 redo admission。</li>
     *     <li>根据发布前态选择预算：新 entry 可能 split/root-grow，必须按结构写估算；revive 只翻转记录头 delete 位。</li>
     *     <li>在单棵索引写 MTR 中执行 insert 或 revive；INSERT 不允许消费 DELETE_MARKED 前态，防止复活历史 identity。</li>
     *     <li>成功提交物理 redo/page dirty；异常时仅回滚仍 ACTIVE 的 MTR，确保 fix/latch 按 memo 逆序释放。</li>
     * </ol>
     *
     * @param plan        已在聚簇 undo 写入前冻结的 exact-version 二级 entry、旧 entry 与发布前态。
     * @param allowRevive {@code true} 表示 UPDATE 的 A→B→A 可复活同主键 marked identity；INSERT 必须传 false。
     * @throws DmlDuplicateKeyException INSERT 计划错误地要求 revive 时抛出。
     * @throws DatabaseRuntimeException root 刷新、redo admission、B+Tree 结构写或 MTR 提交失败时抛出。
     */
    private void publishSecondary(long tableId, long schemaVersion,
                                  SecondaryPlan plan, boolean allowRevive) {
        if (plan.state() == SecondaryPublishState.DELETE_MARKED && !allowRevive) {
            throw new DmlDuplicateKeyException("INSERT cannot revive a marked secondary entry");
        }
        if (secondaryMutations != null) {
            long indexId = plan.secondary().index().indexId();
            faultInjector.onBoundary(TableDmlProgressPhase.BEFORE_MTR,
                    TableDmlSecondaryOperation.INSERT_OR_REVIVE, indexId);
            secondaryMutations.insertOrRevive(tableId, schemaVersion, plan.secondary(), plan.entry(),
                    plan.state() == SecondaryPublishState.DELETE_MARKED,
                    RedoBudgetPurpose.SECONDARY_INDEX);
            faultInjector.onBoundary(TableDmlProgressPhase.AFTER_COMMIT,
                    TableDmlSecondaryOperation.INSERT_OR_REVIVE, indexId);
            return;
        }
        // 1. 读 MTR 只物化 root level，提交释放 S latch 后才创建写 MTR。
        BTreeIndex index = refresh(plan.secondary().index());
        faultInjector.onBoundary(TableDmlProgressPhase.BEFORE_MTR,
                TableDmlSecondaryOperation.INSERT_OR_REVIVE, index.indexId());

        // 2. 新 entry 可能触发 split/root-grow，按刷新后的 root level 估算结构上界；revive 仅做等长 header 改写。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.SECONDARY_INDEX,
                plan.state() == SecondaryPublishState.ABSENT
                        ? BTreeRedoBudgetEstimator.insert(index.rootLevel())
                        : BTreeRedoBudgetEstimator.pointRewrite()));
        try {
            // 3. 发布前态是聚簇 undo 的恢复证据；执行分支必须与它保持一致，不能在页内临时猜测动作。
            if (plan.state() == SecondaryPublishState.DELETE_MARKED) {
                if (!allowRevive) {
                    throw new DmlDuplicateKeyException("INSERT cannot revive a marked secondary entry");
                }
                btree.setSecondaryDeleteMark(mtr, index, secondaryPhysicalKey(plan), false);
            } else {
                LogicalRecord entry = plan.entry();
                btree.insertSecondary(mtr, index, entry);
            }

            // 4. commit 同时封闭 redo group 并发布 dirty 页；失败由 catch 终止仍 ACTIVE 的 MTR。
            mtrManager.commit(mtr);
            faultInjector.onBoundary(TableDmlProgressPhase.AFTER_COMMIT,
                    TableDmlSecondaryOperation.INSERT_OR_REVIVE, index.indexId());
        } catch (RuntimeException error) {
            if (mtr.state() == MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(mtr);
            }
            throw error;
        }
    }

    /**
     * 在独立短 MTR 内翻转 secondary entry 的 delete 位；本方法不物理删除记录，也不持有跨树页 latch。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>用独立只读 MTR 从 root 页头刷新结构快照，随后释放 root S latch。</li>
     *     <li>以 point-rewrite redo 预算开启单树写 MTR，按完整 physical key 执行 delete-mark 或 revive。</li>
     *     <li>成功提交 redo/dirty 状态；异常时回滚仍 ACTIVE 的 MTR，保持调用方 active undo 可重试。</li>
     * </ol>
     *
     * @param metadata 目标二级索引 exact-version descriptor 与紧凑 layout。
     * @param key      覆盖 logical key 和完整聚簇主键后缀的 physical identity。
     * @param deleted  {@code true} 发布 delete mark；{@code false} revive 同一物理 entry。
     * @throws DatabaseRuntimeException root 刷新、redo admission、entry 定位、页改写或 MTR 提交失败时抛出。
     */
    private void markSecondary(long tableId, long schemaVersion, SecondaryIndexMetadata metadata,
                               LogicalRecord entry, boolean deleted) {
        SearchKey key = metadata.layout().physicalKey(entry);
        if (secondaryMutations != null) {
            long indexId = metadata.index().indexId();
            faultInjector.onBoundary(TableDmlProgressPhase.BEFORE_MTR,
                    TableDmlSecondaryOperation.DELETE_MARK, indexId);
            secondaryMutations.setDeleteMark(tableId, schemaVersion, metadata, entry, deleted,
                    RedoBudgetPurpose.SECONDARY_INDEX);
            faultInjector.onBoundary(TableDmlProgressPhase.AFTER_COMMIT,
                    TableDmlSecondaryOperation.DELETE_MARK, indexId);
            return;
        }
        // 1. 刷新读与写 MTR 分离，避免持 root S latch 再申请写路径 latch。
        BTreeIndex index = refresh(metadata.index());
        faultInjector.onBoundary(TableDmlProgressPhase.BEFORE_MTR,
                TableDmlSecondaryOperation.DELETE_MARK, index.indexId());

        // 2. delete 位翻转不改变树形，使用覆盖单页记录头改写的 point-rewrite 预算。
        MiniTransaction mtr = mtrManager.begin(mtrManager.budgetFor(RedoBudgetPurpose.SECONDARY_INDEX,
                BTreeRedoBudgetEstimator.pointRewrite()));
        try {
            btree.setSecondaryDeleteMark(mtr, index, key, deleted);

            // 3. 正常提交物理状态；异常分支只终止 ACTIVE MTR，避免重复 rollback 已完成提交。
            mtrManager.commit(mtr);
            faultInjector.onBoundary(TableDmlProgressPhase.AFTER_COMMIT,
                    TableDmlSecondaryOperation.DELETE_MARK, index.indexId());
        } catch (RuntimeException error) {
            if (mtr.state() == MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(mtr);
            }
            throw error;
        }
    }

    /**
     * 从稳定 root page header 刷新索引结构 level；descriptor 中来自 DDL binding 的 level 只是创建时提示。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>创建独立只读 MTR，确保刷新过程不继承调用方写 MTR 的 latch/fix memo。</li>
     *     <li>读取 root 页头并校验 index id，生成仅 level 更新的不可变 descriptor。</li>
     *     <li>提交读 MTR 释放 fix/S latch 后返回；异常时终止 ACTIVE MTR，调用方不会拿到半有效快照。</li>
     * </ol>
     *
     * @param index 含稳定 root page id 的索引 descriptor；其 level 允许已过期。
     * @return root level 来自当前页头、其它 identity/schema/segment 不变的新 descriptor。
     * @throws DatabaseRuntimeException 页 fix/latch、页头归属校验或 MTR 释放失败时抛出。
     */
    private BTreeIndex refresh(BTreeIndex index) {
        // 1. 刷新使用独立只读 MTR，不与随后任何单树写阶段共享 memo。
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            // 2. root 页头是 level 的物理权威；index id 错配必须 fail-closed。
            BTreeIndex refreshed = rootSnapshots.refresh(read, index);

            // 3. 先提交并释放 root 资源，再把快照交给结构写预算与导航。
            mtrManager.commit(read);
            return refreshed;
        } catch (RuntimeException error) {
            if (read.state() == MiniTransactionState.ACTIVE) {
                mtrManager.rollbackUncommitted(read);
            }
            throw error;
        }
    }

    /** 前台已持有 exact-version 聚合；在可能产生 buffered mutation 前注册不可变 merge metadata。 */
    private void registerChangeBufferMetadata(TableIndexMetadata metadata) {
        if (secondaryMutations != null) {
            secondaryMutations.register(metadata);
        }
    }

    /**
     * 从完整表行按 clustered key definition 提取聚簇主键；不接受未经物化的用户搜索值作为锁 identity。
     *
     * @param row              已按 exact-version 表 schema 完整物化的聚簇行。
     * @param clusteredIndex   目标表的聚簇 descriptor；key part column id 被解释为完整表 ordinal。
     * @return 保持 key part 声明顺序的完整聚簇搜索键，可用于 current-read、事务锁与 row guard。
     * @throws DatabaseValidationException 构造出的搜索键为空时由 {@link SearchKey} 拒绝。
     * @throws IndexOutOfBoundsException metadata key ordinal 与完整行列数错配时抛出；上层应把该情况视为 metadata 损坏。
     */
    private static SearchKey keyFromRow(LogicalRecord row, BTreeIndex clusteredIndex) {
        List<cn.zhangyis.db.storage.record.type.ColumnValue> values = clusteredIndex.keyDef().parts().stream()
                .map(part -> row.columnValues().get(part.columnId().value())).toList();
        return new SearchKey(values);
    }

    /**
     * 使用与 B+Tree 一致的 comparator 判断 UPDATE 前后二级 logical key 是否等价。
     *
     * @param metadata 目标二级 exact-version metadata，提供 logical part 数、prefix、类型与 collation。
     * @param oldEntry 从锁定旧聚簇行投影的紧凑二级 entry。
     * @param newEntry 从命令新完整行投影的紧凑二级 entry。
     * @return comparator 按全部 logical part 判等时返回 {@code true}；此时无需生成二级 mutation。
     * @throws DatabaseValidationException entry/layout 形状或类型不匹配时抛出。
     */
    private boolean sameLogicalKey(SecondaryIndexMetadata metadata, LogicalRecord oldEntry, LogicalRecord newEntry) {
        SearchKey oldKey = metadata.layout().logicalKey(oldEntry);
        SearchKey newKey = metadata.layout().logicalKey(newEntry);
        IndexKeyDef logical = new IndexKeyDef(metadata.index().indexId(),
                metadata.index().keyDef().parts().subList(0, metadata.layout().logicalKeyPartCount()));
        return new cn.zhangyis.db.storage.btree.SearchKeyComparator(registry)
                .compare(oldKey, newKey, logical, metadata.index().schema()) == 0;
    }

    /**
     * 从发布计划中已经物化的紧凑 entry 生成稳定完整 physical identity。
     *
     * @param plan 含 exact-version layout 与目标 entry 的二级发布计划。
     * @return 覆盖 logical key 和完整聚簇主键后缀的搜索键。
     * @throws DatabaseValidationException 计划 entry 与 layout 不匹配时抛出。
     */
    private SearchKey secondaryPhysicalKey(SecondaryPlan plan) {
        return plan.secondary().layout().physicalKey(plan.entry());
    }

    /**
     * 构造表级写入统一使用的 current-read 请求。
     *
     * @param txnId   事务系统分配的稳定 write id，也是 LockManager owner identity。
     * @param txn     当前 ACTIVE 事务，提供隔离级别。
     * @param timeout 聚簇/unique 事务锁的最大等待时长。
     * @return 使用固定三次结构重定位上限的 current-read 请求。
     */
    private static BTreeCurrentReadRequest request(TransactionId txnId, Transaction txn, Duration timeout) {
        return new BTreeCurrentReadRequest(txnId, txn.isolationLevel(), timeout, 3);
    }

    /**
     * 在分配 write id、取得锁或访问页面前校验表级命令事务状态。
     *
     * @param txn 命令携带的事务；命令为 {@code null} 时调用方传入 {@code null} 以统一报告领域错误。
     * @throws DatabaseValidationException 事务缺失或不处于 ACTIVE 状态时抛出。
     */
    private static void requireActive(Transaction txn) {
        if (txn == null || txn.state() != TransactionState.ACTIVE) {
            throw new DatabaseValidationException("table DML requires an ACTIVE transaction");
        }
    }

    /**
     * 二级发布计划；目标 entry、旧 entry 与发布前态在聚簇 undo 规划前冻结，后续短 MTR 不再重新解释用户输入。
     *
     * @param secondary 目标二级索引 exact-version metadata。
     * @param entry     INSERT/UPDATE 要发布，或 DELETE 要标记的紧凑二级 entry。
     * @param oldEntry  UPDATE 变键前的紧凑 entry；INSERT/DELETE 不需要时为 {@code null}。
     * @param state     新完整物理 identity 在 unique 检查时的前态，决定 insert/revive 及 rollback inverse。
     */
    private record SecondaryPlan(SecondaryIndexMetadata secondary, LogicalRecord entry,
                                 LogicalRecord oldEntry, SecondaryPublishState state) {
        /**
         * 提取 UPDATE 旧 entry 的完整 physical identity。
         *
         * @return old entry 为空时返回 {@code null}；否则返回 logical key 加完整聚簇主键后缀的搜索键。
         * @throws DatabaseValidationException old entry 与当前 layout 不匹配时抛出。
         */
        private SearchKey oldPhysicalKey() {
            return oldEntry == null ? null : secondary.layout().physicalKey(oldEntry);
        }
    }
}
