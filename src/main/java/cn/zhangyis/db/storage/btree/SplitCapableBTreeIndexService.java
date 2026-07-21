package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.api.DiskSpaceManager;
import cn.zhangyis.db.storage.api.PageAllocationHint;
import cn.zhangyis.db.storage.api.SegmentRef;
import cn.zhangyis.db.storage.api.index.IndexPageAccess;
import cn.zhangyis.db.storage.api.index.IndexPageHandle;
import cn.zhangyis.db.storage.buf.PageLatchMode;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservation;
import cn.zhangyis.db.storage.fsp.reservation.SpaceReservationKind;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.page.FilePageHeader;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordEncoder;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.IndexPageHeader;
import cn.zhangyis.db.storage.record.page.RecordComparator;
import cn.zhangyis.db.storage.record.page.RecordCursor;
import cn.zhangyis.db.storage.record.page.RecordPage;
import cn.zhangyis.db.storage.record.page.RecordPageDeleter;
import cn.zhangyis.db.storage.record.page.RecordPageInserter;
import cn.zhangyis.db.storage.record.page.RecordPageKeyOrderValidator;
import cn.zhangyis.db.storage.record.page.RecordPageOverflowException;
import cn.zhangyis.db.storage.record.page.RecordPagePurger;
import cn.zhangyis.db.storage.record.page.RecordPageReorganizer;
import cn.zhangyis.db.storage.record.page.RecordPageSearch;
import cn.zhangyis.db.storage.record.page.RecordPageUpdater;
import cn.zhangyis.db.storage.record.page.RecordRef;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.page.UpdateOutcome;
import cn.zhangyis.db.storage.record.page.UpdateResult;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.LobCodec;
import cn.zhangyis.db.storage.record.type.TemporalKind;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.trx.lock.GapLockKey;
import cn.zhangyis.db.storage.trx.lock.NextKeyLockKey;
import cn.zhangyis.db.storage.trx.lock.RecordLockKey;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

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
    /** 既有 leaf/internal 页的 schema-aware record type、charset 与 key 顺序校验器。 */
    private final RecordPageKeyOrderValidator keyOrderValidator;

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
    /**
     * safe-node 早释放的祖先 X latch 计数（0.13d，设计 §10.2 step4-5）：悲观 insert 下降遇到 safe 节点时，
     * 释放其以上全部祖先（含 root）的 X latch 数累加。非权威状态、不影响正确性，仅供测试/观测确认 root X 不再持到 commit。
     */
    private final LongAdder safeNodeAncestorReleases = new LongAdder();
    /**
     * delete/purge 悲观 merge 下降的 safe-node 早释放祖先计数（0.13d）：下降遇到「移除一个 pointer 后仍不欠载」的
     * safe 内部节点时，释放其以上全部祖先 X 的数目累加。与 insert 侧分开计数便于测试定向断言；同为非权威诊断状态。
     */
    private final LongAdder safeNodeDeleteAncestorReleases = new LongAdder();
    /**
     * root SX 首遍下降计数（0.13d SX+restart，设计 §10.3 ROOT_LATCHED_SX）：快照树高 ≥2 的悲观 SMO 第一遍以 root SX
     * （与读者 root S 并存）下降的次数。非权威诊断状态。
     */
    private final LongAdder rootSxDescents = new LongAdder();
    /**
     * root X 重启计数（0.13d SX+restart）：SX 首遍链顶仍是 root（SMO 可能写 root，SX 禁原地升级）→ 零写整链释放、
     * 以 root X 重启第二遍的次数。restart 越少说明 safe-node 吸收越充分。非权威诊断状态。
     */
    private final LongAdder rootXRestarts = new LongAdder();

    /**
     * 创建 {@code SplitCapableBTreeIndexService}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param pageAccess 由组合根提供的 {@code IndexPageAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param disk 由组合根提供的 {@code DiskSpaceManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
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
        this.keyOrderValidator = new RecordPageKeyOrderValidator(registry);
    }

    /**
     * 聚簇 insert：用调用方事务 id 与 undo roll pointer 盖戳隐藏列（DB_TRX_ID=transactionId、
     * DB_ROLL_PTR=rollPointer），再走通用 {@link #insert}。
     *
     * <p>T1.3c 起不再恒写 {@link RollPointer#NULL}：roll pointer 由上层 orchestration
     * （{@code assignWriteId → UndoLogManager.planInsert/appendPlanned → insertClustered}）传入，指向本事务刚追加的
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param rollPointer 本事务 INSERT undo record 的 roll pointer；可为 {@link RollPointer#NULL}，不能为 Java null。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @return {@code insertClustered} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BTreeInsertResult insertClustered(MiniTransaction mtr, BTreeIndex index,
                                             LogicalRecord record, TransactionId transactionId,
                                             RollPointer rollPointer) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (record == null || transactionId == null || rollPointer == null) {
            throw new DatabaseValidationException(
                    "clustered insert record/transactionId/rollPointer must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (!index.clustered()) {
            throw new DatabaseValidationException(
                    "insertClustered requires a clustered index: " + index.indexId());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (transactionId.isNone()) {
            throw new DatabaseValidationException("clustered insert requires a non-NONE transaction id");
        }
        LogicalRecord stamped = new LogicalRecord(record.schemaVersion(), record.columnValues(),
                record.deleted(), record.recordType(),
                new HiddenColumns(transactionId, rollPointer));
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return insert(mtr, index, stamped);
    }

    /**
     * 发布一条紧凑二级索引 entry。二级 entry 必须由 {@link SecondaryIndexMetadata#layout()} 投影得到，
     * 不携带聚簇记录的 DB_TRX_ID/DB_ROLL_PTR；完整物理 key 已包含聚簇主键后缀，因而继续复用通用
     * {@link #insert} 的 duplicate 检查、split 和 root grow 逻辑。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 descriptor 确为非聚簇索引，并拒绝隐藏列或预置 delete-mark，防止把聚簇版本信息复制进二级页。</li>
     *     <li>委托通用插入路径按完整物理 key 检查冲突；delete-marked entry 仍会被视为占用 identity。</li>
     *     <li>复用既有 leaf insert/split/root grow，所有页修改和 redo/dirty 副作用仍归调用方当前 MTR。</li>
     * </ol>
     *
     * @param mtr    当前短物理事务；其提交/失败处理由上层表级 DML 编排负责。
     * @param index  二级索引物理描述符；必须为 physical unique 且非聚簇。
     * @param entry  layout 投影后的紧凑 entry；不得带隐藏列或 delete-mark。
     * @return 通用插入结果；root grow 后调用方必须继续使用 {@code indexAfterInsert}。
     * @throws DatabaseValidationException MTR/descriptor/entry 缺失，索引不是物理唯一二级树，或 entry 携带
     *                                     delete mark/聚簇隐藏列时抛出。
     * @throws BTreeDuplicateKeyException 完整 physical key 已被 live 或 delete-marked entry 占用时抛出。
     * @throws DatabaseRuntimeException 页导航、split/root-grow、FSP 分配或 MTR redo 记录失败时抛出。
     */
    public BTreeInsertResult insertSecondary(MiniTransaction mtr, BTreeIndex index, LogicalRecord entry) {
        // 1. 在获取任何 index page latch 前验证二级记录边界，非法输入不会留下页修改或 MTR memo。
        requireSecondaryIndex(index, "insertSecondary");
        if (entry == null || entry.hiddenColumns() != null || entry.deleted()) {
            throw new DatabaseValidationException(
                    "insertSecondary entry must be live, non-null and carry no clustered hidden columns");
        }

        // 2. 完整物理 key 的唯一检查由通用 insert 在目标 leaf X latch 内执行，marked entry 同样阻止重复发布。
        // 3. 页内插入、split、root grow 与 redo/dirty 发布完全复用成熟路径，不另建一套二级结构算法。
        return insert(mtr, index, entry);
    }

    /**
     * 为 LOB-aware INSERT 固定聚簇页路径与 SMO 资源，但不发布 placeholder row。placeholder 已包含定长 external
     * reference；prepare 用 {@link RollPointer#NULL} 盖同宽隐藏列完成 exact encoded-length/fit 判断。若 leaf 需 split，
     * 在仍持 index X 链时进入显式越序 scope 并预留最坏页数；该 scope 延续到 LOB 分配完成。
     *
     * <p>index→FSP/LOB 的无环证明：所有 LOB-aware writer 都先取得 index prepare guard，普通 SMO 也只沿
     * index→FSP 分配；LOB/FSP 代码不反向获取任何 B+Tree latch，读者只持 index latch 且不等待 FSP，故不存在
     * FSP→index 的反向等待边。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param placeholder 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @return {@code prepareClusteredInsert} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public PreparedClusteredInsert prepareClusteredInsert(MiniTransaction mtr, BTreeIndex index,
                                                          LogicalRecord placeholder,
                                                          TransactionId transactionId) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (mtr == null || index == null || placeholder == null || transactionId == null) {
            throw new DatabaseValidationException("prepare clustered insert args must not be null");
        }
        if (!index.clustered() || transactionId.isNone()) {
            throw new DatabaseValidationException("prepare clustered insert requires clustered index/write id");
        }
        LogicalRecord stampedPlaceholder = new LogicalRecord(placeholder.schemaVersion(), placeholder.columnValues(),
                placeholder.deleted(), placeholder.recordType(),
                new HiddenColumns(transactionId, RollPointer.NULL));
        SearchKey expectedKey = keyOf(stampedPlaceholder, index);
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        int expectedLength = recordEncoder.encode(stampedPlaceholder, index.schema()).length;
        List<IndexPageHandle> path = descendPathInsertSafeNode(mtr, index, expectedKey);
        IndexPageHandle leafHandle = path.getLast();
        RecordPage leaf = leafHandle.recordPage();
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        ensureUniqueAbsent(leaf, leafHandle.pageId(), index, expectedKey);
        boolean requiresSplit = expectedLength > leaf.freeSpace();

        var scope = mtr.allowOutOfOrderPageLatch(
                "prepared clustered insert: all writers acquire index before FSP/LOB; FSP/LOB never wait for index");
        SpaceReservation reservation = null;
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        try {
            if (requiresSplit) {
                reservation = reserveSplitSpace(mtr, index, splitReservationBudget(index, path));
            }
            SpaceReservation frozenReservation = reservation;
            return new PreparedClusteredInsert(mtr, scope, frozenReservation, (actual, rollPointer) -> {
                LogicalRecord stampedActual = new LogicalRecord(actual.schemaVersion(), actual.columnValues(),
                        actual.deleted(), actual.recordType(), new HiddenColumns(transactionId, rollPointer));
                SearchKey actualKey = keyOf(stampedActual, index);
                int actualLength = recordEncoder.encode(stampedActual, index.schema()).length;
                if (!actualKey.equals(expectedKey) || actualLength != expectedLength) {
                    throw new PreparedInsertStateException(
                            "actual clustered row changed key or encoded length after prepare");
                }
                if (requiresSplit) {
                    return splitLeafAndPropagate(mtr, index, path, stampedActual);
                }
                RecordRef ref = inserter.insert(leaf, leafHandle.pageId(), stampedActual,
                        index.keyDef(), index.schema());
                return new BTreeInsertResult(index, ref);
            });
        } catch (RuntimeException error) {
            if (reservation != null) {
                reservation.close();
            }
            scope.close();
            throw error;
        }
    }

    /**
     * 为 LOB-aware UPDATE 固定目标 leaf、旧版本 CAS 证据和 placeholder 物理形状，但不写 placeholder row。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验聚簇 descriptor、write id、旧隐藏列和 placeholder，盖 NULL roll pointer 后冻结 key/encoded length。</li>
     *     <li>以 X latch 定位目标 leaf，确认记录仍存在且隐藏列等于 current-read 证据；未命中按 stale 计划失败。</li>
     *     <li>进入 index→LOB/FSP 显式锁序 scope，返回只允许一次 actual publish 的 guard。</li>
     *     <li>publish 重算 actual key/length，盖真实隐藏列并在已固定 leaf 上执行所有权校验和整记录替换。</li>
     * </ol>
     *
     * @param mtr           覆盖 undo+B+Tree+LOB workload 的 ACTIVE 业务 MTR。
     * @param index         exact-version 聚簇索引 descriptor。
     * @param key           current-read 已锁定行的完整聚簇主键。
     * @param placeholder   新 LOB 首页号尚未知时的目标完整用户行。
     * @param transactionId 前向 UPDATE 的稳定 write id。
     * @param oldHidden     current-read 物化的旧 DB_TRX_ID/DB_ROLL_PTR CAS 证据。
     * @return 固定目标 leaf 和形状、等待 actual undo/row 发布的 guard。
     * @throws DatabaseValidationException 参数、descriptor、write id 或 placeholder 形状无效时抛出。
     * @throws PreparedUpdateStateException 目标记录在 prepare 前已消失、版本不匹配或 actual 形状漂移时抛出。
     * @throws BTreeUnsupportedStructureException 请求的语法形状、类型或运行模式超出当前实现范围时抛出；调用方应改写请求或选择受支持配置
     */
    public PreparedClusteredUpdate prepareClusteredUpdate(MiniTransaction mtr, BTreeIndex index,
                                                          SearchKey key, LogicalRecord placeholder,
                                                          TransactionId transactionId,
                                                          HiddenColumns oldHidden) {
        // 1. 所有纯值校验先于页导航；placeholder 隐藏列由本方法统一生成。
        if (mtr == null || index == null || key == null || placeholder == null
                || transactionId == null || oldHidden == null) {
            throw new DatabaseValidationException("prepare clustered update args must not be null");
        }
        if (!index.clustered() || transactionId.isNone() || placeholder.hiddenColumns() != null) {
            throw new DatabaseValidationException(
                    "prepare clustered update requires clustered index/write id/user row without hidden columns");
        }
        LogicalRecord stampedPlaceholder = new LogicalRecord(placeholder.schemaVersion(),
                placeholder.columnValues(), false, placeholder.recordType(),
                new HiddenColumns(transactionId, RollPointer.NULL));
        SearchKey expectedKey = keyOf(stampedPlaceholder, index);
        int expectedLength = recordEncoder.encode(stampedPlaceholder, index.schema()).length;
        if (!expectedKey.equals(key)) {
            throw new BTreeUnsupportedStructureException("prepared UPDATE cannot change clustered key");
        }

        // 2. 目标 leaf X latch 保持到 actual publish；事务行锁保证没有合法并发 writer 改写该版本。
        LeafLocation location = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        RecordPage leaf = location.page();
        validateLeafPage(leaf, index, location.pageId());
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            throw new PreparedUpdateStateException("prepared clustered update target disappeared before publish");
        }
        RecordCursor cursor = new RecordCursor(leaf, found.getAsInt(), index.schema(), registry);
        if (!oldHidden.dbTrxId().equals(cursor.dbTrxId())
                || !oldHidden.dbRollPtr().equals(cursor.dbRollPtr())) {
            throw new PreparedUpdateStateException("prepared clustered update target version changed");
        }

        // 3. 后续 LOB/FSP 只能沿 index→LOB 单向获取；scope 在 publish/close 中恰好释放一次。
        var scope = mtr.allowOutOfOrderPageLatch(
                "prepared clustered update: index latch precedes LOB/FSP and LOB/FSP never waits for index");
        try {
            return new PreparedClusteredUpdate(mtr, scope, (actual, rollPointer) -> {
                // 4. actual 只允许替换定宽 LOB 首页号；key/encoded length 漂移说明 deferred 计划失效。
                LogicalRecord stampedActual = new LogicalRecord(actual.schemaVersion(), actual.columnValues(),
                        false, actual.recordType(), new HiddenColumns(transactionId, rollPointer));
                SearchKey actualKey = keyOf(stampedActual, index);
                int actualLength = recordEncoder.encode(stampedActual, index.schema()).length;
                if (!actualKey.equals(expectedKey) || actualLength != expectedLength) {
                    throw new PreparedUpdateStateException(
                            "actual clustered UPDATE changed key or encoded length after prepare");
                }
                return replaceInLeaf(leaf, location.pageId(), index, key, stampedActual,
                        oldHidden.dbTrxId(), oldHidden.dbRollPtr());
            });
        } catch (RuntimeException error) {
            scope.close();
            throw error;
        }
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param key             目标聚簇 key。
     * @param expectedTrxId   期望的 DB_TRX_ID（本 undo 的写事务 id），不能为 null。
     * @param expectedRollPtr 期望的 DB_ROLL_PTR（正在应用的 undo roll pointer），不能为 Java null（可为 NULL 指针）。
     * @return {@link BTreeDeleteResult#removed()} 表示是否真正摘除了一条匹配记录。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BTreeDeleteResult deleteClustered(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                             TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (key == null || expectedTrxId == null || expectedRollPtr == null) {
            throw new DatabaseValidationException(
                    "deleteClustered key/expectedTrxId/expectedRollPtr must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (!index.clustered()) {
            throw new DatabaseValidationException(
                    "deleteClustered requires a clustered index: " + index.indexId());
        }
        // 0.13a：乐观优先——多层树 S-crab 到 leaf(X)，删后不欠载则仅改 leaf 即成（跳过 merge）；
        // 未命中/所有权不符=幂等 no-op（纯读，safe）。删后欠载需 merge（unsafe）或单 root leaf 时返回 null，
        // 回退悲观 X 下降 + safe-node 早释放祖先（0.13d，merge 只在保留链内传播、root X 不必持到 commit）。
        BTreeDeleteResult optimistic = tryOptimisticDelete(mtr, index, key, expectedTrxId, expectedRollPtr);
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (optimistic != null) {
            return optimistic;
        }
        List<IndexPageHandle> path = descendPathDeleteSafeNode(mtr, index, key);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code tryOptimisticDelete} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedTrxId 参与 {@code tryOptimisticDelete} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedRollPtr 参与 {@code tryOptimisticDelete} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code tryOptimisticDelete} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code BTreeDeleteResult} 结果
     */
    private BTreeDeleteResult tryOptimisticDelete(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                  TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        if (leafHandle == null) {
            return null; // root 即 leaf：交悲观
        }
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            optimisticDeleteHits.increment();
            return BTreeDeleteResult.noChange(index); // 幂等：未命中，纯读（safe）
        }
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
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
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param key 参与 {@code deleteInLeaf} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedTrxId 参与 {@code deleteInLeaf} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedRollPtr 参与 {@code deleteInLeaf} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code deleteInLeaf} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private BTreeDeleteResult deleteInLeaf(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path,
                                          SearchKey key, TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        IndexPageHandle leafHandle = path.get(path.size() - 1);
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        validateLeafPage(leaf, index, leafId);
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return BTreeDeleteResult.noChange(index);
        }
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        // 所有权校验：dbTrxId+dbRollPtr 同时匹配才是本 undo 插入的行；否则不删（幂等收敛）
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (!expectedTrxId.equals(cursor.dbTrxId()) || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            return BTreeDeleteResult.noChange(index);
        }
        // 已 delete-marked（半失败/重试）则跳过 deleteMark 直接 purge；否则先逻辑标记再物理摘除
        if (!cursor.isDeleted()) {
            deleter.deleteMark(leaf, offset);
        }
        purger.purge(leaf, offset);
        MergeOutcome outcome = reclaimAfterRemoval(mtr, index, path);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
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
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param key             目标聚簇 key。
     * @param expectedTrxId   删除该行的事务 id（undo 的 DB_TRX_ID），不能为 null。
     * @param expectedRollPtr 该 DELETE_MARK undo record 自身的 roll pointer（= 记录应有的 DB_ROLL_PTR），不能为 Java null。
     * @return {@link BTreeDeleteResult#removed()} 是否物理移除；false 表示确认 stale、未改任何内容。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BTreeDeleteResult purgeDeleteMarkedClustered(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                        TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (key == null || expectedTrxId == null || expectedRollPtr == null) {
            throw new DatabaseValidationException(
                    "purgeDeleteMarkedClustered key/expectedTrxId/expectedRollPtr must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        if (!index.clustered()) {
            throw new DatabaseValidationException(
                    "purgeDeleteMarkedClustered requires a clustered index: " + index.indexId());
        }
        // 0.13b：乐观优先，与 deleteClustered 同构（唯一区别：purge 不主动 deleteMark，严格校验未标记即 stale no-op）。
        // 悲观回退同样走 safe-node X 下降（0.13d）。
        BTreeDeleteResult optimistic = tryOptimisticPurge(mtr, index, key, expectedTrxId, expectedRollPtr);
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        if (optimistic != null) {
            return optimistic;
        }
        List<IndexPageHandle> path = descendPathDeleteSafeNode(mtr, index, key);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return purgeInLeaf(mtr, index, path, key, expectedTrxId, expectedRollPtr);
    }

    /**
     * 乐观 purge 尝试（0.13b）：{@link #descendOptimistic} S-crab 到 leaf(X)。与 {@link #tryOptimisticDelete} 同构，
     * 但**绝不主动 deleteMark**——严格校验（命中 + 仍 delete-marked + 隐藏列匹配）任一不符即确认 stale、纯读 no-op（safe）。
     *
     * <p>命中且严格校验通过 → {@link #deleteWouldUnderflow} 预判：不欠载则 {@link RecordPagePurger#purge} **跳过**
     * {@link #reclaimAfterRemoval}（仅 leaf 持 X）；欠载=unsafe **写页前**释放 leaf X → 返回 null 交悲观全 X（带 merge）。
     * root 即 leaf 返回 null 交悲观。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code tryOptimisticPurge} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedTrxId 参与 {@code tryOptimisticPurge} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedRollPtr 参与 {@code tryOptimisticPurge} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code tryOptimisticPurge} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code BTreeDeleteResult} 结果
     */
    private BTreeDeleteResult tryOptimisticPurge(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                 TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        if (leafHandle == null) {
            return null; // root 即 leaf：交悲观
        }
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            optimisticPurgeHits.increment();
            return BTreeDeleteResult.noChange(index); // 未命中，纯读（safe）
        }
        int offset = found.getAsInt();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        // purge 严格校验：仍 delete-marked 且隐藏列匹配才移除；未标记/不符=确认 stale，绝不主动 deleteMark。
        if (!cursor.isDeleted()
                || !expectedTrxId.equals(cursor.dbTrxId())
                || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            optimisticPurgeHits.increment();
            return BTreeDeleteResult.noChange(index); // stale，纯读（safe）
        }
        if (deleteWouldUnderflow(leaf, cursor, index)) {
            pageAccess.releaseHandle(mtr, leafHandle);
            pessimisticPurgeFallbacks.increment();
            return null;
        }
        purger.purge(leaf, offset);
        optimisticPurgeHits.increment();
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return BTreeDeleteResult.removed(index, List.of());
    }

    /** 乐观 purge 命中计数（诊断/观测；含 stale no-op 与不欠载物理移除）。 */
    long optimisticPurgeHitCount() {
        return optimisticPurgeHits.sum();
    }

    /** 乐观 purge 回退悲观计数（诊断/观测；移除后欠载需 merge）。 */
    long pessimisticPurgeFallbackCount() {
        return pessimisticPurgeFallbacks.sum();
    }

    /**
     * 在已定位 leaf（path 末项）上执行 purge 严格校验：命中 + 已 delete-marked + 隐藏列(DB_TRX_ID/DB_ROLL_PTR)匹配才物理摘除；
     * 否则不改任何内容（stale 收敛）。与 {@link #deleteInLeaf} 的差别：未标记记录在此**不**会被 deleteMark。
     * 物理摘除成功后同样触发欠载回收（{@link #reclaimAfterRemoval}）。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param key 参与 {@code purgeInLeaf} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedTrxId 参与 {@code purgeInLeaf} 的稳定领域标识 {@code TransactionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedRollPtr 参与 {@code purgeInLeaf} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code purgeInLeaf} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private BTreeDeleteResult purgeInLeaf(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path,
                                          SearchKey key, TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        IndexPageHandle leafHandle = path.get(path.size() - 1);
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return BTreeDeleteResult.noChange(index);
        }
        int offset = found.getAsInt();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        if (!cursor.isDeleted()
                || !expectedTrxId.equals(cursor.dbTrxId())
                || !expectedRollPtr.equals(cursor.dbRollPtr())) {
            return BTreeDeleteResult.noChange(index);
        }
        purger.purge(leaf, offset);
        MergeOutcome outcome = reclaimAfterRemoval(mtr, index, path);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return BTreeDeleteResult.removed(outcome.indexAfter(), outcome.freedPages());
    }

    /**
     * rollback 物理删除本次 statement 刚发布的 live 二级 entry。该入口要求命中 entry 仍为 live；若已被
     * delete-mark，说明 inverse 顺序、undo mutation state 或并发协调出现错误，返回 STATE_CONFLICT 而不改页。
     *
     * @param mtr   当前 rollback 短物理事务；方法不提交该 MTR。
     * @param index 非聚簇二级索引描述符。
     * @param key   待撤销 entry 的完整物理 key。
     * @return 物理删除状态以及 merge/root shrink 后的 index 快照。
     * @throws DatabaseValidationException MTR/key 缺失或 descriptor 不是物理唯一二级索引时抛出。
     * @throws DatabaseRuntimeException 页导航、物理摘链、merge/root-shrink、FSP 回收或 redo 记录失败时抛出。
     */
    public BTreeSecondaryRemovalResult deletePublishedSecondary(MiniTransaction mtr, BTreeIndex index,
                                                                 SearchKey key) {
        return removeSecondaryByState(mtr, index, key, false, "deletePublishedSecondary");
    }

    /**
     * purge 物理删除已 delete-marked 的二级 entry。live 命中属于 fail-closed 状态冲突：purge 不能像普通删除那样
     * 主动标记当前 entry，否则可能回收仍被较新聚簇版本需要的物理 identity；ABSENT 则是幂等成功证据。
     *
     * @param mtr   当前 purge 短物理事务；方法不提交该 MTR。
     * @param index 非聚簇二级索引描述符。
     * @param key   经版本链安全检查后允许回收的完整物理 key。
     * @return 物理删除状态以及 merge/root shrink 后的 index 快照。
     * @throws DatabaseValidationException MTR/key 缺失或 descriptor 不是物理唯一二级索引时抛出。
     * @throws DatabaseRuntimeException 页导航、物理摘链、merge/root-shrink、FSP 回收或 redo 记录失败时抛出。
     */
    public BTreeSecondaryRemovalResult purgeDeleteMarkedSecondary(MiniTransaction mtr, BTreeIndex index,
                                                                   SearchKey key) {
        return removeSecondaryByState(mtr, index, key, true, "purgeDeleteMarkedSecondary");
    }

    /**
     * 按预期 delete 状态物理摘除二级 entry，并复用聚簇删除已验证的乐观 leaf-only 与悲观 merge 路径。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>页导航前校验二级 descriptor、MTR 与完整物理 key，错误输入不会获取 latch 或产生 redo。</li>
     *     <li>多层树先 S-crab 到 leaf X，定位并检查 delete 状态；ABSENT/STATE_CONFLICT 立即只读返回。</li>
     *     <li>若删除后不欠载，在当前 leaf X 内直接 purge；若可能欠载，写页前释放 leaf 并重启悲观 safe-node X 下降。</li>
     *     <li>悲观路径物理摘链后复用 merge/root shrink/FSP 回收，返回新 level 快照与 freed pages。</li>
     * </ol>
     *
     * @param mtr             调用方拥有的单次 rollback/purge 物理事务；方法不提交或关闭它。
     * @param index           exact-version 物理唯一二级 descriptor；root level 应在调用前刷新。
     * @param key             覆盖 logical key 与完整聚簇主键后缀的 physical identity。
     * @param expectedDeleted true 表示 purge 只接收 marked entry；false 表示 rollback 只接收 live entry。
     * @param operation       仅用于领域校验消息，帮助恢复日志定位错误调用入口。
     * @return ABSENT/STATE_CONFLICT no-op，或携带结构后置快照和释放页的 REMOVED 结果。
     * @throws DatabaseValidationException 参数缺失或 descriptor 不满足二级物理唯一约束时抛出。
     * @throws DatabaseRuntimeException 页导航、摘链、结构回收或 redo 记录失败时抛出。
     */
    private BTreeSecondaryRemovalResult removeSecondaryByState(MiniTransaction mtr, BTreeIndex index,
                                                                SearchKey key, boolean expectedDeleted,
                                                                String operation) {
        // 1. 在任何页资源进入 MTR memo 前完成边界校验。
        requireSecondaryIndex(index, operation);
        if (mtr == null || key == null) {
            throw new DatabaseValidationException(operation + " mtr/key must not be null");
        }

        // 2. 乐观路径仅持 leaf X；明确 no-op 状态可直接返回，不需要悲观结构锁。
        BTreeSecondaryRemovalResult optimistic = tryOptimisticSecondaryRemoval(
                mtr, index, key, expectedDeleted);
        if (optimistic != null) {
            return optimistic;
        }

        // 3. null 表示 root 即 leaf，或删除可能造成欠载；两种情况都在写页前进入悲观 safe-node 下降。
        List<IndexPageHandle> path = descendPathDeleteSafeNode(mtr, index, key);

        // 4. 悲观 leaf 删除成功后复用既有 merge/root shrink 与 FSP 回收，不复制结构维护算法。
        return removeSecondaryInLeaf(mtr, index, path, key, expectedDeleted);
    }

    /**
     * 多层树的二级 entry 乐观物理删除。返回 null 仅表示必须在零页修改状态下改走悲观路径；
     * 非 null 结果均是完整终态，包括 ABSENT、STATE_CONFLICT 和不欠载的 REMOVED。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>S-crab 到目标 leaf 并只保留 leaf X；root 即 leaf 时返回 null 交给悲观路径。</li>
     *     <li>定位完整 physical key 并核对 delete 位；缺失/状态冲突不写页直接返回。</li>
     *     <li>预测删除后欠载；unsafe 时释放尚未修改的 leaf handle 并返回 null。</li>
     *     <li>safe 时在同一 leaf X 内满足 purger 前置条件并物理摘链，返回无结构变化的 REMOVED。</li>
     * </ol>
     *
     * @param mtr             调用方 rollback/purge 短物理事务。
     * @param index           多层物理唯一二级 descriptor。
     * @param key             待删除 entry 的完整 physical identity。
     * @param expectedDeleted purge 传 true 只删 marked；rollback 传 false 只删 live。
     * @return 完整 no-op/removed 结果；必须悲观重启时返回 {@code null}。
     * @throws DatabaseRuntimeException 页导航、记录解析或页内摘链失败时抛出。
     */
    private BTreeSecondaryRemovalResult tryOptimisticSecondaryRemoval(MiniTransaction mtr, BTreeIndex index,
                                                                       SearchKey key,
                                                                       boolean expectedDeleted) {
        // 1. 乐观下降释放祖先，只保留目标 leaf X；单 root leaf 没有 crab 收益，交悲观路径统一处理。
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        if (leafHandle == null) {
            return null;
        }
        // 2. 完整 physical key 与 delete 位都在写页前检查，no-op 分支不产生 redo/dirty。
        RecordPage leaf = leafHandle.recordPage();
        PageId leafId = leafHandle.pageId();
        validateLeafPage(leaf, index, leafId);
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return BTreeSecondaryRemovalResult.noChange(SecondaryEntryRemovalStatus.ABSENT, index);
        }
        RecordCursor cursor = new RecordCursor(leaf, found.getAsInt(), index.schema(), registry);
        if (cursor.isDeleted() != expectedDeleted) {
            return BTreeSecondaryRemovalResult.noChange(SecondaryEntryRemovalStatus.STATE_CONFLICT, index);
        }
        // 3. 欠载预测发生在任何 setDeleted/purge 前；unsafe 时显式提前释放 leaf 后再重启。
        if (deleteWouldUnderflow(leaf, cursor, index)) {
            pageAccess.releaseHandle(mtr, leafHandle);
            return null;
        }
        // 4. rollback live entry 在同一 leaf X 内临时标记后立即摘链，不向其它 MTR 暴露中间状态。
        if (!expectedDeleted) {
            // RecordPagePurger 的页内契约只接受 marked 记录；rollback 已在同一 leaf X 下确认目标仍为 live，
            // 因此先置位再立即摘链，不向其它 MTR 暴露中间状态，也不改变“错误 marked entry 不得删除”的外部语义。
            leaf.setDeleted(found.getAsInt(), true);
        }
        purger.purge(leaf, found.getAsInt());
        return BTreeSecondaryRemovalResult.removed(index, List.of());
    }

    /**
     * 在悲观 safe-node X 路径末端检查二级 entry 状态并物理摘除。状态检查先于任何页写；成功后才允许
     * merge/root shrink 向保留链传播，确保错误 inverse 不会产生半完成结构修改。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>取得悲观路径末端 leaf 并校验页归属/层级。</li>
     *     <li>按完整 physical key 定位并检查 delete 位；缺失或状态冲突不修改路径中任何页。</li>
     *     <li>满足状态前提后物理摘链；rollback live entry 的临时 mark 与摘链位于同一 leaf X 临界区。</li>
     *     <li>复用结构回收算法执行 merge/root-shrink/FSP 归还，返回新索引快照与释放页。</li>
     * </ol>
     *
     * @param mtr             调用方短物理事务，已持 safe-node 悲观路径 latch。
     * @param index           exact-version 物理唯一二级 descriptor。
     * @param path            从保留 safe node 到目标 leaf 的 X-latched handle 列表，末项必须是 leaf。
     * @param key             待删除 entry 的完整 physical identity。
     * @param expectedDeleted purge 传 true，rollback 传 false。
     * @return no-op 状态，或含 merge/root-shrink 后置快照的 REMOVED 结果。
     * @throws DatabaseRuntimeException 页校验、记录解析、摘链、结构回收或 redo 记录失败时抛出。
     */
    private BTreeSecondaryRemovalResult removeSecondaryInLeaf(MiniTransaction mtr, BTreeIndex index,
                                                               List<IndexPageHandle> path, SearchKey key,
                                                               boolean expectedDeleted) {
        // 1. 悲观路径末项必须是目标 leaf；页头归属错误按结构损坏 fail-closed。
        IndexPageHandle leafHandle = path.getLast();
        RecordPage leaf = leafHandle.recordPage();
        validateLeafPage(leaf, index, leafHandle.pageId());
        // 2. 完整 identity 与状态检查先于任何写，错误 inverse 不得触发 merge 或 dirty publish。
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return BTreeSecondaryRemovalResult.noChange(SecondaryEntryRemovalStatus.ABSENT, index);
        }
        RecordCursor cursor = new RecordCursor(leaf, found.getAsInt(), index.schema(), registry);
        if (cursor.isDeleted() != expectedDeleted) {
            return BTreeSecondaryRemovalResult.noChange(SecondaryEntryRemovalStatus.STATE_CONFLICT, index);
        }
        // 3. rollback live entry 在同一 leaf X 中完成临时 mark + 摘链，满足 RecordPagePurger 前置条件。
        if (!expectedDeleted) {
            // 与乐观路径相同：在同一 leaf X 下把 live rollback target 临时标记后立即物理摘链，满足 purger 前置条件。
            leaf.setDeleted(found.getAsInt(), true);
        }
        purger.purge(leaf, found.getAsInt());
        // 4. 只有物理摘链成功后才传播结构回收；结果携带调用方后续必须使用的新 root level。
        MergeOutcome outcome = reclaimAfterRemoval(mtr, index, path);
        return BTreeSecondaryRemovalResult.removed(outcome.indexAfter(), outcome.freedPages());
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code replaceClustered} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param deleted         目标 delete 位。
     * @param newHidden       目标隐藏列（DB_TRX_ID/DB_ROLL_PTR），不能为 null。
     * @param expectedTrxId   期望的当前 DB_TRX_ID，不能为 null。
     * @param expectedRollPtr 期望的当前 DB_ROLL_PTR，不能为 Java null。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code setClusteredDeleteMark} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code setClusteredDeleteMark} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public BTreeDeleteMarkResult setClusteredDeleteMark(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                        boolean deleted, HiddenColumns newHidden,
                                                        TransactionId expectedTrxId, RollPointer expectedRollPtr) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (key == null || newHidden == null || expectedTrxId == null || expectedRollPtr == null) {
            throw new DatabaseValidationException(
                    "setClusteredDeleteMark key/newHidden/expectedTrxId/expectedRollPtr must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (!index.clustered()) {
            throw new DatabaseValidationException("setClusteredDeleteMark requires a clustered index: " + index.indexId());
        }
        // 0.13b：乐观优先。delete-mark 是等长纯写（setDeleted+writeHiddenColumns），无 size 变化/无 overflow/无结构变更 → 恒 safe。
        // 只有 root 即 leaf 交悲观 findLeaf(X)；非法翻转与路径无关，抛出即上抛。
        BTreeDeleteMarkResult optimistic = tryOptimisticMark(mtr, index, key, deleted, newHidden,
                expectedTrxId, expectedRollPtr);
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        if (optimistic != null) {
            return optimistic;
        }
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
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
     * 翻转紧凑二级 entry 的 delete 位。二级记录没有隐藏列，完整物理 key 自身就是稳定 identity；
     * 方法只在目标 leaf X latch 内执行等长 header 写，不触发 split/merge，也不等待事务锁。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>在进入页导航前校验非聚簇 descriptor 与完整物理 key，非法输入不产生 latch/redo 副作用。</li>
     *     <li>多层树使用 S-crab 到 leaf X，单 root leaf 使用普通 X 定位；父页在写 leaf 前已经释放或由 MTR 管理。</li>
     *     <li>命中后先比较当前 delete 位；缺失和已处于目标状态均返回显式 no-op，不写页。</li>
     *     <li>仅在状态确需翻转时写 record header；页字节 redo、pageLSN 与 dirty publish 归当前 MTR。</li>
     * </ol>
     *
     * @param mtr     当前短物理事务；方法不提交该 MTR。
     * @param index   非聚簇二级索引描述符。
     * @param key     logical key + 完整聚簇主键后缀组成的物理 key。
     * @param deleted 目标 delete 状态；true 用于 DML，false 用于 revive/rollback。
     * @return CHANGED、ALREADY_IN_STATE 或 ABSENT；结果不暴露页内资源。
     * @throws DatabaseValidationException MTR/key 缺失或 descriptor 不是物理唯一二级索引时抛出。
     * @throws DatabaseRuntimeException 页导航、记录解析或页头改写/redo 记录失败时抛出。
     */
    public BTreeSecondaryDeleteMarkResult setSecondaryDeleteMark(MiniTransaction mtr, BTreeIndex index,
                                                                  SearchKey key, boolean deleted) {
        // 1. 页 latch 获取前完成模块边界和参数校验，失败时不污染 MTR memo。
        requireSecondaryIndex(index, "setSecondaryDeleteMark");
        if (mtr == null || key == null) {
            throw new DatabaseValidationException("setSecondaryDeleteMark mtr/key must not be null");
        }

        // 2. 多层树用 crab 只保留 leaf X；root 即 leaf 时由 findLeaf 获取唯一目标页 X。
        IndexPageHandle optimisticLeaf = descendOptimistic(mtr, index, key);
        if (optimisticLeaf != null) {
            // 3、4. leaf 内先分类再按需写 delete 位，no-op 分支不产生 redo/dirty 修改。
            return markSecondaryInLeaf(optimisticLeaf.recordPage(), optimisticLeaf.pageId(), index, key, deleted);
        }
        LeafLocation leaf = findLeaf(mtr, index, key, PageLatchMode.EXCLUSIVE);
        // 3、4. 单 root leaf 与多层树共享同一 plan-then-execute 实现。
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return markSecondaryInLeaf(leaf.page(), leaf.pageId(), index, key, deleted);
    }

    /**
     * 在已持 X latch 的二级 leaf 内执行 delete 位 plan-then-execute。先完整定位并分类，只有 CHANGED 分支才写页，
     * 因而 ABSENT/ALREADY_IN_STATE 可安全用于 statement/full/recovery rollback 重试。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 leaf 页头层级、index id 与 descriptor 归属。</li>
     *     <li>按完整 physical key 定位记录；缺失返回 ABSENT，不写页。</li>
     *     <li>比较当前 delete 位；已处于目标状态返回 ALREADY_IN_STATE，不写页。</li>
     *     <li>仅 CHANGED 分支翻转等长记录头位，redo/pageLSN/dirty 副作用归调用方 MTR。</li>
     * </ol>
     *
     * @param leaf    已由调用方以 X latch 打开的二级 leaf record page。
     * @param leafId  用于页归属校验和结构损坏诊断的物理 page id。
     * @param index   exact-version 物理唯一二级 descriptor。
     * @param key     待标记/复活 entry 的完整 physical identity。
     * @param deleted 目标 delete 位。
     * @return 显式 CHANGED、ALREADY_IN_STATE 或 ABSENT 结果。
     * @throws DatabaseRuntimeException 页结构校验、记录解析或页改写失败时抛出。
     */
    private BTreeSecondaryDeleteMarkResult markSecondaryInLeaf(RecordPage leaf, PageId leafId,
                                                                 BTreeIndex index, SearchKey key,
                                                                 boolean deleted) {
        // 1. 页头必须属于目标索引且 level=0，错配时不能继续解释记录格式。
        validateLeafPage(leaf, index, leafId);

        // 2. physical key 包含完整主键后缀，findEqual 至多定位一个二级 identity。
        OptionalInt found = search.findEqual(leaf, key, index.keyDef(), index.schema());
        if (found.isEmpty()) {
            return new BTreeSecondaryDeleteMarkResult(SecondaryDeleteMarkStatus.ABSENT);
        }
        // 3. 已在目标状态是 crash/rollback 重试的幂等完成证明，不重复写相同页字节。
        int offset = found.getAsInt();
        RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
        if (cursor.isDeleted() == deleted) {
            return new BTreeSecondaryDeleteMarkResult(SecondaryDeleteMarkStatus.ALREADY_IN_STATE);
        }
        // 4. 状态确需变化时才执行等长 header 改写。
        leaf.setDeleted(offset, deleted);
        return new BTreeSecondaryDeleteMarkResult(SecondaryDeleteMarkStatus.CHANGED);
    }

    /**
     * 点查索引。数据流：打开 root 并校验 header → level=0 直接查 leaf；
     * level=1 用 root node pointer 选择 leaf，再在 leaf 内执行页内等值查找。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code lookup} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code lookup} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     */
    @Override
    public Optional<BTreeLookupResult> lookup(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        return doLookup(mtr, index, key, false);
    }

    /**
     * 点查但**不过滤** delete-marked 当前版本（T1.3f，供 MVCC）。普通 {@link #lookup} 把 delete-marked 当作"消失"
     * 返回空；一致性读必须看到 delete-marked 当前版本（其 {@code DB_TRX_ID}/{@code DB_ROLL_PTR}）才能按 ReadView
     * 判可见性（可见删除→行消失；不可见删除→沿版本链取删除前版本）。返回的 {@code LogicalRecord.deleted()} 携带删除位。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code lookupIncludingDeleted} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code lookupIncludingDeleted} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
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
        // 0.13c 读路径 crab：S-crab 下降，祖先早释放，仅 leaf S 到 commit。
        LeafLocation leaf = findLeafSharedCrab(mtr, index, key);
        return lookupInLeaf(leaf.page(), leaf.pageId(), index, key, includeDeleted);
    }

    /**
     * current-read 专用点定位。调用方必须在本方法返回后提交/回滚 MTR 释放 page latch/fix，再用返回的
     * record/gap/next-key 值对象进入 LockManager；返回值不包含任何 cursor 或 page handle。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param includeDeleted true 时 delete-marked 同 key 也算命中（unique 物理检查）；false 时按普通当前读视为缺失。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code locatePointForCurrentRead} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code locatePointForCurrentRead} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    BTreeCurrentReadPosition locatePointForCurrentRead(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                       boolean includeDeleted) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (index == null || key == null) {
            throw new DatabaseValidationException("current-read locate index/key must not be null");
        }
        // 0.13c 读路径 crab：S-crab 下降定位 record/gap，祖先早释放。
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        LeafLocation leaf = findLeafSharedCrab(mtr, index, key);
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        OptionalInt found = search.findEqual(leaf.page(), key, index.keyDef(), index.schema());
        if (found.isPresent()) {
            RecordCursor cursor = new RecordCursor(leaf.page(), found.getAsInt(), index.schema(), registry);
            if (includeDeleted || !cursor.isDeleted()) {
                BTreeLookupResult result = materialize(index, leaf.pageId(), cursor);
                RecordLockKey recordKey = RecordLockKey.from(result.recordRef());
                GapLockKey gapKey = gapBeforeRecord(leaf.page(), index, found.getAsInt());
                return new BTreeCurrentReadPosition(Optional.of(result), Optional.of(recordKey), gapKey,
                        Optional.of(new NextKeyLockKey(recordKey, gapKey)));
            }
        }
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return new BTreeCurrentReadPosition(Optional.empty(), Optional.empty(),
                gapForSearchKey(leaf.page(), index, key), Optional.empty());
    }

    /**
     * current-read 专用范围定位。数据流：按 lower bound 找起始 leaf，沿 sibling 链扫描 legacy scan 相同的
     * 当前版本记录；对每条返回记录构造 record/preceding-gap/next-key lock key；再按 upper bound 重新定位
     * 终止 gap。调用方必须在返回后结束 MTR，再进入可能阻塞的事务锁等待。
     *
     * <p>简化点：终止 gap 仍使用当前页级 gap 表达，可能比 SQL 谓词略宽；后续 global gap ref 可进一步收窄。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换与受控 IO，把必要的 redo、dirty 或诊断副作用交给既有下游。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param range 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return {@code locateRangeForCurrentRead} 准备或解码出的中间领域对象；成功时不为 {@code null}，其边界、资源归属和后续发布阶段已明确
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    BTreeCurrentReadRangePosition locateRangeForCurrentRead(MiniTransaction mtr, BTreeIndex index,
                                                            BTreeScanRange range) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        if (index == null || range == null) {
            throw new DatabaseValidationException("current-read range index/range must not be null");
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (range.limit() == 0) {
            return new BTreeCurrentReadRangePosition(List.of(), Optional.empty());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换与受控 IO，并维持领域不变量。
        List<BTreeCurrentReadPosition> positions = new ArrayList<>();
        // 0.13c 读 crab：S-crab 下降到起始 leaf，沿 sibling 链 hand-over-hand 扫；祖先与前驱 leaf 均早释放。
        IndexPageHandle leafHandle = range.lowerBound().isPresent()
                ? descendSharedCrab(mtr, index, range.lowerBound().orElseThrow())
                : descendLeftmostSharedCrab(mtr, index);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        while (true) {
            RecordPage leaf = leafHandle.recordPage();
            PageId leafId = leafHandle.pageId();
            validateLeafPage(leaf, index, leafId);
            if (locateRangeLeaf(leaf, leafId, index, range, positions) || positions.size() >= range.limit()) {
                return new BTreeCurrentReadRangePosition(positions,
                        Optional.of(terminalGapForRange(mtr, index, range)));
            }
            long next = leafHandle.fileHeader().nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                return new BTreeCurrentReadRangePosition(positions,
                        Optional.of(terminalGapForRange(mtr, index, range)));
            }
            // hand-over-hand：先 latch 后继 leaf，再释放当前 leaf。
            PageId nextId = PageId.of(index.rootPageId().spaceId(), PageNo.of(next));
            IndexPageHandle nextHandle = openBTreePageOutOfOrder(mtr, nextId, index, PageLatchMode.SHARED,
                    "btree range current-read sibling hand-over-hand: next leaf is latched before predecessor release");
            pageAccess.releaseHandle(mtr, leafHandle);
            leafHandle = nextHandle;
        }
    }

    /**
     * 历史 scanLeaf 方法在 B3 后委托为真正 scan。leaf-only 语义仍由 LeafOnlyBTreeIndexService 保持。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param range 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    @Override
    public List<BTreeLookupResult> scanLeaf(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        return scan(mtr, index, range);
    }

    /**
     * 有界范围扫描。level=0 扫 root leaf；level=1 从 lowerKey 对应 leaf 开始，沿 FIL sibling next 链顺序扫。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param range 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    @Override
    public List<BTreeLookupResult> scan(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        return doScan(mtr, index, range, false);
    }

    /**
     * 扫描二级索引物理视图，保留 delete-marked entry。该入口供唯一前缀检查、rollback 和 purge 使用；
     * 返回值已完全物化，不携带 secondary page latch、Buffer fix 或 cursor。
     *
     * @param mtr   当前只读短 MTR；调用方必须先结束它，再进入可能阻塞的事务锁等待或聚簇回表。
     * @param index 非聚簇二级索引描述符。
     * @param range 完整物理 key 范围与结果上限。
     * @return 包含 live 与 delete-marked entry 的有序不可变物化结果。
     * @throws DatabaseValidationException range 缺失或 descriptor 不是物理唯一二级索引时抛出。
     * @throws DatabaseRuntimeException 页导航、sibling hand-over-hand 或记录物化失败时抛出。
     */
    public List<BTreeLookupResult> scanIncludingDeleted(MiniTransaction mtr, BTreeIndex index,
                                                        BTreeScanRange range) {
        requireSecondaryIndex(index, "scanIncludingDeleted");
        return doScan(mtr, index, range, true);
    }

    /**
     * 扫描聚簇索引并保留 delete-marked 当前版本，供 range MVCC 逐 identity 沿 undo 链恢复可见版本。
     *
     * @param mtr 调用方拥有的短只读 MTR；返回前不自动提交
     * @param index exact-version 聚簇索引
     * @param range 可无界或有界的批次范围
     * @return 不持 page 资源的当前物理候选，包含 delete-marked 行
     * @throws DatabaseValidationException descriptor 不是聚簇索引或参数缺失时抛出
     */
    public List<BTreeLookupResult> scanClusteredIncludingDeleted(
            MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        if (index == null || !index.clustered()) {
            throw new DatabaseValidationException(
                    "clustered including-deleted scan requires clustered index");
        }
        return doScan(mtr, index, range, true);
    }

    /**
     * 从最左 leaf 开始物化整棵树的 live record，供持 table MDL X 的离线 CREATE INDEX backfill 使用。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 MTR、descriptor 与正 limit；非法调用不取得 root/leaf 资源。</li>
     *     <li>使用 S-crab 下降到最左 leaf，取得 child 后立即释放 parent，避免保留整条祖先路径。</li>
     *     <li>按页内物理顺序物化 live record，delete-marked 记录跳过；达到 limit 时立即返回。</li>
     *     <li>沿 sibling 链 hand-over-hand 前进，链尾返回不可变结果且不泄露 page handle。</li>
     * </ol>
     *
     * <p>v1 简化：返回值在内存中完整物化，调用方应为教学实例配置受控表规模；后续 online/大型构建应改为
     * continuation batch cursor，不能把 page latch 跨写 MTR 持有。</p>
     *
     * @param mtr 调用方短只读 MTR；方法不提交
     * @param index 待扫描的 exact-version B+Tree descriptor
     * @param limit 最大 live record 数，必须为正
     * @return 从最左 leaf 开始按完整物理 key 排序的 live record
     * @throws DatabaseValidationException 参数缺失或 limit 非正时抛出
     * @throws DatabaseRuntimeException 页导航、物化或 sibling latch 失败时抛出
     */
    public List<BTreeLookupResult> scanAll(MiniTransaction mtr, BTreeIndex index, int limit) {
        // 1. 入口拒绝无效上限，避免 Integer.MAX_VALUE 之外的隐式“无限”语义。
        if (mtr == null || index == null || limit <= 0) {
            throw new DatabaseValidationException("BTree full scan requires mtr/index/positive limit");
        }

        // 2. 只保留最左 leaf S latch；祖先随 crab 下降提前释放。
        IndexPageHandle leafHandle = descendLeftmostSharedCrab(mtr, index);
        List<BTreeLookupResult> results = new ArrayList<>();
        while (true) {
            // 3. 页内顺序与 B+Tree comparator 一致，DDL backfill 只复制 live 聚簇版本。
            RecordPage leaf = leafHandle.recordPage();
            validateLeafPage(leaf, index, leafHandle.pageId());
            for (int offset : leaf.recordOffsetsInOrder()) {
                RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
                if (!cursor.materialize().deleted()) {
                    results.add(materialize(index, leafHandle.pageId(), cursor));
                    if (results.size() >= limit) {
                        return List.copyOf(results);
                    }
                }
            }

            // 4. sibling hand-over-hand 保证前驱释放前后继已固定；链尾完成全树物化。
            long next = leafHandle.fileHeader().nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                return List.copyOf(results);
            }
            PageId nextId = PageId.of(index.rootPageId().spaceId(), PageNo.of(next));
            IndexPageHandle nextHandle = openBTreePageOutOfOrder(mtr, nextId, index, PageLatchMode.SHARED,
                    "DDL full clustered scan sibling hand-over-hand");
            pageAccess.releaseHandle(mtr, leafHandle);
            leafHandle = nextHandle;
        }
    }

    /**
     * 扫描与 logical secondary key 等价的物理 entry，并保留 delete-marked 候选。因为二级物理 key 追加了
     * 完整聚簇主键，logical prefix 不是一个可直接传给普通 point lookup 的完整 key；本方法从最左 leaf 起按
     * B+Tree 顺序比较，命中段结束后立即停止。v1 优先保证 collation/prefix 正确性，后续可增加 prefix-aware root descent。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 metadata/layout/key/limit，构造只覆盖声明 logical parts 的比较定义。</li>
     *     <li>从 root S-crab 到最左 leaf，每次取得 child S 后立即释放 parent，避免全路径 pin。</li>
     *     <li>沿 sibling hand-over-hand 物化 entry，用与 B+Tree 相同的类型、prefix、collation、方向比较 logical key。</li>
     *     <li>收集 live/marked 候选至 limit；越过目标有序段或链尾时返回，不泄露 page handle/cursor。</li>
     * </ol>
     *
     * @param mtr        当前短只读 MTR；结束后再进入事务锁等待或聚簇回表。
     * @param metadata   exact-version 二级 descriptor 与 layout。
     * @param logicalKey 声明 logical key parts 的完整值。
     * @param limit      最大候选数；唯一检查通常取 2 以发现非法多主键状态。
     * @return 按完整物理 key 排序的物化候选，包含 delete-marked entry。
     * @throws DatabaseValidationException 参数缺失、limit 为负、descriptor/layout 或 logical part 数错配时抛出。
     * @throws DatabaseRuntimeException 页导航、sibling hand-over-hand、记录物化或 comparator 失败时抛出。
     */
    public List<BTreeLookupResult> scanSecondaryPrefixIncludingDeleted(MiniTransaction mtr,
                                                                        SecondaryIndexMetadata metadata,
                                                                        SearchKey logicalKey,
                                                                        int limit) {
        // 1. 构造 logical-prefix 比较定义；它引用 compact schema 的前 N 个连续字段。
        if (mtr == null || metadata == null || logicalKey == null || limit < 0) {
            throw new DatabaseValidationException("secondary prefix scan args are invalid");
        }
        BTreeIndex index = metadata.index();
        requireSecondaryIndex(index, "scanSecondaryPrefixIncludingDeleted");
        int logicalParts = metadata.layout().logicalKeyPartCount();
        if (logicalKey.size() != logicalParts) {
            throw new DatabaseValidationException("secondary logical key size " + logicalKey.size()
                    + " != declared parts " + logicalParts);
        }
        if (limit == 0) {
            return List.of();
        }
        IndexKeyDef logicalKeyDef = new IndexKeyDef(index.indexId(),
                index.keyDef().parts().subList(0, logicalParts));

        // 2. S-crab 到最左 leaf；祖先在 child 到手后立即释放，避免全树扫描持 root 到 MTR 结束。
        IndexPageHandle leafHandle = descendLeftmostSharedCrab(mtr, index);
        List<BTreeLookupResult> results = new ArrayList<>(Math.min(limit, 2));
        while (true) {
            // 3. 当前 leaf 内按完整物理顺序遍历，marked 与 live 都参与唯一/purge 的物理视图。
            RecordPage leaf = leafHandle.recordPage();
            validateLeafPage(leaf, index, leafHandle.pageId());
            for (int offset : leaf.recordOffsetsInOrder()) {
                RecordCursor cursor = new RecordCursor(leaf, offset, index.schema(), registry);
                LogicalRecord entry = cursor.materialize();
                SearchKey candidate = metadata.layout().logicalKey(entry);
                int compared = keyComparator.compare(candidate, logicalKey, logicalKeyDef, index.schema());
                if (compared < 0) {
                    continue;
                }
                if (compared > 0) {
                    // 4. B+Tree logical prefix 已越过目标；后续 sibling 只会更大，可立即收敛。
                    return List.copyOf(results);
                }
                results.add(materialize(index, leafHandle.pageId(), cursor));
                if (results.size() >= limit) {
                    return List.copyOf(results);
                }
            }

            // 4. 匹配段若在当前 leaf 结束，下一 sibling 仍需看首项才能确认越界；链尾则直接返回。
            long next = leafHandle.fileHeader().nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                return List.copyOf(results);
            }
            PageId nextId = PageId.of(index.rootPageId().spaceId(), PageNo.of(next));
            IndexPageHandle nextHandle = openBTreePageOutOfOrder(mtr, nextId, index, PageLatchMode.SHARED,
                    "secondary logical-prefix scan sibling hand-over-hand");
            pageAccess.releaseHandle(mtr, leafHandle);
            leafHandle = nextHandle;
        }
    }

    /**
     * scan 共用实现。沿 leaf sibling hand-over-hand 读取，每次只同时持当前与后继 leaf 的 S latch；
     * {@code includeDeleted} 只控制结果过滤，不改变页导航、比较器或范围终止语义。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验范围；limit=0 在页访问前返回空结果。</li>
     *     <li>按 lower bound S-crab 到起始 leaf，只保留 leaf S latch。</li>
     *     <li>逐页物化范围内记录，并按 includeDeleted 过滤；达到 upper bound/limit 时停止。</li>
     *     <li>未结束时先取得 next leaf S 再释放当前 leaf，链尾返回不可变结果。</li>
     * </ol>
     *
     * @param mtr            调用方短只读 MTR；方法不提交它。
     * @param index          目标索引 exact-version descriptor。
     * @param range          完整 lower/upper bound、开闭属性与结果上限。
     * @param includeDeleted {@code true} 保留 marked 物理记录；{@code false} 执行普通可见物理过滤。
     * @return 按完整索引 key 排序的不可变物化结果。
     * @throws DatabaseValidationException range 缺失时抛出。
     * @throws DatabaseRuntimeException 页导航、sibling latch 或记录物化失败时抛出。
     */
    private List<BTreeLookupResult> doScan(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range,
                                           boolean includeDeleted) {
        // 1. 空上限在任何 root/leaf fix 前返回，避免无意义占用 Buffer Pool。
        if (range == null) {
            throw new DatabaseValidationException("btree scan range must not be null");
        }
        if (range.limit() == 0) {
            return List.of();
        }
        // 任意树高（0.13c 读 crab）：S-crab 下降到 lowerKey 起始 leaf（祖先早释放），再沿 FIL sibling next 链
        // hand-over-hand 顺序扫（level 0 时起始 leaf 即 root，无 next）。
        // 2. lower bound 决定起始 leaf；S-crab 释放全部祖先，仅保留 leaf handle。
        IndexPageHandle leafHandle = range.lowerBound().isPresent()
                ? descendSharedCrab(mtr, index, range.lowerBound().orElseThrow())
                : descendLeftmostSharedCrab(mtr, index);
        ScanAccumulator acc = new ScanAccumulator(range.limit());
        while (true) {
            // 3. 当前 leaf 内统一执行边界比较、delete 过滤和 limit 累积。
            RecordPage leaf = leafHandle.recordPage();
            PageId leafId = leafHandle.pageId();
            validateLeafPage(leaf, index, leafId);
            boolean stop = scanLeafPage(leaf, leafId, index, range, acc, includeDeleted);
            if (stop || acc.full()) {
                break;
            }
            long next = leafHandle.fileHeader().nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                break;
            }
            // 4. hand-over-hand：读出 next 时仍持当前 leaf S；先 latch 后继 leaf，再释放当前 leaf。
            PageId nextId = PageId.of(index.rootPageId().spaceId(), PageNo.of(next));
            IndexPageHandle nextHandle = openBTreePageOutOfOrder(mtr, nextId, index, PageLatchMode.SHARED,
                    "btree scan sibling hand-over-hand: next leaf is latched before predecessor release");
            pageAccess.releaseHandle(mtr, leafHandle);
            leafHandle = nextHandle;
        }
        // 4. ScanAccumulator 返回不可变物化结果，不携带 cursor、page handle 或 Buffer fix。
        return acc.results();
    }

    /**
     * 插入逻辑记录（0.13a：写路径 latch coupling）。<b>乐观优先</b>：多层树先 {@link #tryOptimisticInsert} 走
     * S-crab 下降 + leaf X，放得下即成（仅 leaf 持 X，放开 root 处写并发）；leaf 溢出需 split（unsafe）或树只有单 root leaf
     * 时返回 null，回退 {@link #pessimisticInsert} 悲观全路径 X（现有 split 引擎不变）。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @return {@code insert} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param key 参与 {@code tryOptimisticInsert} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code tryOptimisticInsert} 未找到或条件不满足时返回 {@code null}；否则返回满足构造不变量的 {@code BTreeInsertResult} 结果
     */
    private BTreeInsertResult tryOptimisticInsert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record,
                                                  SearchKey key) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        IndexPageHandle leafHandle = descendOptimistic(mtr, index, key);
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        if (leafHandle == null) {
            return null; // root 即 leaf：交悲观（单页无并发收益）
        }
        RecordPage leaf = leafHandle.recordPage();
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        PageId leafId = leafHandle.pageId();
        ensureUniqueAbsent(leaf, leafId, index, key);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
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
     * 悲观插入：X 下降 + <b>safe-node 早释放祖先</b>（0.13d，{@link #descendPathInsertSafeNode}）——遇到 safe 祖先即释放其
     * 以上全部 X latch（含 root），保留链收缩为「safe 祖先 … leaf」；放得下直接插、溢出自底向上 split 传播（只在保留链内）。
     * 既是单 root leaf 树的基础路径，也是乐观 unsafe（split）后的重启路径；split 引擎与本次改动无关，行为不变。
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param key 参与 {@code pessimisticInsert} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code pessimisticInsert} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    private BTreeInsertResult pessimisticInsert(MiniTransaction mtr, BTreeIndex index, LogicalRecord record,
                                                SearchKey key) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        List<IndexPageHandle> path = descendPathInsertSafeNode(mtr, index, key);
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        IndexPageHandle leafHandle = path.get(path.size() - 1);
        RecordPage leaf = leafHandle.recordPage();
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        PageId leafId = leafHandle.pageId();
        ensureUniqueAbsent(leaf, leafId, index, key);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        try {
            RecordRef ref = inserter.insert(leaf, leafId, record, index.keyDef(), index.schema());
            return new BTreeInsertResult(index, ref);
        } catch (RecordPageOverflowException overflow) {
            SplitReservationBudget budget = splitReservationBudget(index, path);
            // overflow 前 leaf 未写，整条悲观下降链仍是干净 latch；先释放它再进入 FSP page0 reservation，
            // 避免持 index page latch 等待更低层空间管理页。
            releaseChain(mtr, path);
            try (SpaceReservation ignored = reserveSplitSpace(mtr, index, budget)) {
                List<IndexPageHandle> splitPath = descendPathInsertSafeNode(mtr, index, key);
                IndexPageHandle splitLeafHandle = splitPath.get(splitPath.size() - 1);
                RecordPage splitLeaf = splitLeafHandle.recordPage();
                PageId splitLeafId = splitLeafHandle.pageId();
                ensureUniqueAbsent(splitLeaf, splitLeafId, index, key);
                try {
                    RecordRef ref = inserter.insert(splitLeaf, splitLeafId, record, index.keyDef(), index.schema());
                    return new BTreeInsertResult(index, ref);
                } catch (RecordPageOverflowException stillOverflow) {
                    requireLeafSegment(index);
                    return splitLeafAndPropagate(mtr, index, splitPath, record);
                }
            }
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

    /** safe-node 早释放的祖先 X latch 计数（0.13d，诊断/观测；&gt;0 表示悲观 split 未把 root X 持到 commit）。 */
    long safeNodeAncestorReleaseCount() {
        return safeNodeAncestorReleases.sum();
    }

    /** delete/purge 悲观 merge 下降的 safe-node 早释放祖先计数（0.13d，诊断/观测；&gt;0 表示 merge 未把 root X 持到 commit）。 */
    long safeNodeDeleteAncestorReleaseCount() {
        return safeNodeDeleteAncestorReleases.sum();
    }

    /** root SX 首遍下降计数（0.13d SX+restart，诊断/观测；&gt;0 表示悲观 SMO 以 root SX 起步、读者不被 root 阻塞）。 */
    long rootSxDescentCount() {
        return rootSxDescents.sum();
    }

    /** root X 重启计数（0.13d SX+restart，诊断/观测；SX 首遍链顶仍是 root 时的第二遍 X 重启次数）。 */
    long rootXRestartCount() {
        return rootXRestarts.sum();
    }

    /** root leaf 页满时的稳定 root split：旧 root 内容物化到内存，root 重建为 level-1，两个新 leaf 保存旧/新记录。 */
    private BTreeInsertResult splitRootLeaf(MiniTransaction mtr, BTreeIndex index, IndexPageHandle rootHandle,
                                           LogicalRecord inserted) {
        RecordPage oldRootLeaf = rootHandle.recordPage();
        FilePageHeader oldRootHeader = rootHandle.fileHeader();
        List<LogicalRecord> existing = materializeLeafRecords(oldRootLeaf, index);
        PageAllocationHint leafHint = leafSplitAllocationHint(index.rootPageId(), oldRootHeader, index,
                existing, inserted, 2L);
        List<LogicalRecord> records = sortedWithInserted(existing, inserted, index);
        SplitRows split = splitRows(records);

        PageId leftId = allocateSmoPage(mtr, index.leafSegment(), leafHint);
        RecordPage leftPage = createSmoIndexPage(mtr, leftId, index.indexId(), 0);
        PageId rightId = allocateSmoPage(mtr, index.leafSegment(), leafHint);
        RecordPage rightPage = createSmoIndexPage(mtr, rightId, index.indexId(), 0);
        IndexPageHandle leftHandle = pageAccess.openIndexPageHandle(mtr, leftId, PageLatchMode.EXCLUSIVE);
        IndexPageHandle rightHandle = pageAccess.openIndexPageHandle(mtr, rightId, PageLatchMode.EXCLUSIVE);
        BTreeRedoDeltas.writeSiblingLinks(mtr, leftHandle, index.indexId(),
                FilePageHeader.FIL_NULL, rightId.pageNo().value(), "btree root leaf split left sibling link");
        BTreeRedoDeltas.writeSiblingLinks(mtr, rightHandle, index.indexId(),
                leftId.pageNo().value(), FilePageHeader.FIL_NULL, "btree root leaf split right sibling link");

        RecordRef insertedRef = insertAll(leftPage, leftId, split.left(), index, inserted);
        RecordRef rightInsertedRef = insertAll(rightPage, rightId, split.right(), index, inserted);
        if (insertedRef == null) {
            insertedRef = rightInsertedRef;
        }

        RecordPage root = rootHandle.recordPage();
        root.format(index.indexId(), 1);
        BTreeRedoDeltas.writeSiblingLinks(mtr, rootHandle, index.indexId(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, "btree root leaf split root sibling reset");
        BTreeIndex after = index.withRootLevel(1);
        insertPointer(root, index.rootPageId(), after, new BTreeNodePointer(lowKey(split.left(), index), leftId));
        insertPointer(root, index.rootPageId(), after, new BTreeNodePointer(lowKey(split.right(), index), rightId));
        BTreeRedoDeltas.captureNodePointerPage(mtr, root, index.rootPageId(), index.indexId(), true,
                "btree root leaf split final node pointers");
        return new BTreeInsertResult(index, insertedRef, after, true, List.of(leftId, rightId));
    }

    /**
     * leaf 溢出后的 split 传播入口（任意树高）。调用方已经在不持 index latch 时完成 space reservation。leaf 即 root（path 仅 root）→ 原地长高（level 0→1，复用
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

    /**
     * 0.14b/0.23a split/root split 空间预留：在任何 split 页内容改写、sibling 链调整或 parent separator 插入前，
     * 且不持 index page latch 时，先按本次保留链的最坏传播范围预留可能创建的数据页。MTR 当前没有 content undo，若等到中途
     * {@code allocatePage} 才发现 ENOSPC，已重写的 leaf/root 无法自动撤回；因此 reserve 必须早于所有结构修改。
     *
     * <p>预算是保守上界而非精确页数：root leaf split 需要 2 个 leaf；非 root leaf split 需要 1 个 leaf；
     * 保留链中的非 root 内部层每层最多 split 出 1 个 sibling；若链顶是真 root，则 root internal split 最多再创建
     * 2 个 non-leaf child。safe-node 截断链顶不是 root 时，safe 判据保证传播停在该节点，不为已释放祖先预算。
     */
    private SplitReservationBudget splitReservationBudget(BTreeIndex index, List<IndexPageHandle> path) {
        requireLeafSegment(index);
        int depth = path.size() - 1;
        long leafPages = depth == 0 ? 2L : 1L;
        long nonLeafPages = 0L;
        if (depth > 0) {
            nonLeafPages = depth - 1L;
            if (path.get(0).pageId().equals(index.rootPageId())) {
                nonLeafPages += 2L;
            }
        }
        if (nonLeafPages > 0L) {
            requireNonLeafSegment(index);
        }
        // 0.23a：overflow 后会先释放干净的 index latch，再 reserve，再重新下降。释放窗口内并发 SMO 可能改变树高
        // 或 safe-node 截断点；capacity 本就按完整 extent 承诺，因此页 quota 也保守给到单 extent，避免重进后
        // 合法 split 因旧路径预算过窄在中途触发 SpaceReservationExceeded。
        return new SplitReservationBudget(Math.max(leafPages + nonLeafPages, pageSize.pagesPerExtent()));
    }

    private SpaceReservation reserveSplitSpace(MiniTransaction mtr, BTreeIndex index, SplitReservationBudget budget) {
        return disk.reserveSpace(mtr, index.rootPageId().spaceId(), SpaceReservationKind.NORMAL,
                budget.pages(), 0L);
    }

    /**
     * B+Tree SMO 新页分配的 page-latch-order 例外。split/root split 已在不持 index latch 时完成容量 reservation；
     * 真正 allocatePage 仍需在持保留链 X latch 时触碰 FSP page0/page2。该逆序不会形成等待环，因为 FSP/DiskSpaceManager
     * 不反向获取 B+Tree index page latch，且 SMO 的 index latch 等待边已由 safe-node/SX 协议证明无环。
     */
    private PageId allocateSmoPage(MiniTransaction mtr, SegmentRef segment) {
        return allocateSmoPage(mtr, segment, PageAllocationHint.none());
    }

    /**
     * B+Tree SMO 新页分配的 page-latch-order 例外，带可选分配 hint。hint 只用于 leaf split 的物理邻近性和批量
     * extent 策略；internal/root split 继续传 none，避免把元数据页增长误判为 leaf 顺序写入。
     */
    private PageId allocateSmoPage(MiniTransaction mtr, SegmentRef segment, PageAllocationHint hint) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "btree SMO page allocation: FSP metadata never waits for B+Tree index latches")) {
            return disk.allocatePage(mtr, segment, hint);
        }
    }

    /**
     * 格式化刚分配的 SMO 新页。该页尚未挂入 parent pointer 或 leaf sibling 链，即使物理页号低于当前保留链，
     * 也不会有其它线程先持有它再回等当前 index latch；因此与 allocation 使用同一类局部越序证明。
     */
    private RecordPage createSmoIndexPage(MiniTransaction mtr, PageId pageId, long indexId, int level) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "btree SMO page format: freshly allocated index page is not visible to other btree readers yet")) {
            return pageAccess.createIndexPage(mtr, pageId, indexId, level);
        }
    }

    /**
     * B+Tree SMO 回收 victim/child 页的 page-latch-order 例外。调用方已持有 parent/survivor/root 等 index X latch，
     * {@code freePage} 会访问 FSP page0/page2 等低页号元数据；FSP 不会反向等待 index latch，因此不会形成等待环。
     */
    private void freeSmoPage(MiniTransaction mtr, SegmentRef segment, PageId pageId) {
        try (var ignored = mtr.allowOutOfOrderPageLatch(
                "btree SMO page free: FSP metadata never waits for B+Tree index latches")) {
            disk.freePage(mtr, segment, pageId);
        }
    }

    /**
     * B+Tree 导航/兄弟页维护的 page-latch-order 例外入口。B+Tree 的正确性依赖 hand-over-hand：释放父页或前驱页之前，
     * 必须先 latch 到即将使用的子页/后继页；物理页号与 key 顺序无关，不能用 PageId 升序替代 B+Tree 的局部无环证明。
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param reason 传给 {@code openBTreePageOutOfOrder} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code openBTreePageOutOfOrder} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    private IndexPageHandle openBTreePageOutOfOrder(MiniTransaction mtr, PageId pageId, BTreeIndex index,
                                                    PageLatchMode mode, String reason) {
        IndexPageHandle handle;
        try (var ignored = mtr.allowOutOfOrderPageLatch(reason)) {
            handle = pageAccess.openIndexPageHandle(mtr, pageId, mode);
        }
        validateExistingBTreePage(handle, index);
        return handle;
    }

    /**
     * 在既有 INDEX 页的 S/X latch 内完成 B+Tree 元数据与 schema-aware 用户链校验。
     *
     * <p>数据流：{@link IndexPageAccess} 已先完成物理结构校验；本方法随后核对 indexId，并按页面实际 level 选择
     * leaf schema 或派生 node-pointer schema。校验早于 child 选择、record search 和任何写页/redo 收集。
     * 页实际 level 是树高权威，不能使用可能因 root shrink 而陈旧的 index.rootLevel 快照。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验表空间生命周期、页号、区段身份与容量边界，非法或损坏元数据在分配/IO 前拒绝。</li>
     *     <li>按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，避免锁序反转。</li>
     *     <li>执行空间元数据或物理文件变化，并把需要的 allocation intent、redo、dirty 或 force 副作用交给既有下游。</li>
     *     <li>发布稳定结果并逆序释放 lease、latch 与 fix；失败保留可由恢复流程识别的权威状态。</li>
     * </ol>
     *
     * @param handle 调用方持有的 {@code IndexPageHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws BTreeStructureCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private void validateExistingBTreePage(IndexPageHandle handle, BTreeIndex index) {
        // 1、校验表空间生命周期、页号、区段身份与容量边界，在共享或持久副作用前拒绝非法状态。
        if (handle == null || index == null) {
            throw new DatabaseValidationException("btree existing page handle/index must not be null");
        }
        RecordPage page = handle.recordPage();
        // 2、继续完成范围、身份与候选校验；通过后，按 tablespace lease、space header、XDES、INODE 与数据页顺序取得受控资源，保持处理顺序与资源边界。
        IndexPageHeader header = page.header();
        if (header.indexId() != index.indexId()) {
            throw new BTreeStructureCorruptedException("index page id mismatch at " + handle.pageId()
                    + ": page=" + header.indexId() + " expected=" + index.indexId());
        }
        // 3、在中间分支复核阶段性结果；满足条件后，执行空间元数据或物理文件变化，并维持领域不变量。
        if (header.level() == 0) {
            keyOrderValidator.validate(handle.pageId(), page, index.schema(), index.keyDef(),
                    RecordType.CONVENTIONAL);
            return;
        }
        BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
        // 4、发布稳定结果并逆序释放 lease、latch 与 fix，以稳定返回或领域异常完成收口。
        keyOrderValidator.validate(handle.pageId(), page, pointerSchema.schema(), pointerSchema.keyDef(),
                RecordType.NODE_POINTER);
    }

    /** split/root split 预留预算。
     *
     * @param pages 参与 {@code 构造} 的上界或规格值 {@code pages}；必须非负且不能使容量、页数或编码长度计算溢出
     */
    private record SplitReservationBudget(long pages) {
    }

    /** 非 root leaf split：旧 leaf 改写为左半、分配新右兄弟存右半、维护双向 sibling 链；返回新插入 ref + 右半 lowKey + 新右兄弟页号。 */
    private LeafSplitResult splitNonRootLeaf(MiniTransaction mtr, BTreeIndex index, IndexPageHandle oldLeafHandle,
                                             LogicalRecord inserted, List<PageId> allocated) {
        RecordPage oldLeaf = oldLeafHandle.recordPage();
        PageId oldLeafId = oldLeafHandle.pageId();
        FilePageHeader oldLeafHeader = oldLeafHandle.fileHeader();
        List<LogicalRecord> existing = materializeLeafRecords(oldLeaf, index);
        PageAllocationHint leafHint = leafSplitAllocationHint(oldLeafId, oldLeafHeader, index, existing, inserted, 1L);
        List<LogicalRecord> records = sortedWithInserted(existing, inserted, index);
        SplitRows split = splitRows(records);

        PageId newLeafId = allocateSmoPage(mtr, index.leafSegment(), leafHint);
        allocated.add(newLeafId);
        RecordPage newLeaf = createSmoIndexPage(mtr, newLeafId, index.indexId(), 0);
        IndexPageHandle newLeafHandle = pageAccess.openIndexPageHandle(mtr, newLeafId, PageLatchMode.EXCLUSIVE);

        oldLeaf.format(index.indexId(), 0);
        BTreeRedoDeltas.writeSiblingLinks(mtr, oldLeafHandle, index.indexId(),
                oldLeafHeader.prevPageNo(), newLeafId.pageNo().value(), "btree non-root leaf split left link");
        BTreeRedoDeltas.writeSiblingLinks(mtr, newLeafHandle, index.indexId(),
                oldLeafId.pageNo().value(), oldLeafHeader.nextPageNo(), "btree non-root leaf split new right link");
        if (oldLeafHeader.nextPageNo() != FilePageHeader.FIL_NULL) {
            PageId rightSiblingId = PageId.of(oldLeafId.spaceId(), PageNo.of(oldLeafHeader.nextPageNo()));
            IndexPageHandle rightSibling = openBTreePageOutOfOrder(mtr, rightSiblingId, index,
                    PageLatchMode.EXCLUSIVE,
                    "btree split FIL right sibling repair: right links are followed in one direction");
            BTreeRedoDeltas.writeSiblingLinks(mtr, rightSibling, index.indexId(),
                    newLeafId.pageNo().value(), rightSibling.fileHeader().nextPageNo(),
                    "btree non-root leaf split far right repair");
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
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param depth 参与 {@code insertSeparator} 的树层级或递归深度 {@code depth}；必须非负且不得超过当前页结构、MTR memo 或解析器声明的最大深度
     * @param separator 参与 {@code insertSeparator} 的稳定领域标识 {@code BTreeNodePointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param allocated 参与 {@code insertSeparator} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @return {@code insertSeparator} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    private BTreeIndex insertSeparator(MiniTransaction mtr, BTreeIndex index, List<IndexPageHandle> path,
                                       int depth, BTreeNodePointer separator, List<PageId> allocated) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        IndexPageHandle parentHandle = path.get(depth);
        RecordPage parent = parentHandle.recordPage();
        if (pointerFits(parent, index, separator)) {
            insertPointer(parent, parentHandle.pageId(), index, separator);
            BTreeRedoDeltas.captureNodePointerPage(mtr, parent, parentHandle.pageId(), index.indexId(),
                    parentHandle.pageId().equals(index.rootPageId()),
                    "btree separator insertion final parent image");
            return index;
        }
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        requireNonLeafSegment(index);
        List<BTreeNodePointer> combined = materializePointers(parent, index);
        combined.add(separator);
        combined.sort((a, b) -> keyComparator.compare(a.lowKey(), b.lowKey(), index.keyDef(), index.schema()));
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        SplitPointers split = splitPointers(combined);
        if (depth == 0) {
            return growRootWithInternal(mtr, index, parentHandle, split, allocated);
        }
        PageId newSiblingId = splitNonRootInternal(mtr, index, parentHandle, split, allocated);
        BTreeNodePointer upSeparator = new BTreeNodePointer(split.right().get(0).lowKey(), newSiblingId);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return insertSeparator(mtr, index, path, depth - 1, upSeparator, allocated);
    }

    /** root（非叶，level L）原地长高：分配两个 level-L 新子页存左右半 pointer，root 页号不变重建为 level L+1 两 pointer。 */
    private BTreeIndex growRootWithInternal(MiniTransaction mtr, BTreeIndex index, IndexPageHandle rootHandle,
                                            SplitPointers split, List<PageId> allocated) {
        int oldLevel = index.rootLevel();
        PageId leftId = allocateSmoPage(mtr, index.nonLeafSegment());
        allocated.add(leftId);
        RecordPage leftPage = createSmoIndexPage(mtr, leftId, index.indexId(), oldLevel);
        PageId rightId = allocateSmoPage(mtr, index.nonLeafSegment());
        allocated.add(rightId);
        RecordPage rightPage = createSmoIndexPage(mtr, rightId, index.indexId(), oldLevel);
        writePointers(leftPage, leftId, split.left(), index);
        writePointers(rightPage, rightId, split.right(), index);
        BTreeRedoDeltas.captureNodePointerPage(mtr, leftPage, leftId, index.indexId(), false,
                "btree internal root split final left child image");
        BTreeRedoDeltas.captureNodePointerPage(mtr, rightPage, rightId, index.indexId(), false,
                "btree internal root split final right child image");

        RecordPage root = rootHandle.recordPage();
        root.format(index.indexId(), oldLevel + 1);
        BTreeRedoDeltas.writeSiblingLinks(mtr, rootHandle, index.indexId(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, "btree internal root split sibling reset");
        BTreeIndex after = index.withRootLevel(oldLevel + 1);
        insertPointer(root, index.rootPageId(), index, new BTreeNodePointer(split.left().get(0).lowKey(), leftId));
        insertPointer(root, index.rootPageId(), index, new BTreeNodePointer(split.right().get(0).lowKey(), rightId));
        BTreeRedoDeltas.captureNodePointerPage(mtr, root, index.rootPageId(), index.indexId(), true,
                "btree internal root split final root image");
        return after;
    }

    /** 非 root 内部页 split：该页改写为左半 pointer、分配新右兄弟存右半 pointer（非叶页不参与 leaf sibling 链）；返回新右兄弟页号。 */
    private PageId splitNonRootInternal(MiniTransaction mtr, BTreeIndex index, IndexPageHandle nodeHandle,
                                        SplitPointers split, List<PageId> allocated) {
        RecordPage node = nodeHandle.recordPage();
        int level = node.header().level();
        PageId newSiblingId = allocateSmoPage(mtr, index.nonLeafSegment());
        allocated.add(newSiblingId);
        RecordPage newSibling = createSmoIndexPage(mtr, newSiblingId, index.indexId(), level);
        node.format(index.indexId(), level);
        writePointers(node, nodeHandle.pageId(), split.left(), index);
        writePointers(newSibling, newSiblingId, split.right(), index);
        BTreeRedoDeltas.captureNodePointerPage(mtr, node, nodeHandle.pageId(), index.indexId(), false,
                "btree non-root internal split final left image");
        BTreeRedoDeltas.captureNodePointerPage(mtr, newSibling, newSiblingId, index.indexId(), false,
                "btree non-root internal split final right image");
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
    // 锁：悲观 X 下降（0.13d safe-node：保留链 = 「最近 delete-safe 内部节点 … leaf」，safe 节点以上祖先已早释放）+
    // 额外取 sibling/远兄弟/child 的 X，均入 MTR memo，commit 统一释放。merge 传播只在保留链内自底向上，safe 链顶必吸收。
    // redo：leaf row 仍用 PAGE_BYTES/PAGE_INIT；internal/root 完整结构动作结束后追加 page-local node/root after-image，
    // recovery 只 patch 页面，不重新运行 merge/redistribute/shrink 决策。
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
            // 链顶：真 root（root 不与兄弟 merge、允许欠载）或 delete-safe 节点（判据保证摘一指针后仍不欠载）。均收工。
            return index;
        }
        IndexPageHandle nodeHandle = path.get(depth);
        RecordPage node = nodeHandle.recordPage();
        if (!isUnderfull(node)) {
            return index;
        }
        IndexPageHandle parentHandle = path.get(depth - 1);
        RecordPage parent = parentHandle.recordPage();
        // 0.13d safe-node 后 path 可能是被截断的保留链（下标 0 = 最近 safe 节点而非 root），root 判定必须按页号
        // （root 页号跨 split/shrink 恒稳定），不能按链下标——否则会对非 root 的 safe 链顶误做 shrinkRoot（结构损坏）。
        boolean parentIsRoot = parentHandle.pageId().equals(index.rootPageId());

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
            redistribute(mtr, index, pair, leaf, parent, parentHandle.pageId(), parentIsRoot);
            return index;
        }
        if (leaf) {
            mergeLeaf(mtr, index, pair);
        } else {
            mergeInternal(mtr, index, pair);
        }
        removePointerFromParent(parent, parentHandle.pageId(), index, pair.victimId());
        BTreeRedoDeltas.captureNodePointerPage(mtr, parent, parentHandle.pageId(), index.indexId(), parentIsRoot,
                "btree merge final parent image after victim removal");
        freed.add(pair.victimId());
        freeSmoPage(mtr, leaf ? index.leafSegment() : index.nonLeafSegment(), pair.victimId());

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
            IndexPageHandle left = openBTreePageOutOfOrder(mtr, leftId, index, PageLatchMode.EXCLUSIVE,
                    "btree merge same-parent sibling: parent X latch is held and sibling holder cannot request ancestors");
            return new MergePair(left, nodeHandle, leftId, nodeId);
        }
        if (i + 1 < pointers.size()) {
            PageId rightId = pointers.get(i + 1).childPageId();
            IndexPageHandle right = openBTreePageOutOfOrder(mtr, rightId, index, PageLatchMode.EXCLUSIVE,
                    "btree merge same-parent sibling: parent X latch is held and sibling holder cannot request ancestors");
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
        BTreeRedoDeltas.writeSiblingLinks(mtr, survivorHandle, index.indexId(),
                survivorHandle.fileHeader().prevPageNo(), victimNext, "btree leaf merge survivor link");
        if (victimNext != FilePageHeader.FIL_NULL) {
            PageId farId = PageId.of(pair.victimId().spaceId(), PageNo.of(victimNext));
            IndexPageHandle far = openBTreePageOutOfOrder(mtr, farId, index, PageLatchMode.EXCLUSIVE,
                    "btree merge FIL right sibling repair: right links are followed in one direction");
            BTreeRedoDeltas.writeSiblingLinks(mtr, far, index.indexId(),
                    pair.survivorId().pageNo().value(), far.fileHeader().nextPageNo(),
                    "btree leaf merge far right repair");
        }
    }

    /** 内部页 merge：先 reorganize survivor 压实 garbage，再把 victim 全部 node pointer 并入 survivor（victim key 段整体 &gt; survivor，按序追加）；内部页不参与 FIL 链。 */
    private void mergeInternal(MiniTransaction mtr, BTreeIndex index, MergePair pair) {
        RecordPage survivor = pair.survivor().recordPage();
        RecordPage victim = pair.victim().recordPage();
        reorganizer.reorganize(survivor);
        writePointers(survivor, pair.survivorId(), materializePointers(victim, index), index);
        BTreeRedoDeltas.captureNodePointerPage(mtr, survivor, pair.survivorId(), index.indexId(), false,
                "btree internal merge final survivor image");
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
    private void redistribute(MiniTransaction mtr, BTreeIndex index, MergePair pair, boolean leaf,
                              RecordPage parent, PageId parentId, boolean parentIsRoot) {
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
            BTreeRedoDeltas.captureNodePointerPage(mtr, left, leftId, index.indexId(), false,
                    "btree internal redistribute final left image");
            BTreeRedoDeltas.captureNodePointerPage(mtr, right, rightId, index.indexId(), false,
                    "btree internal redistribute final right image");
            newRightLowKey = s.right().get(0).lowKey();
        }
        removePointerFromParent(parent, parentId, index, rightId);
        insertPointer(parent, parentId, index, new BTreeNodePointer(newRightLowKey, rightId));
        BTreeRedoDeltas.captureNodePointerPage(mtr, parent, parentId, index.indexId(), parentIsRoot,
                "btree redistribute final parent pointer replacement");
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
        IndexPageHandle childHandle = openBTreePageOutOfOrder(mtr, childId, index, PageLatchMode.EXCLUSIVE,
                "btree root shrink child absorption: root X is held and child cannot request ancestors");
        RecordPage child = childHandle.recordPage();

        if (childLevel == 0) {
            // 先物化 child 行，再 format(0) 清空 root 重灌（format 重置 nRecs/infimum/supremum）。
            List<LogicalRecord> rows = materializeLeafRecords(child, index);
            root.format(index.indexId(), 0);
            BTreeRedoDeltas.writeSiblingLinks(mtr, rootHandle, index.indexId(),
                    FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, "btree root shrink to leaf sibling reset");
            for (LogicalRecord row : rows) {
                inserter.insert(root, index.rootPageId(), row, index.keyDef(), index.schema());
            }
            BTreeRedoDeltas.captureLeafRootIdentity(mtr, root, index.rootPageId(), index.indexId(),
                    "btree root shrink final leaf level/index identity");
            freed.add(childId);
            freeSmoPage(mtr, index.leafSegment(), childId);
            return index.withRootLevel(0);
        }
        List<BTreeNodePointer> pointers = materializePointers(child, index);
        root.format(index.indexId(), childLevel);
        BTreeRedoDeltas.writeSiblingLinks(mtr, rootHandle, index.indexId(),
                FilePageHeader.FIL_NULL, FilePageHeader.FIL_NULL, "btree root shrink internal sibling reset");
        writePointers(root, index.rootPageId(), pointers, index);
        BTreeRedoDeltas.captureNodePointerPage(mtr, root, index.rootPageId(), index.indexId(), true,
                "btree root shrink final internal image");
        freed.add(childId);
        freeSmoPage(mtr, index.nonLeafSegment(), childId);
        BTreeIndex after = index.withRootLevel(childLevel);
        if (userRecordCount(root) == 1) {
            return shrinkRoot(mtr, after, rootHandle, freed);
        }
        return after;
    }

    /** 欠载回收结果：操作后索引快照（root shrink 会降 rootLevel）+ 回收页集合。
     *
     * @param indexAfter 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param freedPages 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private record MergeOutcome(BTreeIndex indexAfter, List<PageId> freedPages) {
    }

    /** merge 的相邻同父页对：survivor 恒为左者（保留、父 pointer key 不变），victim 为右者（并入 survivor 后被摘 pointer + free）。
     *
     * @param survivor 调用方持有的 {@code IndexPageHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param victim 调用方持有的 {@code IndexPageHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param survivorId 参与 {@code 构造} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param victimId 参与 {@code 构造} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
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
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param mode 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @return {@code openRoot} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws BTreeStructureCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private IndexPageHandle openRoot(MiniTransaction mtr, BTreeIndex index, PageLatchMode mode) {
        if (mtr == null || index == null || mode == null) {
            throw new DatabaseValidationException("btree mtr/index/mode must not be null");
        }
        IndexPageHandle root = openBTreePageOutOfOrder(mtr, index.rootPageId(), index, mode,
                "btree root entry: index operation may legally follow undo writes in the same MTR");
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
     * 从 root 逐层下降到 leaf，返回完整路径（root 在 0、leaf 在末尾），全路径 latch 持到 commit（不早释放）。
     * 0.13d 后 SMO 路径（insert split / delete merge）已改走 {@link #descendPathSafeNode} 的 safe-node 截断保留链；
     * 本方法仅剩 {@link #findLeaf} 使用（replace/mark 的 root 即 leaf 悲观回退等无 SMO 场景）。
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
            handle = openBTreePageOutOfOrder(mtr, childId, index, mode,
                    "btree full-path descent: child is latched while parent remains held");
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
     * 悲观 <b>insert</b> 的 safe-node 下降（0.13d）：safe 判据 = {@link #insertDescendSafe}（内部节点连续空闲 ≥
     * {@code maxSeparatorSize} ⟹ 必能容纳其下方 split 上插的那一个 separator、{@link #pointerFits} 必真、自身不会 split）。
     * 通用协议、正确性与死锁论证见 {@link #descendPathSafeNode}。
     */
    private List<IndexPageHandle> descendPathInsertSafeNode(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        int maxSeparator = maxSeparatorSize(index);
        return descendPathSafeNode(mtr, index, key,
                node -> insertDescendSafe(node, maxSeparator), safeNodeAncestorReleases);
    }

    /**
     * 悲观 <b>delete/purge</b> 的 safe-node 下降（0.13d）：safe 判据 = {@link #deleteDescendSafe}（内部节点「被摘走一个
     * 最大 pointer 后仍不欠载」⟹ 其下方 merge 摘掉它的 victim pointer 后它不会自身 merge/redistribute，传播必然停在它）。
     * 通用协议、正确性与死锁论证见 {@link #descendPathSafeNode}。
     */
    private List<IndexPageHandle> descendPathDeleteSafeNode(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        int maxSeparator = maxSeparatorSize(index);
        return descendPathSafeNode(mtr, index, key,
                node -> deleteDescendSafe(node, maxSeparator), safeNodeDeleteAncestorReleases);
    }

    /**
     * <b>safe-node + SX/restart</b> 悲观下降通用协议（0.13d，设计 §10.2 step4-5 + §10.3 ROOT_LATCHED_SX）。两遍制：
     * 快照树高 ≥2 时第一遍 root 取 <b>SX</b>（与读者/乐观写者的 root S 并存、排它其它 SMO 的 SX/X），内部 child 与 leaf
     * 恒 X，每 latch 到一个<b>内部</b> child 按 {@code internalChildSafe} 判 safe——safe 意味着来自其下方的 SMO（split 的
     * separator 上插 / merge 的 victim pointer 摘除）不会使它自身结构变更、传播必然停在它——safe 即释放其以上全部祖先
     * （含 root SX），保留链收缩为「safe 内部 child … leaf」；SMO 不达 root 时本 SMO <b>全程未 X 过 root</b>，读者从未被
     * root 阻塞。若首遍链顶仍是 root（SMO 可能写 root，而 SX 禁原地升级），此刻零页写入，整链干净释放后以 root X 重启
     * 第二遍（至多一次；level 0/1 树必写 root，跳过 SX 首遍直接 X）。<b>只判内部页，从不判 leaf</b>：leaf 级 safe 判据
     * 属于叶级算子（乐观路径已判），且 leaf 恒为保留链末项、永不触发释放；leaf 的 SMO 传播到其（被保留的）父页处理。
     *
     * <p><b>正确性（无需 B-link / 无需版本重启）</b>：保留链顶恒为「最近一个 safe 内部节点」或真正的 root。split 传播
     * （{@link #splitLeafAndPropagate}/{@link #insertSeparator}）与 merge 传播（{@link #considerMerge}）都沿保留链自底向上，
     * 遇到 safe 顶即被吸收（split：{@code pointerFits} 必真；merge：safe 顶摘一指针后不欠载，{@code isUnderfull} 必假），
     * 绝不越过它去访问已释放的祖先；root 级动作（growRoot/shrinkRoot）只在链顶恰为真 root 时发生（此时 root 从未释放；
     * {@code considerMerge} 以 rootPageId 判定 root，而非链下标）。每个保留节点至多经历<b>一次</b>来自其唯一在途 child 的
     * pointer 插入/摘除，故单 separator 上界足矣；保留节点持 X 从下降到传播不放手，下降时测得的空闲到传播时不变。
     * 下降阶段尚未写任何页（非 touched），故 {@link MiniTransaction#releaseLatch} 放行祖先 X guard 的释放。
     *
     * <p><b>死锁自由</b>（insert/delete/purge 并发）：任一悲观下降都在起点短持 root SX/X（遇 safe 即放；SX-SX、SX-X 互斥）
     * ⟹ 任一时刻只有一个 SMO「在 root 处」，其余 SMO 的保留 X 链两两不相交（若相交，后到者阻塞在交点、不成环）；
     * restart 在重取 root X 前已释放全链 ⟹ 无 hold-and-wait。跨 SMO 的 latch 等待边只有三类，均无环：① 下降阻塞边——
     * 等待者只持交点以上的页，被等者绝不回头申请其上方页；② 同父兄弟边（{@link #chooseMergePair} 的左/右兄弟）——
     * 保留链连续故任意保留节点的父页恒被本 SMO 持有，同父兄弟只可能被「leaf-only 乐观算子」持有，而乐观算子是终结的
     * （只持单 leaf、绝不再申请任何 latch）；③ FIL 右邻边（split 右兄弟修 prev、merge victim 远邻修 prev）——恒指向
     * 更靠右的 leaf，按叶序全序无环。
     */
    private List<IndexPageHandle> descendPathSafeNode(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                                      Predicate<RecordPage> internalChildSafe,
                                                      LongAdder releaseCounter) {
        if (key == null) {
            throw new DatabaseValidationException("btree descend key must not be null");
        }
        // 0.13d SX+restart（设计 §10.3 ROOT_LATCHED_SX）：快照树高 ≥2 才做 SX 首遍——level 0/1 树的任何 SMO 必写 root
        // （leaf 即 root，或 separator/victim pointer 直达 root），SX 禁原地升级、首遍必重启，直接 X 更省一遍。
        // 快照陈旧只影响模式选择的收益，不影响正确性：链顶判定 + 重启兜底（快照虚高 → SX 首遍多一次重启；
        // 快照虚低 → root 取 X 偏保守，均正确）。
        if (index.rootLevel() >= 2) {
            rootSxDescents.increment();
            List<IndexPageHandle> chain = descendOnce(mtr, index, key, PageLatchMode.SHARED_EXCLUSIVE,
                    internalChildSafe, releaseCounter);
            if (!chain.get(0).pageId().equals(index.rootPageId())) {
                // safe 节点已使 root（SX）早释放：本 SMO 全程不 X root，读者/乐观写者的 root S 从未被阻塞。
                return chain;
            }
            // 链顶仍是 root：本 SMO 可能要写 root（growRoot/shrinkRoot/root 收 separator），而 SX 禁原地升级。
            // 此刻**零页写入**（下降只读），整链干净释放后以 root X 重启第二遍（至多一次，重启后按新导航自然正确）。
            releaseChain(mtr, chain);
            rootXRestarts.increment();
        }
        return descendOnce(mtr, index, key, PageLatchMode.EXCLUSIVE, internalChildSafe, releaseCounter);
    }

    /**
     * safe-node 下降的单遍执行：root 按 {@code rootMode}（SX 首遍 / X 重启遍）latch，内部 child 与 leaf 恒 X；
     * 每 latch 到内部 child 判 safe，safe 即释放其以上全部祖先（含 root，SX/X 均经 {@code releaseHandle} 释放）。
     * 返回保留链（顶 = 最近 safe 内部节点，或 root——后者表示本遍未找到 safe 节点、SMO 可能触达 root）。
     */
    private List<IndexPageHandle> descendOnce(MiniTransaction mtr, BTreeIndex index, SearchKey key,
                                              PageLatchMode rootMode, Predicate<RecordPage> internalChildSafe,
                                              LongAdder releaseCounter) {
        List<IndexPageHandle> retained = new ArrayList<>();
        IndexPageHandle handle = openRoot(mtr, index, rootMode);
        retained.add(handle);
        RecordPage page = handle.recordPage();
        PageId pageId = index.rootPageId();
        while (page.header().level() > 0) {
            PageId childId = chooseChild(page, index, key);
            IndexPageHandle child = openBTreePageOutOfOrder(mtr, childId, index, PageLatchMode.EXCLUSIVE,
                    "btree safe-node descent: child X is latched before ancestor release");
            RecordPage childPage = child.recordPage();
            if (childPage.header().indexId() != index.indexId()) {
                throw new BTreeStructureCorruptedException("child page index id mismatch at " + childId
                        + ": page=" + childPage.header().indexId() + " expected=" + index.indexId());
            }
            // 只对内部 child 判 safe（leaf 不判）：safe 内部节点吸收其下方 SMO 的传播 → 它以上的祖先与本次 SMO 无关，立即释放。
            if (childPage.header().level() > 0 && internalChildSafe.test(childPage)) {
                releaseRetainedAncestors(mtr, retained, releaseCounter);
            }
            retained.add(child);
            page = childPage;
            pageId = childId;
        }
        validateLeafPage(page, index, pageId);
        return retained;
    }

    /**
     * restart-in-X 前的整链释放（0.13d SX+restart）：SX 首遍链顶仍是 root 时，下降尚未写任何页（非 touched），
     * 全链（root SX + 内部/leaf X）经 {@code releaseHandle} 干净释放，随后以 root X 重启。释放顺序沿链自顶向下即可
     * （非 LIFO 由 {@link cn.zhangyis.db.storage.mtr.MtrMemo} 按身份匹配支持）。
     */
    private void releaseChain(MiniTransaction mtr, List<IndexPageHandle> chain) {
        for (IndexPageHandle handle : chain) {
            pageAccess.releaseHandle(mtr, handle);
        }
    }

    /**
     * 释放并清空当前保留链中的全部 latch（已找到 safe 内部 child，其全部祖先与本次 SMO 无关）；按早释放的祖先数累加
     * {@code releaseCounter}（insert 与 delete/purge 分开计数，便于测试定向断言）。释放的祖先在下降阶段未写入（非 touched），
     * {@link MiniTransaction#releaseLatch} 放行。
     * @param mtr 调用方拥有的短物理事务；不得为 {@code null}，且必须处于可获取资源或可追加 redo 的合法阶段
     * @param retained 参与 {@code releaseRetainedAncestors} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param releaseCounter 调用方提供的长度或容量值对象；不得为 {@code null}，且必须已通过其构造范围校验
     */
    private void releaseRetainedAncestors(MiniTransaction mtr, List<IndexPageHandle> retained,
                                          LongAdder releaseCounter) {
        for (IndexPageHandle ancestor : retained) {
            pageAccess.releaseHandle(mtr, ancestor);
            releaseCounter.increment();
        }
        retained.clear();
    }

    /**
     * insert 下降 safe 判据（0.13d safe-node）：内部节点连续空闲 ≥ {@code maxSeparatorSize}（本索引任一 node pointer 编码长度的
     * 严格上界）⟹ 必能再容纳来自其下方 split 的那一个 separator，故它自身不会 split、它以上祖先可提前释放。与 {@link #pointerFits}
     * 用同一个 {@code freeSpace()} 度量比较（且上界 ≥ 实际），严格保证「判 safe ⟹ 传播时 {@code pointerFits} 必真」。保守（漏判
     * 只少释放祖先、仍正确），严格（绝不把真会 split 的节点误判 safe）。
     */
    private boolean insertDescendSafe(RecordPage node, int maxSeparatorSize) {
        return node.freeSpace() >= maxSeparatorSize;
    }

    /**
     * delete/purge 下降 safe 判据（0.13d safe-node）：内部节点「被摘走一个 pointer 后仍不欠载」⟹ 其下方 merge 摘除 victim
     * pointer 后它不会自身 merge/redistribute，传播停在它。用与 {@link #isUnderfull} 完全一致的阈值公式（可回收空闲 =
     * {@code freeSpace + garbage}，欠载 = 可回收 × 2 &gt; 页大小），对被摘 pointer 的释放量取上界 {@code maxSeparatorSize}
     * （实际释放 ≤ 上界 → 投影可回收偏大 → 更易判不 safe → 偏保守，绝不把真会 merge 的节点误判 safe）。
     */
    private boolean deleteDescendSafe(RecordPage node, int maxSeparatorSize) {
        int reclaimableAfter = node.freeSpace() + node.header().garbage() + maxSeparatorSize;
        return reclaimableAfter * 2 <= pageSize.bytes();
    }

    /**
     * 计算本索引任一可能 separator（上插/摘除的 node pointer）编码长度的<b>严格上界</b>（0.13d safe-node 用）：node pointer 的
     * key 列按列类型<b>合成</b>最大字节值（变长列填满声明 {@code length}；定长列编码宽度由类型决定、与值无关，任一合法值即最大），
     * child 定位列为定长 BIGINT。据此编码一个「最大 node pointer」记录取其字节长，再加 {@link #MERGE_ENTRY_MARGIN}
     * 与 {@link #pointerFits}/{@link #deleteWouldUnderflow} 的目录/对齐余量对齐。合成而非取调用方实际值，使 insert
     * （有完整记录）与 delete/purge（只有 {@link SearchKey}，可为前缀）共用同一上界。
     */
    private int maxSeparatorSize(BTreeIndex index) {
        BTreeNodePointerSchema ps = BTreeNodePointerSchema.from(index);
        List<ColumnValue> maxKey = new ArrayList<>(index.keyDef().parts().size());
        for (var part : index.keyDef().parts()) {
            maxKey.add(worstCaseKeyValue(index.schema().column(part.columnId().value()).type()));
        }
        LogicalRecord maxPointer = pointerCodec.toRecord(
                new BTreeNodePointer(new SearchKey(maxKey), index.rootPageId()), ps);
        return recordEncoder.encode(maxPointer, ps.schema()).length + MERGE_ENTRY_MARGIN;
    }

    /**
     * 按列类型合成「最大字节编码」取值：变长列（VARCHAR/VARBINARY）填满声明 {@code length}（单字节字符 × length =
     * 恰好 length 字节，UTF-8 下即编码上界）；定长列（整数/浮点/DECIMAL/CHAR/BINARY/时间）编码宽度由类型决定、与值无关，
     * 取任一合法值即等价于最大。
     */
    private ColumnValue worstCaseKeyValue(ColumnType type) {
        return switch (type.typeId()) {
            case TINYINT, SMALLINT, INT, BIGINT -> new ColumnValue.IntValue(0);
            case FLOAT, DOUBLE -> new ColumnValue.DoubleValue(0);
            case DECIMAL -> new ColumnValue.DecimalValue(BigDecimal.ZERO);
            case CHAR, VARCHAR -> new ColumnValue.StringValue("x".repeat(type.length()));
            case BINARY, VARBINARY -> new ColumnValue.BinaryValue(new byte[type.length()]);
            case DATE -> new ColumnValue.TemporalValue(TemporalKind.DATE, 0);
            case TIME -> new ColumnValue.TemporalValue(TemporalKind.TIME, 0);
            case DATETIME -> new ColumnValue.TemporalValue(TemporalKind.DATETIME, 0);
            case TIMESTAMP -> new ColumnValue.TemporalValue(TemporalKind.TIMESTAMP, 0);
            case YEAR -> new ColumnValue.TemporalValue(TemporalKind.YEAR, 0);
            case BIT -> new ColumnValue.BitValue(new byte[(type.length() + 7) / 8]);
            case ENUM -> new ColumnValue.EnumValue(1);
            case SET -> new ColumnValue.SetValue(0);
            case TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT ->
                    new ColumnValue.StringValue("x".repeat(Math.min(type.length(), LobCodec.INLINE_PAYLOAD_LIMIT)));
            case TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB ->
                    new ColumnValue.BinaryValue(new byte[Math.min(type.length(), LobCodec.INLINE_PAYLOAD_LIMIT)]);
            case JSON -> new ColumnValue.StringValue("{}");
        };
    }

    /**
     * 乐观下降（0.13a，latch coupling / crab，设计 §10.2）：内部层 S-crab——持父页 S → latch 子页 → 释放父页 S；
     * 最后一层（leaf）取 X 返回。至多同时持「1 内部父 + 1 子」，越过的祖先立即释放，放开 root 处写并发。
     * 仅供 {@link #tryOptimisticInsert}/{@link #tryOptimisticDelete} 等乐观写算子；unsafe（split/merge）时调用方释放
     * leaf X 后改走 {@link #descendPathSafeNode} 悲观 X + safe-node 重启。
     *
     * <p><b>root 即 leaf</b>（level 0，单页树）无 crab 收益：释放 S root 返回 {@code null}，交悲观（悲观重取 root X）。
     *
     * <p><b>并发正确性</b>（与悲观路径协作）：核心不变量是 <b>crab 的 hand-over-hand</b>——释放某祖先前本线程已先 latch 到
     * 仍有效的子页，故本线程恒持有「即将使用的那一页」的 latch，该页不会在本线程脚下被结构变更（split/merge 需 X，与本线程
     * 的 latch 冲突而阻塞；re-parent 只移动指针不移动子页内容）。此不变量<b>不依赖</b>结构变更是否持 root X 到 commit。
     * 补充：0.13d safe-node 后悲观 <b>insert</b> 不再把 root X 持到 commit（split 不传播到 root 时提前释放），但 merge（delete/purge）
     * 仍走 {@link #descendPath} 悲观全 X 持到 commit、tree-wide 串行于 root。释放的内部页均为 S（从不写→非 touched），
     * 满足 {@link MiniTransaction#releaseLatch} 防护。
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
            IndexPageHandle child = openBTreePageOutOfOrder(mtr, childId, index,
                    childIsLeaf ? PageLatchMode.EXCLUSIVE : PageLatchMode.SHARED,
                    "btree optimistic crab descent: child is latched before parent S release");
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

    /**
     * 读路径乐观下降（0.13c，latch coupling / crab，设计 §10）：全程 S——持父页 S → latch 子页 S → 释放父页 S，
     * 至多同时持「1 内部父 + 1 子」，越过的祖先立即释放，缩短 root/内部页 latch 持有窗口、放开 root 处写并发。
     * 读从不结构变更，故无 {@link #descendOptimistic} 的 unsafe/悲观回退分支；root 即 leaf（level 0）直接返回 root（S）。
     *
     * <p><b>并发正确性</b>（与悲观写路径协作，同 {@link #descendOptimistic} 论证）：核心不变量是 crab 的 hand-over-hand——
     * 释放某祖先前本线程已先 latch 到仍有效的子页，故读者恒持有「即将使用的那一页」的 S latch，该页不会在读者脚下被结构变更
     * （split/merge 需 X，与读者 S 冲突而阻塞；re-parent 只移动指针不移动子页内容；split 不跨子树边界搬 key）。此不变量
     * <b>不依赖</b>结构变更是否持 root X 到 commit——0.13d safe-node 后悲观 insert 已不再把 root X 持到 commit，读者仍安全。
     * 释放的内部页均为 S（从不写→非 touched），满足 {@link MiniTransaction#releaseLatch} 防护。返回的 leaf S 由调用方读毕后
     * 随 MTR commit 释放。
     */
    private IndexPageHandle descendSharedCrab(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        if (key == null) {
            throw new DatabaseValidationException("btree descend key must not be null");
        }
        IndexPageHandle node = openRoot(mtr, index, PageLatchMode.SHARED);
        RecordPage page = node.recordPage();
        while (page.header().level() > 0) {
            PageId childId = chooseChild(page, index, key);
            IndexPageHandle child = openBTreePageOutOfOrder(mtr, childId, index, PageLatchMode.SHARED,
                    "btree shared crab descent: child S is latched before parent S release");
            // crab：latch 到子页后立即释放父页（内部 S，未写→非 touched，可提前释放）。
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

    /**
     * S-crab 到整棵树最左 leaf。每个内部页取第一条 node pointer 作为最小子树；空内部页或错误 index id
     * 表示结构损坏。该入口只供无完整 lower physical key 的 logical-prefix 扫描使用。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>以 S latch 打开稳定 root，并读取当前页头 level。</li>
     *     <li>每层选择第一条 node pointer，先取得 child S 再释放 parent S。</li>
     *     <li>逐层校验 child index id，避免跨索引 sibling/指针损坏被当作正常导航。</li>
     *     <li>到达 level-0 后校验 leaf 页头并返回唯一仍由 MTR 持有的 handle。</li>
     * </ol>
     *
     * @param mtr   调用方短只读 MTR；所有 fix/latch memo 归它管理。
     * @param index 目标索引 descriptor；root page id 是稳定导航起点。
     * @return 整棵树最左 leaf 的 S-latched handle；调用方必须经 pageAccess/MTR 释放。
     * @throws BTreeStructureCorruptedException 内部页无 node pointer、child index id 错配或目标页不是合法 leaf 时抛出。
     */
    private IndexPageHandle descendLeftmostSharedCrab(MiniTransaction mtr, BTreeIndex index) {
        // 1. root 页头 level 是当前结构权威，不能使用 descriptor 的创建时层级猜测循环次数。
        IndexPageHandle node = openRoot(mtr, index, PageLatchMode.SHARED);
        RecordPage page = node.recordPage();
        while (page.header().level() > 0) {
            // 2. 最左子树由第一条 node pointer 定义；child S 到手后才释放 parent，防止导航窗口断裂。
            List<Integer> offsets = page.recordOffsetsInOrder();
            if (offsets.isEmpty()) {
                throw new BTreeStructureCorruptedException(
                        "secondary prefix scan found empty internal page " + node.pageId());
            }
            BTreeNodePointerSchema pointerSchema = BTreeNodePointerSchema.from(index);
            RecordCursor first = new RecordCursor(page, offsets.getFirst(), pointerSchema.schema(), registry);
            PageId childId = pointerCodec.fromRecord(first.materialize(), pointerSchema).childPageId();
            IndexPageHandle child = openBTreePageOutOfOrder(mtr, childId, index, PageLatchMode.SHARED,
                    "btree leftmost shared crab: child S is latched before parent S release");
            pageAccess.releaseHandle(mtr, node);
            page = child.recordPage();
            // 3. 每个 child 必须仍属于同一 index；错配说明页指针/恢复状态损坏。
            if (page.header().indexId() != index.indexId()) {
                throw new BTreeStructureCorruptedException("leftmost child page index id mismatch at "
                        + child.pageId() + ": page=" + page.header().indexId()
                        + " expected=" + index.indexId());
            }
            node = child;
        }
        // 4. 返回前确认 level=0 与页归属；此时祖先 handle 均已提前释放。
        validateLeafPage(page, index, node.pageId());
        return node;
    }

    /**
     * 读路径 crab 定位（0.13c）：{@link #descendSharedCrab} S-crab 下降到 leaf(S) 并包成 {@link LeafLocation}；
     * 返回时 root/内部祖先均已早释放，仅 leaf S 仍在 MTR memo 中（读毕随 commit 释放）。
     */
    private LeafLocation findLeafSharedCrab(MiniTransaction mtr, BTreeIndex index, SearchKey key) {
        IndexPageHandle leaf = descendSharedCrab(mtr, index, key);
        return new LeafLocation(leaf, leaf.recordPage(), leaf.pageId());
    }

    /** {@link #findLeaf} 定位结果：leaf 句柄（scan 取 sibling 链/header）、leaf 页视图、leaf 物理页号。
     * @param handle 调用方持有的 {@code IndexPageHandle} 资源句柄；不得为 {@code null} 且必须处于有效期，方法返回前所有权仍归调用方
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     */
    private record LeafLocation(IndexPageHandle handle, RecordPage page, PageId pageId) {
    }

    /**
     * 校验 {@code validateLeafPage} 涉及的B+Tree 索引结构、范围与交叉字段；合法输入不修改状态，非法输入在副作用前抛出领域异常。
     *
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @throws BTreeStructureCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
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
        if (!index.physicalUnique()) {
            return;
        }
        if (search.findEqual(leaf, key, index.keyDef(), index.schema()).isPresent()) {
            throw new BTreeDuplicateKeyException("duplicate key in unique btree index " + index.indexId()
                    + " at leaf " + leafId);
        }
    }

    /**
     * 扫描单个已持 S latch 的 leaf。delete-mark 过滤在范围比较前执行，但 including-deleted 模式会保留标记项；
     * 两种模式均复用同一 key comparator，保证普通查询与物理检查的边界排序一致。
     *
     * @param leaf 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param leafId 参与 {@code scanLeafPage} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param range 调用方已校验的执行计划、批次、范围或候选对象；不得为 {@code null}，边界必须有序且不得跨越所属事务、表或日志批次
     * @param acc 当前算法已准备的中间状态；不得为 {@code null}，必须由本次扫描、日志组装或事务终结流程创建且尚未发布
     * @param includeDeleted 资源是否处于删除、空闲、静默、持久化或终态；必须与权威状态机一致，不能由调用方猜测
     * @return {@code scanLeafPage} 成功完成其命名的受控动作并发布结果时为 {@code true}；未命中、未执行或状态竞争失败时为 {@code false}
     */
    private boolean scanLeafPage(RecordPage leaf, PageId leafId, BTreeIndex index, BTreeScanRange range,
                                 ScanAccumulator acc, boolean includeDeleted) {
        for (int off : leaf.recordOffsetsInOrder()) {
            RecordCursor cursor = new RecordCursor(leaf, off, index.schema(), registry);
            if (!includeDeleted && cursor.isDeleted()) {
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

    /**
     * 校验调用入口确实面向紧凑二级索引。完整物理 key 必须唯一，否则 rollback/purge 无法凭 key 精确定位一条 entry；
     * 聚簇 descriptor 则携带隐藏列，不能走没有版本所有权校验的二级原语。
     *
     * @param index     调用方提供的 exact-version B+Tree descriptor。
     * @param operation 进入该校验的二级原语名，仅用于领域错误诊断。
     * @throws DatabaseValidationException descriptor 缺失、属于聚簇索引或未声明完整 physical key 唯一时抛出。
     */
    private void requireSecondaryIndex(BTreeIndex index, String operation) {
        if (index == null) {
            throw new DatabaseValidationException(operation + " index must not be null");
        }
        if (index.clustered() || !index.physicalUnique()) {
            throw new DatabaseValidationException(operation
                    + " requires a non-clustered physically unique index: " + index.indexId());
        }
    }

    private boolean locateRangeLeaf(RecordPage leaf, PageId leafId, BTreeIndex index, BTreeScanRange range,
                                    List<BTreeCurrentReadPosition> positions) {
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
            BTreeLookupResult result = materialize(index, leafId, cursor);
            RecordLockKey recordKey = RecordLockKey.from(result.recordRef());
            GapLockKey gapKey = gapBeforeRecord(leaf, index, off);
            positions.add(new BTreeCurrentReadPosition(Optional.of(result), Optional.of(recordKey), gapKey,
                    Optional.of(new NextKeyLockKey(recordKey, gapKey))));
            if (positions.size() >= range.limit()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 把输入转换为 {@code materializeLeafRecords} 对应的B+Tree 索引结果；转换保持稳定顺序与身份映射，不修改调用方持有的输入对象。
     *
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return 按物理页、日志或 SQL 源顺序扫描并物化的元素；无匹配内容时返回空集合，不用 {@code null} 表示缺失
     */
    private List<LogicalRecord> materializeLeafRecords(RecordPage page, BTreeIndex index) {
        List<LogicalRecord> records = new ArrayList<>();
        for (int off : page.recordOffsetsInOrder()) {
            records.add(new RecordCursor(page, off, index.schema(), registry).materialize());
        }
        return records;
    }

    /**
     * 校验输入与当前状态后修改B+Tree 索引领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param records 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param inserted 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @return {@code sortedWithInserted} 产生的非空集合容器；元素身份与顺序遵循当前模块契约，无元素时返回空集合而非 {@code null}
     */
    private List<LogicalRecord> sortedWithInserted(List<LogicalRecord> records, LogicalRecord inserted,
                                                   BTreeIndex index) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        List<LogicalRecord> all = new ArrayList<>(records.size() + 1);
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        all.addAll(records);
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        all.add(inserted);
        all.sort(Comparator.comparing(rec -> keyOf(rec, index),
                (a, b) -> keyComparator.compare(a, b, index.keyDef(), index.schema())));
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return all;
    }

    private SplitRows splitRows(List<LogicalRecord> records) {
        if (records.size() < 2) {
            throw new BTreeStructureCorruptedException("cannot split fewer than two records");
        }
        int mid = records.size() >>> 1;
        return new SplitRows(List.copyOf(records.subList(0, mid)), List.copyOf(records.subList(mid, records.size())));
    }

    /**
     * 校验输入与当前状态后修改B+Tree 索引领域数据；成功发布完整结果，异常路径保留既有持久化与并发不变量。
     *
     * @param page 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param pageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param rows 参与 {@code insertAll} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param inserted 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @return {@code insertAll} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
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

    private SearchKey highKey(List<LogicalRecord> rows, BTreeIndex index) {
        if (rows.isEmpty()) {
            throw new BTreeStructureCorruptedException("split child must not be empty");
        }
        return keyOf(rows.get(rows.size() - 1), index);
    }

    private PageAllocationHint leafSplitAllocationHint(PageId leafId, FilePageHeader header, BTreeIndex index,
                                                       List<LogicalRecord> existing, LogicalRecord inserted,
                                                       long pagesNeeded) {
        if (existing.isEmpty()) {
            return PageAllocationHint.none();
        }
        return BTreeAllocationHintPlanner.leafSplitHint(
                leafId,
                keyOf(inserted, index),
                lowKey(existing, index),
                highKey(existing, index),
                header.prevPageNo() != FilePageHeader.FIL_NULL,
                header.nextPageNo() != FilePageHeader.FIL_NULL,
                pagesNeeded,
                index,
                keyComparator);
    }

    private BTreeLookupResult materialize(BTreeIndex index, PageId pageId, RecordCursor cursor) {
        return new BTreeLookupResult(index, cursor.recordRef(pageId, index.indexId()), cursor.materialize());
    }

    /**
     * 按 leaf 的物理记录顺序定位目标 offset，并用前驱 key 与目标 key 构造其左侧 gap lock 身份；offset 不存在视为页结构损坏。
     *
     * @param leaf 已固定的页面、frame 或页头视图；不得为 {@code null}，必须指向目标 PageId，并在访问期间持有契约要求的 fix/latch
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param recordOffset 目标结构内的零基偏移；必须落在当前页、记录或持久槽位的合法范围
     * @return {@code gapBeforeRecord} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws BTreeStructureCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    private GapLockKey gapBeforeRecord(RecordPage leaf, BTreeIndex index, int recordOffset) {
        SearchKey leftKey = null;
        for (int off : leaf.recordOffsetsInOrder()) {
            if (off == recordOffset) {
                return new GapLockKey(index.indexId(), leftKey, keyAt(leaf, index, off));
            }
            leftKey = keyAt(leaf, index, off);
        }
        throw new BTreeStructureCorruptedException("record offset not found while building current-read gap: "
                + recordOffset);
    }

    private GapLockKey gapForSearchKey(RecordPage leaf, BTreeIndex index, SearchKey key) {
        int prev = search.findInsertPosition(leaf, key, index.keyDef(), index.schema());
        SearchKey leftKey = prev == leaf.infimumOffset() ? null : keyAt(leaf, index, prev);
        int next = leaf.nextRecord(prev);
        SearchKey rightKey = next == leaf.supremumOffset() ? null : keyAt(leaf, index, next);
        return new GapLockKey(index.indexId(), leftKey, rightKey);
    }

    private GapLockKey terminalGapForRange(MiniTransaction mtr, BTreeIndex index, BTreeScanRange range) {
        if (range.upperBound().isEmpty()) {
            return new GapLockKey(index.indexId(), rightmostKey(mtr, index), null);
        }
        SearchKey upper = range.upperBound().orElseThrow();
        LeafLocation upperLeaf = findLeafSharedCrab(mtr, index, upper);
        OptionalInt found = search.findEqual(upperLeaf.page(), upper, index.keyDef(), index.schema());
        if (found.isPresent() && !range.upperInclusive()) {
            return gapBeforeRecord(upperLeaf.page(), index, found.getAsInt());
        }
        return gapForSearchKey(upperLeaf.page(), index, upper);
    }

    /**
     * 定位最右 leaf 的最后一个 key；空树返回 null，从而形成 (-∞,+∞) terminal gap。
     */
    private SearchKey rightmostKey(MiniTransaction mtr, BTreeIndex index) {
        IndexPageHandle leaf = descendLeftmostSharedCrab(mtr, index);
        SearchKey last = null;
        while (true) {
            List<Integer> offsets = leaf.recordPage().recordOffsetsInOrder();
            if (!offsets.isEmpty()) {
                last = keyAt(leaf.recordPage(), index, offsets.getLast());
            }
            long next = leaf.fileHeader().nextPageNo();
            if (next == FilePageHeader.FIL_NULL) {
                return last;
            }
            PageId nextId = PageId.of(index.rootPageId().spaceId(), PageNo.of(next));
            IndexPageHandle nextHandle = openBTreePageOutOfOrder(mtr, nextId, index,
                    PageLatchMode.SHARED,
                    "btree rightmost search: next leaf is latched before predecessor release");
            pageAccess.releaseHandle(mtr, leaf);
            leaf = nextHandle;
        }
    }

    private SearchKey keyAt(RecordPage leaf, BTreeIndex index, int recordOffset) {
        return keyOf(new RecordCursor(leaf, recordOffset, index.schema(), registry).materialize(), index);
    }

    private boolean belowLower(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        if (range.lowerBound().isEmpty()) {
            return false;
        }
        int c = recordComparator.compare(
                cursor, range.lowerBound().orElseThrow(), index.keyDef(), index.schema());
        return c < 0 || (c == 0 && !range.lowerInclusive());
    }

    private boolean aboveUpper(RecordCursor cursor, BTreeIndex index, BTreeScanRange range) {
        if (range.upperBound().isEmpty()) {
            return false;
        }
        int c = recordComparator.compare(
                cursor, range.upperBound().orElseThrow(), index.keyDef(), index.schema());
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

    /** 分裂后的左右记录集；两侧都必须非空。
     *
     * @param left 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param right 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private record SplitRows(List<LogicalRecord> left, List<LogicalRecord> right) {
    }

    /** 非叶页内部 split 的左右 pointer 集；两侧都必须非空。
     *
     * @param left 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     * @param right 参与 {@code 构造} 的有序或去重元素集合；不得为 {@code null}，空集合表示没有元素，集合内不得包含 Java {@code null}
     */
    private record SplitPointers(List<BTreeNodePointer> left, List<BTreeNodePointer> right) {
    }

    /** 非 root leaf split 结果：新插入记录页内短期 ref、右半最小 key（separator）、新右兄弟页号。
     *
     * @param insertedRef 参与 {@code 构造} 的稳定领域标识 {@code RecordRef}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param rightLowKey 参与 {@code 构造} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param newRightId 参与 {@code 构造} 的稳定领域标识 {@code PageId}；不得为 {@code null}，并须由对应值对象构造校验产生
     */
    private record LeafSplitResult(RecordRef insertedRef, SearchKey rightLowKey, PageId newRightId) {
    }

    /** scan 结果累积器，集中管理 limit 与不可变输出。 */
    private static final class ScanAccumulator {
        /**
         * 记录 {@code limit} 的非负位置、容量或计数；写入前必须校验所属页/集合上界，溢出会破坏布局或资源记账。
         */
        private final int limit;
        /**
         * 本对象拥有的 {@code rows} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
         */
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
