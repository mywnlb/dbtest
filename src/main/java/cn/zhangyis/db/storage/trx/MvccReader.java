package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.btree.BTreeLookupResult;
import cn.zhangyis.db.storage.btree.SplitCapableBTreeIndexService;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.undo.UndoLogFormatException;
import cn.zhangyis.db.storage.undo.UndoLogSegmentAccess;
import cn.zhangyis.db.storage.undo.UndoRecord;
import cn.zhangyis.db.storage.undo.UndoRecordType;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * MVCC 一致性读门面（设计 §7.5/§8.2/§14.1，T1.4，consumer of 版本链）。给定 {@link ReadView}，取聚簇记录最新版本，
 * 若对该快照不可见则沿记录版本链（{@code DB_ROLL_PTR} → update undo → {@code oldHidden.dbRollPtr}）反向构造旧版本，
 * 直到找到可见版本，或确认记录在该快照中尚不存在。
 *
 * <p><b>依赖方向</b>：{@code storage.trx → storage.btree + storage.undo}（与 RollbackService 同模式）。
 *
 * <p><b>Latch 纪律</b>（§17/§8，关键不变量）：写路径在同一 MTR 内为 undo→index；一致性读**绝不反转**该顺序、也绝不
 * 同时持 index 与 undo page latch——先用一个 MTR 物化 btree 当前版本并立即提交释放 index latch，之后**每个**旧版本
 * 各用独立只读 MTR 读 undo、读完即提交释放 undo S latch。物化的记录字节已是本地副本（{@code RecordCursor} 构造时拷出），
 * 故释放 latch 后仍可用。
 *
 * <p><b>损坏快速失败</b>：环（visited 集合）或链长超 {@code maxVersionHops}、undo 类型/索引/cluster key 与链节点不一致
 * 均抛 {@link UndoLogFormatException}，不返回拼错的历史行、不无限遍历。
 *
 * <p><b>本片范围</b>：聚簇点读、RR/RC（ReadView 由 {@link ReadViewManager} 按隔离级别给出）；INSERT undo 是版本链终点
 * （其 insert 位即表达"更早版本不存在"，不解码可复用的 INSERT undo）。delete-mark 可见性、二级索引、locking read 留后续片。
 */
public final class MvccReader {

    /** 物理短事务工厂；index 读一个 MTR、每个旧版本 undo 读各一个 MTR。 */
    private final MiniTransactionManager mgr;
    /** 聚簇最新版本入口。 */
    private final SplitCapableBTreeIndexService btree;
    /** 跨事务 undo 直读（按 roll pointer），构造旧版本。 */
    private final UndoLogSegmentAccess undoAccess;
    /** 单 undo 表空间（roll pointer 不编码 spaceId）。 */
    private final SpaceId undoSpace;
    /** 版本链最大跳数；超出视为损坏链快速失败（兜底环检测）。 */
    private final int maxVersionHops;

    /**
     * 创建 {@code MvccReader}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param mgr 由组合根提供的 {@code MiniTransactionManager} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param btree 由组合根提供的 {@code SplitCapableBTreeIndexService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param undoAccess 由组合根提供的 {@code UndoLogSegmentAccess} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @param undoSpace 参与 {@code 构造} 的稳定领域标识 {@code SpaceId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param maxVersionHops 参与 {@code 构造} 的单调版本值 {@code maxVersionHops}；必须非负，回退或与权威快照冲突时拒绝
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public MvccReader(MiniTransactionManager mgr, SplitCapableBTreeIndexService btree,
                      UndoLogSegmentAccess undoAccess, SpaceId undoSpace, int maxVersionHops) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (mgr == null || btree == null || undoAccess == null || undoSpace == null) {
            throw new DatabaseValidationException("mvcc reader collaborators must not be null");
        }
        if (maxVersionHops <= 0) {
            throw new DatabaseValidationException("maxVersionHops must be positive: " + maxVersionHops);
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.mgr = mgr;
        this.btree = btree;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.undoAccess = undoAccess;
        this.undoSpace = undoSpace;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.maxVersionHops = maxVersionHops;
    }

    /**
     * 一致性读：返回 {@code key} 对应聚簇记录对 {@code readView} 可见的版本，或 empty（不存在/不可见且无可见旧版本）。
     * 数据流见类注释（index MTR 物化当前版本 → 释放 → 逐旧版本独立 undo MTR 遍历）。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param readView 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code read} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code read} 按身份或键定位到的对象；未找到、不可见或尚未持久化时为空 {@code Optional}，从不返回 Java {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     * @throws UndoLogFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public Optional<LogicalRecord> read(ReadView readView, BTreeIndex index, SearchKey key) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        if (readView == null || index == null || key == null) {
            throw new DatabaseValidationException("mvcc read readView/index/key must not be null");
        }
        if (!index.clustered()) {
            throw new DatabaseValidationException("mvcc read requires a clustered index: " + index.indexId());
        }

        // MTR-1：物化当前版本（含 delete-marked，供判删除可见性），提交释放 index latch（之后绝不与 undo latch 同时持有）
        LogicalRecord current = lookupCurrentIncludingDeleted(index, key);
        if (current == null) {
            return Optional.empty();
        }
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        HiddenColumns hidden = current.hiddenColumns();
        if (hidden == null) {
            throw new DatabaseRuntimeException("clustered record missing hidden columns for mvcc read");
        }
        List<ColumnValue> columns = current.columnValues();
        TransactionId trxId = hidden.dbTrxId();
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        RollPointer rollPtr = hidden.dbRollPtr();
        boolean deleted = current.deleted();

        Set<RollPointer> visited = new HashSet<>();
        int hops = 0;
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        while (true) {
            if (readView.isVisible(trxId)) {
                // 可见的删除 → 该 ReadView 视该行已删除；可见的存活版本 → 返回
                return deleted ? Optional.empty()
                        : Optional.of(new LogicalRecord(index.schema().schemaVersion(), columns, false,
                                RecordType.CONVENTIONAL, new HiddenColumns(trxId, rollPtr)));
            }
            // 不可见且无更早版本（NULL），或指向 INSERT undo（记录在该快照尚不存在；不解码可复用 INSERT undo）
            if (rollPtr.isNull() || rollPtr.insert()) {
                return Optional.empty();
            }
            if (!visited.add(rollPtr) || ++hops > maxVersionHops) {
                throw new UndoLogFormatException(
                        "version chain cycle or exceeds maxVersionHops " + maxVersionHops + " for index " + index.indexId());
            }
            UndoRecord undo = readUndoByRollPointer(index, rollPtr);
            // 所有权链校验：undo 必由产生当前版本的事务写（== 当前版本 DB_TRX_ID），否则串错指针/损坏拼出伪造历史
            if (!undo.transactionId().equals(trxId)) {
                throw new UndoLogFormatException("version chain owner mismatch: undo txn " + undo.transactionId()
                        + " != version DB_TRX_ID " + trxId + " for index " + index.indexId());
            }
            // 版本链节点必为本行的 UPDATE_ROW 或 DELETE_MARK undo（indexId/insert 位已在 readRecordByRollPointer 校验）
            if (undo.type() != UndoRecordType.UPDATE_ROW && undo.type() != UndoRecordType.DELETE_MARK) {
                throw new UndoLogFormatException("expected UPDATE_ROW/DELETE_MARK in version chain, got " + undo.type());
            }
            if (!undo.clusterKey().equals(key.values())) {
                throw new UndoLogFormatException("undo cluster key mismatch in version chain for index " + index.indexId());
            }
            // 构造上一版本（删除/更新 undo 的旧 image 都是存活版本）：旧列值 + 旧隐藏列（其 dbRollPtr 即更早版本指针）
            HiddenColumns oldHidden = undo.oldHiddenColumns();
            columns = undo.oldColumnValues();
            trxId = oldHidden.dbTrxId();
            rollPtr = oldHidden.dbRollPtr();
            deleted = false;
        }
    }

    /**
     * MTR-1：物化聚簇当前版本（含 delete-marked，T1.3f）。读完即提交释放 index latch；异常时
     * {@code rollbackUncommitted} 释放 MTR，避免线程残留绑定 MTR 致后续 {@code begin()} 失败。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param key 参与 {@code lookupCurrentIncludingDeleted} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code lookupCurrentIncludingDeleted} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    private LogicalRecord lookupCurrentIncludingDeleted(BTreeIndex index, SearchKey key) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        MiniTransaction m = mgr.beginReadOnly();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        Optional<BTreeLookupResult> found;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        try {
            found = btree.lookupIncludingDeleted(m, index, key);
        } catch (RuntimeException e) {
            mgr.rollbackUncommitted(m);
            throw e;
        }
        mgr.commit(m);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return found.map(BTreeLookupResult::record).orElse(null);
    }

    /**
     * 每个旧版本独立只读 MTR：按 roll pointer 直读 undo → 提交释放 undo S latch；异常时 {@code rollbackUncommitted}
     * 释放 MTR（同上，防泄漏）。任一时刻只持一个 page latch（index 或单个 undo），不反转写路径 undo→index 锁序。
     * <p>数据流：</p>
     * <ol>
     *     <li>校验事务身份、状态、undo 绑定与冻结计划，所有可重试冲突必须发生在物理修改开始之前。</li>
     *     <li>按既定 lease、MTR、page3 与 undo 页顺序取得资源；进入事务锁等待前不得持有页闩或 buffer fix。</li>
     *     <li>执行 undo/redo、history 或事务终态更新，使物理证据与内存投影在规定提交边界保持一致。</li>
     *     <li>发布 live 状态或返回持久结果并逆序释放资源；越过物理边界后的失败按既有策略 fail-stop。</li>
     * </ol>
     *
     * @param index 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param rollPtr 参与 {@code readUndoByRollPointer} 的稳定领域标识 {@code RollPointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @return {@code readUndoByRollPointer} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    private UndoRecord readUndoByRollPointer(BTreeIndex index, RollPointer rollPtr) {
        // 1、校验事务身份、状态、undo 绑定与冻结计划，在共享或持久副作用前拒绝非法状态。
        MiniTransaction u = mgr.beginReadOnly();
        // 2、继续完成范围、身份与候选校验；通过后，按既定 lease、MTR、page3 与 undo 页顺序取得资源，保持处理顺序与资源边界。
        UndoRecord undo;
        // 3、在中间分支复核阶段性结果；满足条件后，执行 undo/redo、history 或事务终态更新，并维持领域不变量。
        try {
            undo = undoAccess.readRecordByRollPointer(u, undoSpace, rollPtr, index.keyDef(), index.schema());
        } catch (RuntimeException e) {
            mgr.rollbackUncommitted(u);
            throw e;
        }
        mgr.commit(u);
        // 4、发布 live 状态或返回持久结果并逆序释放资源，以稳定返回或领域异常完成收口。
        return undo;
    }
}
