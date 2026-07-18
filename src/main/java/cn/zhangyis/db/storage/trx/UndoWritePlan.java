package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.redo.RedoBudgetWorkload;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoRecordWritePlan;

/**
 * 事务层不可变 undo 写计划。它同时冻结事务全局 undoNo 高水位与目标 kind 的局部 binding/header 快照，
 * 从而允许 INSERT/UPDATE log 的本地序号有间隙，又能在进入物理写前拒绝同事务的陈旧计划。
 */
public final class UndoWritePlan {

    /**
     * 构造时冻结的 {@code transactionId} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final TransactionId transactionId;
    /**
     * 本次事务链路持有的 {@code kind} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoLogKind kind;
    /**
     * 本次事务链路持有的 {@code acquisition} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoSegmentAcquisition acquisition;
    /**
     * 构造时冻结的 {@code expectedFirstPageId} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final PageId expectedFirstPageId;
    /**
     * 构造时冻结的 {@code expectedGlobalLastUndoNo} 稳定领域标识；必须属于本对象的表空间、事务或日志上下文，下游定位与恢复均依赖其身份不变。
     */
    private final UndoNo expectedGlobalLastUndoNo;
    /**
     * 本次事务链路持有的 {@code expectedLogicalHead} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoLogicalHead expectedLogicalHead;
    /**
     * 本次事务链路持有的 {@code persistentSnapshot} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoAppendSnapshot persistentSnapshot;
    /**
     * 本对象持有的 {@code cachedCandidate} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final UndoSegmentReuseDirectory.CacheCandidate cachedCandidate;
    /**
     * 本次事务链路持有的 {@code freeCandidate} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoSegmentReuseDirectory.FreeCandidate freeCandidate;
    /**
     * 本次事务链路持有的 {@code recordPlan} undo/rollback 状态；事务身份、roll pointer 与段代际必须一致，提交、回滚和 purge 路径依赖它完成收口。
     */
    private final UndoRecordWritePlan recordPlan;
    /**
     * 记录 {@code pagesToReserve} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int pagesToReserve;
    /**
     * 本次物理修改持有的 {@code redoWorkload} redo 状态；LSN、预算和批次边界必须单调，刷页与恢复路径依赖它维护 WAL。
     */
    private final RedoBudgetWorkload redoWorkload;

    /**
     * 创建 {@code UndoWritePlan}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param transactionId 事务的稳定标识；不得为 {@code null}，{@code NONE} 只表示尚未绑定事务，不能代替活跃事务身份
     * @param kind 选择 {@code 构造} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param acquisition 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param expectedFirstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param expectedGlobalLastUndoNo 参与 {@code 构造} 的稳定领域标识 {@code UndoNo}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param expectedLogicalHead 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param persistentSnapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @param cachedCandidate 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param freeCandidate 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param recordPlan 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param pagesToReserve 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param redoWorkload redo 收集、定位或重放所需的日志对象；不得为 {@code null}，其 LSN 范围和记录格式必须连续且属于当前恢复或 MTR 上下文
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    UndoWritePlan(TransactionId transactionId, UndoLogKind kind, UndoSegmentAcquisition acquisition,
                  PageId expectedFirstPageId,
                   UndoNo expectedGlobalLastUndoNo, UndoLogicalHead expectedLogicalHead,
                   UndoAppendSnapshot persistentSnapshot,
                   UndoSegmentReuseDirectory.CacheCandidate cachedCandidate,
                   UndoSegmentReuseDirectory.FreeCandidate freeCandidate,
                   UndoRecordWritePlan recordPlan,
                   int pagesToReserve, RedoBudgetWorkload redoWorkload) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (transactionId == null || kind == null || acquisition == null || expectedGlobalLastUndoNo == null
                || expectedLogicalHead == null || recordPlan == null || redoWorkload == null) {
            throw new DatabaseValidationException("undo write plan fields must not be null");
        }
        boolean invalidTarget = switch (acquisition) {
            case ALLOCATE_NEW -> expectedFirstPageId != null || persistentSnapshot != null
                    || cachedCandidate != null || freeCandidate != null || !expectedLogicalHead.isEmpty();
            case REUSE_CACHED -> expectedFirstPageId == null || persistentSnapshot != null
                    || cachedCandidate == null || freeCandidate != null || !expectedLogicalHead.isEmpty()
                    || !expectedFirstPageId.equals(cachedCandidate.segment().handle().firstPageId())
                    || cachedCandidate.segment().kind() != kind;
            case REUSE_FREE -> expectedFirstPageId == null || persistentSnapshot != null
                    || cachedCandidate != null || freeCandidate == null || !expectedLogicalHead.isEmpty()
                    || !expectedFirstPageId.equals(freeCandidate.segment().handle().firstPageId());
            case APPEND_EXISTING -> expectedFirstPageId == null || persistentSnapshot == null
                    || cachedCandidate != null || freeCandidate != null;
        };
        if (transactionId.isNone() || kind == UndoLogKind.TEMPORARY || pagesToReserve < 0 || invalidTarget) {
            throw new DatabaseValidationException("invalid undo write plan snapshot/bounds");
        }
        this.transactionId = transactionId;
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        this.kind = kind;
        this.acquisition = acquisition;
        this.expectedFirstPageId = expectedFirstPageId;
        this.expectedGlobalLastUndoNo = expectedGlobalLastUndoNo;
        this.expectedLogicalHead = expectedLogicalHead;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.persistentSnapshot = persistentSnapshot;
        this.cachedCandidate = cachedCandidate;
        this.freeCandidate = freeCandidate;
        this.recordPlan = recordPlan;
        this.pagesToReserve = pagesToReserve;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.redoWorkload = redoWorkload;
    }

    public TransactionId transactionId() { return transactionId; }
    public UndoLogKind kind() { return kind; }
    /** 本次 append 的 segment 获取方式。 */
    public UndoSegmentAcquisition acquisition() { return acquisition; }
    /** 是否为目标 kind 开启新的事务局部 undo log（fresh allocation 与 cached reuse 都为 true）。 */
    public boolean startsNewLogicalLog() { return acquisition.startsNewLogicalLog(); }
    /** 兼容旧调用名称；语义等同于 {@link #startsNewLogicalLog()}。 */
    public boolean newLog() { return startsNewLogicalLog(); }
    public int pagesToReserve() { return pagesToReserve; }
    public RedoBudgetWorkload redoWorkload() { return redoWorkload; }
    public boolean external() { return recordPlan.external(); }
    public int externalPageCount() { return recordPlan.externalPageCount(); }

    PageId expectedFirstPageId() { return expectedFirstPageId; }
    UndoNo expectedGlobalLastUndoNo() { return expectedGlobalLastUndoNo; }
    UndoLogicalHead expectedLogicalHead() { return expectedLogicalHead; }
    UndoAppendSnapshot persistentSnapshot() { return persistentSnapshot; }
    UndoSegmentReuseDirectory.CacheCandidate cachedCandidate() { return cachedCandidate; }
    UndoSegmentReuseDirectory.FreeCandidate freeCandidate() { return freeCandidate; }
    UndoRecordWritePlan recordPlan() { return recordPlan; }
}
