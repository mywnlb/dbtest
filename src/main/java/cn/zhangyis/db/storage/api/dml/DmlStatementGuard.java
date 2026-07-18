package cn.zhangyis.db.storage.api.dml;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.btree.BTreeIndex;
import cn.zhangyis.db.storage.trx.EmptyUndoBoundary;
import cn.zhangyis.db.storage.trx.RollbackService;
import cn.zhangyis.db.storage.trx.RollbackSummary;
import cn.zhangyis.db.storage.trx.Transaction;
import cn.zhangyis.db.storage.trx.TransactionSavepoint;

/**
 * DML facade 暴露的显式语句边界 Guard。它在语句开始时固定当前事务的逻辑 undo 边界，失败时仅反向应用该
 * 边界之后的 undo，使数据库事务继续保持 ACTIVE；成功时只消费运行期边界，完整事务 undo 链仍由后续
 * commit/full rollback 处理。
 *
 * <p>本对象由创建它的语句执行线程独占，不提供跨线程并发保护。v1 不自动感知 try-with-resources 作用域中是否
 * 抛出了异常：{@link #close()} 明确表示“语句成功”，不会自动回滚。调用方必须在异常处理分支先调用
 * {@link #rollback()}，随后 finally/try-with-resources 再关闭 Guard；一旦成功关闭，边界已消费，不能再回滚。
 *
 * <p>若语句开始时事务已有 undo context，Guard 持有真实 {@link TransactionSavepoint}；若事务尚未首写，Guard
 * 持有 service/transaction 归属的一次性 {@link EmptyUndoBoundary}，失败时撤销语句首写产生的整条当前逻辑链。
 * 两种路径都保留 undo append 高水位、slot、ReadView 和事务级 row locks，符合当前 statement rollback v1
 * 的简化范围。
 */
public final class DmlStatementGuard implements AutoCloseable {

    /** partial rollback 与运行期保存点生命周期的唯一执行入口。 */
    private final RollbackService rollbackService;
    /** Guard 所属数据库事务；整个 Guard 生命周期内必须保持 ACTIVE。 */
    private final Transaction transaction;
    /** 本语句写入的聚簇索引快照；RollbackService 用它解码并反向应用 undo。 */
    private final BTreeIndex clusteredIndex;
    /** 创建 Guard 时固定的边界类型，决定失败时走空链还是保存点回滚。 */
    private final BoundaryKind boundaryKind;
    /** 首写前路径的一次性能力令牌；保存点路径必须为 null。 */
    private final EmptyUndoBoundary emptyUndoBoundary;
    /** 已有 undo 路径的运行期保存点；空 undo 边界路径必须为 null。 */
    private final TransactionSavepoint savepoint;
    /** Guard 的单线程生命周期状态；只有 OPEN 能执行 rollback 或成功 close。 */
    private GuardState state = GuardState.OPEN;

    /**
     * statement guard 支持的两种 undo 边界。该枚举避免用裸布尔值隐藏“首写前”与“已有 undo”两套收尾语义。
     */
    private enum BoundaryKind {
        /** Guard 创建时事务还没有 undo context，失败时回滚到空逻辑链。 */
        EMPTY_UNDO,
        /** Guard 创建时事务已有 undo context，失败时回滚到真实保存点。 */
        SAVEPOINT
    }

    /** Guard 的一次性状态机。 */
    private enum GuardState {
        /** 边界有效，语句仍可选择 rollback 或成功 close。 */
        OPEN,
        /** statement rollback 已完成，后续 close 为幂等 no-op。 */
        ROLLED_BACK,
        /** rollback 结果未完整确认，禁止通过同一 Guard 重试，调用方应执行事务级 abort。 */
        ROLLBACK_FAILED,
        /** 成功路径已释放边界，不能再执行 statement rollback。 */
        CLOSED
    }

    /** 创建首写前空 undo 边界 Guard；仅由同包 DML facade 调用。
     *
     * @param rollbackService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param boundary 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @return {@code emptyBoundary} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    static DmlStatementGuard emptyBoundary(RollbackService rollbackService, Transaction transaction,
                                           BTreeIndex clusteredIndex, EmptyUndoBoundary boundary) {
        return new DmlStatementGuard(rollbackService, transaction, clusteredIndex,
                BoundaryKind.EMPTY_UNDO, boundary, null);
    }

    /** 创建已有 undo context 的保存点 Guard；仅由同包 DML facade 调用。
     *
     * @param rollbackService 由组合根注入的下游协作者；不得为 {@code null}，生命周期至少覆盖本对象
     * @param transaction 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @param clusteredIndex 目标索引的 B+Tree 访问入口；不得为 {@code null}，必须与当前表、索引定义和表空间绑定一致
     * @param savepoint 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
     * @return {@code savepointBoundary} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     */
    static DmlStatementGuard savepointBoundary(RollbackService rollbackService, Transaction transaction,
                                               BTreeIndex clusteredIndex, TransactionSavepoint savepoint) {
        return new DmlStatementGuard(rollbackService, transaction, clusteredIndex,
                BoundaryKind.SAVEPOINT, null, savepoint);
    }

    /**
     * 构造完成后边界种类必须与能力对象一致：EMPTY_UNDO 只携带空边界令牌，SAVEPOINT 只携带真实保存点。
     */
    private DmlStatementGuard(RollbackService rollbackService, Transaction transaction,
                              BTreeIndex clusteredIndex, BoundaryKind boundaryKind,
                              EmptyUndoBoundary emptyUndoBoundary, TransactionSavepoint savepoint) {
        if (rollbackService == null || transaction == null || clusteredIndex == null || boundaryKind == null) {
            throw new DatabaseValidationException("DML statement guard fields must not be null");
        }
        if ((boundaryKind == BoundaryKind.EMPTY_UNDO && (emptyUndoBoundary == null || savepoint != null))
                || (boundaryKind == BoundaryKind.SAVEPOINT && (emptyUndoBoundary != null || savepoint == null))) {
            throw new DatabaseValidationException("DML statement guard boundary/savepoint mismatch");
        }
        this.rollbackService = rollbackService;
        this.transaction = transaction;
        this.clusteredIndex = clusteredIndex;
        this.boundaryKind = boundaryKind;
        this.emptyUndoBoundary = emptyUndoBoundary;
        this.savepoint = savepoint;
    }

    /**
     * 回滚本语句边界之后的全部 DML 修改。RollbackService 先完成所有记录级反向命令并移动逻辑链头；保存点路径
     * 随后释放目标及其嵌套保存点。任一步失败时 Guard 进入 {@code ROLLBACK_FAILED}：MTR commit 异常可能是
     * outcome-uncertain，事务同时被标为 rollback-only，不能继续写入或提交；调用方应转入完整事务
     * rollback/连接错误收尾。
     *
     * @return 本次实际反向应用的 undo record 数量。
     * @throws DmlOperationException Guard 已离开 OPEN 状态，或底层抛出非项目运行时异常时抛出。
     */
    public RollbackSummary rollback() {
        if (state != GuardState.OPEN) {
            throw new DmlOperationException("DML statement guard cannot rollback from state " + state);
        }
        try {
            RollbackSummary summary;
            if (boundaryKind == BoundaryKind.EMPTY_UNDO) {
                summary = rollbackService.rollbackToEmptyStatementBoundary(
                        transaction, clusteredIndex, emptyUndoBoundary);
            } else {
                summary = rollbackService.rollbackToSavepoint(transaction, clusteredIndex, savepoint);
                rollbackService.releaseSavepoint(transaction, savepoint);
            }
            state = GuardState.ROLLED_BACK;
            return summary;
        } catch (RuntimeException e) {
            state = GuardState.ROLLBACK_FAILED;
            try {
                rollbackService.markRollbackOnly(transaction, e);
            } catch (RuntimeException markError) {
                // 事务可能已经离开 ACTIVE；保留原 rollback 根因，并把无法标记 doomed 的状态异常附加用于诊断。
                e.addSuppressed(markError);
            }
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("DML statement rollback failed", e);
        }
    }

    /**
     * 完成语句成功路径。保存点会被释放或空边界令牌会被消费，undo 链保持原样；两者都不持有物理页资源。
     * rollback 完成、rollback 失败后调用或重复 close 均为 no-op，便于异常分支与 finally/try-with-resources
     * 组合使用；其中失败状态不释放边界，必须由事务级收尾处理。
     *
     * @throws DmlOperationException DML/DDL 的校验、物理变更或原子收口失败时抛出；调用方应按语句与事务边界回滚
     */
    @Override
    public void close() {
        if (state != GuardState.OPEN) {
            return;
        }
        try {
            if (boundaryKind == BoundaryKind.EMPTY_UNDO) {
                rollbackService.releaseEmptyStatementBoundary(transaction, emptyUndoBoundary);
            } else {
                rollbackService.releaseSavepoint(transaction, savepoint);
            }
            state = GuardState.CLOSED;
        } catch (RuntimeException e) {
            if (e instanceof DatabaseRuntimeException databaseError) {
                throw databaseError;
            }
            throw new DmlOperationException("DML statement guard close failed", e);
        }
    }
}
