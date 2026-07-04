package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.trx.IsolationLevel;
import cn.zhangyis.db.storage.trx.lock.InsertIntentionLockKey;
import cn.zhangyis.db.storage.trx.lock.LockHandle;
import cn.zhangyis.db.storage.trx.lock.LockManager;
import cn.zhangyis.db.storage.trx.lock.TransactionLockKey;
import cn.zhangyis.db.storage.trx.lock.TransactionLockMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * B+Tree current-read 协调器。它把物理短 MTR 定位和事务逻辑锁等待拆开：先定位 record/gap 并提交 MTR 释放
 * page latch/buffer fix，再进入 LockManager 等待；锁授予后重新定位校验，避免持旧 RecordRef 继续访问。
 *
 * <p>当前支持 point current-read、unique insert 物理检查，以及批量 range current-read。2.1 起单聚簇
 * {@code ClusteredDmlService} 已调用 point/unique 路径并在 facade commit/rollback 释放锁；SQL/session/executor
 * 与 range DML 仍在上层后续切片完成。
 */
public final class BTreeCurrentReadService {

    /** 短 MTR 工厂；每次定位/重定位各用一个独立 MTR。 */
    private final MiniTransactionManager mtrManager;

    /** B+Tree 物理定位服务；包内 current-read 定位方法不泄露 page 资源。 */
    private final SplitCapableBTreeIndexService btree;

    /** 事务锁真相来源；本服务只构造 key 和调用 acquire。 */
    private final LockManager lockManager;

    public BTreeCurrentReadService(MiniTransactionManager mtrManager,
                                   SplitCapableBTreeIndexService btree,
                                   LockManager lockManager) {
        if (mtrManager == null || btree == null || lockManager == null) {
            throw new DatabaseValidationException("current-read collaborators must not be null");
        }
        this.mtrManager = mtrManager;
        this.btree = btree;
        this.lockManager = lockManager;
    }

    /**
     * 对单个 key 执行 current-read 锁定读。命中记录按 FOR_SHARE/FOR_UPDATE 取 REC_S/REC_X；
     * RC miss 不锁 gap，RR miss 按模式取 GAP_S/GAP_X。
     *
     * @return 命中时返回授锁后重新定位得到的当前记录；miss 返回 empty。
     */
    public Optional<BTreeLookupResult> lockPoint(BTreeIndex index, SearchKey key,
                                                 BTreeCurrentReadRequest request,
                                                 BTreeCurrentReadMode mode) {
        validatePoint(index, key, request, mode);
        for (int attempt = 0; attempt < request.maxRelocationRetries(); attempt++) {
            BTreeCurrentReadPosition position = locate(index, key, false);
            if (!position.hit() && request.isolationLevel() == IsolationLevel.READ_COMMITTED) {
                return Optional.empty();
            }
            TransactionLockKey lockKey = position.hit() ? position.recordKey().orElseThrow() : position.gapKey();
            TransactionLockMode lockMode = pointLockMode(position.hit(), mode);
            LockHandle handle = lockManager.acquire(request.owner(), lockKey, lockMode, request.lockWaitTimeout());
            try {
                BTreeCurrentReadPosition relocated = locate(index, key, false);
                if (samePointPosition(position, relocated)) {
                    return relocated.record();
                }
            } catch (RuntimeException e) {
                handle.close();
                throw e;
            }
            handle.close();
        }
        throw new BTreeCurrentReadRelocationException(
                "current-read point relocation exceeded retries for index " + index.indexId());
    }

    /**
     * unique insert 前的 current-read 物理重复检查。若同 key 记录存在，先取 REC_S 并重定位确认后返回 duplicate；
     * 若不存在，则对目标 gap 取 INSERT_INTENTION 并重定位确认后返回 available。
     */
    public BTreeUniqueCheckResult checkUniqueForInsert(BTreeIndex index, SearchKey key,
                                                       BTreeCurrentReadRequest request) {
        validateUnique(index, key, request);
        for (int attempt = 0; attempt < request.maxRelocationRetries(); attempt++) {
            BTreeCurrentReadPosition position = locate(index, key, true);
            TransactionLockKey lockKey = position.hit()
                    ? position.recordKey().orElseThrow()
                    : new InsertIntentionLockKey(position.gapKey());
            TransactionLockMode lockMode = position.hit()
                    ? TransactionLockMode.REC_S
                    : TransactionLockMode.INSERT_INTENTION;
            LockHandle handle = lockManager.acquire(request.owner(), lockKey, lockMode, request.lockWaitTimeout());
            try {
                BTreeCurrentReadPosition relocated = locate(index, key, true);
                if (samePointPosition(position, relocated)) {
                    return relocated.record().map(BTreeUniqueCheckResult::duplicate)
                            .orElseGet(BTreeUniqueCheckResult::available);
                }
            } catch (RuntimeException e) {
                handle.close();
                throw e;
            }
            handle.close();
        }
        throw new BTreeCurrentReadRelocationException(
                "unique-check relocation exceeded retries for index " + index.indexId());
    }

    /**
     * 对有界范围执行 current-read 锁定读。数据流为：短 MTR 定位 range 当前记录和终止 gap → 释放 page latch/fix →
     * 按隔离级别申请事务锁 → 短 MTR 重扫并校验锁落点。RC 只锁返回记录；RR 使用 next-key 加终止 gap 防幻读。
     *
     * @return 授锁后重新定位得到的当前范围记录，按索引顺序排列。
     */
    public List<BTreeLookupResult> lockRange(BTreeIndex index, BTreeScanRange range,
                                             BTreeCurrentReadRequest request,
                                             BTreeCurrentReadMode mode) {
        validateRange(index, range, request, mode);
        for (int attempt = 0; attempt < request.maxRelocationRetries(); attempt++) {
            BTreeCurrentReadRangePosition position = locateRange(index, range);
            if (position.records().isEmpty()
                    && request.isolationLevel() == IsolationLevel.READ_COMMITTED) {
                return List.of();
            }
            List<LockHandle> handles = new ArrayList<>();
            try {
                acquireRangeLocks(position, request, mode, handles);
                BTreeCurrentReadRangePosition relocated = locateRange(index, range);
                if (sameRangePosition(position, relocated, request.isolationLevel())) {
                    return relocated.records().stream()
                            .map(hit -> hit.record().orElseThrow())
                            .toList();
                }
            } catch (RuntimeException e) {
                closeAll(handles);
                throw e;
            }
            closeAll(handles);
        }
        throw new BTreeCurrentReadRelocationException(
                "current-read range relocation exceeded retries for index " + index.indexId());
    }

    private BTreeCurrentReadPosition locate(BTreeIndex index, SearchKey key, boolean includeDeleted) {
        MiniTransaction mtr = mtrManager.begin();
        try {
            BTreeCurrentReadPosition position = btree.locatePointForCurrentRead(mtr, index, key, includeDeleted);
            mtrManager.commit(mtr);
            return position;
        } catch (RuntimeException e) {
            mtrManager.rollbackUncommitted(mtr);
            throw e;
        }
    }

    private BTreeCurrentReadRangePosition locateRange(BTreeIndex index, BTreeScanRange range) {
        MiniTransaction mtr = mtrManager.begin();
        try {
            BTreeCurrentReadRangePosition position = btree.locateRangeForCurrentRead(mtr, index, range);
            mtrManager.commit(mtr);
            return position;
        } catch (RuntimeException e) {
            mtrManager.rollbackUncommitted(mtr);
            throw e;
        }
    }

    private static void validatePoint(BTreeIndex index, SearchKey key,
                                      BTreeCurrentReadRequest request, BTreeCurrentReadMode mode) {
        if (index == null || key == null || request == null || mode == null) {
            throw new DatabaseValidationException("current-read point args must not be null");
        }
    }

    private static void validateUnique(BTreeIndex index, SearchKey key, BTreeCurrentReadRequest request) {
        if (index == null || key == null || request == null) {
            throw new DatabaseValidationException("unique current-read args must not be null");
        }
        if (!index.unique()) {
            throw new DatabaseValidationException("unique current-read requires unique index: " + index.indexId());
        }
    }

    private static void validateRange(BTreeIndex index, BTreeScanRange range,
                                      BTreeCurrentReadRequest request, BTreeCurrentReadMode mode) {
        if (index == null || range == null || request == null || mode == null) {
            throw new DatabaseValidationException("current-read range args must not be null");
        }
    }

    private void acquireRangeLocks(BTreeCurrentReadRangePosition position, BTreeCurrentReadRequest request,
                                   BTreeCurrentReadMode mode, List<LockHandle> handles) {
        for (BTreeCurrentReadPosition record : position.records()) {
            TransactionLockKey key = request.isolationLevel() == IsolationLevel.REPEATABLE_READ
                    ? record.nextKey().orElseThrow()
                    : record.recordKey().orElseThrow();
            TransactionLockMode lockMode = rangeRecordLockMode(request.isolationLevel(), mode);
            handles.add(lockManager.acquire(request.owner(), key, lockMode, request.lockWaitTimeout()));
        }
        if (request.isolationLevel() == IsolationLevel.REPEATABLE_READ
                && position.terminalGap().isPresent()) {
            handles.add(lockManager.acquire(request.owner(), position.terminalGap().orElseThrow(),
                    rangeGapLockMode(mode), request.lockWaitTimeout()));
        }
    }

    private static TransactionLockMode pointLockMode(boolean hit, BTreeCurrentReadMode mode) {
        if (hit) {
            return mode == BTreeCurrentReadMode.FOR_SHARE
                    ? TransactionLockMode.REC_S
                    : TransactionLockMode.REC_X;
        }
        return mode == BTreeCurrentReadMode.FOR_SHARE
                ? TransactionLockMode.GAP_S
                : TransactionLockMode.GAP_X;
    }

    private static TransactionLockMode rangeRecordLockMode(IsolationLevel isolationLevel,
                                                           BTreeCurrentReadMode mode) {
        if (isolationLevel == IsolationLevel.REPEATABLE_READ) {
            return mode == BTreeCurrentReadMode.FOR_SHARE
                    ? TransactionLockMode.NEXT_KEY_S
                    : TransactionLockMode.NEXT_KEY_X;
        }
        return mode == BTreeCurrentReadMode.FOR_SHARE
                ? TransactionLockMode.REC_S
                : TransactionLockMode.REC_X;
    }

    private static TransactionLockMode rangeGapLockMode(BTreeCurrentReadMode mode) {
        return mode == BTreeCurrentReadMode.FOR_SHARE
                ? TransactionLockMode.GAP_S
                : TransactionLockMode.GAP_X;
    }

    private static boolean samePointPosition(BTreeCurrentReadPosition before,
                                             BTreeCurrentReadPosition after) {
        if (before.hit() != after.hit()) {
            return false;
        }
        if (before.hit()) {
            return before.recordKey().orElseThrow().equals(after.recordKey().orElseThrow());
        }
        return before.gapKey().equals(after.gapKey());
    }

    private static boolean sameRangePosition(BTreeCurrentReadRangePosition before,
                                             BTreeCurrentReadRangePosition after,
                                             IsolationLevel isolationLevel) {
        if (before.records().size() != after.records().size()) {
            return false;
        }
        for (int i = 0; i < before.records().size(); i++) {
            BTreeCurrentReadPosition left = before.records().get(i);
            BTreeCurrentReadPosition right = after.records().get(i);
            if (isolationLevel == IsolationLevel.REPEATABLE_READ) {
                if (!left.nextKey().orElseThrow().equals(right.nextKey().orElseThrow())) {
                    return false;
                }
            } else if (!left.recordKey().orElseThrow().equals(right.recordKey().orElseThrow())) {
                return false;
            }
        }
        return isolationLevel != IsolationLevel.REPEATABLE_READ
                || before.terminalGap().equals(after.terminalGap());
    }

    private static void closeAll(List<LockHandle> handles) {
        for (int i = handles.size() - 1; i >= 0; i--) {
            handles.get(i).close();
        }
    }
}
