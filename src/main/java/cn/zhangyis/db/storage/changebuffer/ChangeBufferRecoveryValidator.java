package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.storage.mtr.MiniTransaction;
import cn.zhangyis.db.storage.mtr.MiniTransactionManager;
import cn.zhangyis.db.storage.mtr.MiniTransactionState;
import cn.zhangyis.db.storage.recovery.ChangeBufferRecoveryParticipant;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * CHANGE_BUFFER_RECOVER 阶段的只读实现。redo 已经把 page image 恢复到连续边界，本类再证明 header 计数与
 * 可遍历全局树一致；目标页保持惰性，后续首次加载仍由发布前拦截器合并。
 */
public final class ChangeBufferRecoveryValidator implements ChangeBufferRecoveryParticipant {

    /** redo 后读取并校验 system.ibd page3 权威 header 的仓储。 */
    private final ChangeBufferHeaderRepository headers;
    /** redo 后完整遍历并校验 stable identity/CRC 的全局 mutation 树。 */
    private final ChangeBufferStore store;
    /** 逐个 target 交叉验证 buffered/internal 位的用户空间持久 bitmap 仓储。 */
    private final ChangeBufferBitmapRepository bitmaps;
    /** 恢复阶段只读 MTR 的 owner，确保失败时释放所有 system page fix/latch。 */
    private final MiniTransactionManager mtrManager;
    /** 用于识别用户空间重复 IBUF_BITMAP 固定页的实例页大小。 */
    private final PageSize pageSize;
    /** 最近一次成功校验得到的权威 pending 数；仅在完整扫描与 MTR 释放成功后发布。 */
    private volatile long validatedPendingOperations;

    /**
     * @param headers system.ibd page3 仓储
     * @param store 全局树
     * @param bitmaps 用户空间持久 bitmap 仓储
     * @param mtrManager 恢复期只读 MTR 工厂
     * @param pageSize 实例固定页大小，用于重复 bitmap 页分类
     */
    public ChangeBufferRecoveryValidator(ChangeBufferHeaderRepository headers, ChangeBufferStore store,
                                         ChangeBufferBitmapRepository bitmaps,
                                         MiniTransactionManager mtrManager, PageSize pageSize) {
        if (headers == null || store == null || bitmaps == null
                || mtrManager == null || pageSize == null) {
            throw new DatabaseValidationException("change buffer recovery dependencies must not be null");
        }
        this.headers = headers;
        this.store = store;
        this.bitmaps = bitmaps;
        this.mtrManager = mtrManager;
        this.pageSize = pageSize;
    }

    /**
     * 在 redo 连续边界安装后验证 Change Buffer 的全部持久证据，不加载或修改真实二级 leaf。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>清除上次诊断值，并读取 page 3；校验生命周期、固定 root/index/segment identity 和计数边界。</li>
     *     <li>以 {@code pending+1} 哨兵完整扫描全局树，验证 header 计数，防止损坏 header 隐藏树尾记录。</li>
     *     <li>验证每条 target 属于 v1 可管理用户 leaf 范围、单页不超过运行时上限，sequence 全局唯一且严格小于 nextSequence。</li>
     *     <li>提交并释放全部 system.ibd 页资源，再按不同 target 使用短 MTR 交叉验证用户 bitmap。</li>
     *     <li>只有全部证据一致后发布 validated pending；任一步失败都保持零值并释放活动 MTR。</li>
     * </ol>
     *
     * @throws ChangeBufferFormatException header、tree、sequence 或 bitmap 持久证据互相矛盾时抛出，恢复必须停止
     * @throws ChangeBufferStateException pending 超过当前实现可有界物化的上限时抛出，恢复不得开放普通流量
     */
    @Override
    public void validateAfterRedo() {
        validatedPendingOperations = 0L;
        MiniTransaction read = mtrManager.beginReadOnly();
        try {
            // 1、codec 已验证 magic/version/CRC；这里继续验证跨字段稳定 identity 与生命周期证据。
            ChangeBufferHeaderSnapshot header = headers.read(read);
            if (header.state() == ChangeBufferHeaderState.CORRUPTED) {
                throw new ChangeBufferFormatException("change buffer header is marked CORRUPTED");
            }
            if (!header.rootPageId().equals(ChangeBufferBootstrap.ROOT_PAGE_ID)
                    || header.indexId() != ChangeBufferRecordSchema.INDEX_ID
                    || header.leafSegment().equals(header.nonLeafSegment())) {
                throw new ChangeBufferFormatException(
                        "change buffer header has invalid stable root/index/segment identity");
            }
            if (header.nextSequence() <= header.pendingOperations()) {
                throw new ChangeBufferFormatException(
                        "change buffer next sequence cannot cover pending count: next="
                                + header.nextSequence() + ", pending=" + header.pendingOperations());
            }
            if (header.pendingOperations() >= Integer.MAX_VALUE) {
                throw new ChangeBufferStateException(
                        "change buffer pending count exceeds recoverable scan bound: "
                                + header.pendingOperations());
            }

            // 2、pending+1 是完整性哨兵；即使 header 偏小，也会读出额外一条并按精确计数拒绝。
            int limit = Math.toIntExact(header.pendingOperations() + 1L);
            List<ChangeBufferMutation> mutations = store.scanAll(read, limit);
            if (mutations.size() != header.pendingOperations()) {
                throw new ChangeBufferFormatException("change buffer header/tree pending mismatch: header="
                        + header.pendingOperations() + ", tree=" + mutations.size());
            }

            // 3、target 必须落在 v1 已预留 bitmap 范围；sequence 在跨 target 的物理 key 顺序之外仍须全局唯一。
            Set<Long> sequences = new HashSet<>(mutations.size());
            PageId previousTarget = null;
            int mutationsForTarget = 0;
            for (ChangeBufferMutation mutation : mutations) {
                if (mutation.targetPageId().spaceId().equals(ChangeBufferHeaderSnapshot.SYSTEM_SPACE_ID)
                        || ChangeBufferBitmapLayout.isBitmapPage(pageSize, mutation.targetPageId().pageNo())
                        || !bitmaps.supportsTarget(mutation.targetPageId())) {
                    throw new ChangeBufferFormatException(
                            "change buffer mutation targets an internal or unmanaged page: "
                                    + mutation.targetPageId());
                }
                if (!mutation.targetPageId().equals(previousTarget)) {
                    previousTarget = mutation.targetPageId();
                    mutationsForTarget = 0;
                }
                mutationsForTarget++;
                if (mutationsForTarget > SecondaryIndexMutationCoordinator.MAX_PENDING_PER_PAGE) {
                    throw new ChangeBufferFormatException(
                            "change buffer target exceeds per-page mutation limit: " + previousTarget);
                }
                if (mutation.sequence() >= header.nextSequence() || !sequences.add(mutation.sequence())) {
                    throw new ChangeBufferFormatException(
                            "change buffer mutation sequence is duplicate or outside header boundary: "
                                    + mutation.sequence());
                }
            }

            // 4、先释放 system root/header latch，再逐 target 短读用户 bitmap，维持 system→user 的单向资源边界。
            mtrManager.commit(read);
            for (PageId target : mutations.stream()
                    .map(ChangeBufferMutation::targetPageId)
                    .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new))) {
                validateTargetBitmap(target);
            }
            // 5、只有 system tree 与全部用户 bitmap 都验证成功，才向诊断/组合根发布非零结果。
            validatedPendingOperations = header.pendingOperations();
        } catch (RuntimeException failure) {
            if (read.state() == MiniTransactionState.ACTIVE) {
                try {
                    mtrManager.rollbackUncommitted(read);
                } catch (RuntimeException releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            throw failure;
        }
    }

    /**
     * 在不持 system.ibd 页资源时验证一个 pending target 的 bitmap。全局树存在记录而 buffered 位为 false 会使
     * 发布前拦截器跳过全局扫描并发布陈旧 leaf，因此属于不可忽略的持久格式矛盾。
     *
     * @param target 全局树扫描得到的非系统、非 bitmap 目标页
     * @throws ChangeBufferFormatException bitmap 页损坏、目标被标为内部页或 buffered 位缺失时抛出
     */
    private void validateTargetBitmap(PageId target) {
        MiniTransaction bitmapRead = mtrManager.beginReadOnly();
        try {
            ChangeBufferBitmapState bitmap = bitmaps.read(bitmapRead, target);
            if (!bitmap.buffered() || bitmap.changeBufferInternal()) {
                throw new ChangeBufferFormatException(
                        "change buffer global record/bitmap mismatch at " + target
                                + ": buffered=" + bitmap.buffered()
                                + ", internal=" + bitmap.changeBufferInternal());
            }
            mtrManager.commit(bitmapRead);
        } catch (RuntimeException failure) {
            if (bitmapRead.state() == MiniTransactionState.ACTIVE) {
                try {
                    mtrManager.rollbackUncommitted(bitmapRead);
                } catch (RuntimeException releaseFailure) {
                    failure.addSuppressed(releaseFailure);
                }
            }
            throw failure;
        }
    }

    /**
     * 返回最近一次成功完成 redo 后校验时观察到的 pending 数。
     *
     * @return 未成功执行校验前为 {@code 0}；成功后等于同次校验的 header/tree 记录数
     */
    public long validatedPendingOperations() {
        return validatedPendingOperations;
    }
}
