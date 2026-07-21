package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.RollbackSegmentId;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 1.6 双 undo log 事务上下文单元测试。 */
class UndoContextTest {
    private static final RollbackSegmentId RSEG = RollbackSegmentId.of(0);
    private static final UndoSlotId SLOT = UndoSlotId.of(3);
    private static final PageId FIRST = PageId.of(SpaceId.of(77), PageNo.of(65));

    /**
     * 验证 {@code freshContextHasNoBindingsAndNoneHighWater} 所描述的 SQL 解析或绑定语义，并断言 AST、名称解析、类型推导及错误位置。
     */
    @Test void freshContextHasNoBindingsAndNoneHighWater() {
        UndoContext context = new UndoContext(RSEG);
        assertTrue(context.bindings().isEmpty());
        assertEquals(UndoNo.NONE, context.lastUndoNo());
    }

    /**
     * 验证 {@code attachInsertBindingKeepsIdentity} 所描述的返回值或状态会按契约保留，并断言原始信息与领域不变量未丢失。
     */
    @Test void attachInsertBindingKeepsIdentity() {
        UndoContext context = freshInsert();
        assertEquals(SLOT, context.binding(UndoLogKind.INSERT).slotId());
        assertEquals(FIRST, context.binding(UndoLogKind.INSERT).firstPageId());
    }

    /**
     * 验证 {@code duplicateKindIsRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test void duplicateKindIsRejected() {
        UndoContext context = freshInsert();
        assertThrows(DatabaseValidationException.class, () -> context.attach(new UndoLogBinding(
                UndoLogKind.INSERT, UndoSlotId.of(4), PageId.of(SpaceId.of(77), PageNo.of(66)),
                UndoLogicalHead.EMPTY)));
    }

    /**
     * 验证 {@code globalHighWaterAndLocalHeadAdvanceTogetherAfterAppend} 所描述的页内记录行为，并断言偏移、编码边界、隐藏列及 page-directory 结构保持一致。
     */
    @Test void globalHighWaterAndLocalHeadAdvanceTogetherAfterAppend() {
        UndoContext context = freshInsert();
        RollPointer pointer = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer);
        assertEquals(UndoNo.of(1), context.lastUndoNo());
        assertEquals(new UndoLogicalHead(UndoNo.of(1), pointer), context.head(UndoLogKind.INSERT));
    }

    /**
     * 验证 {@code appendMustAdvanceGlobalUndoNumber} 所描述的恢复场景能够依据持久证据幂等重建状态，且不会重复产生副作用。
     */
    @Test void appendMustAdvanceGlobalUndoNumber() {
        UndoContext context = freshInsert();
        RollPointer pointer = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer);
        assertThrows(DatabaseValidationException.class,
                () -> context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer));
    }

    /**
     * 验证 {@code savepointCapturesBothHeads} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test void savepointCapturesBothHeads() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext context = freshInsert();
        txn.setUndoContext(context);
        RollPointer pointer = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), pointer);
        TransactionSavepoint savepoint = context.createSavepoint(txn);
        assertEquals(new UndoLogicalHead(UndoNo.of(1), pointer), savepoint.insertHead());
        assertEquals(UndoLogicalHead.EMPTY, savepoint.updateHead());
    }

    /**
     * 验证 {@code rollbackToSavepointDoesNotRewindGlobalHighWater} 所描述的边界场景保持既有领域不变量，不产生方法名明确禁止的副作用。
     */
    @Test void rollbackToSavepointDoesNotRewindGlobalHighWater() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext context = freshInsert();
        txn.setUndoContext(context);
        RollPointer first = new RollPointer(true, PageNo.of(65), 120);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1), first);
        TransactionSavepoint savepoint = context.createSavepoint(txn);
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(2),
                new RollPointer(true, PageNo.of(65), 150));
        context.completeRollbackToSavepoint(savepoint);
        assertEquals(UndoNo.of(2), context.lastUndoNo());
        assertEquals(new UndoLogicalHead(UndoNo.of(1), first), context.head(UndoLogKind.INSERT));
    }

    /**
     * 验证 {@code emptyBoundaryClearsEveryExistingLocalHead} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test void emptyBoundaryClearsEveryExistingLocalHead() {
        UndoContext context = freshInsert();
        context.publishAppend(UndoLogKind.INSERT, UndoNo.of(1),
                new RollPointer(true, PageNo.of(65), 120));
        context.completeRollbackToEmptyBoundary();
        assertEquals(UndoLogicalHead.EMPTY, context.head(UndoLogKind.INSERT));
        assertEquals(UndoNo.of(1), context.lastUndoNo());
    }

    /** UPDATE affected-table 投影只描述当前 logical head 可达记录，savepoint/full rollback 后不得保留假引用。 */
    @Test
    void affectedTablesFollowReachableUpdateLogicalHead() {
        UndoContext context = freshUpdate();
        RollPointer first = new RollPointer(false, PageNo.of(65), 120);
        RollPointer second = new RollPointer(false, PageNo.of(65), 180);
        context.publishAppend(UndoLogKind.UPDATE, UndoNo.of(1), first, 11L);
        context.publishAppend(UndoLogKind.UPDATE, UndoNo.of(2), second, 12L);

        assertEquals(java.util.Set.of(11L, 12L), context.affectedTableIds());

        context.publishRollbackProgress(UndoLogKind.UPDATE, new UndoLogicalHead(UndoNo.of(1), first));
        assertEquals(java.util.Set.of(11L), context.affectedTableIds());

        context.completeRollbackToEmptyBoundary();
        assertTrue(context.affectedTableIds().isEmpty());
    }

    /**
     * 验证释放旧保存点只撤销该 runtime handle，不会误删随后创建的替代边界。
     *
     * <p>命名 SAVEPOINT 同名替换必须先创建新边界，再释放旧边界；若底层按区间删除，替代边界会被一并
     * 清除，Session 名称表将持有失效 capability。两个边界随后都能独立释放，证明 owner stack 没有泄漏。</p>
     */
    @Test void releaseSavepointKeepsNestedBoundaries() {
        Transaction txn = new Transaction(TransactionOptions.defaults(), 1L);
        UndoContext context = freshInsert();
        txn.setUndoContext(context);
        TransactionSavepoint first = context.createSavepoint(txn);
        TransactionSavepoint nested = context.createSavepoint(txn);
        context.releaseSavepoint(first);
        assertEquals(1, context.savepointCount());
        context.releaseSavepoint(nested);
        assertEquals(0, context.savepointCount());
    }

    /**
     * 验证 {@code temporaryBindingIsRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test void temporaryBindingIsRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoLogBinding(
                UndoLogKind.TEMPORARY, SLOT, FIRST, UndoLogicalHead.EMPTY));
    }

    /**
     * 验证 {@code nonEmptyBindingHeadMustMatchPersistentLogKind} 所描述的 SQL 解析或绑定语义，并断言 AST、名称解析、类型推导及错误位置。
     */
    @Test void nonEmptyBindingHeadMustMatchPersistentLogKind() {
        assertThrows(DatabaseValidationException.class, () -> new UndoLogBinding(
                UndoLogKind.INSERT, SLOT, FIRST,
                new UndoLogicalHead(UndoNo.of(1), new RollPointer(false, PageNo.of(65), 120))));
        assertThrows(DatabaseValidationException.class, () -> new UndoLogBinding(
                UndoLogKind.UPDATE, SLOT, FIRST,
                new UndoLogicalHead(UndoNo.of(1), new RollPointer(true, PageNo.of(65), 120))));
    }

    /**
     * 验证 {@code nullRollbackSegmentIsRejected} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test void nullRollbackSegmentIsRejected() {
        assertThrows(DatabaseValidationException.class, () -> new UndoContext(null));
        assertFalse(RSEG.value() < 0);
    }

    private static UndoContext freshInsert() {
        UndoContext context = new UndoContext(RSEG);
        context.attach(new UndoLogBinding(UndoLogKind.INSERT, SLOT, FIRST, UndoLogicalHead.EMPTY));
        return context;
    }

    private static UndoContext freshUpdate() {
        UndoContext context = new UndoContext(RSEG);
        context.attach(new UndoLogBinding(UndoLogKind.UPDATE, SLOT, FIRST, UndoLogicalHead.EMPTY));
        return context;
    }
}
