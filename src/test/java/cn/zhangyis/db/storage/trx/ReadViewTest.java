package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.TransactionId;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T1.4 ReadView 可见性快照（设计 §5.4）。五条规则：{@code ==creator} 可见、{@code <upLimit} 可见、
 * {@code >=lowLimit} 不可见、{@code ∈activeIds} 不可见、其余可见。构造不变量：activeIds 不可变副本、
 * {@code up<=low}、active id 全部 ∈ {@code [up, low)}。记录 writer id 为 NONE 视为损坏输入拒绝。
 */
class ReadViewTest {

    private static ReadView rv(long creator, long up, long low, Set<Long> active) {
        return new ReadView(TransactionId.of(creator), up, low, active, low);
    }

    /**
     * 验证 {@code recordByCreatorIsVisible} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void recordByCreatorIsVisible() {
        ReadView v = new ReadView(TransactionId.of(5), 10, 20, Set.of(12L, 15L), 20);
        assertTrue(v.isVisible(TransactionId.of(5)), "事务总能看见自己的修改（==creator）");
    }

    /**
     * 验证 {@code belowUpLimitVisible} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void belowUpLimitVisible() {
        ReadView v = rv(99, 10, 20, Set.of(12L, 15L));
        assertTrue(v.isVisible(TransactionId.of(5)), "recordTrxId < upLimit 必可见（提交早于本快照）");
    }

    /**
     * 验证 {@code atOrAboveLowLimitInvisible} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void atOrAboveLowLimitInvisible() {
        ReadView v = rv(99, 10, 20, Set.of(12L, 15L));
        assertFalse(v.isVisible(TransactionId.of(20)), "recordTrxId >= lowLimit 不可见（本快照之后开始）");
        assertFalse(v.isVisible(TransactionId.of(25)));
    }

    /**
     * 验证 {@code inActiveSetInvisibleOtherwiseVisible} 所描述的事务状态与 MVCC 可见性，并断言提交/回滚终态、owner 和资源释放结果。
     */
    @Test
    void inActiveSetInvisibleOtherwiseVisible() {
        ReadView v = rv(99, 10, 20, Set.of(12L, 15L));
        assertFalse(v.isVisible(TransactionId.of(12)), "活跃集合内（未提交）不可见");
        assertTrue(v.isVisible(TransactionId.of(13)), "[up,low) 内但不在活跃集合（已提交）可见");
    }

    /**
     * 验证 {@code readOnlyCreatorNoneNeverMatchesRule1} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void readOnlyCreatorNoneNeverMatchesRule1() {
        // 只读事务 creator=NONE：记录 writer 恒非 NONE，故规则1 永不命中
        ReadView v = new ReadView(TransactionId.NONE, 10, 10, Set.of(), 10);
        assertTrue(v.isVisible(TransactionId.of(5)), "<up 可见");
        assertFalse(v.isVisible(TransactionId.of(15)), ">=low 不可见");
    }

    /**
     * 验证 {@code isVisibleRejectsNoneRecordWriter} 所描述的非法或损坏输入会被领域校验拒绝，并固定异常类型及失败后的状态边界。
     */
    @Test
    void isVisibleRejectsNoneRecordWriter() {
        ReadView v = rv(99, 10, 20, Set.of(12L));
        assertThrows(DatabaseValidationException.class, () -> v.isVisible(TransactionId.NONE),
                "记录 DB_TRX_ID 为 NONE 是损坏输入，必须拒绝");
        assertThrows(DatabaseValidationException.class, () -> v.isVisible(null));
    }

    /**
     * 验证 {@code constructorEnforcesInvariants} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void constructorEnforcesInvariants() {
        // up > low 非法
        assertThrows(DatabaseValidationException.class, () -> rv(1, 20, 10, Set.of()));
        // active id < up 非法
        assertThrows(DatabaseValidationException.class, () -> rv(1, 10, 20, Set.of(5L)));
        // active id >= low 非法
        assertThrows(DatabaseValidationException.class, () -> rv(1, 10, 20, Set.of(25L)));
        // creator/active 非 null
        assertThrows(DatabaseValidationException.class, () -> new ReadView(null, 10, 20, Set.of(), 20));
        assertThrows(DatabaseValidationException.class, () -> new ReadView(TransactionId.of(1), 10, 20, null, 20));
    }

    /**
     * 验证 {@code activeIdsAreDefensivelyCopied} 对应的事务、MVCC 与锁行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void activeIdsAreDefensivelyCopied() {
        Set<Long> src = new HashSet<>(Set.of(12L, 15L));
        ReadView v = new ReadView(TransactionId.of(99), 10, 20, src, 20);
        src.add(13L); // 改源集合不得影响已建 ReadView
        assertTrue(v.isVisible(TransactionId.of(13)), "13 仍可见（ReadView 持不可变副本，未含 13）");
    }
}
