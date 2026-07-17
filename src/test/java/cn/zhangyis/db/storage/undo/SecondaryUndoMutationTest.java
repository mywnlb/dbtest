package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 undo secondary tail 的领域约束。mutation 只保存 index identity/action/state，物理 key 在执行时由
 * 当前聚簇行与 undo old image 通过 exact-version {@code SecondaryIndexLayout} 重建，避免在 undo 中复制类型协议。
 */
class SecondaryUndoMutationTest {

    /** 测试写事务；用于构造合法 undo record，不参与事务状态机。 */
    private static final TransactionId TRANSACTION_ID = TransactionId.of(51);
    /** 聚簇主键；tail mutation 不重复保存该 key。 */
    private static final List<ColumnValue> CLUSTER_KEY = List.of(new ColumnValue.IntValue(7));
    /** UPDATE/DELETE 的旧隐藏列；证明新增 tail 不改变既有版本链字段。 */
    private static final HiddenColumns OLD_HIDDEN = new HiddenColumns(
            TransactionId.of(9), new RollPointer(false, cn.zhangyis.db.domain.PageNo.of(64), 3));

    /**
     * INSERT/UPDATE/DELETE mutation 工厂必须生成与 action 对应的唯一 before-state 组合。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>分别通过三个领域工厂构造 INSERT、UPDATE(absent/revive) 与 DELETE mutation。</li>
     *     <li>断言 action 与 before-state 组合能唯一决定 rollback inverse，不依赖 nullable/默认状态。</li>
     * </ol>
     */
    @Test
    void createsActionSpecificMutationShapes() {
        // 1. UPDATE 的两种前态必须分别保留，INSERT/DELETE 则固定使用 NOT_APPLICABLE。
        SecondaryUndoMutation insert = SecondaryUndoMutation.insertEntry(11);
        SecondaryUndoMutation updateAbsent = SecondaryUndoMutation.changeKey(
                12, SecondaryEntryBeforeState.ABSENT);
        SecondaryUndoMutation updateRevived = SecondaryUndoMutation.changeKey(
                13, SecondaryEntryBeforeState.DELETE_MARKED);
        SecondaryUndoMutation delete = SecondaryUndoMutation.deleteMarkEntry(14);

        // 2. 每个工厂只能产生与主 undo type 兼容的唯一 action/state 形状。
        assertEquals(SecondaryUndoAction.INSERT_ENTRY, insert.action());
        assertEquals(SecondaryEntryBeforeState.NOT_APPLICABLE, insert.newEntryBeforeState());
        assertEquals(SecondaryEntryBeforeState.ABSENT, updateAbsent.newEntryBeforeState());
        assertEquals(SecondaryEntryBeforeState.DELETE_MARKED, updateRevived.newEntryBeforeState());
        assertEquals(SecondaryUndoAction.DELETE_MARK_ENTRY, delete.action());
    }

    /**
     * 非法 action/state 组合会让 rollback 无法判断“删除新 entry”还是“恢复原 delete mark”，必须在写盘前拒绝。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>直接构造三个违反 action/state 约束的 mutation。</li>
     *     <li>断言全部在领域对象构造期抛校验异常，不进入 codec 或 recovery。</li>
     * </ol>
     */
    @Test
    void rejectsInvalidActionStateCombinations() {
        // 1、2. 每个错误组合都必须在 immutable mutation 创建时 fail-closed。
        assertThrows(DatabaseValidationException.class, () -> new SecondaryUndoMutation(
                11, SecondaryUndoAction.INSERT_ENTRY, SecondaryEntryBeforeState.ABSENT));
        assertThrows(DatabaseValidationException.class, () -> new SecondaryUndoMutation(
                11, SecondaryUndoAction.CHANGE_KEY, SecondaryEntryBeforeState.NOT_APPLICABLE));
        assertThrows(DatabaseValidationException.class, () -> new SecondaryUndoMutation(
                11, SecondaryUndoAction.DELETE_MARK_ENTRY, SecondaryEntryBeforeState.DELETE_MARKED));
    }

    /**
     * UndoRecord 必须按 index id 严格递增并限制 action 与 undo type 一致；该顺序同时定义 DML/rollback 的跨树顺序，
     * 防止恢复时因集合迭代顺序漂移而改变 latch acquisition 顺序。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>构造合法递增 INSERT mutation 列表并冻结到 UndoRecord。</li>
     *     <li>验证倒序和主 undo type/action 错配均被构造器拒绝。</li>
     *     <li>构造合法 UPDATE/DELETE record，确认各自 tail action 保持不变。</li>
     * </ol>
     */
    @Test
    void enforcesSortedTypeCompatibleMutations() {
        // 1. 合法 INSERT tail 按稳定 index id 排序并原样保留。
        List<SecondaryUndoMutation> insertMutations = List.of(
                SecondaryUndoMutation.insertEntry(11), SecondaryUndoMutation.insertEntry(12));
        UndoRecord insert = UndoRecord.insert(UndoNo.of(1), TRANSACTION_ID, 1, 3, CLUSTER_KEY,
                List.of(), insertMutations, RollPointer.NULL);
        assertEquals(insertMutations, insert.secondaryMutations());

        // 2. 倒序会改变跨树 inverse 顺序，action 错配会改变恢复语义，两者必须在编码前失败。
        assertThrows(DatabaseValidationException.class, () -> UndoRecord.insert(
                UndoNo.of(1), TRANSACTION_ID, 1, 3, CLUSTER_KEY, List.of(),
                List.of(SecondaryUndoMutation.insertEntry(12), SecondaryUndoMutation.insertEntry(11)),
                RollPointer.NULL));
        assertThrows(DatabaseValidationException.class, () -> UndoRecord.insert(
                UndoNo.of(1), TRANSACTION_ID, 1, 3, CLUSTER_KEY, List.of(),
                List.of(SecondaryUndoMutation.changeKey(11, SecondaryEntryBeforeState.ABSENT)),
                RollPointer.NULL));

        // 3. UPDATE/DELETE 只接收与主 record type 一致的 CHANGE_KEY/DELETE_MARK_ENTRY。
        UndoRecord update = UndoRecord.update(UndoNo.of(2), TRANSACTION_ID, 1, 3, CLUSTER_KEY, CLUSTER_KEY,
                OLD_HIDDEN, List.of(SecondaryUndoMutation.changeKey(
                        11, SecondaryEntryBeforeState.DELETE_MARKED)), RollPointer.NULL);
        assertEquals(SecondaryUndoAction.CHANGE_KEY, update.secondaryMutations().getFirst().action());

        UndoRecord delete = UndoRecord.deleteMark(UndoNo.of(3), TRANSACTION_ID, 1, 3, CLUSTER_KEY, CLUSTER_KEY,
                OLD_HIDDEN, List.of(SecondaryUndoMutation.deleteMarkEntry(11)), RollPointer.NULL);
        assertEquals(SecondaryUndoAction.DELETE_MARK_ENTRY, delete.secondaryMutations().getFirst().action());
    }

    /**
     * mutation 列表必须做防御性复制，write plan 冻结后调用方修改原集合不能改变 redo workload 或落盘内容。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>用可变列表构造 INSERT UndoRecord，再修改原列表。</li>
     *     <li>断言 record 中 tail 长度不变且返回列表不可修改。</li>
     * </ol>
     */
    @Test
    void defensivelyCopiesMutationList() {
        // 1. record 构造时必须复制集合，不能保存调用方 mutable list 引用。
        List<SecondaryUndoMutation> mutable = new ArrayList<>();
        mutable.add(SecondaryUndoMutation.insertEntry(11));
        UndoRecord record = UndoRecord.insert(UndoNo.of(1), TRANSACTION_ID, 1, 3, CLUSTER_KEY,
                List.of(), mutable, RollPointer.NULL);

        mutable.add(SecondaryUndoMutation.insertEntry(12));

        // 2. 后续 append 与通过 accessor 修改都不能改变已冻结 record。
        assertEquals(1, record.secondaryMutations().size());
        assertThrows(UnsupportedOperationException.class,
                () -> record.secondaryMutations().add(SecondaryUndoMutation.insertEntry(13)));
    }
}
