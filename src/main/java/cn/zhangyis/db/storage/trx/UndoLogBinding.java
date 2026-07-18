package cn.zhangyis.db.storage.trx;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.UndoSlotId;
import cn.zhangyis.db.storage.undo.UndoLogKind;
import cn.zhangyis.db.storage.undo.UndoLogicalHead;
import cn.zhangyis.db.storage.undo.UndoAppendSnapshot;

/**
 * 事务拥有的一条独立 undo log 运行期绑定。slot/first page 在创建后不可变，logical head 只在 append 或
 * rollback marker 已成功提交后发布；append head 会在同一业务 MTR 的物理 undo 写完成后、聚簇写之前发布，
 * 后续业务 MTR 失败必须走 fail-stop。对象仅由所属事务线程修改，不承担跨事务同步。
 */
public final class UndoLogBinding {

    /** log 种类；只允许普通 INSERT/UPDATE，决定 record 类型与终结策略。 */
    private final UndoLogKind kind;
    /** page3 中独占的 slot；直到 commit/rollback/purge 原子终结后才可释放。 */
    private final UndoSlotId slotId;
    /** FSP segment 首页，也是该 log header 的持久权威入口。 */
    private final PageId firstPageId;
    /** 当前已持久化的局部逻辑链头；partial/full rollback 只推进所属 log 的该字段。 */
    private UndoLogicalHead logicalHead;
    /** 单 writer 最近一次 append/marker 后的完整物理快照；新 log 首条 append 发布前可为 null。 */
    private UndoAppendSnapshot appendSnapshot;

    /**
     * 创建 {@code UndoLogBinding}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝 null、越界和相互矛盾的组合。</li>
     *     <li>完成跨参数校验并推导不可变配置；若构造过程创建自有资源，后续失败必须在异常路径关闭。</li>
     *     <li>把已校验协作者与配置绑定到字段，并初始化本对象拥有的状态、显式锁、队列或缓存，不允许 this 提前逃逸。</li>
     *     <li>构造完成后对象处于类契约声明的初始状态；任一步失败都抛出领域异常且不发布半初始化实例。</li>
     * </ol>
     *
     * @param kind 选择 {@code 构造} 分支的 {@code UndoLogKind} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param slotId 参与 {@code 构造} 的稳定领域标识 {@code UndoSlotId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param firstPageId 目标页的稳定物理标识；必须属于当前已准入表空间，且不得为 {@code null}
     * @param logicalHead 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public UndoLogBinding(UndoLogKind kind, UndoSlotId slotId, PageId firstPageId,
                          UndoLogicalHead logicalHead) {
        // 1、校验必需协作者、身份与配置边界，在字段赋值或资源打开前拒绝非法组合。
        if (kind == null || slotId == null || firstPageId == null || logicalHead == null) {
            throw new DatabaseValidationException("undo log binding fields must not be null");
        }
        if (kind == UndoLogKind.TEMPORARY) {
            throw new DatabaseValidationException("temporary undo binding is not supported");
        }
        // 2、完成跨参数校验并推导不可变配置；后续失败仍由当前构造路径收口已创建资源。
        requireHeadKind(kind, logicalHead);
        this.kind = kind;
        // 3、绑定已校验协作者并初始化本对象拥有的状态、显式锁、队列或缓存，不允许半初始化实例逃逸。
        this.slotId = slotId;
        this.firstPageId = firstPageId;
        // 4、完成初始状态发布；失败以领域异常终止构造，成功对象满足类级生命周期不变量。
        this.logicalHead = logicalHead;
    }

    public UndoLogKind kind() { return kind; }
    public UndoSlotId slotId() { return slotId; }
    public PageId firstPageId() { return firstPageId; }
    public UndoLogicalHead logicalHead() { return logicalHead; }
    UndoAppendSnapshot appendSnapshot() { return appendSnapshot; }

    /** 在所属物理写已提交后发布新的局部头。
     *
     * @param head 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void publishHead(UndoLogicalHead head) {
        if (head == null) {
            throw new DatabaseValidationException("undo log binding head must not be null");
        }
        requireHeadKind(kind, head);
        this.logicalHead = head;
        if (appendSnapshot != null) {
            appendSnapshot = new UndoAppendSnapshot(appendSnapshot.firstPageId(), appendSnapshot.lastPageId(),
                    appendSnapshot.segmentId(), appendSnapshot.inodeSlot(), appendSnapshot.transactionId(),
                    appendSnapshot.lastUndoNo(), head, appendSnapshot.kind(), appendSnapshot.recordCount(),
                    appendSnapshot.tailFreeOffset());
        }
    }

    /** append 页写与 header 推进成功后一起发布新的局部头和完整物理快照。
     *
     * @param head 事务回滚链上的 undo 记录、计划或段访问对象；不得为 {@code null}，其事务身份、roll pointer 和段生命周期必须相互一致
     * @param snapshot 调用方提供的不可变领域输入；必须先通过其构造校验且不得为 {@code null}
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    void publishAppend(UndoLogicalHead head, UndoAppendSnapshot snapshot) {
        if (head == null || snapshot == null || !snapshot.firstPageId().equals(firstPageId)
                || snapshot.kind() != kind || !snapshot.logicalHead().equals(head)
                || !snapshot.lastUndoNo().equals(head.undoNo())) {
            throw new DatabaseValidationException("undo binding append snapshot does not match published head");
        }
        requireHeadKind(kind, head);
        this.logicalHead = head;
        this.appendSnapshot = snapshot;
    }

    /** 非空局部头的 RollPointer.insert 位必须与持久 log kind 一致。 */
    private static void requireHeadKind(UndoLogKind kind, UndoLogicalHead head) {
        if (!head.isEmpty() && head.rollPointer().insert() != (kind == UndoLogKind.INSERT)) {
            throw new DatabaseValidationException("undo logical head pointer kind does not match " + kind);
        }
    }
}
