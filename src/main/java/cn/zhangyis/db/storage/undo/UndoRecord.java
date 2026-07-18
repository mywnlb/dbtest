package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.record.format.HiddenColumns;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * undo record（类型判别命令对象，设计 §5.5/§6.5）。
 *
 * <ul>
 *   <li><b>INSERT_ROW</b>（T1.3a 起）：保存按 cluster key 物理删除未提交插入所需信息；rollback 用
 *       {@code indexId}+{@code clusterKey} 反查聚簇记录，并以 {@code transactionId}+roll pointer 校验
 *       「找到的是本事务的未提交插入」。不带旧 image（insert 无旧版本）。</li>
 *   <li><b>UPDATE_ROW</b>（T1.3e 起）：额外保存**全量旧 image**——{@code oldColumnValues}（更新前全列值）+
 *       {@code oldHiddenColumns}（更新前 DB_TRX_ID/DB_ROLL_PTR）。rollback 用旧 image 整记录恢复；T1.4 MVCC 经
 *       {@code oldHiddenColumns.dbRollPtr()} 串记录版本链（§6.5 第397行：phase-1 存全量旧 image，简化 vs InnoDB
 *       changed-columns diff）。</li>
 * </ul>
 *
 * <p><b>两条链区分</b>（设计 §5.3/§7.5）：{@code prevRollPointer}=**事务回滚链**（RollbackService 反向走，
 * 串本事务所有 undo by undoNo）；**记录版本链**=聚簇记录 {@code DB_ROLL_PTR} → 本 update undo →
 * {@code oldHiddenColumns.dbRollPtr()}（上一版本）。二者用不同字段表达，勿混用。
 *
 * <p>{@code undoNo} 必须 &gt; 0（非 {@code NONE}）。INSERT 可携带新分配 LOB 的 ownership；UPDATE/DELETE
 * 通过独立 version ownership 指定 rollback 新链和 purge 旧链，不能把 old image 引用或 INSERT ownership
 * 猜测成另一类释放授权。
 *
 * @param type            undo 类型（INSERT_ROW、UPDATE_ROW 或 DELETE_MARK）。
 * @param undoNo          事务内序号（&gt; 0）。
 * @param transactionId   写入该 undo 的事务 id。
 * @param tableId         表 id（rollback 定位用）。
 * @param indexId         聚簇索引 id（rollback 定位用）。
 * @param clusterKey      主键列值，顺序对应 IndexKeyDef.parts()；可含 {@link ColumnValue.NullValue}。
 * @param oldColumnValues UPDATE_ROW 的更新前全列值（按 schema 列序）；INSERT_ROW 必为 null。
 * @param oldHiddenColumns UPDATE_ROW 的更新前隐藏列；INSERT_ROW 必为 null。
 * @param insertedLobs    INSERT_ROW 新分配并随记录发布的 LOB ownership，按列 ordinal 严格递增；其它类型必为空。
 * @param lobVersionOwnerships UPDATE/DELETE 的旧链 purge 与新链 rollback ownership；INSERT 必为空。
 * @param secondaryMutations 本次逻辑行操作涉及的二级索引反向证据，按 index id 严格递增；旧记录解码为空列表。
 * @param prevRollPointer 同 kind undo log 的事务回滚局部链前驱；记录版本链前驱保存在 oldHiddenColumns.dbRollPtr。
 */
public record UndoRecord(UndoRecordType type, UndoNo undoNo, TransactionId transactionId,
                         long tableId, long indexId, List<ColumnValue> clusterKey,
                         List<ColumnValue> oldColumnValues, HiddenColumns oldHiddenColumns,
                         List<InsertedLobOwnership> insertedLobs,
                         List<LobVersionOwnership> lobVersionOwnerships,
                         List<SecondaryUndoMutation> secondaryMutations,
                         RollPointer prevRollPointer) {

    /**
     * 校验并冻结一条可编码、可重放且反向动作无歧义的逻辑 undo record。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 type、undo/transaction identity、聚簇键、三个可选 tail 容器和局部链前驱完整，拒绝 NONE undoNo 与空主键。</li>
     *     <li>防御性复制聚簇键、INSERT/版本 LOB ownership 和 secondary mutation，分别校验 ordinal 严格递增。</li>
     *     <li>校验 secondary index id 严格递增，并强制 mutation action 与 INSERT/UPDATE/DELETE record type 一致。</li>
     *     <li>按 undo type 校验旧 image、旧隐藏列与 LOB ownership 的互斥关系，并冻结 UPDATE/DELETE 全量旧列。</li>
     * </ol>
     *
     * @param type                 决定 rollback inverse 与类型专用尾部的逻辑 undo 类型。
     * @param undoNo               事务内严格递增且非 NONE 的序号。
     * @param transactionId        写入并拥有该记录的事务 id。
     * @param tableId              rollback/recovery 定位 exact-version 表 metadata 的稳定 id。
     * @param indexId              聚簇索引稳定 id；不指向任一二级索引。
     * @param clusterKey           按聚簇 key definition 顺序物化的完整主键值。
     * @param oldColumnValues      UPDATE/DELETE 的全量旧用户列；INSERT 必须为 {@code null}。
     * @param oldHiddenColumns     UPDATE/DELETE 的旧 DB_TRX_ID/DB_ROLL_PTR；INSERT 必须为 {@code null}。
     * @param insertedLobs         INSERT 新分配 LOB ownership；其它类型必须为空。
     * @param lobVersionOwnerships UPDATE/DELETE 的旧链 purge 与新链 rollback ownership；INSERT 必须为空。
     * @param secondaryMutations   按二级 index id 递增的反向证据；action 必须与 {@code type} 一致。
     * @param prevRollPointer      同 kind undo log 中该事务前一条逻辑记录的 roll pointer。
     * @throws DatabaseValidationException 字段缺失、identity/排序/action/type 或旧 image/tail 组合不满足恢复不变量时抛出。
     */
    public UndoRecord {
        // 1. 基础 identity 与链字段必须在复制集合前完整，失败不会发布半有效 record。
        if (type == null || undoNo == null || transactionId == null
                || clusterKey == null || insertedLobs == null || lobVersionOwnerships == null
                || secondaryMutations == null
                || prevRollPointer == null) {
            throw new DatabaseValidationException("undo record fields must not be null");
        }
        if (undoNo.isNone()) {
            throw new DatabaseValidationException("undo record undoNo must be > 0 (not NONE)");
        }
        if (clusterKey.isEmpty()) {
            throw new DatabaseValidationException("undo record clusterKey must not be empty");
        }

        // 2. 冻结调用方集合，并确保 LOB ownership 顺序可作为稳定磁盘协议与释放顺序。
        clusterKey = List.copyOf(clusterKey);
        insertedLobs = List.copyOf(insertedLobs);
        lobVersionOwnerships = List.copyOf(lobVersionOwnerships);
        secondaryMutations = List.copyOf(secondaryMutations);
        int previousOrdinal = -1;
        for (InsertedLobOwnership ownership : insertedLobs) {
            if (ownership.columnOrdinal() <= previousOrdinal) {
                throw new DatabaseValidationException(
                        "inserted LOB ownership ordinals must be strictly increasing and unique");
            }
            previousOrdinal = ownership.columnOrdinal();
        }
        previousOrdinal = -1;
        for (LobVersionOwnership ownership : lobVersionOwnerships) {
            if (ownership.columnOrdinal() <= previousOrdinal) {
                throw new DatabaseValidationException(
                        "LOB version ownership ordinals must be strictly increasing and unique");
            }
            previousOrdinal = ownership.columnOrdinal();
        }

        // 3. 二级 inverse 的稳定顺序同时约束跨树 rollback 顺序；action 必须能由主 undo type 唯一解释。
        long previousIndexId = -1;
        for (SecondaryUndoMutation mutation : secondaryMutations) {
            if (mutation.indexId() <= previousIndexId) {
                throw new DatabaseValidationException(
                        "secondary undo mutation index ids must be strictly increasing and unique");
            }
            previousIndexId = mutation.indexId();
            SecondaryUndoAction expectedAction = switch (type) {
                case INSERT_ROW -> SecondaryUndoAction.INSERT_ENTRY;
                case UPDATE_ROW -> SecondaryUndoAction.CHANGE_KEY;
                case DELETE_MARK -> SecondaryUndoAction.DELETE_MARK_ENTRY;
            };
            if (mutation.action() != expectedAction) {
                throw new DatabaseValidationException(type + " undo cannot carry secondary action "
                        + mutation.action() + "; expected " + expectedAction);
            }
        }

        // 4. INSERT 与 UPDATE/DELETE 的旧 image、LOB ownership 互斥；旧全列在通过形状检查后再冻结。
        switch (type) {
            case INSERT_ROW -> {
                // insert 无旧版本：携带旧 image 是构造错误
                if (oldColumnValues != null || oldHiddenColumns != null) {
                    throw new DatabaseValidationException("INSERT_ROW undo must not carry old image");
                }
                if (!lobVersionOwnerships.isEmpty()) {
                    throw new DatabaseValidationException("INSERT_ROW undo must not carry LOB version ownership");
                }
            }
            // UPDATE_ROW / DELETE_MARK 都带删改前的旧 image（删除前是存活版本：列不变 + 旧隐藏列）
            case UPDATE_ROW, DELETE_MARK -> {
                if (oldColumnValues == null || oldHiddenColumns == null) {
                    throw new DatabaseValidationException(
                            type + " undo requires old image (oldColumnValues + oldHiddenColumns)");
                }
                if (oldColumnValues.isEmpty()) {
                    throw new DatabaseValidationException(type + " undo oldColumnValues must not be empty");
                }
                if (!insertedLobs.isEmpty()) {
                    throw new DatabaseValidationException(type + " undo must not carry inserted LOB ownership");
                }
                oldColumnValues = List.copyOf(oldColumnValues);
                for (LobVersionOwnership ownership : lobVersionOwnerships) {
                    int ordinal = ownership.columnOrdinal();
                    if (ordinal >= oldColumnValues.size()) {
                        throw new DatabaseValidationException(
                                "LOB version ownership ordinal exceeds old image: " + ordinal);
                    }
                    if (ownership.purgeOldValue()
                            && !(oldColumnValues.get(ordinal) instanceof ColumnValue.ExternalValue oldExternal)) {
                        throw new DatabaseValidationException(
                                "purge-old LOB ownership requires external old image at ordinal " + ordinal);
                    }
                    if (ownership.purgeOldValue() && ownership.rollbackNewValue().isPresent()
                            && oldColumnValues.get(ordinal) instanceof ColumnValue.ExternalValue oldExternal
                            && oldExternal.typeId() != ownership.rollbackNewValue().orElseThrow().typeId()) {
                        throw new DatabaseValidationException(
                                "old/new LOB ownership type mismatch at ordinal " + ordinal);
                    }
                    if (type == UndoRecordType.DELETE_MARK && ownership.rollbackNewValue().isPresent()) {
                        throw new DatabaseValidationException(
                                "DELETE_MARK undo cannot carry rollback-new LOB ownership");
                    }
                }
            }
        }
    }

    /**
     * 兼容 LV tail 引入前、已显式携带 secondary tail 的直接构造调用。该入口把 LOB version ownership
     * 明确映射为空列表，因此旧源码和旧磁盘语义不会因为新增 record component 而漂移。
     *
     * @param type                undo 类型，决定旧 image 与 secondary action 约束。
     * @param undoNo              事务内非 NONE 序号。
     * @param transactionId       undo owner 事务 id。
     * @param tableId             exact-version metadata 解析使用的稳定表 id。
     * @param indexId             聚簇索引稳定 id。
     * @param clusterKey          完整物化聚簇主键。
     * @param oldColumnValues     UPDATE/DELETE 全量旧用户列；INSERT 为 {@code null}。
     * @param oldHiddenColumns    UPDATE/DELETE 旧隐藏列；INSERT 为 {@code null}。
     * @param insertedLobs        INSERT 新链 ownership；其它类型为空。
     * @param secondaryMutations  按 index id 递增的二级反向证据。
     * @param prevRollPointer     同 kind undo log 的逻辑前驱。
     * @throws DatabaseValidationException 任一 identity、旧 image、ownership 或 mutation 组合无效时抛出。
     */
    public UndoRecord(UndoRecordType type, UndoNo undoNo, TransactionId transactionId,
                      long tableId, long indexId, List<ColumnValue> clusterKey,
                      List<ColumnValue> oldColumnValues, HiddenColumns oldHiddenColumns,
                      List<InsertedLobOwnership> insertedLobs,
                      List<SecondaryUndoMutation> secondaryMutations,
                      RollPointer prevRollPointer) {
        this(type, undoNo, transactionId, tableId, indexId, clusterKey, oldColumnValues, oldHiddenColumns,
                insertedLobs, List.of(), secondaryMutations, prevRollPointer);
    }

    /**
     * 兼容 secondary tail 引入前的直接构造调用。旧调用明确映射为空 mutation 列表，保证源码和磁盘语义不漂移；
     * 新生产写路径应使用带 {@code secondaryMutations} 的工厂方法冻结完整反向证据。
     *
     * @param type             逻辑 undo 类型。
     * @param undoNo           事务内非 NONE 序号。
     * @param transactionId    undo owner 事务 id。
     * @param tableId          稳定表 id。
     * @param indexId          聚簇索引稳定 id。
     * @param clusterKey       完整物化聚簇主键。
     * @param oldColumnValues  UPDATE/DELETE 的全量旧用户列；INSERT 为 {@code null}。
     * @param oldHiddenColumns UPDATE/DELETE 的旧隐藏列；INSERT 为 {@code null}。
     * @param insertedLobs     INSERT 新 LOB ownership；其它类型为空。
     * @param prevRollPointer  同 kind undo 局部链前驱。
     * @throws DatabaseValidationException 任一领域不变量无效时由主构造器抛出。
     */
    public UndoRecord(UndoRecordType type, UndoNo undoNo, TransactionId transactionId,
                      long tableId, long indexId, List<ColumnValue> clusterKey,
                      List<ColumnValue> oldColumnValues, HiddenColumns oldHiddenColumns,
                      List<InsertedLobOwnership> insertedLobs, RollPointer prevRollPointer) {
        this(type, undoNo, transactionId, tableId, indexId, clusterKey, oldColumnValues, oldHiddenColumns,
                insertedLobs, List.of(), List.of(), prevRollPointer);
    }

    /**
     * 构造不带 LOB ownership 和 secondary tail 的兼容 INSERT_ROW undo。
     *
     * @param undoNo          事务内非 NONE 序号。
     * @param transactionId   写事务 owner id。
     * @param tableId         稳定表 id。
     * @param indexId         聚簇索引稳定 id。
     * @param clusterKey      完整物化聚簇主键。
     * @param prevRollPointer INSERT undo 局部链前驱。
     * @return 可按旧 EOF 格式编码的 INSERT_ROW record。
     * @throws DatabaseValidationException identity、主键或链前驱无效时抛出。
     */
    public static UndoRecord insert(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                null, null, List.of(), List.of(), List.of(), prevRollPointer);
    }

    /**
     * 构造携带新 LOB 页链 ownership、但不带 secondary tail 的 INSERT_ROW undo。
     *
     * @param undoNo          事务内非 NONE 序号。
     * @param transactionId   写事务 owner id。
     * @param tableId         稳定表 id。
     * @param indexId         聚簇索引稳定 id。
     * @param clusterKey      完整物化聚簇主键。
     * @param insertedLobs    新分配 LOB ownership，必须按 column ordinal 严格递增。
     * @param prevRollPointer INSERT undo 局部链前驱。
     * @return 带 LOB ownership tail 的 INSERT_ROW record。
     * @throws DatabaseValidationException ownership 排序或其它 record 不变量无效时抛出。
     */
    public static UndoRecord insert(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<InsertedLobOwnership> insertedLobs,
                                    RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                null, null, insertedLobs, List.of(), List.of(), prevRollPointer);
    }

    /**
     * 构造同时携带 INSERT LOB ownership 与二级发布证据的 INSERT_ROW undo；两份列表都在 record 构造时冻结，
     * codec 固定按 LOB tail -> secondary tail 排列。
     *
     * @param undoNo                事务内非 NONE 序号。
     * @param transactionId         写事务 owner id。
     * @param tableId               稳定表 id。
     * @param indexId               聚簇索引稳定 id。
     * @param clusterKey            完整物化聚簇主键。
     * @param insertedLobs          按 column ordinal 递增的新 LOB ownership。
     * @param secondaryMutations    按 index id 递增的 INSERT_ENTRY 反向证据。
     * @param prevRollPointer       INSERT undo 局部链前驱。
     * @return 同时携带 LOB 与 secondary 可选尾部的 INSERT_ROW record。
     * @throws DatabaseValidationException ownership/mutation 排序、action 或其它 record 不变量无效时抛出。
     */
    public static UndoRecord insert(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<InsertedLobOwnership> insertedLobs,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.INSERT_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                null, null, insertedLobs, List.of(), secondaryMutations, prevRollPointer);
    }

    /**
     * 构造不带 secondary tail 的兼容 UPDATE_ROW undo。
     *
     * @param undoNo           事务内非 NONE 序号。
     * @param transactionId    写事务 owner id。
     * @param tableId          稳定表 id。
     * @param indexId          聚簇索引稳定 id。
     * @param clusterKey       被更新行的完整物化聚簇主键。
     * @param oldColumnValues  更新前按 schema 列序排列的全量用户列。
     * @param oldHiddenColumns 更新前 DB_TRX_ID/DB_ROLL_PTR。
     * @param prevRollPointer  UPDATE-kind undo 局部链前驱。
     * @return 带全量旧版本、secondary tail 为空的 UPDATE_ROW record。
     * @throws DatabaseValidationException 旧 image 或其它 record 不变量无效时抛出。
     */
    public static UndoRecord update(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns, RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.UPDATE_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, List.of(), List.of(), List.of(), prevRollPointer);
    }

    /**
     * 构造携带二级 key-change 反向证据的 UPDATE_ROW undo。
     *
     * @param undoNo                事务内非 NONE 序号。
     * @param transactionId         写事务 owner id。
     * @param tableId               稳定表 id。
     * @param indexId               聚簇索引稳定 id。
     * @param clusterKey            被更新行的完整物化聚簇主键。
     * @param oldColumnValues       更新前按 schema 列序排列的全量用户列。
     * @param oldHiddenColumns      更新前 DB_TRX_ID/DB_ROLL_PTR。
     * @param secondaryMutations    按 index id 递增的 CHANGE_KEY 反向证据。
     * @param prevRollPointer       UPDATE-kind undo 局部链前驱。
     * @return rollback 可同时恢复聚簇旧版本和变键二级 entry 的 UPDATE_ROW record。
     * @throws DatabaseValidationException mutation action/排序、旧 image 或其它 record 不变量无效时抛出。
     */
    public static UndoRecord update(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.UPDATE_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, List.of(), List.of(), secondaryMutations, prevRollPointer);
    }

    /**
     * 构造同时携带 LOB version ownership 与二级变键证据的 UPDATE_ROW。旧 external chain 由 committed purge
     * 消费，新 external chain 只由 rollback marker 消费；两类动作与全量旧 image 一起形成唯一恢复解释。
     *
     * @param undoNo                 事务内非 NONE 序号。
     * @param transactionId          写事务稳定 id。
     * @param tableId                exact-version 表 id。
     * @param indexId                聚簇索引稳定 id。
     * @param clusterKey             被更新行的完整聚簇主键。
     * @param oldColumnValues        更新前全量用户列，purge-old ownership 从中读取旧 external envelope。
     * @param oldHiddenColumns       更新前 DB_TRX_ID/DB_ROLL_PTR。
     * @param lobVersionOwnerships   按 column ordinal 递增的 LOB 版本 ownership。
     * @param secondaryMutations     按 index id 递增的 CHANGE_KEY 证据。
     * @param prevRollPointer        UPDATE-kind logical chain 前驱。
     * @return 可由 rollback、MVCC 与 purge 唯一解释的不可变 UPDATE_ROW。
     * @throws DatabaseValidationException 旧 image、ownership、secondary action 或排序不满足不变量时抛出。
     */
    public static UndoRecord update(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                    List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                    HiddenColumns oldHiddenColumns,
                                    List<LobVersionOwnership> lobVersionOwnerships,
                                    List<SecondaryUndoMutation> secondaryMutations,
                                    RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.UPDATE_ROW, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, List.of(), lobVersionOwnerships,
                secondaryMutations, prevRollPointer);
    }

    /**
     * 构造 DELETE_MARK undo（带删除前**存活**版本的全量旧 image：列不变 + 旧隐藏列）。rollback 用它取消删除标记并
     * 还原旧隐藏列；MVCC 旧版本遍历与 UPDATE 同路。本片不存 old delete flag（旧状态隐含 false，见 slice 决策 1）。
     *
     * @param undoNo           事务内非 NONE 序号。
     * @param transactionId    写事务 owner id。
     * @param tableId          稳定表 id。
     * @param indexId          聚簇索引稳定 id。
     * @param clusterKey       被逻辑删除行的完整物化聚簇主键。
     * @param oldColumnValues  删除前存活版本的全量用户列。
     * @param oldHiddenColumns 删除前 DB_TRX_ID/DB_ROLL_PTR。
     * @param prevRollPointer  UPDATE-kind undo 局部链前驱。
     * @return secondary tail 为空的 DELETE_MARK record。
     * @throws DatabaseValidationException 旧 image 或其它 record 不变量无效时抛出。
     */
    public static UndoRecord deleteMark(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                        List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                        HiddenColumns oldHiddenColumns, RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.DELETE_MARK, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, List.of(), List.of(), List.of(), prevRollPointer);
    }

    /**
     * 构造携带全部二级 delete-mark 反向证据的 DELETE_MARK undo。
     *
     * @param undoNo                事务内非 NONE 序号。
     * @param transactionId         写事务 owner id。
     * @param tableId               稳定表 id。
     * @param indexId               聚簇索引稳定 id。
     * @param clusterKey            被逻辑删除行的完整物化聚簇主键。
     * @param oldColumnValues       删除前存活版本的全量用户列。
     * @param oldHiddenColumns      删除前 DB_TRX_ID/DB_ROLL_PTR。
     * @param secondaryMutations    按 index id 递增的 DELETE_MARK_ENTRY 反向证据。
     * @param prevRollPointer       UPDATE-kind undo 局部链前驱。
     * @return rollback 可 revive 聚簇及全部二级 entry 的 DELETE_MARK record。
     * @throws DatabaseValidationException mutation action/排序、旧 image 或其它 record 不变量无效时抛出。
     */
    public static UndoRecord deleteMark(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                        List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                        HiddenColumns oldHiddenColumns,
                                        List<SecondaryUndoMutation> secondaryMutations,
                                        RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.DELETE_MARK, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, List.of(), List.of(), secondaryMutations, prevRollPointer);
    }

    /**
     * 构造携带旧 LOB purge ownership 与全部二级 delete-mark 证据的 DELETE_MARK。DELETE 不产生新 external chain，
     * 因此 ownership 只能声明 committed purge 回收 old image 中的旧链。
     *
     * @param undoNo                 事务内非 NONE 序号。
     * @param transactionId          删除事务稳定 id。
     * @param tableId                exact-version 表 id。
     * @param indexId                聚簇索引稳定 id。
     * @param clusterKey             被 delete-mark 行的完整聚簇主键。
     * @param oldColumnValues        删除前存活版本的全量用户列。
     * @param oldHiddenColumns       删除前 DB_TRX_ID/DB_ROLL_PTR。
     * @param lobVersionOwnerships   按 ordinal 递增且只含 purge-old 动作的 ownership。
     * @param secondaryMutations     按 index id 递增的 DELETE_MARK_ENTRY 证据。
     * @param prevRollPointer        UPDATE-kind logical chain 前驱。
     * @return rollback 可 revive、purge 可回收旧 LOB 的不可变 DELETE_MARK record。
     * @throws DatabaseValidationException ownership 携带 rollback-new、旧 image 不匹配或 mutation 无效时抛出。
     */
    public static UndoRecord deleteMark(UndoNo undoNo, TransactionId transactionId, long tableId, long indexId,
                                        List<ColumnValue> clusterKey, List<ColumnValue> oldColumnValues,
                                        HiddenColumns oldHiddenColumns,
                                        List<LobVersionOwnership> lobVersionOwnerships,
                                        List<SecondaryUndoMutation> secondaryMutations,
                                        RollPointer prevRollPointer) {
        return new UndoRecord(UndoRecordType.DELETE_MARK, undoNo, transactionId, tableId, indexId, clusterKey,
                oldColumnValues, oldHiddenColumns, List.of(), lobVersionOwnerships,
                secondaryMutations, prevRollPointer);
    }
}
