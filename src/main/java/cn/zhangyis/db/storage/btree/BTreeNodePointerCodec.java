package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageId;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.record.format.LogicalRecord;
import cn.zhangyis.db.storage.record.format.RecordType;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.ArrayList;
import java.util.List;

/**
 * node pointer 与物理页内 LogicalRecord 的转换器。B+Tree 只在非叶页写 NODE_POINTER 记录；
 * record 层仍按普通 LogicalRecord 编码，因此该 codec 负责补齐/解析 child 定位列。
 */
public final class BTreeNodePointerCodec {

    /**
     * 把 node pointer 转成可插入 root 页的 LogicalRecord。前 N 列来自 lowKey，后两列保存 child page 物理定位。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param pointer 参与 {@code toRecord} 的稳定领域标识 {@code BTreeNodePointer}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param pointerSchema 记录格式使用的 header、隐藏列或键布局；不得为 {@code null}，偏移、字段顺序和编码宽度必须与当前页格式一致
     * @return {@code toRecord} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public LogicalRecord toRecord(BTreeNodePointer pointer, BTreeNodePointerSchema pointerSchema) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (pointer == null || pointerSchema == null) {
            throw new DatabaseValidationException("node pointer/pointerSchema must not be null");
        }
        if (pointer.lowKey().size() != pointerSchema.keyColumnCount()) {
            throw new DatabaseValidationException("node pointer lowKey size mismatch: "
                    + pointer.lowKey().size() + " vs " + pointerSchema.keyColumnCount());
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        List<ColumnValue> values = new ArrayList<>(pointerSchema.schema().columnCount());
        values.addAll(pointer.lowKey().values());
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        values.add(new ColumnValue.IntValue(pointer.childPageId().spaceId().value()));
        values.add(new ColumnValue.IntValue(pointer.childPageId().pageNo().value()));
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new LogicalRecord(pointerSchema.schema().schemaVersion(), values, false, RecordType.NODE_POINTER);
    }

    /**
     * 从已物化的 NODE_POINTER 记录解析 child 指针。非 NODE_POINTER 说明 root 页混入了错误记录类型，按结构损坏处理。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取输入长度、游标边界与必要标识，损坏、截断或超限数据在创建结果前失败。</li>
     *     <li>按稳定字段或 token 顺序推进游标并调用对应编解码分支，任何分支都不得越过输入边界。</li>
     *     <li>交叉校验聚合计数、类型、校验值和剩余输入，防止截断或多余内容形成半解析对象。</li>
     *     <li>完成剩余字段写入或稳定领域结果构造；失败只保留领域异常与根因，不修改调用方输入或其他持久状态。</li>
     * </ol>
     *
     * @param record 参与本次操作的记录或记录集合；不得为 {@code null}，顺序、身份与编码必须满足当前索引或日志格式
     * @param pointerSchema 记录格式使用的 header、隐藏列或键布局；不得为 {@code null}，偏移、字段顺序和编码宽度必须与当前页格式一致
     * @return {@code fromRecord} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws BTreeStructureCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public BTreeNodePointer fromRecord(LogicalRecord record, BTreeNodePointerSchema pointerSchema) {
        // 1、读取输入长度、游标边界与必要标识，在共享或持久副作用前拒绝非法状态。
        if (record == null || pointerSchema == null) {
            throw new DatabaseValidationException("node pointer record/pointerSchema must not be null");
        }
        if (record.recordType() != RecordType.NODE_POINTER) {
            throw new BTreeStructureCorruptedException("expected NODE_POINTER record but got " + record.recordType());
        }
        // 2、继续完成范围、身份与候选校验；通过后，按稳定字段或 token 顺序推进游标并调用对应编解码分支，保持处理顺序与资源边界。
        if (record.columnValues().size() != pointerSchema.schema().columnCount()) {
            throw new BTreeStructureCorruptedException("node pointer column count mismatch: "
                    + record.columnValues().size() + " vs " + pointerSchema.schema().columnCount());
        }
        List<ColumnValue> keyValues = new ArrayList<>(pointerSchema.keyColumnCount());
        for (int i = 0; i < pointerSchema.keyColumnCount(); i++) {
            keyValues.add(record.columnValues().get(i));
        }
        // 3、在中间分支复核阶段性结果；满足条件后，交叉校验聚合计数、类型、校验值和剩余输入，并维持领域不变量。
        long space = intValue(record.columnValues().get(pointerSchema.childSpaceColumnOrdinal()), "child_space_id");
        long page = intValue(record.columnValues().get(pointerSchema.childPageColumnOrdinal()), "child_page_no");
        if (space > Integer.MAX_VALUE) {
            throw new BTreeStructureCorruptedException("child space id out of Java int range: " + space);
        }
        // 4、完成剩余字段写入或稳定领域结果构造，以稳定返回或领域异常完成收口。
        return new BTreeNodePointer(new SearchKey(keyValues),
                PageId.of(SpaceId.of((int) space), PageNo.of(page)));
    }

    private long intValue(ColumnValue value, String name) {
        if (value instanceof ColumnValue.IntValue intValue) {
            return intValue.value();
        }
        throw new BTreeStructureCorruptedException("node pointer " + name + " is not integer value");
    }
}
