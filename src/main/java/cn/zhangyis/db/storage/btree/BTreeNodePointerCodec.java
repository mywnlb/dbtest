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
     */
    public LogicalRecord toRecord(BTreeNodePointer pointer, BTreeNodePointerSchema pointerSchema) {
        if (pointer == null || pointerSchema == null) {
            throw new DatabaseValidationException("node pointer/pointerSchema must not be null");
        }
        if (pointer.lowKey().size() != pointerSchema.keyColumnCount()) {
            throw new DatabaseValidationException("node pointer lowKey size mismatch: "
                    + pointer.lowKey().size() + " vs " + pointerSchema.keyColumnCount());
        }
        List<ColumnValue> values = new ArrayList<>(pointerSchema.schema().columnCount());
        values.addAll(pointer.lowKey().values());
        values.add(new ColumnValue.IntValue(pointer.childPageId().spaceId().value()));
        values.add(new ColumnValue.IntValue(pointer.childPageId().pageNo().value()));
        return new LogicalRecord(pointerSchema.schema().schemaVersion(), values, false, RecordType.NODE_POINTER);
    }

    /**
     * 从已物化的 NODE_POINTER 记录解析 child 指针。非 NODE_POINTER 说明 root 页混入了错误记录类型，按结构损坏处理。
     */
    public BTreeNodePointer fromRecord(LogicalRecord record, BTreeNodePointerSchema pointerSchema) {
        if (record == null || pointerSchema == null) {
            throw new DatabaseValidationException("node pointer record/pointerSchema must not be null");
        }
        if (record.recordType() != RecordType.NODE_POINTER) {
            throw new BTreeStructureCorruptedException("expected NODE_POINTER record but got " + record.recordType());
        }
        if (record.columnValues().size() != pointerSchema.schema().columnCount()) {
            throw new BTreeStructureCorruptedException("node pointer column count mismatch: "
                    + record.columnValues().size() + " vs " + pointerSchema.schema().columnCount());
        }
        List<ColumnValue> keyValues = new ArrayList<>(pointerSchema.keyColumnCount());
        for (int i = 0; i < pointerSchema.keyColumnCount(); i++) {
            keyValues.add(record.columnValues().get(i));
        }
        long space = intValue(record.columnValues().get(pointerSchema.childSpaceColumnOrdinal()), "child_space_id");
        long page = intValue(record.columnValues().get(pointerSchema.childPageColumnOrdinal()), "child_page_no");
        if (space > Integer.MAX_VALUE) {
            throw new BTreeStructureCorruptedException("child space id out of Java int range: " + space);
        }
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
