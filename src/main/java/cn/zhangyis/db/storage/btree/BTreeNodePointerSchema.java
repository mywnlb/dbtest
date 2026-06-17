package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnDef;
import cn.zhangyis.db.storage.record.schema.ColumnId;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * 非叶 node pointer 的派生 schema。它不是用户表 schema：前 N 列是索引 key part 的类型拷贝，
 * 后两列是 child_space_id/child_page_no，专供 root 页上的 NODE_POINTER 记录编码、比较和物化。
 *
 * @param schema                  node pointer 记录的物理 schema。
 * @param keyDef                  node pointer 页内排序用 keyDef，只覆盖前 N 个 lowKey 列。
 * @param keyColumnCount          lowKey 列数，等于 leaf 索引 key part 数。
 * @param childSpaceColumnOrdinal child_space_id 所在 ordinal。
 * @param childPageColumnOrdinal  child_page_no 所在 ordinal。
 */
public record BTreeNodePointerSchema(TableSchema schema, IndexKeyDef keyDef, int keyColumnCount,
                                     int childSpaceColumnOrdinal, int childPageColumnOrdinal) {

    public BTreeNodePointerSchema {
        if (schema == null || keyDef == null) {
            throw new DatabaseValidationException("node pointer schema/keyDef must not be null");
        }
        if (keyColumnCount <= 0 || childSpaceColumnOrdinal != keyColumnCount
                || childPageColumnOrdinal != keyColumnCount + 1) {
            throw new DatabaseValidationException("invalid node pointer column ordinals");
        }
    }

    /**
     * 从 leaf 索引元数据派生非叶 schema。数据流：按 index.keyDef 的 part 顺序拷贝 leaf 列类型到连续 key 列，
     * 生成新的 keyDef 指向这些派生列，再追加两个无符号 BIGINT child 定位列。
     */
    public static BTreeNodePointerSchema from(BTreeIndex index) {
        if (index == null) {
            throw new DatabaseValidationException("btree index must not be null");
        }
        List<ColumnDef> columns = new ArrayList<>(index.keyDef().parts().size() + 2);
        List<KeyPartDef> keyParts = new ArrayList<>(index.keyDef().parts().size());
        int ordinal = 0;
        for (KeyPartDef part : index.keyDef().parts()) {
            ColumnDef source = index.schema().column(part.columnId().value());
            ColumnId derivedId = new ColumnId(ordinal);
            columns.add(new ColumnDef(derivedId, "np_" + source.name(), source.type(), ordinal));
            keyParts.add(new KeyPartDef(derivedId, part.order(), part.prefixBytes()));
            ordinal++;
        }
        int childSpace = ordinal;
        columns.add(new ColumnDef(new ColumnId(childSpace), "child_space_id",
                ColumnType.bigint(true, false), childSpace));
        int childPage = ordinal + 1;
        columns.add(new ColumnDef(new ColumnId(childPage), "child_page_no",
                ColumnType.bigint(true, false), childPage));
        return new BTreeNodePointerSchema(new TableSchema(index.schema().schemaVersion(), columns),
                new IndexKeyDef(index.indexId(), keyParts), index.keyDef().parts().size(), childSpace, childPage);
    }
}
