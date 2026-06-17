package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * SearchKey 与 SearchKey 的比较器。split 过程中需要在内存列表里按 leaf key 排序和计算 lowKey；
 * 页内 child 选择仍优先复用 {@code RecordPageSearch.findInsertPosition}，避免 root 页查找另走一套路径。
 */
public final class SearchKeyComparator {

    /** 类型 codec 注册表；比较时复用 record 层保序编码规则，减少与 RecordComparator 的排序漂移。 */
    private final TypeCodecRegistry registry;

    public SearchKeyComparator(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * 比较两个 key。返回值小于 0 表示 left 在前，大于 0 表示 right 在前；前缀全相等时返回 0。
     */
    public int compare(SearchKey left, SearchKey right, IndexKeyDef keyDef, TableSchema schema) {
        if (left == null || right == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("search key comparator args must not be null");
        }
        int m = Math.min(Math.min(left.size(), right.size()), keyDef.parts().size());
        for (int i = 0; i < m; i++) {
            KeyPartDef part = keyDef.parts().get(i);
            ColumnType ct = schema.column(part.columnId().value()).type();
            boolean desc = part.order() == KeyOrder.DESC;
            boolean leftNull = left.value(i) instanceof ColumnValue.NullValue;
            boolean rightNull = right.value(i) instanceof ColumnValue.NullValue;
            if (leftNull && rightNull) {
                continue;
            }
            if (leftNull || rightNull) {
                int c = leftNull ? -1 : 1;
                return desc ? -c : c;
            }
            TypeCodec codec = registry.codecFor(ct);
            FieldSlice leftSlice = encodeKey(left.value(i), ct, codec);
            FieldSlice rightSlice = encodeKey(right.value(i), ct, codec);
            int c = codec.compare(leftSlice, rightSlice, ct);
            if (c != 0) {
                return desc ? -c : c;
            }
        }
        return 0;
    }

    private FieldSlice encodeKey(ColumnValue value, ColumnType ct, TypeCodec codec) {
        codec.validate(value, ct);
        byte[] buf = new byte[codec.encodedLength(value, ct)];
        codec.encode(value, ct, new FieldWriter(buf, 0));
        return new FieldSlice(buf, 0, buf.length);
    }
}
