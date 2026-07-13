package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.EncodedKeyPartComparator;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.TypeCodec;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;
import cn.zhangyis.db.storage.record.format.RecordType;

/**
 * 记录与查找 key 的保序比较器（innodb-record-design §11）。比较走**编码切片**：记录侧取列的 {@link FieldSlice}，
 * key 侧用同列 codec 编码进临时 buffer，再 {@code codec.compare}——保序编码下即类型自然序，无需构造 ColumnValue。
 *
 * <p>规则：复合 key 按 part 顺序逐列比；ASC 中 NULL &lt; 非 NULL，DESC 反转；infimum/supremum 系统记录作哨兵
 * （恒小于/大于任何 key），不读其字段。key 短于 key part 数时按前缀比，前缀全相等返回 0。无状态、线程安全。
 *
 * <p>每个 part 的 NULL/prefix/collation/方向统一委托 {@link EncodedKeyPartComparator}，与 B+Tree node pointer
 * 比较保持同序。
 */
public final class RecordComparator {

    private final TypeCodecRegistry registry;

    /** leaf record 与 node pointer 共享的单 part 排序规则。 */
    private final EncodedKeyPartComparator keyPartComparator;

    public RecordComparator(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
        this.keyPartComparator = new EncodedKeyPartComparator(registry);
    }

    /**
     * 返回 record 相对 key 的序：&lt;0 record 在前、0 相等（或前缀匹配）、&gt;0 record 在后。
     *
     * <p>数据流：先按记录类型判哨兵（infimum/supremum 直接返回，避免对系统记录解析字段）；否则逐 key part
     * 取记录列 NULL/切片与 key 值，按 NULL 序或 codec.compare 比较，遇不等即返回（DESC 取反）。
     */
    public int compare(RecordCursor record, SearchKey key, IndexKeyDef keyDef, TableSchema schema) {
        RecordType type = record.recordType();
        if (type == RecordType.INFIMUM) {
            return -1;
        }
        if (type == RecordType.SUPREMUM) {
            return 1;
        }
        int m = Math.min(key.size(), keyDef.parts().size());
        for (int i = 0; i < m; i++) {
            KeyPartDef part = keyDef.parts().get(i);
            ColumnType ct = schema.column(part.columnId().value()).type();
            boolean recordNull = record.isNull(part.columnId());
            boolean keyNull = key.value(i) instanceof ColumnValue.NullValue;
            FieldSlice recordSlice = recordNull ? null : record.columnSlice(part.columnId());
            FieldSlice keySlice = keyNull ? null : encodeKey(key.value(i), ct, registry.codecFor(ct));
            int c = keyPartComparator.compare(recordNull, recordSlice, keyNull, keySlice, ct, part);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /**
     * 返回两条页内记录的索引序：&lt;0 left 在前、0 key 等价、&gt;0 left 在后。
     *
     * <p>数据流：先处理 infimum/supremum 哨兵，避免按用户 schema 解析系统标签；普通记录随后遍历完整 keyDef，
     * 从两侧 cursor 读取 NULL 标志与编码切片，并复用 {@link EncodedKeyPartComparator} 的 prefix/collation/方向语义。
     * 本入口不物化 {@link ColumnValue}，供 schema-aware 页内顺序校验以 O(n) 相邻扫描复用。
     *
     * @param left 左侧记录游标。
     * @param right 右侧记录游标。
     * @param keyDef 索引 key 定义。
     * @param schema 两条记录共享的物理 schema。
     * @return 规范化为 -1、0、1 的索引序比较结果。
     */
    public int compare(RecordCursor left, RecordCursor right, IndexKeyDef keyDef, TableSchema schema) {
        if (left == null || right == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("record comparison inputs must not be null");
        }
        RecordType leftType = left.recordType();
        RecordType rightType = right.recordType();
        if (leftType == RecordType.INFIMUM || rightType == RecordType.SUPREMUM) {
            return leftType == rightType ? 0 : -1;
        }
        if (leftType == RecordType.SUPREMUM || rightType == RecordType.INFIMUM) {
            return leftType == rightType ? 0 : 1;
        }
        for (KeyPartDef part : keyDef.parts()) {
            ColumnType columnType = schema.column(part.columnId().value()).type();
            boolean leftNull = left.isNull(part.columnId());
            boolean rightNull = right.isNull(part.columnId());
            FieldSlice leftSlice = leftNull ? null : left.columnSlice(part.columnId());
            FieldSlice rightSlice = rightNull ? null : right.columnSlice(part.columnId());
            int comparison = keyPartComparator.compare(
                    leftNull, leftSlice, rightNull, rightSlice, columnType, part);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    /** 把 key 值编码进临时 buffer，得到与记录侧同编码的切片用于保序比较。 */
    private FieldSlice encodeKey(ColumnValue value, ColumnType ct, TypeCodec codec) {
        codec.validate(value, ct);
        byte[] buf = new byte[codec.encodedLength(value, ct)];
        codec.encode(value, ct, new FieldWriter(buf, 0));
        return new FieldSlice(buf, 0, buf.length);
    }
}
