package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.schema.ColumnType;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.KeyOrder;
import cn.zhangyis.db.storage.record.schema.KeyPartDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.ColumnValue;
import cn.zhangyis.db.storage.record.type.FieldSlice;
import cn.zhangyis.db.storage.record.type.FieldWriter;
import cn.zhangyis.db.storage.record.type.KeyPrefix;
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
 * <p>前缀索引（{@code KeyPartDef.prefixBytes>0}）经 {@link KeyPrefix} 只比列的前 N 字节（字节类型专用，见其文档）。
 */
public final class RecordComparator {

    private final TypeCodecRegistry registry;

    public RecordComparator(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
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
            boolean desc = part.order() == KeyOrder.DESC;
            boolean recordNull = record.isNull(part.columnId());
            boolean keyNull = key.value(i) instanceof ColumnValue.NullValue;
            if (recordNull && keyNull) {
                continue;
            }
            if (recordNull || keyNull) {
                // ASC：NULL 较小。record NULL → record<key → 负；key NULL → 正。DESC 取反。
                int c = recordNull ? -1 : 1;
                return desc ? -c : c;
            }
            TypeCodec codec = registry.codecFor(ct);
            // 前缀索引（prefixBytes>0）只比列的前 N 字节：对 record 侧列切片与 key 侧编码切片同截再比。
            FieldSlice recordSlice = KeyPrefix.apply(record.columnSlice(part.columnId()), ct, part.prefixBytes());
            FieldSlice keySlice = KeyPrefix.apply(encodeKey(key.value(i), ct, codec), ct, part.prefixBytes());
            int c = codec.compare(recordSlice, keySlice, ct);
            if (c != 0) {
                return desc ? -c : c;
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
