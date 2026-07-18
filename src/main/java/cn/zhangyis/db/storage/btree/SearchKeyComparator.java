package cn.zhangyis.db.storage.btree;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.page.SearchKey;
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

/**
 * SearchKey 与 SearchKey 的比较器。split 过程中需要在内存列表里按 leaf key 排序和计算 lowKey；
 * 页内 child 选择仍优先复用 {@code RecordPageSearch.findInsertPosition}，避免 root 页查找另走一套路径。
 */
public final class SearchKeyComparator {

    /** 类型 codec 注册表；比较时复用 record 层保序编码规则，减少与 RecordComparator 的排序漂移。 */
    private final TypeCodecRegistry registry;

    /** 与 leaf record 比较共享的 NULL/prefix/collation/方向规则。 */
    private final EncodedKeyPartComparator keyPartComparator;

    /**
     * 创建 {@code SearchKeyComparator}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param registry 由组合根提供的 {@code TypeCodecRegistry} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public SearchKeyComparator(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("type codec registry must not be null");
        }
        this.registry = registry;
        this.keyPartComparator = new EncodedKeyPartComparator(registry);
    }

    /**
     * 比较两个 key。返回值小于 0 表示 left 在前，大于 0 表示 right 在前；前缀全相等时返回 0。
     *
     * @param left 参与 {@code compare} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param right 参与 {@code compare} 的稳定领域标识 {@code SearchKey}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @param keyDef 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @param schema 由 data dictionary 提供的名称、schema、版本或物理绑定快照；不得为 {@code null}，且必须属于同一可见字典版本
     * @return 左值小于、等于或大于右值时分别返回负数、零或正数；排序规则与对应索引或无符号格式一致
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public int compare(SearchKey left, SearchKey right, IndexKeyDef keyDef, TableSchema schema) {
        if (left == null || right == null || keyDef == null || schema == null) {
            throw new DatabaseValidationException("search key comparator args must not be null");
        }
        int m = Math.min(Math.min(left.size(), right.size()), keyDef.parts().size());
        for (int i = 0; i < m; i++) {
            KeyPartDef part = keyDef.parts().get(i);
            ColumnType ct = schema.column(part.columnId().value()).type();
            boolean leftNull = left.value(i) instanceof ColumnValue.NullValue;
            boolean rightNull = right.value(i) instanceof ColumnValue.NullValue;
            TypeCodec codec = leftNull && rightNull ? null : registry.codecFor(ct);
            FieldSlice leftSlice = leftNull ? null : encodeKey(left.value(i), ct, codec);
            FieldSlice rightSlice = rightNull ? null : encodeKey(right.value(i), ct, codec);
            int c = keyPartComparator.compare(leftNull, leftSlice, rightNull, rightSlice, ct, part);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /**
     * 把调用方领域值编码为B+Tree 索引的稳定表示；编码前校验范围，成功不修改输入对象。
     *
     * @param value 参与记录编解码或索引比较的字段值；不得为 {@code null}，其类型、字节边界和 SQL NULL 语义必须与当前 schema 一致
     * @param ct 选择 {@code encodeKey} 分支的 {@code ColumnType} 枚举值；不得为 {@code null}，未知语义不能用默认分支猜测
     * @param codec 由组合根提供的 {@code TypeCodec} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code encodeKey} 调用
     * @return {@code encodeKey} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    private FieldSlice encodeKey(ColumnValue value, ColumnType ct, TypeCodec codec) {
        codec.validate(value, ct);
        byte[] buf = new byte[codec.encodedLength(value, ct)];
        codec.encode(value, ct, new FieldWriter(buf, 0));
        return new FieldSlice(buf, 0, buf.length);
    }
}
