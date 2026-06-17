package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.record.type.ColumnValue;

import java.util.List;

/**
 * 查找/插入用的索引 key 值序列（innodb-record-design §11，不可变）。按 IndexKeyDef 的 key part 顺序给出列值，
 * 长度可少于 key part 数（前缀查找）；NULL 用 {@link ColumnValue.NullValue#INSTANCE} 表达（非 Java null）。
 *
 * @param values 各 key part 的值（防御性不可变副本，元素非 null）。
 */
public record SearchKey(List<ColumnValue> values) {

    public SearchKey {
        if (values == null) {
            throw new DatabaseValidationException("search key values must not be null");
        }
        values = List.copyOf(values);
    }

    /** 提供的 key part 数。 */
    public int size() {
        return values.size();
    }

    /** 第 i 个 key part 值。 */
    public ColumnValue value(int i) {
        return values.get(i);
    }
}
