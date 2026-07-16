package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 字典发布版本；只增不减，cache read view 和 stale pin 以它判断可见性。 */
public record DictionaryVersion(long value) implements Comparable<DictionaryVersion> {
    public DictionaryVersion {
        if (value <= 0) {
            throw new DatabaseValidationException("dictionary version must be positive: " + value);
        }
    }

    public static DictionaryVersion of(long value) {
        return new DictionaryVersion(value);
    }

    @Override
    public int compareTo(DictionaryVersion other) {
        if (other == null) {
            throw new DatabaseValidationException("dictionary version comparison target must not be null");
        }
        return Long.compare(value, other.value);
    }
}
