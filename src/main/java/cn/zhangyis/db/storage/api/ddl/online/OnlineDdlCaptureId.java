package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** storage层通用Online DDL capture identity；与DD的正ddl id一一对应。 */
public record OnlineDdlCaptureId(long value) implements Comparable<OnlineDdlCaptureId> {

    public OnlineDdlCaptureId {
        if (value <= 0) {
            throw new DatabaseValidationException("online DDL capture id must be positive: " + value);
        }
    }

    public static OnlineDdlCaptureId of(long value) {
        return new OnlineDdlCaptureId(value);
    }

    @Override
    public int compareTo(OnlineDdlCaptureId other) {
        if (other == null) {
            throw new DatabaseValidationException("online DDL capture compare target must not be null");
        }
        return Long.compare(value, other.value);
    }
}
