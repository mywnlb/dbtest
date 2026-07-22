package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** `OALTLOG1` frame的稳定类型；与旧OnlineIndexLogRecordType使用独立格式空间。 */
public enum OnlineAlterLogRecordType {
    GENERATION_STARTED(1), CAPTURING(2), CANDIDATE(3), FORCE_WATERMARK(4),
    SEALED(5), READY_TO_PUBLISH(6), RECONCILED(7), ABORT_REQUIRED(8);

    private final int stableCode;

    OnlineAlterLogRecordType(int stableCode) {
        this.stableCode = stableCode;
    }

    public int stableCode() {
        return stableCode;
    }

    public static OnlineAlterLogRecordType fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown online ALTER log record type: " + code));
    }
}
