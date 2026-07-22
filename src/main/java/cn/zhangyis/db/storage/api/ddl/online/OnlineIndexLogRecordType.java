package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** Row-log append-only frame 的稳定类型；stable code 一经落盘不得重排或复用。 */
public enum OnlineIndexLogRecordType {
    GENERATION_STARTED(1),
    CAPTURING(2),
    CANDIDATE(3),
    FORCE_WATERMARK(4),
    SEALED(5),
    RECONCILED(6),
    ABORT_REQUIRED(7);

    /** 跨版本持久码。 */
    private final int stableCode;

    OnlineIndexLogRecordType(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return 当前类型的正持久码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码稳定类型。
     *
     * @param code 文件中的无符号码
     * @return 唯一对应类型
     * @throws DatabaseValidationException 未知码不能安全跳过时抛出
     */
    public static OnlineIndexLogRecordType fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown online index log record type: " + code));
    }
}
