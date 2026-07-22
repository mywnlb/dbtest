package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * 通用 Online ALTER manifest 中的动作类别。稳定码只描述动作语义，动作的完整冻结参数由
 * {@link OnlineAlterActionDescriptor#payload()} 保存，禁止依赖 Java 枚举声明顺序。
 */
public enum OnlineAlterActionType {
    ADD_COLUMN(1),
    DROP_COLUMN(2),
    ADD_INDEX(3),
    DROP_INDEX(4),
    RENAME(5),
    COMMENT(6),
    DEFAULT_CHARSET(7),
    CONVERT_CHARSET(8);

    /** manifest v1 中跨版本稳定的正编码。 */
    private final int stableCode;

    OnlineAlterActionType(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return manifest v1 中的稳定编码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码持久动作类型。
     *
     * @param code manifest 中的正稳定码
     * @return 唯一对应的动作类型
     * @throws DatabaseValidationException 未知码无法安全解释恢复命令时抛出
     */
    public static OnlineAlterActionType fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown online ALTER action stable code: " + code));
    }
}
