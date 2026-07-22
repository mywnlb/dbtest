package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** DDL marker v4的单调方向裁决；只能从OPEN进入一个最终方向，不能互转或回退。 */
public enum DdlControlState {
    /** 尚无durable方向；只有cancel-capable protocol允许竞争。 */
    OPEN(1),
    /** 取消已经durable，后续phase只能进入ROLLED_BACK。 */
    CANCEL_REQUESTED(2),
    /** 不可回退点已经durable，后续只能沿operation前滚图推进。 */
    FORWARD_ONLY(3);

    /** marker v4 中跨版本稳定的正编码。 */
    private final int stableCode;

    DdlControlState(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return marker v4使用的稳定正编码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码marker中的控制状态。
     *
     * @param code payload中读取的无符号稳定码
     * @return 唯一对应的控制状态
     * @throws DatabaseValidationException 未知码无法安全决定恢复方向时抛出
     */
    public static DdlControlState fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown DDL control state stable code: " + code));
    }
}
