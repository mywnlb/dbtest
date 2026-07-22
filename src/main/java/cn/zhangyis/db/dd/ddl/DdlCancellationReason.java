package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** 用户/生命周期发起的持久取消原因；普通容量或验证失败继续使用operation abort reason。 */
public enum DdlCancellationReason {
    /** 管理员或已授权调用方主动请求取消。 */
    USER_REQUEST(1),
    /** 未来session控制面把被终止statement映射为DDL取消。 */
    SESSION_KILLED(2),
    /** engine关闭流程在可回退点请求停止后台DDL。 */
    ENGINE_CLOSING(3);

    /** marker v4 中跨版本稳定的正编码。 */
    private final int stableCode;

    DdlCancellationReason(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return marker v4使用的稳定正编码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码持久取消原因。
     *
     * @param code payload中读取的无符号稳定码
     * @return 唯一对应的取消原因
     * @throws DatabaseValidationException 未知码不能降级成用户取消时抛出
     */
    public static DdlCancellationReason fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown DDL cancellation reason stable code: " + code));
    }
}
