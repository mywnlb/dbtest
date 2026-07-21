package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * `mysql.xa` 的稳定状态编码。ordinal 不进入持久格式；新增状态必须分配新 code 并补恢复兼容测试。
 */
public enum XaRegistryState {

    /** 协调器已持久记录 XID→transactionId，storage phase one 尚未确认。 */
    PREPARING(1),
    /** storage PREPARED 已 fsync，等待外部最终决议。 */
    PREPARED(2),
    /** 全局提交决议已持久化，storage phase two 只能提交。 */
    COMMIT_DECIDED(3),
    /** 全局回滚决议已持久化，storage phase two 只能回滚。 */
    ROLLBACK_DECIDED(4),
    /** storage terminal 与 registry 收尾均已确认。 */
    COMPLETED(5);

    /** 磁盘稳定单字节编码。 */
    private final int code;

    XaRegistryState(int code) {
        this.code = code;
    }

    /** @return 磁盘稳定编码 */
    public int code() {
        return code;
    }

    /**
     * 解码持久状态。
     *
     * @param code 文件中的 unsigned 单字节编码
     * @return 已知 registry 状态
     * @throws DatabaseFatalException 未知编码意味着无法安全裁决 PREPARED
     */
    public static XaRegistryState fromCode(int code) {
        for (XaRegistryState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new DatabaseFatalException("unknown XA registry state code: " + code);
    }
}
