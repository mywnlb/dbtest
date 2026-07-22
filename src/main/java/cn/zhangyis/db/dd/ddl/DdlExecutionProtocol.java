package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/**
 * DDL operation 的可恢复执行协议。operation 说明“做什么”，protocol 说明 blocking/online 的阶段与控制能力；
 * 二者必须同时持久化，恢复不能从sidecar是否存在反推。
 */
public enum DdlExecutionProtocol {
    /** 仅由v1-v3 decoder和其后续phase transition产生；没有digest与control能力。 */
    LEGACY_PHASE_ONLY(0, false),
    /** 当前CREATE/DROP/transfer/rebuild的既有operation-specific阻塞协议。 */
    ATOMIC_BLOCKING_V1(1, false),
    /** Online ADD INDEX v1；PREPARED阶段允许cancel/forward唯一竞争。 */
    ONLINE_INDEX_V1(2, true),
    /** Online DROP INDEX v1；退役屏障前允许取消，跨过FORWARD_ONLY后只允许前滚回收。 */
    ONLINE_DROP_INDEX_V1(3, true),
    /** In-place ALTER v1；承载instant metadata以及一个manifest下原子ADD/DROP多个secondary index。 */
    ONLINE_ALTER_INPLACE_V1(4, true),
    /** Shadow rebuild v1；以独立tablespace、通用change-log和ReadView generation屏障发布row-layout变化。 */
    ONLINE_ALTER_SHADOW_V1(5, true);

    /** marker v4 中跨版本稳定的非负编码。 */
    private final int stableCode;
    /** 是否允许public control最终映射到repository cancel CAS。 */
    private final boolean cancelCapable;

    DdlExecutionProtocol(int stableCode, boolean cancelCapable) {
        this.stableCode = stableCode;
        this.cancelCapable = cancelCapable;
    }

    /** @return marker v4使用的稳定编码。 */
    public int stableCode() {
        return stableCode;
    }

    /** @return 当前协议是否声明了crash-safe cancel/forward竞争点。 */
    public boolean cancelCapable() {
        return cancelCapable;
    }

    /**
     * 解码marker中的执行协议。
     *
     * @param code payload中读取的无符号稳定码
     * @return 唯一对应的执行协议
     * @throws DatabaseValidationException 未知码不能安全决定恢复方向时抛出
     */
    public static DdlExecutionProtocol fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown DDL execution protocol stable code: " + code));
    }
}
