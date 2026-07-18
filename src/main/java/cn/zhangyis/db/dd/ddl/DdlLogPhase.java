package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;

import java.util.Arrays;

/** Atomic DDL 的 durable 阶段；阶段顺序由 operation-specific 状态机解释，而不是按数值大小猜测。 */
public enum DdlLogPhase {
    /** identity/path 已固定，尚不能证明物理引擎动作完成。 */
    PREPARED(1),
    /** 当前 operation 的物理 CREATE 或 DROP 已完成。 */
    ENGINE_DONE(2),
    /** ACTIVE 或 DROP_PENDING 字典提交点已经 durable。 */
    DICTIONARY_COMMITTED(3),
    /** DDL 的字典、物理和 cache 可观察结果均已收敛。 */
    COMMITTED(4),
    /** DDL 未越过提交裁决点，恢复已完成精确 cleanup/no-op rollback。 */
    ROLLED_BACK(5);

    /** DDL log key/payload 使用且跨版本不可重排的持久码。 */
    private final int stableCode;

    DdlLogPhase(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return DDL log key/payload 使用的正稳定码。 */
    public int stableCode() {
        return stableCode;
    }

    /** @return 当前阶段是否禁止任何后续 transition。 */
    public boolean terminal() {
        return this == COMMITTED || this == ROLLED_BACK;
    }

    /**
     * 解码稳定 phase code。
     *
     * @param code 落盘无符号码。
     * @return 对应阶段。
     * @throws DictionaryCatalogCorruptionException 未知阶段无法恢复时抛出。
     */
    public static DdlLogPhase fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DictionaryCatalogCorruptionException(
                        "unknown DDL log phase code: " + code));
    }
}
