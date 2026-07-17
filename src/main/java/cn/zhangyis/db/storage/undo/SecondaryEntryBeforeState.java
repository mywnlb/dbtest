package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * UPDATE 发布新二级 entry 前，该完整物理 identity 的状态。该证据决定 rollback 是物理删除新 entry，
 * 还是恢复其原有 delete mark；其它 undo action 使用 NOT_APPLICABLE，避免 nullable 状态进入恢复路径。
 */
public enum SecondaryEntryBeforeState {

    /** INSERT_ENTRY/DELETE_MARK_ENTRY 不需要新 entry 前态。 */
    NOT_APPLICABLE(0),

    /** UPDATE 新物理 identity 原先不存在，rollback 应物理删除本次新发布的 live entry。 */
    ABSENT(1),

    /** UPDATE 新物理 identity 是同主键的 delete-marked entry，前向操作 revive，rollback 应恢复 marked。 */
    DELETE_MARKED(2);

    /** secondary tail 中的稳定 u8 编码。 */
    private final int code;

    /**
     * 绑定不会随版本重排的落盘编码。
     *
     * @param code secondary tail 使用的稳定无符号字节值。
     */
    SecondaryEntryBeforeState(int code) {
        this.code = code;
    }

    /**
     * 返回 secondary tail 使用的稳定落盘编码。
     *
     * @return 当前前态对应的无符号字节值。
     */
    public int code() {
        return code;
    }

    /**
     * 从稳定无符号字节编码恢复前态；未知值由磁盘 decoder 进一步包装为格式损坏。
     *
     * @param code 从 undo secondary tail 读取的无符号字节值。
     * @return 唯一对应的 entry 前态。
     * @throws DatabaseValidationException 编码未注册时抛出，禁止猜测默认状态继续 recovery。
     */
    public static SecondaryEntryBeforeState fromCode(int code) {
        for (SecondaryEntryBeforeState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new DatabaseValidationException("unknown secondary entry before-state code: " + code);
    }
}
