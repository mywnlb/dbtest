package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 单条逻辑 undo 对一个二级索引的反向动作。code 是 secondary tail 的稳定落盘值，新增动作只能追加 code，
 * 不能重解释已有值，否则旧 history 在 crash recovery 时会执行错误 inverse。
 */
public enum SecondaryUndoAction {

    /** INSERT 发布了一个此前不存在的 entry；rollback 需物理删除当前 live entry。 */
    INSERT_ENTRY(1),

    /** UPDATE 改变 logical key；rollback 需撤销新 entry 并把旧 entry 恢复为 live。 */
    CHANGE_KEY(2),

    /** DELETE 把既有 entry 标记为删除；rollback 需把该 entry 恢复为 live。 */
    DELETE_MARK_ENTRY(3);

    /** secondary tail 中的稳定 u8 编码。 */
    private final int code;

    /**
     * 绑定不会随枚举声明顺序改变的落盘编码。
     *
     * @param code secondary tail 使用的稳定无符号字节值。
     */
    SecondaryUndoAction(int code) {
        this.code = code;
    }

    /**
     * 返回 secondary tail 使用的稳定落盘编码。
     *
     * @return 当前反向动作对应的无符号字节值。
     */
    public int code() {
        return code;
    }

    /**
     * 从落盘 code 恢复动作；未知值表示调用方输入或磁盘 tail 损坏，不允许猜测默认动作。
     *
     * @param code 无符号 u8 落盘值。
     * @return 对应动作。
     * @throws DatabaseValidationException code 未注册时抛出，恢复层不得猜测默认 inverse。
     */
    public static SecondaryUndoAction fromCode(int code) {
        for (SecondaryUndoAction action : values()) {
            if (action.code == code) {
                return action;
            }
        }
        throw new DatabaseValidationException("unknown secondary undo action code: " + code);
    }
}
