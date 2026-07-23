package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** Change Buffer header 生命周期；code 持久化，顺序不得改变语义。 */
public enum ChangeBufferHeaderState {
    /** 接受新 mutation 并允许 merge。 */
    ACTIVE(1),
    /** DDL/关闭正在排空，不接受新 mutation。 */
    DRAINING(2),
    /** 结构保留但不接受新 mutation；仍可诊断和显式清理。 */
    DISABLED(3),
    /** header/tree/bitmap 证据损坏，普通启动必须 fail-closed。 */
    CORRUPTED(4);

    /** 写入 page3 header 的跨重启稳定编码；不得使用 ordinal 替代。 */
    private final int code;

    ChangeBufferHeaderState(int code) {
        this.code = code;
    }

    /** @return 持久稳定 code。 */
    public int code() {
        return code;
    }

    /**
     * @param code header 中的状态编码
     * @return 对应已知状态
     * @throws DatabaseValidationException code 未知时抛出
     */
    public static ChangeBufferHeaderState fromCode(int code) {
        for (ChangeBufferHeaderState state : values()) {
            if (state.code == code) {
                return state;
            }
        }
        throw new DatabaseValidationException("unknown change buffer header state code: " + code);
    }
}
