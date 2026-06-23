package cn.zhangyis.db.storage.fil.state;
import cn.zhangyis.db.storage.fil.io.PageStore;


import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间生命周期状态。普通 PageStore 路径只能访问 NORMAL/ACTIVE，恢复路径后续会单独处理 CORRUPTED。
 */
public enum TablespaceState {
    /** 尚未完成初始化，不允许普通 IO。 */
    EMPTY(0),
    /** 普通表空间的可访问状态。 */
    NORMAL(1),
    /** undo 表空间可分配、可访问。 */
    ACTIVE(2),
    /** undo 表空间保留文件但停止分配。 */
    INACTIVE(3),
    /** undo 表空间已越过持久化截断标记，恢复时必须续作。 */
    TRUNCATING(4),
    /** 表空间已被逻辑丢弃，普通 IO 永久拒绝。 */
    DISCARDED(5),
    /** 表空间检测到不可安全继续访问的损坏。 */
    CORRUPTED(6)

    ;

    /** page-0 生命周期头使用的稳定编码；禁止改为 ordinal。 */
    private final int persistentCode;

    TablespaceState(int persistentCode) {
        this.persistentCode = persistentCode;
    }

    /**
     * 返回 page-0 持久化状态码。该值是磁盘协议的一部分，不随枚举声明顺序变化。
     *
     * @return 稳定状态码。
     */
    public int persistentCode() {
        return persistentCode;
    }

    /**
     * 从 page-0 稳定状态码恢复生命周期状态。
     *
     * @param code 磁盘状态码。
     * @return 对应状态。
     * @throws DatabaseValidationException 状态码未知，说明页头损坏或格式版本不兼容。
     */
    public static TablespaceState fromPersistentCode(int code) {
        for (TablespaceState state : values()) {
            if (state.persistentCode == code) {
                return state;
            }
        }
        throw new DatabaseValidationException("unknown persistent tablespace state code: " + code);
    }

    /**
     * 判断表空间状态是否允许流转。该状态机保护普通 IO、DDL 和恢复路径共享的生命周期不变量。
     *
     * @param next 目标状态。
     * @return 允许流转返回 true，否则返回 false。
     */
    public boolean canTransitTo(TablespaceState next) {
        if (next == null) {
            return false;
        }
        if (this == next) {
            return true;
        }
        return switch (this) {
            case EMPTY -> next == NORMAL || next == CORRUPTED;
            case NORMAL -> next == ACTIVE || next == INACTIVE || next == DISCARDED || next == CORRUPTED;
            case ACTIVE -> next == INACTIVE || next == TRUNCATING || next == DISCARDED || next == CORRUPTED;
            case INACTIVE -> next == ACTIVE || next == TRUNCATING || next == DISCARDED || next == CORRUPTED;
            case TRUNCATING -> next == ACTIVE || next == INACTIVE || next == CORRUPTED;
            case DISCARDED, CORRUPTED -> false;
        };
    }

    /**
     * 校验状态流转是否合法；非法时抛项目异常，避免调用方绕过状态机直接构造不安全状态。
     *
     * @param next 目标状态。
     */
    public void validateTransitTo(TablespaceState next) {
        if (!canTransitTo(next)) {
            throw new DatabaseValidationException("illegal tablespace state transition: " + this + " -> " + next);
        }
    }
}
