package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间生命周期状态。普通 PageStore 路径只能访问 NORMAL/ACTIVE，恢复路径后续会单独处理 CORRUPTED。
 */
public enum TablespaceState {
    EMPTY,
    NORMAL,
    ACTIVE,
    INACTIVE,
    DISCARDED,
    CORRUPTED

    ;

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
            case ACTIVE -> next == INACTIVE || next == DISCARDED || next == CORRUPTED;
            case INACTIVE -> next == ACTIVE || next == DISCARDED || next == CORRUPTED;
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
