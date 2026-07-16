package cn.zhangyis.db.dd.mdl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * MySQL 风格六种 metadata lock mode。COMPATIBILITY 是唯一授锁矩阵，行/列顺序与 enum 声明严格一致。
 */
public enum MdlMode {
    INTENTION_EXCLUSIVE,
    SHARED_READ,
    SHARED_WRITE,
    SHARED_UPGRADABLE,
    SHARED_NO_WRITE,
    EXCLUSIVE;

    private static final boolean[][] COMPATIBILITY = {
            {true, true, true, true, true, false},
            {true, true, true, true, true, false},
            {true, true, true, true, false, false},
            {true, true, true, false, false, false},
            {true, true, false, false, true, false},
            {false, false, false, false, false, false}
    };

    /** requested mode 与已授予 mode 是否能并存；矩阵对称。 */
    public boolean compatibleWith(MdlMode granted) {
        if (granted == null) {
            throw new DatabaseValidationException("granted metadata lock mode must not be null");
        }
        return COMPATIBILITY[ordinal()][granted.ordinal()];
    }
}
