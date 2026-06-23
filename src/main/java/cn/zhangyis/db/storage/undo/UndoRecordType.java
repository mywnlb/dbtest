package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * undo record 类型。{@code code} 作为 undo record 首字节落盘（稳定，{@code UndoRecordCodecTest} 钉死）；
 * code 从 1 起（0 留作「非法/零页」可检测）。本片 codec 仅实现 {@code INSERT_ROW}，其余类型 payload 留 T1.3d。
 */
public enum UndoRecordType {
    /** 插入未提交行的撤销：rollback 时按 cluster key 物理删除该插入。 */
    INSERT_ROW(1),
    /** 更新前镜像（T1.3d）。 */
    UPDATE_ROW(2),
    /** delete-mark 前镜像（T1.3d）。 */
    DELETE_MARK(3);

    private final int code;

    UndoRecordType(int code) {
        this.code = code;
    }

    /** 落盘 code（undo record 首字节）。 */
    public int code() {
        return code;
    }

    /** 由落盘 code 还原；未知 code 视为 undo record 类型损坏。 */
    public static UndoRecordType fromCode(int code) {
        for (UndoRecordType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new DatabaseValidationException("unknown undo record type code: " + code);
    }
}
