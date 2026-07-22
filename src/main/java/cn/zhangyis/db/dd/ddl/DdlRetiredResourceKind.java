package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.util.Arrays;

/** retirement fence保护的物理资源类别；resource id只在类别内解释。 */
public enum DdlRetiredResourceKind {
    /** target DD不再引用、但旧undo/pin仍可能使用的secondary index。 */
    INDEX(1),
    /** shadow swap后等待删除的source file-per-table space。 */
    TABLESPACE(2);

    /** marker v4 中跨版本稳定的正编码。 */
    private final int stableCode;

    DdlRetiredResourceKind(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return marker v4使用的稳定正编码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码retired resource类别。
     *
     * @param code payload中读取的无符号稳定码
     * @return 唯一对应的资源类别
     * @throws DatabaseValidationException 未知类别可能指向不同ownership域时抛出
     */
    public static DdlRetiredResourceKind fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DatabaseValidationException(
                        "unknown DDL retired resource kind stable code: " + code));
    }
}
