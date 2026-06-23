package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间类型。用于区分 system、独立表空间、general tablespace、undo 和 temporary 的加载来源与扩展策略。
 *
 * <p>{@code code} 是写入 page-0 spaceFlags 的稳定落盘值，不能依赖 enum ordinal。后续即使调整 enum 顺序，
 * 磁盘上的 type code 仍必须保持兼容，否则 reopen/recovery 会把表空间解释成错误类型。
 */
public enum TablespaceType {
    /**
     * 系统表空间。承载 InnoDB 系统级元数据和部分内部结构，通常在实例启动时由固定配置加载。
     */
    SYSTEM(0),

    /**
     * 独立表空间。对应 file-per-table 的 .ibd 文件，是普通用户表最常见的数据文件形态。
     */
    FILE_PER_TABLE(1),

    /**
     * 通用表空间。一个 tablespace 可承载多个表或索引，元数据需要结合数据字典判断对象归属。
     */
    GENERAL(2),

    /**
     * Undo 表空间。承载 undo segment/page，扩展策略和普通用户表空间不同，恢复和 purge 会重点访问。
     */
    UNDO(3),

    /**
     * 临时表空间。用于临时对象和内部临时结构，通常允许简化 redo 持久化语义，实例重启后可重建。
     */
    TEMPORARY(4);

    /**
     * page-0 spaceFlags 低位保存的稳定类型编号。该值是磁盘格式的一部分，不随 Java enum 顺序变化。
     */
    private final int code;

    TablespaceType(int code) {
        this.code = code;
    }

    /**
     * 返回稳定落盘 code。调用方通常通过 {@link TablespaceTypeFlags} 将它编入 spaceFlags。
     *
     * @return 表空间类型的磁盘编码。
     */
    public int code() {
        return code;
    }

    /**
     * 从 page-0 spaceFlags 中解出的 type code 还原表空间类型。未知 code 表示磁盘元数据与当前实现不兼容或已损坏。
     *
     * @param code 磁盘中的表空间类型编码。
     * @return 对应表空间类型。
     */
    public static TablespaceType fromCode(int code) {
        for (TablespaceType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new DatabaseValidationException("unknown tablespace type code: " + code);
    }
}
