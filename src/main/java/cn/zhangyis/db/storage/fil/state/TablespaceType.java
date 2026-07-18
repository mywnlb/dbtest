package cn.zhangyis.db.storage.fil.state;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * page0 flags 中持久化的表空间类型。
 *
 * <p>{@code code} 是写入 page-0 spaceFlags 的稳定落盘值，不能依赖 enum ordinal。后续即使调整 enum 顺序，
 * 磁盘上的 type code 仍必须保持兼容，否则 reopen/recovery 会把表空间解释成错误类型。</p>
 *
 * <p>当前生产生命周期分支只对 {@link #GENERAL} 与 {@link #UNDO} 建立了完整 create/open/truncate
 * 语义；其它类型保留稳定编码和元数据表达能力，并不表示对应 MySQL 文件管理特性已经接线。</p>
 */
public enum TablespaceType {
    /**
     * 系统表空间稳定编码。当前没有独立的系统表空间 bootstrap/文件布局实现。
     */
    SYSTEM(0),

    /**
     * file-per-table 稳定编码。当前 DDL 创建路径仍使用 {@link #GENERAL}，未接独立生命周期分支。
     */
    FILE_PER_TABLE(1),

    /**
     * 当前普通表/索引 DDL 使用的通用表空间类型；page0 生命周期状态为 {@code NORMAL}。
     */
    GENERAL(2),

    /**
     * 当前 undo segment/page 使用的专用类型；支持 {@code ACTIVE/INACTIVE/TRUNCATING} 生命周期和
     * truncate recovery。
     */
    UNDO(3),

    /**
     * 临时表空间稳定编码。当前没有创建、NO_REDO 或重启重建生产路径。
     */
    TEMPORARY(4);

    /**
     * page0 {@code SPACE_FLAGS} 低位保存的稳定类型编号。该值是磁盘格式的一部分，不随 Java enum
     * 顺序变化，也不能在已有格式上复用为其它类型。
     */
    private final int code;

    /**
     * 绑定枚举常量与稳定磁盘编码。
     *
     * @param code 由 v1 page0 flags 协议分配的 0..4 编码
     */
    TablespaceType(int code) {
        this.code = code;
    }

    /**
     * 返回稳定落盘 code。调用方通常通过 {@link TablespaceTypeFlags} 将它编入 spaceFlags。
     *
     * @return 可由 {@link TablespaceTypeFlags} 写入低三位的稳定磁盘编码
     */
    public int code() {
        return code;
    }

    /**
     * 从 page-0 spaceFlags 中解出的 type code 还原表空间类型。未知 code 表示磁盘元数据与当前实现不兼容或已损坏。
     *
     * @param code 从 flags 低三位提取的类型编码；当前合法范围为 0..4
     * @return 与稳定编码一一对应的表空间类型
     * @throws DatabaseValidationException code 未分配给当前格式中的任何类型时抛出；调用方应把
     *                                     page0 视为损坏或不兼容，而不是猜测默认类型
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
