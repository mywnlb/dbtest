package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间类型与 page-0 {@code spaceFlags} 的编解码器。
 *
 * <p>当前布局只定义低 3 位：bits 0..2 保存 {@link TablespaceType#code()}；bits 3..31 保留给压缩、加密等后续
 * flags。decode 时必须忽略高保留位，否则未来扩展 flags 会导致老代码把可识别的表空间类型误判为损坏。
 */
public final class TablespaceTypeFlags {

    /**
     * type code 使用低 3 位，当前可表示 0..7；0..4 已由 {@link TablespaceType} 使用。
     */
    private static final int TYPE_MASK = 0x7;

    private TablespaceTypeFlags() {
    }

    /**
     * 将表空间类型编码到 spaceFlags 低 3 位。当前创建路径不设置高位，后续 feature flags 可在此基础上按位合成。
     *
     * @param type 表空间类型。
     * @return 可写入 page-0 SPACE_FLAGS 字段的 flags 整数。
     */
    public static int encode(TablespaceType type) {
        if (type == null) {
            throw new DatabaseValidationException("tablespace type must not be null");
        }
        return type.code() & TYPE_MASK;
    }

    /**
     * 从 spaceFlags 低 3 位还原表空间类型。高位保留 flags 不参与 type 判定；未知低位 code 由 enum 层抛领域异常。继续
     *
     * @param spaceFlags page-0 SPACE_FLAGS 原始整数。
     * @return 解码后的表空间类型。
     */
    public static TablespaceType decode(int spaceFlags) {
        return TablespaceType.fromCode(spaceFlags & TYPE_MASK);
    }
}
