package cn.zhangyis.db.storage.fil.state;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 表空间类型与 page-0 {@code spaceFlags} 的编解码器。
 *
 * <p>当前布局只定义低 3 位：bits 0..2 保存 {@link TablespaceType#code()}；bits 3..31 保留给压缩、加密等后续
 * flags。decode 时只屏蔽高保留位而不修改传入原值，避免未知高位影响已定义类型识别。encode 当前只生成
 * 类型位，不负责与其它 feature bits 合并。</p>
 */
public final class TablespaceTypeFlags {

    /**
     * 类型编码占用的低三位掩码。它可表达 0..7，其中 0..4 已稳定分配，5..7 必须由
     * {@link TablespaceType#fromCode(int)} 拒绝，不能静默映射为默认类型。
     */
    private static final int TYPE_MASK = 0x7;

    /**
     * 纯静态持久格式工具，不允许实例化。
     */
    private TablespaceTypeFlags() {
    }

    /**
     * 将表空间类型编码为只包含低三位类型字段的 flags。
     *
     * @param type 待持久化的非空表空间类型
     * @return 可写入 page0 {@code SPACE_FLAGS} 的类型位整数；当前返回值高 29 位均为零
     * @throws DatabaseValidationException type 为空时抛出；不会产生默认类型编码
     */
    public static int encode(TablespaceType type) {
        if (type == null) {
            throw new DatabaseValidationException("tablespace type must not be null");
        }
        return type.code() & TYPE_MASK;
    }

    /**
     * 从完整 spaceFlags 的低三位还原表空间类型。
     *
     * <p>高 29 位不参与类型判定，也不会在本方法中被解释。低三位为未分配值时直接失败，
     * 防止 reopen/recovery 把未知磁盘格式当成其它空间类型。</p>
     *
     * @param spaceFlags 从 page0 读取的完整 {@code SPACE_FLAGS} 原始整数
     * @return 低三位稳定编码对应的表空间类型
     * @throws DatabaseValidationException 低三位为当前格式未分配的 5..7 时抛出
     */
    public static TablespaceType decode(int spaceFlags) {
        return TablespaceType.fromCode(spaceFlags & TYPE_MASK);
    }
}
