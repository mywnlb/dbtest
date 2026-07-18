package cn.zhangyis.db.storage.fil.state;

/**
 * page0 {@code SPACE_FLAGS} 的不可变原始位快照。
 *
 * <p>当前生产代码只解释 bits 0..2 的稳定 {@link TablespaceType} 编码，具体编解码由
 * {@link TablespaceTypeFlags} 负责；其余高位原样保留，fil 元数据层不擅自赋予压缩、加密或
 * atomic DDL 语义。保留原始整数使 page0 loader 与 registry 不会丢失尚未解释的 feature bits。</p>
 *
 * @param rawValue 从 FSP_HDR、数据字典或创建配置取得的完整 32 位原始值；可以包含当前未解释的高位
 */
public record SpaceFlags(int rawValue) {

    /**
     * 创建所有 bit 均为零的 flags 快照。
     *
     * <p>零值不是“无表空间类型”的特殊哨兵；按当前低三位协议解码时对应
     * {@link TablespaceType#SYSTEM}。普通创建路径应显式使用
     * {@link TablespaceTypeFlags#encode(TablespaceType)} 写入所需类型。</p>
     *
     * @return {@code rawValue == 0} 的不可变 flags
     */
    public static SpaceFlags empty() {
        return new SpaceFlags(0);
    }
}
