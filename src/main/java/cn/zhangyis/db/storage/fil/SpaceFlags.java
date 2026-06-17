package cn.zhangyis.db.storage.fil;

/**
 * 表空间标志位快照。首版只保存原始位，后续再解释压缩、加密、atomic DDL 等 MySQL/InnoDB 标志。
 *
 * @param rawValue 从 FSP_HDR、数据字典或配置读取的原始标志位；当前不解释具体 bit，避免提前绑定未实现语义。
 */
public record SpaceFlags(int rawValue) {

    /**
     * 创建空标志位，用于普通未启用特殊能力的表空间。
     *
     * @return 原始值为 0 的 SpaceFlags。
     */
    public static SpaceFlags empty() {
        return new SpaceFlags(0);
    }
}
