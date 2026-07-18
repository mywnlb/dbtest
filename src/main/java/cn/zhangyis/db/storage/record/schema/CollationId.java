package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 排序规则稳定标识。具体 charset/collation 合法组合由字符 registry 精确注册。 */
public enum CollationId {

    /** 按编码字节做无符号字典序比较，可用于当前支持的所有字符集。 */
    BINARY(1),

    /** UTF8 教学型大小写不敏感排序：仅折叠 ASCII A-Z。 */
    UTF8_ASCII_CI(2),

    /** LATIN1 教学型大小写不敏感排序：仅折叠 ASCII A-Z。 */
    LATIN1_ASCII_CI(3),

    /** UTF8 稳定教学权重 V1：固定 case/accent/combining-mark 主权重规则。 */
    UTF8_UNICODE_CI_V1(4);

    /** 与枚举声明顺序无关的稳定编号。 */
    private final int stableId;

    /**
     * 创建 {@code CollationId}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param stableId 目标表的原始字典标识；必须为已分配的正数并与当前元数据和物理绑定一致
     */
    CollationId(int stableId) {
        this.stableId = stableId;
    }

    /**
     * 返回供 schema/DD 使用的稳定编号。
     *
     * @return 正整数稳定编号。
     */
    public int stableId() {
        return stableId;
    }

    /**
     * 从稳定编号恢复排序规则，未知编号禁止回退为 BINARY。
     *
     * @param stableId 持久化编号。
     * @return 对应排序规则。
     * @throws DatabaseValidationException 编号未知时抛出。
     */
    public static CollationId fromStableId(int stableId) {
        for (CollationId collation : values()) {
            if (collation.stableId == stableId) {
                return collation;
            }
        }
        throw new DatabaseValidationException("unknown collation stable id: " + stableId);
    }
}
