package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 排序规则稳定标识。具体 charset/collation 合法组合由字符 registry 精确注册。 */
public enum CollationId {

    /** 按编码字节做无符号字典序比较，可用于当前支持的所有字符集。 */
    BINARY(1),

    /** UTF8 教学型大小写不敏感排序：仅折叠 ASCII A-Z。 */
    UTF8_ASCII_CI(2),

    /** LATIN1 教学型大小写不敏感排序：仅折叠 ASCII A-Z。 */
    LATIN1_ASCII_CI(3);

    /** 与枚举声明顺序无关的稳定编号。 */
    private final int stableId;

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
