package cn.zhangyis.db.storage.record.schema;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 字符集稳定标识。显式编号用于未来 schema/DD 持久化，禁止使用 enum ordinal，避免新增枚举项改变磁盘含义。
 */
public enum CharsetId {

    /** UTF-8 编码；保留首版名称与默认行为。 */
    UTF8(1),

    /** ISO-8859-1 单字节编码；只接受可映射到 Latin-1 的字符。 */
    LATIN1(2);

    /** 与枚举声明顺序无关的稳定编号。 */
    private final int stableId;

    CharsetId(int stableId) {
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
     * 从稳定编号恢复字符集，未知编号不得猜测或回退默认字符集。
     *
     * @param stableId 持久化编号。
     * @return 对应字符集。
     * @throws DatabaseValidationException 编号未知时抛出。
     */
    public static CharsetId fromStableId(int stableId) {
        for (CharsetId charset : values()) {
            if (charset.stableId == stableId) {
                return charset;
            }
        }
        throw new DatabaseValidationException("unknown charset stable id: " + stableId);
    }
}
