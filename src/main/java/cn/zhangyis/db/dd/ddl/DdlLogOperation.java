package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;

import java.util.Arrays;

/** DDL log v1 支持的物理原子操作；stableCode 落盘，不能使用 enum ordinal。 */
public enum DdlLogOperation {
    /** 创建 file-per-table tablespace、索引 segments/root，再发布 ACTIVE DD。 */
    CREATE_TABLE(1),
    /** 发布 DROP_PENDING、删除物理 tablespace，再发布 DROPPED DD。 */
    DROP_TABLE(2),
    /** 在既有表空间构建二级 B+Tree，再原子发布新 table/index aggregate。 */
    CREATE_INDEX(3);

    /** DDL log key/payload 使用且跨版本不可重排的持久码。 */
    private final int stableCode;

    DdlLogOperation(int stableCode) {
        this.stableCode = stableCode;
    }

    /** @return DDL log key/payload 使用的正稳定码。 */
    public int stableCode() {
        return stableCode;
    }

    /**
     * 解码稳定 operation code。
     *
     * @param code 落盘无符号码。
     * @return 对应 operation。
     * @throws DictionaryCatalogCorruptionException 未知码无法安全决定恢复动作时抛出。
     */
    public static DdlLogOperation fromStableCode(int code) {
        return Arrays.stream(values()).filter(value -> value.stableCode == code).findFirst()
                .orElseThrow(() -> new DictionaryCatalogCorruptionException(
                        "unknown DDL log operation code: " + code));
    }
}
