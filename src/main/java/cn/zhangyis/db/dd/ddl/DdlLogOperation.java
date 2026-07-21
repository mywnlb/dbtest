package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.dd.exception.DictionaryCatalogCorruptionException;

import java.util.Arrays;

/** DDL log 支持的物理原子操作；stableCode 落盘并跨格式版本稳定，不能使用 enum ordinal。 */
public enum DdlLogOperation {
    /** 创建 file-per-table tablespace、索引 segments/root，再发布 ACTIVE DD。 */
    CREATE_TABLE(1),
    /** 发布 DROP_PENDING、删除物理 tablespace，再发布 DROPPED DD。 */
    DROP_TABLE(2),
    /** 在既有表空间构建二级 B+Tree，再原子发布新 table/index aggregate。 */
    CREATE_INDEX(3),
    /** 将 GENERAL 表空间从 canonical path 移入受控 discarded 目录。 */
    DISCARD_TABLESPACE(4),
    /** 校验外部 DISCARDED 文件并重新挂载为 ACTIVE 表空间。 */
    IMPORT_TABLESPACE(5),
    /** 先发布不含目标二级索引的 ACTIVE aggregate，再回收其 leaf/non-leaf segment。 */
    DROP_INDEX(6),
    /**
     * 阻塞式结构 ALTER：path/spaceId 是旧 binding，auxiliaryPath/secondaryObjectId 是新 shadow
     * path/space id；committed DD 是交换裁决点。
     */
    REBUILD_TABLE(7);

    /** DDL log key/payload 使用且跨版本不可重排的持久码。 */
    private final int stableCode;

    /**
     * 创建 {@code DdlLogOperation}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param stableCode 参与 {@code 构造} 的稳定编码 {@code stableCode}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
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
