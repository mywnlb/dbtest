package cn.zhangyis.db.storage.record.page;

/**
 * 页内插入方向（innodb-record-design §7 PAGE_DIRECTION）。落盘 u16 code，配合 PAGE_N_DIRECTION 让 B+Tree
 * 识别顺序插入并优化 split 点。本片只编解码该字段，不实现方向驱动的 split 优化（归 R4/B+Tree）。
 */
public enum IndexPageDirection {
    /** 无明显方向（初始或方向翻转）。 */
    NO_DIRECTION(0),
    /** 连续向较小 key 方向插入。 */
    LEFT(1),
    /** 连续向较大 key 方向插入。 */
    RIGHT(2);

    /**
     * 记录 {@code code} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int code;

    /**
     * 创建 {@code IndexPageDirection}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param code 参与 {@code 构造} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    IndexPageDirection(int code) {
        this.code = code;
    }

    /** 落盘 code。 */
    public int code() {
        return code;
    }

    /** 由 code 还原；未知 code 视为 page header 损坏。
     *
     * @param code 参与 {@code fromCode} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     * @return {@code fromCode} 取得或创建的受控存储资源；成功时不为 {@code null}，调用方必须按其 Guard/lease 契约释放
     * @throws PageDirectoryCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public static IndexPageDirection fromCode(int code) {
        for (IndexPageDirection d : values()) {
            if (d.code == code) {
                return d;
            }
        }
        throw new PageDirectoryCorruptedException("unknown page direction code: " + code);
    }
}
