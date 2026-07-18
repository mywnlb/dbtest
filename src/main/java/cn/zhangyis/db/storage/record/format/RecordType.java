package cn.zhangyis.db.storage.record.format;

/**
 * 物理记录类型（innodb-record-design §5.2）。code 落盘（记录头 2 bit），取值不可改。
 */
public enum RecordType {
    /** 普通用户记录。 */
    CONVENTIONAL(0),
    /** 非叶子页的 node pointer 记录（指向子页）。 */
    NODE_POINTER(1),
    /** 页内最小边界记录。 */
    INFIMUM(2),
    /** 页内最大边界记录。 */
    SUPREMUM(3);

    /**
     * 记录 {@code code} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final int code;

    /**
     * 创建 {@code RecordType}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param code 参与 {@code 构造} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     */
    RecordType(int code) {
        this.code = code;
    }

    /** 落盘 code（0..3）。 */
    public int code() {
        return code;
    }

    /** 由 code 还原；未知 code 视为记录头损坏。
     *
     * @param code 参与 {@code fromCode} 的稳定编码 {@code code}；必须命中当前版本声明的编码集合，未知值以格式或校验异常拒绝
     * @return {@code fromCode} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     * @throws RecordFormatException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public static RecordType fromCode(int code) {
        for (RecordType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        throw new RecordFormatException("unknown record type code: " + code);
    }
}
