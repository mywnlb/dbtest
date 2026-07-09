package cn.zhangyis.db.storage.redo;

/**
 * R1 redo 落盘 record tag。取值写入 redo 文件，后续新增逻辑 redo 时只能追加，不能改变既有 tag 语义。
 */
public enum RedoRecordType {
    /** 页初始化 record，对应 {@link PageInitRecord}。 */
    PAGE_INIT((byte) 1),
    /** 页内连续字节覆盖 record，对应 {@link PageBytesRecord}。 */
    PAGE_BYTES((byte) 2),
    /** FSP 页分配意图 record，对应 {@link FspPageAllocationRecord}。 */
    FSP_PAGE_ALLOC((byte) 3),
    /** FSP 元数据字段 after-image record，对应 {@link FspMetadataDeltaRecord}。 */
    FSP_METADATA_DELTA((byte) 4),
    /** FSP 页释放意图 record，对应 {@link FspPageFreeRecord}。 */
    FSP_PAGE_FREE((byte) 5);

    private final byte tag;

    RedoRecordType(byte tag) {
        this.tag = tag;
    }

    /** redo 文件中的 1 字节 tag。 */
    public byte tag() {
        return tag;
    }

    /** 从 redo 文件 tag 还原 record 类型；未知 tag 视为 redo 损坏。 */
    public static RedoRecordType fromTag(byte tag) {
        for (RedoRecordType t : values()) {
            if (t.tag == tag) {
                return t;
            }
        }
        throw new RedoLogCorruptedException("unknown redo record tag: " + tag);
    }
}
