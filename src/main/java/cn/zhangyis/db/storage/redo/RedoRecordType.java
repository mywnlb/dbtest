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
    FSP_PAGE_FREE((byte) 5),
    /** Undo/rseg 元数据字段 after-image record，对应 {@link UndoMetadataDeltaRecord}。 */
    UNDO_METADATA_DELTA((byte) 6),
    /** 完整 undo record 槽 after-image record，对应 {@link UndoRecordPayloadRecord}。 */
    UNDO_RECORD_PAYLOAD((byte) 7),
    /** B+Tree 结构页 after-image record，对应 {@link BTreePageDeltaRecord}。 */
    BTREE_PAGE_DELTA((byte) 8),
    /** Non-page 事务状态 logical redo，对应 {@link TransactionStateDeltaRecord}。 */
    TRX_STATE_DELTA((byte) 9);

    /**
     * 记录 {@code tag} 的权威数值状态；仅由本类受控路径更新，取值范围和特殊值遵循所属格式或状态机，溢出必须拒绝。
     */
    private final byte tag;

    /**
     * 创建 {@code RedoRecordType}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param tag 参与 {@code 构造} 的单字节编码 {@code tag}；必须符合目标格式的 tag、填充值或无符号字节范围约定
     */
    RedoRecordType(byte tag) {
        this.tag = tag;
    }

    /** redo 文件中的 1 字节 tag。 */
    public byte tag() {
        return tag;
    }

    /** 从 redo 文件 tag 还原 record 类型；未知 tag 视为 redo 损坏。
     *
     * @param tag 参与 {@code fromTag} 的单字节编码 {@code tag}；必须符合目标格式的 tag、填充值或无符号字节范围约定
     * @return {@code fromTag} 解析或选择出的已知领域类型；成功时不为 {@code null}，未知编码或非法状态通过领域异常报告
     * @throws RedoLogCorruptedException 检测到不能安全解释的持久数据损坏时抛出；调用方不得继续发布普通服务或覆盖原始证据
     */
    public static RedoRecordType fromTag(byte tag) {
        for (RedoRecordType t : values()) {
            if (t.tag == tag) {
                return t;
            }
        }
        throw new RedoLogCorruptedException("unknown redo record tag: " + tag);
    }
}
