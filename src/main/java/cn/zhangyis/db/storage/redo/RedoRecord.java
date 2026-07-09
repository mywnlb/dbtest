package cn.zhangyis.db.storage.redo;

/**
 * 持久 redo 记录。基础物理记录包括页初始化 {@link PageInitRecord} 与页内字节覆盖 {@link PageBytesRecord}；
 * 0.19b 起增加 FSP 页分配意图 {@link FspPageAllocationRecord}；0.19c 起增加 FSP metadata delta 与 page-free intent。
 * record 定义保持纯值，不依赖 repository/PageGuard 或恢复编排实现。
 *
 * <p>记录本身不带 LSN 字段；LSN 由 {@link RedoLogManager#append} 以 batch 区间分配，per-record LSN 元数据留后续切片。
 */
public sealed interface RedoRecord permits FspMetadataDeltaRecord, FspPageAllocationRecord, FspPageFreeRecord,
        PageBytesRecord, PageInitRecord {

    /**
     * 落盘字节数（权威值，非估算）。R1 之后该值必须精确等于 {@code RedoLogFileRepository} 的文件编码长度
     * （例如 PAGE_INIT=17、PAGE_BYTES=21+payloadLength、FSP_PAGE_ALLOC=26），并被两处依赖：
     * {@link RedoLogManager#append} 据此分配 LSN 区间，
     * {@link RedoLogBatch} 校验「range 长度 == Σ byteLength」。新增 redo 类型时 byteLength 必须与其文件编码同步推进，
     * 否则批次 LSN 区间会与文件实际字节错位，恢复 reader 将拒绝该批次或读出错位数据。
     */
    int byteLength();
}
