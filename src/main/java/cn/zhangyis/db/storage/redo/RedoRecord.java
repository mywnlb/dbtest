package cn.zhangyis.db.storage.redo;

/**
 * 物理 redo 记录（物理字节区间 redo）。D3 仅两种：整页字节区间写 {@link PageBytesRecord}、页初始化 {@link PageInitRecord}。
 * 纯值，仅依赖 domain + storage.page 纯层 PageType，不依赖任何 repository/PageGuard（redo record 定义不依赖具体实现）。
 *
 * <p>D3 记录本身不带 LSN 字段；LSN 由 {@link RedoLogManager#append} 以 batch 区间分配，per-record LSN 元数据留 R1。
 */
public sealed interface RedoRecord permits PageBytesRecord, PageInitRecord {

    /**
     * 落盘字节数（权威值，非估算）。R1 之后该值必须精确等于 {@code RedoLogFileRepository} 的文件编码长度
     * （PAGE_INIT=17、PAGE_BYTES=21+payloadLength），并被两处依赖：{@link RedoLogManager#append} 据此分配 LSN 区间，
     * {@link RedoLogBatch} 校验「range 长度 == Σ byteLength」。新增 redo 类型时 byteLength 必须与其文件编码同步推进，
     * 否则批次 LSN 区间会与文件实际字节错位，恢复 reader 将拒绝该批次或读出错位数据。
     */
    int byteLength();
}
