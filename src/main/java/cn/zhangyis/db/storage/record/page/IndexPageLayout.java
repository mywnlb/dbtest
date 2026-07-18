package cn.zhangyis.db.storage.record.page;

import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

import java.nio.charset.StandardCharsets;

/**
 * INDEX 页内记录区几何常量（innodb-record-design §7）。页体在 {@code [FIL_PAGE_HEADER_BYTES, pageSize-FIL_PAGE_TRAILER_BYTES)} 内：
 * <pre>
 *   [0,38)            FilePageHeader（§5.3 信封，本层不碰，调用方盖）
 *   [38,66)           INDEX page header（PAGE HEADER 教学子集）
 *   [66,82)           INFIMUM 系统记录（8 头 + 8 标签）
 *   [82,98)           SUPREMUM 系统记录（8 头 + 8 标签）
 *   [98,heapTop)      UserRecordHeap（向高地址增长）
 *   [heapTop,dirStart) FreeSpace
 *   [dirStart,pageSize-8) PageDirectory（向低地址增长，每槽 2 字节）
 *   [pageSize-8,pageSize) FilePageTrailer（§5.3 信封，本层不碰）
 * </pre>
 *
 * <p>简化点：{@link #FIL_PAGE_HEADER_BYTES}/{@link #FIL_PAGE_TRAILER_BYTES} 必须与 §5.3
 * {@code fsp.FilePageHeaderLayout.SIZE}（38）/ trailer（8）一致；{@link #REC_HEADER_BYTES}/{@link #REC_NEXT_FIELD_OFFSET}
 * 必须与 {@code record.format.RecordHeaderLayout.SIZE}（8）/{@code NEXT_RECORD_OFFSET}（4）一致。
 * 因这些常量在各自包内为包私有，此处教学性本地复刻，由 {@code IndexPageLayoutTest} 钉死；后续可把页信封提到共享 {@code storage.page} 包消除复刻。
 */
final class IndexPageLayout {

    private IndexPageLayout() {
    }

    /** 页首 FilePageHeader 字节数（引用共享信封常量，不再本地复刻）。 */
    static final int FIL_PAGE_HEADER_BYTES = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES;
    /** 页尾 FilePageTrailer 字节数（引用共享信封常量）。 */
    static final int FIL_PAGE_TRAILER_BYTES = PageEnvelopeLayout.FIL_PAGE_TRAILER_BYTES;

    /** INDEX page header 起始偏移（紧跟信封头）。 */
    static final int PAGE_HEADER_START = FIL_PAGE_HEADER_BYTES; // 38
    /** INDEX page header 结束偏移（= 系统记录起始）。 */
    static final int PAGE_HEADER_END = 66;

    /** INFIMUM 系统记录偏移（记录头起始；前向布局）。 */
    static final int INFIMUM_OFFSET = PAGE_HEADER_END; // 66
    /** 单条系统记录字节数（8 头 + 8 标签）。 */
    static final int SYS_REC_BYTES = 16;
    /** SUPREMUM 系统记录偏移。 */
    static final int SUPREMUM_OFFSET = INFIMUM_OFFSET + SYS_REC_BYTES; // 82
    /** 用户记录 heap 起始偏移（= 空页 heapTop）。 */
    static final int USER_RECORDS_START = SUPREMUM_OFFSET + SYS_REC_BYTES; // 98

    /** 单个 PageDirectory 槽字节数（u16 偏移）。 */
    static final int DIR_SLOT_BYTES = 2;

    /** 记录头字节数（= record.format.RecordHeaderLayout.SIZE）。 */
    static final int REC_HEADER_BYTES = 8;
    /** 记录头内 flags 字节偏移（u8；= record.format.RecordHeaderLayout.FLAGS；bit0 deleted、bit1 minRec、bit2-3 recordType）。 */
    static final int REC_FLAGS_FIELD_OFFSET = 0;
    /** 记录头内 heapNo 字段偏移（u16；= record.format.RecordHeaderLayout.HEAP_NO）。 */
    static final int REC_HEAPNO_FIELD_OFFSET = 1;
    /** 记录头内 n_owned 字段偏移（u8；= record.format.RecordHeaderLayout.N_OWNED）。 */
    static final int REC_NOWNED_FIELD_OFFSET = 3;
    /** 记录头内 next_record 字段偏移（u16；= record.format.RecordHeaderLayout.NEXT_RECORD_OFFSET）。 */
    static final int REC_NEXT_FIELD_OFFSET = 4;

    /** 系统记录标签（教学：8 字节 ASCII，便于人工识别 + 固定 heapTop）。InnoDB 同样保留 "infimum\0"/"supremum"。 */
    static final byte[] INFIMUM_LABEL = "infimum\0".getBytes(StandardCharsets.UTF_8);
    /**
     * 类级不可变配置常量；所有实例共享该边界，非法调整会破坏记录格式与页内组织的不变量。
     */
    static final byte[] SUPREMUM_LABEL = "supremum".getBytes(StandardCharsets.UTF_8);
}
