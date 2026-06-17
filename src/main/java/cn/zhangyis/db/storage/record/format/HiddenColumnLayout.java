package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;

/**
 * 聚簇记录尾部隐藏区编解码（15 字节，innodb-record-design §6）：
 * DB_TRX_ID（8 字节无符号 big-endian）+ DB_ROLL_PTR（7 字节，见 {@link RollPointer}）。
 *
 * <p>仅聚簇 leaf CONVENTIONAL 记录写此区，贴在用户字段区之后、作为记录字节的尾部 15 字节；
 * node-pointer/非聚簇/系统记录无隐藏区。隐藏区是否存在由 schema 的 clustered 标志决定，由 encoder/decoder 控制。
 */
public final class HiddenColumnLayout {

    /** 隐藏区字节宽度 = 8(trx id) + 7(roll ptr)。 */
    public static final int HIDDEN_BYTES = 15;
    private static final int DB_TRX_ID_OFFSET = 0;
    private static final int DB_ROLL_PTR_OFFSET = 8;

    private HiddenColumnLayout() {
    }

    /** 在 {@code off} 处写 8 字节 trxId + 7 字节 rollPtr。 */
    public static void encode(byte[] buf, int off, TransactionId trxId, RollPointer rollPtr) {
        if (buf == null || off < 0 || off + HIDDEN_BYTES > buf.length) {
            throw new DatabaseValidationException("hidden area buffer too short");
        }
        long v = trxId.value();
        for (int i = 0; i < 8; i++) {
            buf[off + DB_TRX_ID_OFFSET + i] = (byte) (v >>> (56 - 8 * i));
        }
        byte[] rp = rollPtr.encode();
        System.arraycopy(rp, 0, buf, off + DB_ROLL_PTR_OFFSET, RollPointer.BYTES);
    }

    /** 从 {@code off} 处解码 DB_TRX_ID（8 字节无符号）。 */
    public static TransactionId decodeTrxId(byte[] buf, int off) {
        if (buf == null || off < 0 || off + HIDDEN_BYTES > buf.length) {
            throw new DatabaseValidationException("hidden area buffer too short");
        }
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[off + DB_TRX_ID_OFFSET + i] & 0xFFL);
        }
        return TransactionId.of(v);
    }

    /** 从 {@code off} 处解码 DB_ROLL_PTR（7 字节）。 */
    public static RollPointer decodeRollPtr(byte[] buf, int off) {
        return RollPointer.decode(buf, off + DB_ROLL_PTR_OFFSET);
    }
}
