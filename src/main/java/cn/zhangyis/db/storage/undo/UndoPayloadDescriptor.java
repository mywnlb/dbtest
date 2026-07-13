package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.TransactionId;
import cn.zhangyis.db.domain.UndoNo;
import cn.zhangyis.db.storage.page.FilePageHeader;

import java.nio.ByteBuffer;

/**
 * 普通 UNDO record 槽中的 external payload 根描述符。旧 inline record 的首字节固定为 1/2/3，故保留
 * {@code 0x7F} 作为不改变既有编码的分流标记。描述符只发布不可变页链身份，不承担业务 LOB 引用语义。
 *
 * @param type          完整外置 UndoRecord 的逻辑类型。
 * @param transactionId 记录 owner，用于拒绝跨事务拼链。
 * @param undoNo        事务内唯一序号，用于拒绝同 segment 内不同 payload 页互换。
 * @param firstPageNo   payload 链首页。
 * @param totalLength   完整 UndoRecord 编码长度。
 * @param pageCount     payload 链精确页数。
 * @param crc32         完整编码字节的 CRC32。
 */
record UndoPayloadDescriptor(UndoRecordType type, TransactionId transactionId, UndoNo undoNo,
                             PageNo firstPageNo, int totalLength, int pageCount, long crc32) {

    /** external descriptor 首字节；与现有 UndoRecordType code 1/2/3 不冲突。 */
    static final int TAG = 0x7F;
    /** 当前 descriptor 持久版本。 */
    static final int VERSION = 1;
    /** 稳定落盘长度：1+1+1+8+8+4+4+4+4。 */
    static final int BYTES = 35;

    UndoPayloadDescriptor {
        if (type == null || transactionId == null || undoNo == null || firstPageNo == null) {
            throw new DatabaseValidationException("undo payload descriptor fields must not be null");
        }
        if (transactionId.isNone() || undoNo.isNone() || firstPageNo.value() >= FilePageHeader.FIL_NULL
                || totalLength <= 0 || pageCount <= 0 || crc32 < 0 || crc32 > 0xFFFF_FFFFL) {
            throw new DatabaseValidationException("invalid undo payload descriptor bounds");
        }
    }

    /** 按大端稳定格式编码根描述符。 */
    byte[] encode() {
        ByteBuffer out = ByteBuffer.allocate(BYTES);
        out.put((byte) TAG);
        out.put((byte) VERSION);
        out.put((byte) type.code());
        out.putLong(transactionId.value());
        out.putLong(undoNo.value());
        out.putInt((int) firstPageNo.value());
        out.putInt(totalLength);
        out.putInt(pageCount);
        out.putInt((int) crc32);
        return out.array();
    }

    /** 严格解析 descriptor；长度、tag、版本或领域字段不合法均视为 undo 物理格式损坏。 */
    static UndoPayloadDescriptor decode(byte[] bytes) {
        if (bytes == null) {
            throw new DatabaseValidationException("undo payload descriptor bytes must not be null");
        }
        if (bytes.length != BYTES) {
            throw new UndoLogFormatException("external undo descriptor length must be " + BYTES
                    + ", got " + bytes.length);
        }
        try {
            ByteBuffer in = ByteBuffer.wrap(bytes);
            int tag = in.get() & 0xFF;
            int version = in.get() & 0xFF;
            int typeCode = in.get() & 0xFF;
            if (tag != TAG || version != VERSION) {
                throw new UndoLogFormatException("external undo descriptor tag/version mismatch: tag="
                        + tag + " version=" + version);
            }
            return new UndoPayloadDescriptor(UndoRecordType.fromCode(typeCode),
                    TransactionId.of(in.getLong()), UndoNo.of(in.getLong()),
                    PageNo.of(in.getInt() & 0xFFFF_FFFFL), in.getInt(), in.getInt(),
                    in.getInt() & 0xFFFF_FFFFL);
        } catch (UndoLogFormatException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new UndoLogFormatException("cannot decode external undo descriptor", error);
        }
    }

    /** 判断普通 record 槽是否使用 external descriptor。 */
    static boolean isExternal(byte[] payload) {
        return payload != null && payload.length > 0 && (payload[0] & 0xFF) == TAG;
    }
}
