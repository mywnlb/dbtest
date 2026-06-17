package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 物理记录头（innodb-record-design §5.2）。heapNo/nextRecordOffset/recordLength 为 u16；nOwned 为 u8。
 * 本片（R2）nextRecordOffset/heapNo/nOwned 仅编解码，真实值由页层（R3）维护。
 *
 * @param deletedFlag      delete-mark。
 * @param minRecFlag       最小记录标志。
 * @param recordType       记录类型。
 * @param heapNo           页内 heap 物理序号（0..65535）。
 * @param nOwned           本 group 成员数（仅 group 末记录非 0；0..255）。
 * @param nextRecordOffset 下一记录页内偏移（0..65535；R2 恒 0）。
 * @param recordLength     整条记录字节数（含头；0..65535）。
 */
public record RecordHeader(boolean deletedFlag, boolean minRecFlag, RecordType recordType,
                           int heapNo, int nOwned, int nextRecordOffset, int recordLength) {

    public RecordHeader {
        if (recordType == null) {
            throw new DatabaseValidationException("record type must not be null");
        }
        requireU16("heapNo", heapNo);
        requireU16("nextRecordOffset", nextRecordOffset);
        requireU16("recordLength", recordLength);
        if (nOwned < 0 || nOwned > 0xFF) {
            throw new DatabaseValidationException("nOwned out of range: " + nOwned);
        }
    }

    /** 写头部到 buf 的 at 处（at 通常为 0，记录起始）。 */
    public void writeTo(byte[] buf, int at) {
        int flags = (deletedFlag ? 1 : 0) | (minRecFlag ? 2 : 0) | (recordType.code() << 2);
        buf[at + RecordHeaderLayout.FLAGS] = (byte) flags;
        U16.put(buf, at + RecordHeaderLayout.HEAP_NO, heapNo);
        buf[at + RecordHeaderLayout.N_OWNED] = (byte) nOwned;
        U16.put(buf, at + RecordHeaderLayout.NEXT_RECORD_OFFSET, nextRecordOffset);
        U16.put(buf, at + RecordHeaderLayout.RECORD_LENGTH, recordLength);
    }

    /** 从 buf 的 at 处读头部。 */
    public static RecordHeader readFrom(byte[] buf, int at) {
        int flags = buf[at + RecordHeaderLayout.FLAGS] & 0xFF;
        boolean deleted = (flags & 1) != 0;
        boolean minRec = (flags & 2) != 0;
        RecordType type = RecordType.fromCode((flags >> 2) & 0x3);
        int heapNo = U16.get(buf, at + RecordHeaderLayout.HEAP_NO);
        int nOwned = buf[at + RecordHeaderLayout.N_OWNED] & 0xFF;
        int next = U16.get(buf, at + RecordHeaderLayout.NEXT_RECORD_OFFSET);
        int len = U16.get(buf, at + RecordHeaderLayout.RECORD_LENGTH);
        return new RecordHeader(deleted, minRec, type, heapNo, nOwned, next, len);
    }

    private static void requireU16(String name, int v) {
        if (v < 0 || v > 0xFFFF) {
            throw new DatabaseValidationException(name + " out of u16 range: " + v);
        }
    }
}
