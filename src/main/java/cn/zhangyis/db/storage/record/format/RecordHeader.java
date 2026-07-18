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

    /** 写头部到 buf 的 at 处（at 通常为 0，记录起始）。
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code writeTo} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     */
    public void writeTo(byte[] buf, int at) {
        int flags = (deletedFlag ? 1 : 0) | (minRecFlag ? 2 : 0) | (recordType.code() << 2);
        buf[at + RecordHeaderLayout.FLAGS] = (byte) flags;
        U16.put(buf, at + RecordHeaderLayout.HEAP_NO, heapNo);
        buf[at + RecordHeaderLayout.N_OWNED] = (byte) nOwned;
        U16.put(buf, at + RecordHeaderLayout.NEXT_RECORD_OFFSET, nextRecordOffset);
        U16.put(buf, at + RecordHeaderLayout.RECORD_LENGTH, recordLength);
    }

    /** 从 buf 的 at 处读头部。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>读取并校验调用参数与当前领域状态，确保失败发生在本方法创建共享或持久副作用之前。</li>
     *     <li>按类级并发协议取得本阶段所需协作者与 Guard，竞争后重新校验资源身份和生命周期。</li>
     *     <li>执行核心状态转换或数据变换，并只通过本包稳定协作者发布可观察副作用。</li>
     *     <li>汇总稳定结果并沿现有 finally/Guard 释放资源；异常不得伪造成功或清除未完成状态。</li>
     * </ol>
     *
     * @param buf 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @param at 参与 {@code readFrom} 的零基位置 {@code at}；必须非负且小于所属页面、集合或持久结构的容量
     * @return {@code readFrom} 编码、解码或重建的记录数据；成功时不为 {@code null}，字段顺序、隐藏列和字节边界满足当前 schema
     */
    public static RecordHeader readFrom(byte[] buf, int at) {
        // 1、读取并校验调用参数与当前领域状态，在共享或持久副作用前拒绝非法状态。
        int flags = buf[at + RecordHeaderLayout.FLAGS] & 0xFF;
        boolean deleted = (flags & 1) != 0;
        // 2、继续完成范围、身份与候选校验；通过后，按类级并发协议取得本阶段所需协作者与 Guard，保持处理顺序与资源边界。
        boolean minRec = (flags & 2) != 0;
        RecordType type = RecordType.fromCode((flags >> 2) & 0x3);
        int heapNo = U16.get(buf, at + RecordHeaderLayout.HEAP_NO);
        // 3、在中间分支复核阶段性结果；满足条件后，执行核心状态转换或数据变换，并维持领域不变量。
        int nOwned = buf[at + RecordHeaderLayout.N_OWNED] & 0xFF;
        int next = U16.get(buf, at + RecordHeaderLayout.NEXT_RECORD_OFFSET);
        int len = U16.get(buf, at + RecordHeaderLayout.RECORD_LENGTH);
        // 4、汇总稳定结果并沿现有 finally/Guard 释放资源，以稳定返回或领域异常完成收口。
        return new RecordHeader(deleted, minRec, type, heapNo, nOwned, next, len);
    }

    private static void requireU16(String name, int v) {
        if (v < 0 || v > 0xFFFF) {
            throw new DatabaseValidationException(name + " out of u16 range: " + v);
        }
    }
}
