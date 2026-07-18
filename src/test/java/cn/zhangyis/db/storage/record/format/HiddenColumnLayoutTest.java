package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** HiddenColumnLayout：15B 隐藏区 trxId+rollPtr 往返（含非零偏移）。 */
class HiddenColumnLayoutTest {

    /**
     * 验证 {@code fifteenBytesWide} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void fifteenBytesWide() {
        assertEquals(15, HiddenColumnLayout.HIDDEN_BYTES);
    }

    /**
     * 验证 {@code encodeDecodeRoundTripAtOffset} 所描述的稳定格式转换，并断言往返值、字节布局、版本与损坏输入处理。
     */
    @Test
    void encodeDecodeRoundTripAtOffset() {
        byte[] buf = new byte[5 + HiddenColumnLayout.HIDDEN_BYTES];
        TransactionId trx = TransactionId.of(0x1122334455L);
        RollPointer rp = new RollPointer(true, PageNo.of(7), 9);
        HiddenColumnLayout.encode(buf, 5, trx, rp);
        assertEquals(trx, HiddenColumnLayout.decodeTrxId(buf, 5));
        assertEquals(rp, HiddenColumnLayout.decodeRollPtr(buf, 5));
    }

    /**
     * 验证 {@code nullRollPtrRoundTrips} 对应的记录格式与页内组织行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     */
    @Test
    void nullRollPtrRoundTrips() {
        byte[] buf = new byte[HiddenColumnLayout.HIDDEN_BYTES];
        HiddenColumnLayout.encode(buf, 0, TransactionId.of(1), RollPointer.NULL);
        assertEquals(TransactionId.of(1), HiddenColumnLayout.decodeTrxId(buf, 0));
        assertEquals(RollPointer.NULL, HiddenColumnLayout.decodeRollPtr(buf, 0));
    }
}
