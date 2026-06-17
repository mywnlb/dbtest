package cn.zhangyis.db.storage.record.format;

import cn.zhangyis.db.domain.PageNo;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.domain.TransactionId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** HiddenColumnLayout：15B 隐藏区 trxId+rollPtr 往返（含非零偏移）。 */
class HiddenColumnLayoutTest {

    @Test
    void fifteenBytesWide() {
        assertEquals(15, HiddenColumnLayout.HIDDEN_BYTES);
    }

    @Test
    void encodeDecodeRoundTripAtOffset() {
        byte[] buf = new byte[5 + HiddenColumnLayout.HIDDEN_BYTES];
        TransactionId trx = TransactionId.of(0x1122334455L);
        RollPointer rp = new RollPointer(true, PageNo.of(7), 9);
        HiddenColumnLayout.encode(buf, 5, trx, rp);
        assertEquals(trx, HiddenColumnLayout.decodeTrxId(buf, 5));
        assertEquals(rp, HiddenColumnLayout.decodeRollPtr(buf, 5));
    }

    @Test
    void nullRollPtrRoundTrips() {
        byte[] buf = new byte[HiddenColumnLayout.HIDDEN_BYTES];
        HiddenColumnLayout.encode(buf, 0, TransactionId.of(1), RollPointer.NULL);
        assertEquals(TransactionId.of(1), HiddenColumnLayout.decodeTrxId(buf, 0));
        assertEquals(RollPointer.NULL, HiddenColumnLayout.decodeRollPtr(buf, 0));
    }
}
