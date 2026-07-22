package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecord;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogRecordType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Row-log codec 必须对 immutable manifest、identity、frame length 与 CRC 做完整闭包校验。 */
class OnlineIndexRowLogCodecTest {

    private final OnlineIndexRowLogCodec codec = new OnlineIndexRowLogCodec();

    /** header 往返必须保留所有 owner/version 字段和 opaque DD manifest。 */
    @Test
    void shouldRoundTripHeader() {
        OnlineIndexLogHeader header = header();

        OnlineIndexLogHeader decoded = codec.decodeHeader(ByteBuffer.wrap(codec.encodeHeader(header)));

        assertEquals(header.buildId(), decoded.buildId());
        assertEquals(header.tableId(), decoded.tableId());
        assertEquals(header.indexId(), decoded.indexId());
        assertEquals(header.sourceDictionaryVersion(), decoded.sourceDictionaryVersion());
        assertEquals(header.targetDictionaryVersion(), decoded.targetDictionaryVersion());
        assertEquals(header.rowFormatVersion(), decoded.rowFormatVersion());
        assertArrayEquals(header.manifest(), decoded.manifest());
    }

    /** candidate frame 往返必须保留 generation/sequence/transaction/payload。 */
    @Test
    void shouldRoundTripFrame() {
        OnlineIndexLogRecord record = new OnlineIndexLogRecord(
                OnlineIndexLogRecordType.CANDIDATE, 3, 17, 29,
                "candidate".getBytes(StandardCharsets.UTF_8));

        OnlineIndexLogRecord decoded = codec.decodeRecord(ByteBuffer.wrap(codec.encodeRecord(record)));

        assertEquals(record.type(), decoded.type());
        assertEquals(record.generation(), decoded.generation());
        assertEquals(record.sequence(), decoded.sequence());
        assertEquals(record.transactionId(), decoded.transactionId());
        assertArrayEquals(record.payload(), decoded.payload());
    }

    /** 任意 payload bit 损坏都必须被 CRC 拒绝，不能把未知 candidate 带入 final reconciliation。 */
    @Test
    void shouldRejectCorruptedFrame() {
        byte[] encoded = codec.encodeRecord(new OnlineIndexLogRecord(
                OnlineIndexLogRecordType.CANDIDATE, 1, 1, 7, new byte[]{1, 2, 3, 4}));
        encoded[encoded.length - Integer.BYTES - 1] ^= 0x20;

        assertThrows(DatabaseValidationException.class,
                () -> codec.decodeRecord(ByteBuffer.wrap(encoded)));
    }

    /** frame 声明长度与实际输入不一致时必须拒绝，尾帧截断由文件 scanner 而非 codec 猜测。 */
    @Test
    void shouldRejectTruncatedFrame() {
        byte[] encoded = codec.encodeRecord(new OnlineIndexLogRecord(
                OnlineIndexLogRecordType.SEALED, 1, 9, 0, new byte[0]));

        assertThrows(DatabaseValidationException.class,
                () -> codec.decodeRecord(ByteBuffer.wrap(encoded, 0, encoded.length - 1)));
    }

    private static OnlineIndexLogHeader header() {
        return new OnlineIndexLogHeader(OnlineIndexBuildId.of(11), 22, 33,
                44, 45, 7, "manifest".getBytes(StandardCharsets.UTF_8));
    }
}
