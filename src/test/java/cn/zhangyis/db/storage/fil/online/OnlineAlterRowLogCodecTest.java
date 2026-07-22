package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecord;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogRecordType;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 通用 Online ALTER journal 必须与旧 OIDXLOG1 使用独立 magic、状态码和CRC。 */
class OnlineAlterRowLogCodecTest {

    private final OnlineAlterRowLogCodec codec = new OnlineAlterRowLogCodec();

    /** header 往返保留 protocol、双row-format、可选shadow space及opaque manifest。 */
    @Test
    void roundTripsHeader() {
        OnlineAlterLogHeader expected = new OnlineAlterLogHeader(
                OnlineDdlCaptureId.of(7), 8, 9, 10, 11, 12,
                4, 5, 13, "manifest".getBytes(StandardCharsets.UTF_8));

        OnlineAlterLogHeader actual = codec.decodeHeader(ByteBuffer.wrap(codec.encodeHeader(expected)));

        assertEquals(expected.captureId(), actual.captureId());
        assertEquals(expected.tableId(), actual.tableId());
        assertEquals(expected.sourceDictionaryVersion(), actual.sourceDictionaryVersion());
        assertEquals(expected.targetDictionaryVersion(), actual.targetDictionaryVersion());
        assertEquals(expected.sourceRowFormatVersion(), actual.sourceRowFormatVersion());
        assertEquals(expected.targetRowFormatVersion(), actual.targetRowFormatVersion());
        assertEquals(expected.executionProtocolCode(), actual.executionProtocolCode());
        assertEquals(expected.shadowSpaceId(), actual.shadowSpaceId());
        assertArrayEquals(expected.manifest(), actual.manifest());
    }

    /** READY_TO_PUBLISH 是独立稳定码，frame 往返必须保留 generation/sequence/payload。 */
    @Test
    void roundTripsReadyToPublishFrame() {
        OnlineAlterLogRecord expected = new OnlineAlterLogRecord(
                OnlineAlterLogRecordType.READY_TO_PUBLISH, 3, 17, 0,
                "ready-evidence".getBytes(StandardCharsets.UTF_8));

        OnlineAlterLogRecord actual = codec.decodeRecord(ByteBuffer.wrap(codec.encodeRecord(expected)));

        assertEquals(6, actual.type().stableCode());
        assertEquals(expected.type(), actual.type());
        assertEquals(expected.generation(), actual.generation());
        assertEquals(expected.sequence(), actual.sequence());
        assertEquals(expected.transactionId(), actual.transactionId());
        assertArrayEquals(expected.payload(), actual.payload());
    }

    /** 任意frame损坏、截断或尾随都必须fail-closed。 */
    @Test
    void rejectsCorruptedAndNonExactFrame() {
        byte[] encoded = codec.encodeRecord(new OnlineAlterLogRecord(
                OnlineAlterLogRecordType.CANDIDATE, 1, 2, 3, new byte[]{4, 5, 6}));
        byte[] corrupted = encoded.clone();
        corrupted[corrupted.length - Integer.BYTES - 1] ^= 0x20;

        assertThrows(DatabaseValidationException.class,
                () -> codec.decodeRecord(ByteBuffer.wrap(corrupted)));
        assertThrows(DatabaseValidationException.class,
                () -> codec.decodeRecord(ByteBuffer.wrap(Arrays.copyOf(encoded, encoded.length - 1))));
        assertThrows(DatabaseValidationException.class,
                () -> codec.decodeRecord(ByteBuffer.wrap(Arrays.copyOf(encoded, encoded.length + 1))));
    }
}
