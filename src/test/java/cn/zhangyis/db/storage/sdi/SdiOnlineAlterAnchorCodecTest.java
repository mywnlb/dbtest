package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** page3 ALT1/v1 anchor 的96字节几何、摘要、CRC和保留区测试。 */
class SdiOnlineAlterAnchorCodecTest {

    private final SdiOnlineAlterAnchorCodec codec = new SdiOnlineAlterAnchorCodec();

    /** 编码必须精确占用既有footer，前88字节为字段，最后8字节保持全零。 */
    @Test
    void encodesExactNinetySixByteAnchor() {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) 0x5a);
        SdiOnlineAlterAnchor expected = new SdiOnlineAlterAnchor(
                11, 12, 13, 14, 15, 16, digest);

        byte[] encoded = codec.encode(expected);
        SdiOnlineAlterAnchor actual = codec.decode(encoded);

        assertEquals(SdiPageLayout.INDEX_BUILD_FOOTER_BYTES, encoded.length);
        assertEquals(0x414c5431, ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN).getInt());
        assertArrayEquals(new byte[8], Arrays.copyOfRange(encoded, 88, 96));
        assertEquals(expected.ddlOperationId(), actual.ddlOperationId());
        assertEquals(expected.targetDictionaryVersion(), actual.targetDictionaryVersion());
        assertEquals(expected.tableId(), actual.tableId());
        assertEquals(expected.generation(), actual.generation());
        assertEquals(expected.descriptorRootPageNo(), actual.descriptorRootPageNo());
        assertEquals(expected.descriptorCount(), actual.descriptorCount());
        assertArrayEquals(expected.manifestDigest(), actual.manifestDigest());
    }

    /** CRC、reserved byte或长度不合法时不能把anchor用于资源回收。 */
    @Test
    void rejectsCorruptionReservedBytesAndWrongLength() {
        byte[] encoded = codec.encode(new SdiOnlineAlterAnchor(
                1, 2, 3, 4, 5, 0, new byte[32]));
        byte[] corrupt = encoded.clone();
        corrupt[20] ^= 1;
        byte[] reserved = encoded.clone();
        reserved[95] = 1;

        assertThrows(DatabaseValidationException.class, () -> codec.decode(corrupt));
        assertThrows(DatabaseValidationException.class, () -> codec.decode(reserved));
        assertThrows(DatabaseValidationException.class,
                () -> codec.decode(Arrays.copyOf(encoded, encoded.length - 1)));
    }
}
