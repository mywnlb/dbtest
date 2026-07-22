package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.IndexId;
import cn.zhangyis.db.dd.domain.IndexKeyPart;
import cn.zhangyis.db.dd.domain.IndexOrder;
import cn.zhangyis.db.dd.domain.ObjectName;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Online ADD INDEX durable manifest 的 exact identity、版本和 key-part 格式测试。 */
class OnlineIndexBuildManifestCodecTest {

    /** UTF-8 名称、UNIQUE、ASC/DESC 与 prefix 必须完整往返，不能依赖 enum ordinal 或 Java serialization。 */
    @Test
    void roundTripsStableManifestDefinition() {
        OnlineIndexBuildManifest expected = manifest();
        OnlineIndexBuildManifestCodec codec = new OnlineIndexBuildManifestCodec();

        OnlineIndexBuildManifest actual = codec.decode(codec.encode(expected));

        assertEquals(expected, actual);
    }

    /** 截断或尾随字节都会改变恢复命令边界，解码器必须拒绝而不是猜测兼容。 */
    @Test
    void rejectsTruncatedAndTrailingManifestBytes() {
        OnlineIndexBuildManifestCodec codec = new OnlineIndexBuildManifestCodec();
        byte[] encoded = codec.encode(manifest());

        assertThrows(DatabaseValidationException.class,
                () -> codec.decode(Arrays.copyOf(encoded, encoded.length - 1)));
        assertThrows(DatabaseValidationException.class,
                () -> codec.decode(Arrays.copyOf(encoded, encoded.length + 1)));
    }

    /** CRC/摘要只能证明字节未变；名称本身不是严格 UTF-8 时，恢复不得用 replacement 字符猜对象身份。 */
    @Test
    void rejectsMalformedUtf8IndexName() {
        OnlineIndexBuildManifestCodec codec = new OnlineIndexBuildManifestCodec();
        byte[] encoded = codec.encode(manifest());
        ByteBuffer fields = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        int nameLengthOffset = Integer.BYTES + Short.BYTES * 2 + Long.BYTES * 5 + 1;
        int nameLength = fields.getInt(nameLengthOffset);
        int nameOffset = nameLengthOffset + Integer.BYTES;
        if (nameLength < 2) {
            throw new AssertionError("manifest fixture name must contain at least two UTF-8 bytes");
        }
        encoded[nameOffset] = (byte) 0xC3;
        encoded[nameOffset + 1] = 0x28;

        assertThrows(DatabaseValidationException.class, () -> codec.decode(encoded));
    }

    private static OnlineIndexBuildManifest manifest() {
        return new OnlineIndexBuildManifest(
                OnlineIndexBuildId.of(41), TableId.of(51),
                DictionaryVersion.of(61), DictionaryVersion.of(62),
                new IndexDefinition(IndexId.of(71), ObjectName.of("索引_value"), true, false,
                        List.of(new IndexKeyPart(81, IndexOrder.ASC, 0),
                                new IndexKeyPart(82, IndexOrder.DESC, 12))));
    }
}
