package cn.zhangyis.db.storage.sdi;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32C;

/** `ALT1/v1` 96-byte page3 anchor codec；最后8字节保留且必须为零。 */
public final class SdiOnlineAlterAnchorCodec {

    private static final int MAGIC = 0x414c5431; // ALT1
    private static final int FORMAT_VERSION = 1;
    private static final int CRC_OFFSET = 84;
    private static final int RESERVED_OFFSET = 88;

    public byte[] encode(SdiOnlineAlterAnchor anchor) {
        if (anchor == null) {
            throw new DatabaseValidationException("SDI online ALTER anchor must not be null");
        }
        ByteBuffer output = ByteBuffer.allocate(SdiPageLayout.INDEX_BUILD_FOOTER_BYTES)
                .order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(FORMAT_VERSION)
                .putLong(anchor.ddlOperationId())
                .putLong(anchor.targetDictionaryVersion())
                .putLong(anchor.tableId())
                .putLong(anchor.generation())
                .putLong(anchor.descriptorRootPageNo())
                .putInt(anchor.descriptorCount())
                .put(anchor.manifestDigest());
        output.putInt(crc32c(Arrays.copyOf(output.array(), CRC_OFFSET)));
        return output.array();
    }

    public SdiOnlineAlterAnchor decode(byte[] encoded) {
        if (encoded == null || encoded.length != SdiPageLayout.INDEX_BUILD_FOOTER_BYTES) {
            throw new DatabaseValidationException("SDI online ALTER anchor length is invalid");
        }
        for (int i = RESERVED_OFFSET; i < encoded.length; i++) {
            if (encoded[i] != 0) {
                throw new DatabaseValidationException("SDI online ALTER anchor reserved bytes are non-zero");
            }
        }
        ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC || input.getInt() != FORMAT_VERSION) {
            throw new DatabaseValidationException("SDI online ALTER anchor magic/version is invalid");
        }
        long ddlId = input.getLong();
        long targetVersion = input.getLong();
        long tableId = input.getLong();
        long generation = input.getLong();
        long rootPage = input.getLong();
        int descriptorCount = input.getInt();
        byte[] digest = new byte[32];
        input.get(digest);
        int expectedCrc = input.getInt();
        if (expectedCrc != crc32c(Arrays.copyOf(encoded, CRC_OFFSET))) {
            throw new DatabaseValidationException("SDI online ALTER anchor CRC32C mismatch");
        }
        return new SdiOnlineAlterAnchor(ddlId, targetVersion, tableId, generation,
                rootPage, descriptorCount, digest);
    }

    private static int crc32c(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
