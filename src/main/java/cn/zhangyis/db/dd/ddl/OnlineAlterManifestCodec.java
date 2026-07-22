package cn.zhangyis.db.dd.ddl;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.SpaceId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;

/** `OALTMAN1` 通用 Online ALTER manifest v1 的确定性编解码器。 */
public final class OnlineAlterManifestCodec {

    private static final long MAGIC = 0x4f414c544d414e31L; // OALTMAN1
    private static final int FORMAT_VERSION = 1;
    public static final int MAX_MANIFEST_BYTES = 1024 * 1024;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_PATH_BYTES = 32 * 1024;

    /**
     * 编码完整manifest并追加CRC32C。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>由manifest构造器先证明协议、action顺序和identity完整。</li>
     *     <li>按固定big-endian grammar写identity、digest、动作和shadow目标。</li>
     *     <li>回填总长并对CRC前全部字节计算CRC32C；超过1MiB时拒绝发布。</li>
     * </ol>
     *
     * @param manifest initial X下冻结的完整恢复命令
     * @return 独立v1字节数组
     * @throws DatabaseValidationException 编码溢出、路径非法或超过格式上限时抛出
     */
    public byte[] encode(OnlineAlterManifest manifest) {
        if (manifest == null) {
            throw new DatabaseValidationException("online ALTER manifest must not be null");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeLong(MAGIC);
                out.writeInt(FORMAT_VERSION);
                out.writeInt(0);
                out.writeLong(manifest.ddlOperationId());
                out.writeLong(manifest.tableId().value());
                out.writeLong(manifest.sourceVersion().value());
                out.writeLong(manifest.targetVersion().value());
                out.writeInt(manifest.executionProtocol().stableCode());
                writeDigest(out, manifest.sourceSchemaDigest());
                writeDigest(out, manifest.targetSchemaDigest());
                out.writeLong(manifest.sourceRowFormatVersion());
                out.writeLong(manifest.targetRowFormatVersion());
                out.writeLong(manifest.freezeReadViewGeneration());
                out.writeInt(manifest.actions().size());
                for (OnlineAlterActionDescriptor action : manifest.actions()) {
                    byte[] payload = action.payload();
                    out.writeInt(action.type().stableCode());
                    out.writeInt(action.ordinal());
                    out.writeLong(action.primaryObjectId());
                    out.writeLong(action.secondaryObjectId());
                    out.writeInt(payload.length);
                    out.write(payload);
                }
                out.writeByte(manifest.shadowTarget().isPresent() ? 1 : 0);
                if (manifest.shadowTarget().isPresent()) {
                    OnlineAlterShadowTarget shadow = manifest.shadowTarget().orElseThrow();
                    out.writeInt(shadow.spaceId().value());
                    writeUtf8(out, shadow.path().toString(), "shadow path", MAX_PATH_BYTES);
                }
                out.flush();
            }
            byte[] withoutCrc = bytes.toByteArray();
            int total;
            try {
                total = Math.addExact(withoutCrc.length, Integer.BYTES);
            } catch (ArithmeticException overflow) {
                throw new DatabaseValidationException("online ALTER manifest length overflows", overflow);
            }
            if (total > MAX_MANIFEST_BYTES) {
                throw new DatabaseValidationException(
                        "online ALTER manifest exceeds format limit: " + total);
            }
            ByteBuffer.wrap(withoutCrc).putInt(Long.BYTES + Integer.BYTES, total);
            ByteBuffer output = ByteBuffer.allocate(total);
            output.put(withoutCrc).putInt(crc32c(withoutCrc));
            return output.array();
        } catch (IOException error) {
            throw new DatabaseValidationException("encode online ALTER manifest failed", error);
        }
    }

    /**
     * 严格解码manifest；总长、CRC、稳定码、连续ordinal和EOF任一不满足都阻止恢复。
     *
     * @param encoded journal header中已取得的完整manifest bytes
     * @return 重建全部构造不变量的不可变manifest
     * @throws DatabaseValidationException 损坏、截断、尾随或未知格式时抛出
     */
    public OnlineAlterManifest decode(byte[] encoded) {
        if (encoded == null || encoded.length < 128 || encoded.length > MAX_MANIFEST_BYTES) {
            throw new DatabaseValidationException("online ALTER manifest length is invalid");
        }
        int expectedCrc = ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES, Integer.BYTES).getInt();
        byte[] body = java.util.Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
        if (expectedCrc != crc32c(body)) {
            throw new DatabaseValidationException("online ALTER manifest CRC32C mismatch");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(body))) {
            if (in.readLong() != MAGIC || in.readInt() != FORMAT_VERSION
                    || in.readInt() != encoded.length) {
                throw new DatabaseValidationException("online ALTER manifest magic/version/length is invalid");
            }
            long ddlId = in.readLong();
            TableId tableId = TableId.of(in.readLong());
            DictionaryVersion source = DictionaryVersion.of(in.readLong());
            DictionaryVersion target = DictionaryVersion.of(in.readLong());
            DdlExecutionProtocol protocol = DdlExecutionProtocol.fromStableCode(in.readInt());
            DdlSchemaDigest sourceDigest = readDigest(in);
            DdlSchemaDigest targetDigest = readDigest(in);
            long sourceRowFormat = in.readLong();
            long targetRowFormat = in.readLong();
            long freezeGeneration = in.readLong();
            int actionCount = boundedCount(in.readInt(), OnlineAlterManifest.MAX_ACTIONS, "action");
            List<OnlineAlterActionDescriptor> actions = new ArrayList<>(actionCount);
            for (int ordinal = 0; ordinal < actionCount; ordinal++) {
                OnlineAlterActionType type = OnlineAlterActionType.fromStableCode(in.readInt());
                int declaredOrdinal = in.readInt();
                long primaryId = in.readLong();
                long secondaryId = in.readLong();
                int payloadLength = boundedLength(in.readInt(),
                        OnlineAlterActionDescriptor.MAX_PAYLOAD_BYTES, in.available(), "action payload");
                byte[] payload = in.readNBytes(payloadLength);
                if (payload.length != payloadLength || declaredOrdinal != ordinal) {
                    throw new DatabaseValidationException(
                            "online ALTER action ordinal/payload is truncated or non-contiguous");
                }
                actions.add(new OnlineAlterActionDescriptor(
                        type, declaredOrdinal, primaryId, secondaryId, payload));
            }
            int shadowPresent = in.readUnsignedByte();
            if (shadowPresent > 1) {
                throw new DatabaseValidationException("online ALTER shadow presence code is invalid");
            }
            Optional<OnlineAlterShadowTarget> shadow = Optional.empty();
            if (shadowPresent == 1) {
                shadow = Optional.of(new OnlineAlterShadowTarget(
                        SpaceId.of(in.readInt()), Path.of(readUtf8(in, "shadow path", MAX_PATH_BYTES))));
            }
            if (in.available() != 0) {
                throw new DatabaseValidationException("online ALTER manifest has trailing bytes");
            }
            return new OnlineAlterManifest(ddlId, tableId, source, target, protocol,
                    sourceDigest, targetDigest, sourceRowFormat, targetRowFormat,
                    freezeGeneration, actions, shadow);
        } catch (EOFException truncated) {
            throw new DatabaseValidationException("online ALTER manifest is truncated", truncated);
        } catch (IOException error) {
            throw new DatabaseValidationException("decode online ALTER manifest failed", error);
        }
    }

    private static void writeDigest(DataOutputStream out, DdlSchemaDigest digest) throws IOException {
        out.writeInt(digest.algorithm().stableCode());
        out.writeInt(digest.canonicalFormat().stableCode());
        out.write(digest.bytes());
    }

    private static DdlSchemaDigest readDigest(DataInputStream in) throws IOException {
        DdlDigestAlgorithm algorithm = DdlDigestAlgorithm.fromStableCode(in.readInt());
        DdlSchemaCanonicalFormat format = DdlSchemaCanonicalFormat.fromStableCode(in.readInt());
        byte[] digest = in.readNBytes(DIGEST_BYTES);
        if (digest.length != DIGEST_BYTES) {
            throw new DatabaseValidationException("online ALTER schema digest is truncated");
        }
        return new DdlSchemaDigest(algorithm, format, digest);
    }

    private static void writeUtf8(DataOutputStream out, String value, String field, int max)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length == 0 || encoded.length > max) {
            throw new DatabaseValidationException("online ALTER " + field + " length is invalid");
        }
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static String readUtf8(DataInputStream in, String field, int max) throws IOException {
        int length = boundedLength(in.readInt(), max, in.available(), field);
        byte[] encoded = in.readNBytes(length);
        if (encoded.length != length) {
            throw new DatabaseValidationException("online ALTER " + field + " is truncated");
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString();
        } catch (CharacterCodingException malformed) {
            throw new DatabaseValidationException(
                    "online ALTER " + field + " is not strict UTF-8", malformed);
        }
    }

    private static int boundedCount(int count, int max, String field) {
        if (count <= 0 || count > max) {
            throw new DatabaseValidationException("online ALTER " + field + " count is invalid");
        }
        return count;
    }

    private static int boundedLength(int length, int max, int available, String field) {
        if (length < 0 || length > max || length > available) {
            throw new DatabaseValidationException("online ALTER " + field + " length is invalid");
        }
        return length;
    }

    private static int crc32c(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
