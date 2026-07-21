package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.storage.fil.state.TablespaceType;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;

/** manifest 的稳定二进制编码、HMAC 签名、验证与原子写入协作者。 */
public final class RecoveryBackupManifestCodec {

    private static final int MAGIC = 0x52424D31;
    private static final int MAX_BYTES = 16 * 1024;

    /** @return 覆盖所有无签名字段的新 manifest。 */
    public RecoveryBackupManifest sign(
            RecoveryBackupManifest unsigned, RecoveryIdentity identity) {
        if (unsigned == null || identity == null || !unsigned.hmacSha256().isEmpty()) {
            throw new RecoveryBackupException(
                    "manifest signing requires unsigned manifest and recovery identity");
        }
        return unsigned.withHmac(hmac(unsignedBytes(unsigned), identity.hmacKey()));
    }

    /**
     * 恒定时间比较 manifest HMAC，并同时要求 source instance UUID 与本地 identity 一致。
     *
     * @param manifest 从固定 incoming 路径解码的候选 manifest
     * @param identity 本地已有且已通过 CRC 的恢复身份
     * @throws RecoveryBackupException 来源 UUID 或 HMAC 不匹配时抛出
     */
    public void verify(RecoveryBackupManifest manifest, RecoveryIdentity identity) {
        if (manifest == null || identity == null
                || !manifest.sourceInstanceId().equals(identity.instanceId())) {
            throw new RecoveryBackupException(
                    "recovery backup source instance identity mismatch");
        }
        byte[] expected = HexFormat.of().parseHex(
                hmac(unsignedBytes(manifest.withHmac("")), identity.hmacKey()));
        byte[] actual;
        try {
            actual = HexFormat.of().parseHex(manifest.hmacSha256());
        } catch (IllegalArgumentException invalidHex) {
            throw new RecoveryBackupException(
                    "recovery backup manifest HMAC is not hexadecimal", invalidHex);
        }
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new RecoveryBackupException(
                    "recovery backup manifest HMAC mismatch");
        }
    }

    /**
     * manifest 必须最后发布：临时文件完整写入并 force 后才原子移动到最终路径，且拒绝覆盖既有备份证据。
     *
     * @param path 最终 archive manifest 路径
     * @param manifest 已签名 manifest
     */
    public void writeAtomic(Path path, RecoveryBackupManifest manifest) {
        if (path == null || manifest == null || manifest.hmacSha256().isEmpty()) {
            throw new RecoveryBackupException(
                    "signed manifest and target path are required");
        }
        Path target = path.toAbsolutePath().normalize();
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        byte[] bytes = encode(manifest);
        try {
            if (Files.exists(target)) {
                throw new RecoveryBackupException(
                        "recovery backup manifest already exists: " + target);
            }
            Files.createDirectories(target.getParent());
            Files.deleteIfExists(temporary);
            try (FileChannel channel = FileChannel.open(
                    temporary, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException failure) {
            throw new RecoveryBackupException(
                    "write recovery backup manifest failed: " + target, failure);
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException ignored) {
                // final manifest 是唯一有效证据；残留 tmp 永不参与 import。
            }
        }
    }

    /** @param path 固定 incoming/archive manifest 路径 @return 完整解码的签名 manifest */
    public RecoveryBackupManifest read(Path path) {
        if (path == null) {
            throw new RecoveryBackupException("manifest path must not be null");
        }
        try {
            byte[] bytes = Files.readAllBytes(path.toAbsolutePath().normalize());
            if (bytes.length <= 0 || bytes.length > MAX_BYTES) {
                throw new RecoveryBackupException(
                        "recovery backup manifest length is invalid: " + bytes.length);
            }
            try (DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(bytes))) {
                if (input.readInt() != MAGIC) {
                    throw new RecoveryBackupException(
                            "recovery backup manifest magic mismatch");
                }
                int format = input.readInt();
                UUID backupId = new UUID(input.readLong(), input.readLong());
                UUID instanceId = new UUID(input.readLong(), input.readLong());
                TableId tableId = TableId.of(input.readLong());
                SpaceId spaceId = SpaceId.of(input.readInt());
                DictionaryVersion dictionaryVersion = DictionaryVersion.of(input.readLong());
                TablespaceFileIdentity fileIdentity = new TablespaceFileIdentity(
                        SpaceId.of(input.readInt()), PageSize.ofBytes(input.readInt()),
                        TablespaceType.fromCode(input.readInt()), input.readInt(), input.readLong());
                String definitionHash = input.readUTF();
                Lsn cleanLsn = Lsn.of(input.readLong());
                long length = input.readLong();
                String fileHash = input.readUTF();
                String hmac = input.readUTF();
                if (input.available() != 0) {
                    throw new RecoveryBackupException(
                            "recovery backup manifest has trailing bytes");
                }
                return new RecoveryBackupManifest(
                        format, backupId, instanceId, tableId, spaceId,
                        dictionaryVersion, fileIdentity, definitionHash,
                        cleanLsn, length, fileHash, hmac);
            }
        } catch (IOException failure) {
            throw new RecoveryBackupException(
                    "read recovery backup manifest failed: " + path, failure);
        }
    }

    /** 生成包含 HMAC 字段的最终文件编码。 */
    private static byte[] encode(RecoveryBackupManifest manifest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeFields(output, manifest);
                output.writeUTF(manifest.hmacSha256());
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new RecoveryBackupException(
                    "encode recovery backup manifest failed", impossible);
        }
    }

    /** HMAC 输入只含稳定无签名字段，编码顺序与最终文件前缀完全一致。 */
    private static byte[] unsignedBytes(RecoveryBackupManifest manifest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeFields(output, manifest);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new RecoveryBackupException(
                    "encode unsigned recovery backup manifest failed", impossible);
        }
    }

    /** 写入 manifest 的 canonical 字段前缀。 */
    private static void writeFields(
            DataOutputStream output, RecoveryBackupManifest manifest) throws IOException {
        output.writeInt(MAGIC);
        output.writeInt(manifest.formatVersion());
        output.writeLong(manifest.backupId().getMostSignificantBits());
        output.writeLong(manifest.backupId().getLeastSignificantBits());
        output.writeLong(manifest.sourceInstanceId().getMostSignificantBits());
        output.writeLong(manifest.sourceInstanceId().getLeastSignificantBits());
        output.writeLong(manifest.tableId().value());
        output.writeInt(manifest.spaceId().value());
        output.writeLong(manifest.sourceDictionaryVersion().value());
        output.writeInt(manifest.fileIdentity().spaceId().value());
        output.writeInt(manifest.fileIdentity().pageSize().bytes());
        output.writeInt(manifest.fileIdentity().type().code());
        output.writeInt(manifest.fileIdentity().serverVersion());
        output.writeLong(manifest.fileIdentity().spaceVersion());
        output.writeUTF(manifest.tableDefinitionSha256());
        output.writeLong(manifest.cleanLsn().value());
        output.writeLong(manifest.fileLengthBytes());
        output.writeUTF(manifest.fileSha256());
    }

    /** 计算 HMAC-SHA256；算法缺失属于当前 JVM 无法继续可信恢复的配置失败。 */
    private static String hmac(byte[] bytes, byte[] key) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(bytes));
        } catch (GeneralSecurityException failure) {
            throw new RecoveryBackupException(
                    "HmacSHA256 is unavailable for recovery backup", failure);
        }
    }
}
