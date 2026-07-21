package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.dd.domain.DictionaryVersion;
import cn.zhangyis.db.dd.domain.TableId;
import cn.zhangyis.db.domain.Lsn;
import cn.zhangyis.db.domain.PageSize;
import cn.zhangyis.db.domain.SpaceId;
import cn.zhangyis.db.storage.api.tablespace.TablespaceFileIdentity;
import cn.zhangyis.db.storage.fil.state.TablespaceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** recovery identity 与签名 manifest 的持久格式、篡改拒绝和懒创建边界测试。 */
class RecoveryBackupManifestCodecTest {

    @TempDir
    Path directory;

    /** identity 必须跨重读稳定；CRC 损坏不得自动覆盖或生成新 key。 */
    @Test
    void persistsStableIdentityAndRejectsCorruptedCrc() throws Exception {
        RecoveryIdentityStore store = new RecoveryIdentityStore(directory);

        RecoveryIdentity created = store.openOrCreate();
        RecoveryIdentity reopened = store.openExisting();

        assertEquals(created.instanceId(), reopened.instanceId());
        assertArrayEquals(created.hmacKey(), reopened.hmacKey());
        byte[] corrupted = Files.readAllBytes(store.path());
        corrupted[corrupted.length - 1] ^= 0x5A;
        Files.write(store.path(), corrupted);
        assertThrows(RecoveryIdentityException.class, store::openExisting);
    }

    /** HMAC 覆盖 table/space/version/identity/definition/LSN/length/hash，任一字段变化均不能通过验证。 */
    @Test
    void roundTripsSignedManifestAndRejectsFieldTampering() {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) 7);
        RecoveryIdentity identity = new RecoveryIdentity(UUID.randomUUID(), key);
        RecoveryBackupManifestCodec codec = new RecoveryBackupManifestCodec();
        RecoveryBackupManifest unsigned = manifest(identity, "a".repeat(64), "b".repeat(64));
        RecoveryBackupManifest signed = codec.sign(unsigned, identity);
        Path path = directory.resolve("backup.manifest");

        codec.writeAtomic(path, signed);
        RecoveryBackupManifest decoded = codec.read(path);
        codec.verify(decoded, identity);

        assertEquals(signed, decoded);
        assertTrue(Files.isRegularFile(path));
        RecoveryBackupManifest tampered = new RecoveryBackupManifest(
                decoded.formatVersion(), decoded.backupId(), decoded.sourceInstanceId(),
                decoded.tableId(), decoded.spaceId(), decoded.sourceDictionaryVersion(),
                decoded.fileIdentity(), "c".repeat(64), decoded.cleanLsn(),
                decoded.fileLengthBytes(), decoded.fileSha256(), decoded.hmacSha256());
        assertThrows(RecoveryBackupException.class,
                () -> codec.verify(tampered, identity));
    }

    /** 构造一份字段完整但尚未签名的 manifest。 */
    private static RecoveryBackupManifest manifest(
            RecoveryIdentity identity, String definitionHash, String fileHash) {
        SpaceId spaceId = SpaceId.of(1024);
        return new RecoveryBackupManifest(
                RecoveryBackupManifest.CURRENT_FORMAT, UUID.randomUUID(),
                identity.instanceId(), TableId.of(11), spaceId,
                DictionaryVersion.of(19), new TablespaceFileIdentity(
                spaceId, PageSize.ofBytes(16 * 1024), TablespaceType.GENERAL,
                80046, 3), definitionHash, Lsn.of(81), 16 * 1024L,
                fileHash, "");
    }
}
