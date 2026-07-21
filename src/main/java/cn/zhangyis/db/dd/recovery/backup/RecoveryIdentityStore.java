package cn.zhangyis.db.dd.recovery.backup;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * 懒创建的实例恢复身份仓储。普通启动不会读取或创建该文件；只有 backup/import 操作才调用本对象。
 * 文件包含 magic/version/UUID/256-bit key/CRC，并通过同目录临时文件 force 后原子发布。
 */
public final class RecoveryIdentityStore {

    private static final int MAGIC = 0x52494431;
    private static final int VERSION = 1;
    private static final int KEY_BYTES = 32;
    private static final int FILE_BYTES = Integer.BYTES * 3 + Long.BYTES * 2 + KEY_BYTES;

    /** 固定 identity 文件路径。 */
    private final Path path;
    /** 同一组合根内串行首次创建和读取，避免不同表并发备份生成两个 key。 */
    private final ReentrantLock lock = new ReentrantLock();
    /** 只在首次创建时使用的强随机源。 */
    private final SecureRandom random = new SecureRandom();

    /** @param baseDirectory 数据库实例根目录 */
    public RecoveryIdentityStore(Path baseDirectory) {
        if (baseDirectory == null) {
            throw new DatabaseValidationException(
                    "recovery identity base directory must not be null");
        }
        this.path = baseDirectory.toAbsolutePath().normalize()
                .resolve("mysql.recovery.identity");
    }

    /**
     * 读取已有身份；文件缺失时生成一次并原子发布。损坏文件永远不会被自动覆盖。
     *
     * @return 已通过 CRC 校验的实例身份
     * @throws RecoveryIdentityException 创建、force、读取或格式校验失败时抛出
     */
    public RecoveryIdentity openOrCreate() {
        lock.lock();
        try {
            if (Files.exists(path)) {
                return read();
            }
            byte[] key = new byte[KEY_BYTES];
            random.nextBytes(key);
            RecoveryIdentity identity = new RecoveryIdentity(UUID.randomUUID(), key);
            writeNew(identity);
            return identity;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 只读取已有身份；IMPORT 不允许因文件丢失而生成新 key，否则旧 manifest 会失去可验证来源。
     *
     * @return 已校验身份
     * @throws RecoveryIdentityException 文件缺失、损坏或不可读时抛出
     */
    public RecoveryIdentity openExisting() {
        lock.lock();
        try {
            if (!Files.isRegularFile(path) || Files.isSymbolicLink(path)) {
                throw new RecoveryIdentityException(
                        "recovery identity is missing or unsafe: " + path);
            }
            return read();
        } finally {
            lock.unlock();
        }
    }

    /** @return 固定 identity 文件路径，只用于诊断与测试。 */
    public Path path() {
        return path;
    }

    /** 编码、force 并原子发布；manifest 永远不会包含这里的 key。 */
    private void writeNew(RecoveryIdentity identity) {
        Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
        byte[] bytes = encode(identity);
        try {
            Files.createDirectories(path.getParent());
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
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException failure) {
            throw new RecoveryIdentityException(
                    "create recovery identity failed: " + path, failure);
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException ignored) {
                // 临时文件不具备合法 magic/CRC 发布路径，后续操作仍以 final identity 为唯一真相。
            }
        }
    }

    /** 读取精确长度并交叉校验 magic/version/CRC。 */
    private RecoveryIdentity read() {
        try {
            byte[] bytes = Files.readAllBytes(path);
            if (bytes.length != FILE_BYTES) {
                throw new RecoveryIdentityException(
                        "recovery identity length mismatch: " + path);
            }
            ByteBuffer input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
            if (input.getInt() != MAGIC || input.getInt() != VERSION) {
                throw new RecoveryIdentityException(
                        "recovery identity magic/version mismatch: " + path);
            }
            UUID instanceId = new UUID(input.getLong(), input.getLong());
            byte[] key = new byte[KEY_BYTES];
            input.get(key);
            int storedCrc = input.getInt();
            CRC32 crc = new CRC32();
            crc.update(bytes, 0, bytes.length - Integer.BYTES);
            if (storedCrc != (int) crc.getValue()) {
                throw new RecoveryIdentityException(
                        "recovery identity CRC mismatch: " + path);
            }
            return new RecoveryIdentity(instanceId, key);
        } catch (IOException failure) {
            throw new RecoveryIdentityException(
                    "read recovery identity failed: " + path, failure);
        }
    }

    /** 生成固定大端格式并在最后四字节写覆盖全部前缀的 CRC32。 */
    private static byte[] encode(RecoveryIdentity identity) {
        byte[] bytes = new byte[FILE_BYTES];
        ByteBuffer output = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(VERSION)
                .putLong(identity.instanceId().getMostSignificantBits())
                .putLong(identity.instanceId().getLeastSignificantBits())
                .put(identity.hmacKey());
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        output.putInt((int) crc.getValue());
        return bytes;
    }
}
