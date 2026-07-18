package cn.zhangyis.db.storage.fil.io;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * 表空间 DISCARD/IMPORT 的受控文件搬运协作者。
 *
 * <p>它只负责文件系统动作，不解释 page0、DD 或 BufferPool。DISCARD 要求同文件系统原子移动；
 * IMPORT 先复制并 force 临时文件，再原子替换目标，source 始终保留给恢复和重试。</p>
 */
public final class TablespaceFileTransfer {

    /** 将已关闭的 canonical 文件原子移入 quarantine。
     *
     * @param source 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param quarantine 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @throws TablespaceFileTransferException 表空间生命周期、文件身份或导入导出协作失败时抛出；调用方应关闭准入并保留文件证据
     */
    public void discard(Path source, Path quarantine) {
        requirePaths(source, quarantine);
        try {
            Files.createDirectories(quarantine.toAbsolutePath().normalize().getParent());
            Files.move(source.toAbsolutePath().normalize(), quarantine.toAbsolutePath().normalize(),
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException failure) {
            throw new TablespaceFileTransferException("atomically move tablespace to discarded path failed: " + source,
                    failure);
        }
    }

    /** 复制外部 source 到 canonical target，并以原子移动发布目标文件。
     *
     * @param source 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param target 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param temporary 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @throws IOException 底层文件读写失败时抛出；调用方不得据此发布持久化成功状态
     * @throws TablespaceFileTransferException 表空间生命周期、文件身份或导入导出协作失败时抛出；调用方应关闭准入并保留文件证据
     */
    public void importFile(Path source, Path target, Path temporary) {
        requirePaths(source, target);
        requirePaths(source, temporary);
        try {
            Path normalizedTarget = target.toAbsolutePath().normalize();
            Files.createDirectories(normalizedTarget.getParent());
            Files.deleteIfExists(temporary.toAbsolutePath().normalize());
            try (FileChannel input = FileChannel.open(source.toAbsolutePath().normalize(), StandardOpenOption.READ);
                 FileChannel output = FileChannel.open(temporary.toAbsolutePath().normalize(), StandardOpenOption.CREATE_NEW,
                         StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                long position = 0;
                long size = input.size();
                while (position < size) {
                    long transferred = input.transferTo(position, size - position, output);
                    if (transferred <= 0) {
                        ByteBuffer buffer = ByteBuffer.allocate(8192);
                        input.position(position);
                        int read = input.read(buffer);
                        if (read < 0) {
                            throw new IOException("unexpected EOF while copying tablespace source");
                        }
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            output.write(buffer);
                        }
                        transferred = read;
                    }
                    position += transferred;
                }
                output.force(true);
            }
            Files.move(temporary.toAbsolutePath().normalize(), normalizedTarget,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException failure) {
            throw new TablespaceFileTransferException("atomically import tablespace file failed: " + source, failure);
        } finally {
            try {
                Files.deleteIfExists(temporary.toAbsolutePath().normalize());
            } catch (IOException ignored) {
                // recovery 会根据 IMPORT_PENDING 和临时文件 identity 再次清理；不能覆盖原始失败原因。
            }
        }
    }

    private static void requirePaths(Path source, Path target) {
        if (source == null || target == null) {
            throw new DatabaseValidationException("tablespace transfer paths must not be null");
        }
        if (source.toAbsolutePath().normalize().equals(target.toAbsolutePath().normalize())) {
            throw new DatabaseValidationException("tablespace transfer source and target must differ");
        }
    }

    /** 文件移动或复制无法安全完成时抛出。 */
    public static final class TablespaceFileTransferException extends DatabaseRuntimeException {
        /**
         * 创建 {@code TablespaceFileTransferException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
         *
         * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
         * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
         */
        public TablespaceFileTransferException(String message, Throwable cause) { super(message, cause); }
    }
}
