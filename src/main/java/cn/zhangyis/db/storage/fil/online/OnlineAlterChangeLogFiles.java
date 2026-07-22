package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineAlterLogHeader;
import cn.zhangyis.db.storage.api.ddl.online.OnlineDdlCaptureId;
import cn.zhangyis.db.storage.engine.OnlineDdlConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

/**
 * 通用Online ALTER journal受控路径工厂。create/open/delete均从capture id重新推导规范路径，
 * marker提供的任意路径不能直接进入FileChannel或删除操作。
 */
public final class OnlineAlterChangeLogFiles {

    /** EngineConfig冻结的`<baseDir>/online-ddl`规范目录。 */
    private final Path directory;
    /** 单文件容量与terminal reserve。 */
    private final OnlineDdlConfig config;

    /**
     * @param directory 实例专属online-ddl目录
     * @param config immutable有界资源配置
     */
    public OnlineAlterChangeLogFiles(Path directory, OnlineDdlConfig config) {
        if (directory == null || config == null) {
            throw new DatabaseValidationException(
                    "online ALTER log directory/config must not be null");
        }
        this.directory = directory.toAbsolutePath().normalize();
        this.config = config;
    }

    /** @param captureId 正operation identity；@return 纯推导的规范文件路径 */
    public Path pathFor(OnlineDdlCaptureId captureId) {
        if (captureId == null) {
            throw new DatabaseValidationException(
                    "online ALTER capture id must not be null");
        }
        return directory.resolve("online-alter-" + captureId.value() + ".log").normalize();
    }

    /**
     * @param header 已冻结owner/manifest
     * @return header已force的新journal
     */
    public FileOnlineAlterChangeLog create(OnlineAlterLogHeader header) {
        if (header == null) {
            throw new DatabaseValidationException(
                    "online ALTER log header must not be null");
        }
        requireControlledDirectory();
        return FileOnlineAlterChangeLog.create(pathFor(header.captureId()), header,
                config.maxRowLogBytes(), config.abortReserveBytes());
    }

    /**
     * @param captureId marker记录的owner
     * @param markerPath marker auxiliary path
     * @return 完成owner/frame恢复扫描的journal
     */
    public FileOnlineAlterChangeLog open(OnlineDdlCaptureId captureId, Path markerPath) {
        Path expected = requireExactPath(captureId, markerPath);
        requireControlledDirectory();
        return FileOnlineAlterChangeLog.open(expected,
                config.maxRowLogBytes(), config.abortReserveBytes());
    }

    /** exact删除terminal operation的journal；缺失视为幂等。 */
    public void delete(OnlineDdlCaptureId captureId, Path markerPath) {
        Path expected = requireExactPath(captureId, markerPath);
        requireControlledDirectory();
        try {
            Files.deleteIfExists(expected);
        } catch (IOException error) {
            throw new DatabaseRuntimeException(
                    "delete online ALTER journal failed: " + expected, error);
        }
    }

    private Path requireExactPath(OnlineDdlCaptureId captureId, Path markerPath) {
        if (markerPath == null) {
            throw new DatabaseValidationException(
                    "online ALTER marker path must not be null");
        }
        Path expected = pathFor(captureId);
        Path actual = markerPath.toAbsolutePath().normalize();
        if (!expected.equals(actual)) {
            throw new DatabaseValidationException(
                    "online ALTER marker path escapes operation-owned file: expected="
                            + expected + " actual=" + actual);
        }
        return expected;
    }

    private void requireControlledDirectory() {
        try {
            Files.createDirectories(directory);
            if (Files.isSymbolicLink(directory)
                    || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new DatabaseValidationException(
                        "online ALTER log directory must be non-symlink: " + directory);
            }
        } catch (IOException error) {
            throw new DatabaseRuntimeException(
                    "prepare online ALTER log directory failed: " + directory, error);
        }
    }
}
