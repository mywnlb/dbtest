package cn.zhangyis.db.storage.fil.online;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexBuildId;
import cn.zhangyis.db.storage.api.ddl.online.OnlineIndexLogHeader;
import cn.zhangyis.db.storage.engine.OnlineDdlConfig;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Online row-log 的受控路径工厂。调用方只能提供正 build id，不能把 DDL marker 中的任意路径直接交给
 * FileChannel；open/delete 会重新推导并精确比较规范路径。
 */
public final class OnlineIndexChangeLogFiles {

    /** EngineConfig 派生的固定规范目录 {@code <baseDir>/online-ddl}。 */
    private final Path directory;
    /** 单文件容量、terminal reserve 与恢复重开边界。 */
    private final OnlineDdlConfig config;

    /**
     * 构造受控文件工厂；目录在首次 create 时创建，符号链接目录永远拒绝。
     *
     * @param directory EngineConfig.onlineDdlDirectory 的绝对或可规范化路径
     * @param config 当前实例的 immutable Online DDL 配置
     */
    public OnlineIndexChangeLogFiles(Path directory, OnlineDdlConfig config) {
        if (directory == null || config == null) {
            throw new DatabaseValidationException("online index log directory/config must not be null");
        }
        this.directory = directory.toAbsolutePath().normalize();
        this.config = config;
    }

    /**
     * @param buildId DDL control 已分配的正 build identity
     * @return 只由固定目录与 build id 推导的规范文件路径；不检查文件是否存在
     */
    public Path pathFor(OnlineIndexBuildId buildId) {
        requireBuild(buildId);
        return directory.resolve("online-index-" + buildId.value() + ".log").normalize();
    }

    /**
     * 创建 header 已 force 的全新 row log；已有同 id 文件时拒绝覆盖。
     *
     * @param header 已冻结 manifest 和 table/index/version owner 的 immutable header
     * @return 调用方拥有且必须关闭的 OPEN 日志实例
     * @throws DatabaseRuntimeException 目录、CREATE_NEW、完整写或 header force 失败时抛出并保留根因
     */
    public FileOnlineIndexChangeLog create(OnlineIndexLogHeader header) {
        if (header == null) {
            throw new DatabaseValidationException("online index log header must not be null");
        }
        requireControlledDirectory();
        return FileOnlineIndexChangeLog.create(pathFor(header.buildId()), header,
                config.maxRowLogBytes(), config.abortReserveBytes());
    }

    /**
     * 从 DDL marker 恢复日志；marker 路径必须与当前 baseDir/build id 推导值精确相等。
     *
     * @param buildId marker 的稳定 DDL identity
     * @param markerPath marker auxiliaryPath 的规范文件名
     * @return 完成尾截断与 frame 校验的开放日志
     * @throws DatabaseRuntimeException 路径越界、文件缺失、损坏或 I/O 失败时抛出；恢复调用方必须 fail-closed
     */
    public FileOnlineIndexChangeLog open(OnlineIndexBuildId buildId, Path markerPath) {
        Path expected = requireExactPath(buildId, markerPath);
        requireControlledDirectory();
        return FileOnlineIndexChangeLog.open(expected,
                config.maxRowLogBytes(), config.abortReserveBytes());
    }

    /**
     * terminal marker/物理 cleanup 完成后精确删除单个 build 文件；文件不存在视为幂等完成。
     *
     * @param buildId 已进入 COMMITTED/ROLLED_BACK 的正 build identity
     * @param markerPath 同一 DDL marker 冻结的 auxiliary path，必须等于工厂推导路径
     * @throws DatabaseRuntimeException 路径不一致或精确删除失败时抛出，terminal marker 保持恢复重试依据
     */
    public void delete(OnlineIndexBuildId buildId, Path markerPath) {
        Path expected = requireExactPath(buildId, markerPath);
        requireControlledDirectory();
        try {
            Files.deleteIfExists(expected);
        } catch (IOException error) {
            throw new DatabaseRuntimeException("delete online index row log failed: " + expected, error);
        }
    }

    /**
     * 枚举受控目录中 exact 命名的 build identity，供启动恢复清理“manifest 已 force、marker 尚未写入”的孤儿。
     * 本方法不打开或解释日志内容；目录中的其它文件不属于本仓储，保持不动。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>建立并验证固定 online-ddl 目录，随后只枚举受控前后缀匹配项。</li>
     *     <li>逐项拒绝符号链接、非普通文件、非正十进制或非规范名称，最终按 build id 排序返回。</li>
     * </ol>
     *
     * @return 按 build id 升序排列的现有日志 identity
     * @throws DatabaseRuntimeException 目录扫描失败或 exact 前后缀内不是正十进制 identity 时抛出并阻止宽泛清理
     */
    public List<OnlineIndexBuildId> existingBuildIds() {
        // 1. 所有扫描都锚定构造时冻结的受控目录，不接受调用方路径或递归枚举。
        requireControlledDirectory();
        List<OnlineIndexBuildId> result = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                directory, "online-index-*.log")) {
            // 2. 目录项必须能往返到 pathFor(buildId)；异常项会阻止宽泛 orphan cleanup。
            for (Path file : files) {
                if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)
                        || Files.isSymbolicLink(file)) {
                    throw new DatabaseValidationException(
                            "online index log entry must be a regular non-symlink file: " + file);
                }
                String name = file.getFileName().toString();
                String encodedId = name.substring("online-index-".length(),
                        name.length() - ".log".length());
                try {
                    OnlineIndexBuildId buildId = OnlineIndexBuildId.of(Long.parseLong(encodedId));
                    if (!pathFor(buildId).equals(file.toAbsolutePath().normalize())) {
                        throw new DatabaseValidationException(
                                "online index log filename is not canonical: " + file);
                    }
                    result.add(buildId);
                } catch (NumberFormatException invalidId) {
                    throw new DatabaseValidationException(
                            "online index log filename has invalid build id: " + file, invalidId);
                }
            }
        } catch (IOException error) {
            throw new DatabaseRuntimeException(
                    "scan online index log directory failed: " + directory, error);
        }
        result.sort(Comparator.naturalOrder());
        return List.copyOf(result);
    }

    private Path requireExactPath(OnlineIndexBuildId buildId, Path markerPath) {
        if (markerPath == null) {
            throw new DatabaseValidationException("online index marker path must not be null");
        }
        Path expected = pathFor(buildId);
        Path actual = markerPath.toAbsolutePath().normalize();
        if (!expected.equals(actual)) {
            throw new DatabaseValidationException(
                    "online index marker path escapes build-owned file: expected=" + expected
                            + " actual=" + actual);
        }
        return expected;
    }

    private void requireControlledDirectory() {
        try {
            Files.createDirectories(directory);
            if (Files.isSymbolicLink(directory)
                    || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new DatabaseValidationException(
                        "online index log directory must be a non-symlink directory: " + directory);
            }
        } catch (IOException error) {
            throw new DatabaseRuntimeException(
                    "prepare online index log directory failed: " + directory, error);
        }
    }

    private static void requireBuild(OnlineIndexBuildId buildId) {
        if (buildId == null) {
            throw new DatabaseValidationException("online index build id must not be null");
        }
    }
}
