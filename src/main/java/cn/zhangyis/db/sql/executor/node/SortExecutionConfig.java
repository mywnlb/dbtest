package cn.zhangyis.db.sql.executor.node;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.executor.exception.SqlExecutionException;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQL 排序的实例级资源边界。
 *
 * @param temporaryRoot 受控临时根；每个 SortNode 在其下创建独占 statement 目录
 * @param rowsPerRun 分堆归并时每个内存 run 的最大行数
 * @param memoryBudgetBytes 单语句 retained row snapshot 与序列化 frame 的最大内存预算
 * @param temporaryFileBudgetBytes 单语句全部 run（含多轮归并峰值）的最大临时文件字节数
 * @param mergeFanIn 每轮归并最多同时打开的 run 数
 */
public record SortExecutionConfig(
        Path temporaryRoot,
        int rowsPerRun,
        long memoryBudgetBytes,
        long temporaryFileBudgetBytes,
        int mergeFanIn) {

    /** 默认单语句排序内存 4 MiB。 */
    private static final long DEFAULT_MEMORY_BYTES = 4L * 1024 * 1024;
    /** 默认单语句临时文件峰值 256 MiB。 */
    private static final long DEFAULT_TEMPORARY_BYTES =
            256L * 1024 * 1024;
    /** 默认 16 路归并，限制同时打开的文件描述符。 */
    private static final int DEFAULT_MERGE_FAN_IN = 16;
    /** 启动清理与 SortNode statement 目录创建共享显式锁，禁止目录枚举和创建交错。 */
    static final ReentrantLock TEMP_ROOT_LOCK =
            new ReentrantLock();

    /**
     * 兼容按行数强制分堆的测试与旧调用点，其余资源边界使用生产默认值。
     *
     * @param temporaryRoot 受控临时根
     * @param rowsPerRun 单 run 最大行数
     */
    public SortExecutionConfig(
            Path temporaryRoot, int rowsPerRun) {
        this(temporaryRoot, rowsPerRun,
                DEFAULT_MEMORY_BYTES,
                DEFAULT_TEMPORARY_BYTES,
                DEFAULT_MERGE_FAN_IN);
    }

    public SortExecutionConfig {
        if (temporaryRoot == null || rowsPerRun <= 0
                || memoryBudgetBytes <= 0
                || temporaryFileBudgetBytes <= 0
                || temporaryFileBudgetBytes < memoryBudgetBytes
                || mergeFanIn < 2 || mergeFanIn > 128) {
            throw new DatabaseValidationException(
                    "sort temporary root/resource bounds are invalid");
        }
        temporaryRoot = temporaryRoot.toAbsolutePath().normalize();
    }

    /**
     * 使用 JVM 临时根和教学型固定 run 行数。
     *
     * @return 可直接用于生产组合根的不可变配置
     */
    public static SortExecutionConfig defaults() {
        return new SortExecutionConfig(
                Path.of(System.getProperty("java.io.tmpdir"), "minimysql-sort"),
                65_536, DEFAULT_MEMORY_BYTES,
                DEFAULT_TEMPORARY_BYTES,
                DEFAULT_MERGE_FAN_IN);
    }

    /**
     * 从数据库实例根派生受控 SQL 临时目录，避免多个实例共享 JVM 全局 temp 命名空间。
     *
     * @param instanceRoot 已由 DatabaseEngine 取得独占锁的实例根
     * @return 使用生产默认预算、但路径隔离到当前实例的配置
     */
    public static SortExecutionConfig forInstance(
            Path instanceRoot) {
        if (instanceRoot == null) {
            throw new DatabaseValidationException(
                    "sort instance root must not be null");
        }
        return new SortExecutionConfig(
                instanceRoot.toAbsolutePath().normalize()
                        .resolve("tmp").resolve("sql-sort"),
                65_536, DEFAULT_MEMORY_BYTES,
                DEFAULT_TEMPORARY_BYTES,
                DEFAULT_MERGE_FAN_IN);
    }

    /**
     * 创建并验证临时根的真实路径，防止通过根自身或祖先符号链接把 run IO 引到实例目录之外。
     *
     * @throws IOException 创建目录或解析真实路径失败时抛出，由调用方包装为 SQL 执行异常
     * @throws SqlExecutionException 目录是符号链接、非目录，或解析后的真实路径不等于配置路径时抛出
     */
    void ensureControlledRoot() throws IOException {
        Files.createDirectories(temporaryRoot);
        if (Files.isSymbolicLink(temporaryRoot)
                || !Files.isDirectory(
                temporaryRoot, LinkOption.NOFOLLOW_LINKS)
                || !temporaryRoot.toRealPath().equals(temporaryRoot)) {
            throw new SqlExecutionException(
                    "SQL sort temporary root is not an exact controlled directory: "
                            + temporaryRoot);
        }
    }

    /**
     * 在 DatabaseEngine 已取得实例文件锁、尚未发布 Session 前清理上次崩溃遗留的受控 statement 目录。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>锁住当前 JVM 的 sort temp root 协调点并创建 exact 配置根，不扫描实例其它目录。</li>
     *     <li>只接受根的直接子目录 {@code statement-*}，拒绝符号链接、嵌套目录和未知文件名。</li>
     *     <li>逐个删除 {@code run-*.bin} 后删除空 statement 目录；失败阻止数据库 OPEN。</li>
     * </ol>
     *
     * @throws SqlExecutionException 遗留项逃逸命名规则、包含未知内容或受控删除失败时抛出
     */
    public void cleanupStaleStatements() {
        // 1. 调用点已持有跨进程 instance lock；这里的显式锁只协调同 JVM 目录动作。
        TEMP_ROOT_LOCK.lock();
        try {
            ensureControlledRoot();
            try (DirectoryStream<Path> statements =
                         Files.newDirectoryStream(
                                 temporaryRoot, "statement-*")) {
                for (Path statement : statements) {
                    Path normalized = statement.toAbsolutePath()
                            .normalize();
                    // 2. 不跟随链接，也不递归删除未知结构；可疑遗留必须由管理员诊断。
                    if (!normalized.getParent().equals(temporaryRoot)
                            || Files.isSymbolicLink(normalized)
                            || !Files.isDirectory(normalized)) {
                        throw new SqlExecutionException(
                                "unsafe stale SQL sort statement entry: "
                                        + normalized);
                    }
                    try (DirectoryStream<Path> files =
                                 Files.newDirectoryStream(normalized)) {
                        for (Path file : files) {
                            Path candidate = file.toAbsolutePath()
                                    .normalize();
                            String name = candidate.getFileName()
                                    .toString();
                            if (!candidate.getParent()
                                    .equals(normalized)
                                    || Files.isSymbolicLink(candidate)
                                    || !Files.isRegularFile(candidate)
                                    || !name.startsWith("run-")
                                    || !name.endsWith(".bin")) {
                                throw new SqlExecutionException(
                                        "unknown stale SQL sort entry: "
                                                + candidate);
                            }
                            Files.delete(candidate);
                        }
                    }
                    // 3. statement 目录此时必须为空；删除失败不能被普通 Session 静默忽略。
                    Files.delete(normalized);
                }
            }
        } catch (IOException failure) {
            throw new SqlExecutionException(
                    "cleanup stale SQL sort files failed: "
                            + temporaryRoot, failure);
        } finally {
            TEMP_ROOT_LOCK.unlock();
        }
    }
}
