package cn.zhangyis.db.engine.recovery;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

/**
 * 普通引擎与离线 catalog recovery 共用的跨进程实例独占锁。
 *
 * <p>获取使用 {@link FileChannel#tryLock()} 和 monotonic deadline；每轮最多停顿 10ms，并响应中断。
 * 它不进入事务 wait-for graph，也不能在持有 MDL/page latch 时获取。</p>
 */
public final class DatabaseInstanceFileLock implements AutoCloseable {

    /** 有界重试的最大停顿，避免长 park 隐藏中断或 deadline。 */
    private static final long MAX_PARK_NANOS = Duration.ofMillis(10).toNanos();

    /** 锁文件 channel，必须晚于 FileLock 关闭。 */
    private final FileChannel channel;
    /** JVM/OS 持有的独占区域锁。 */
    private final FileLock lock;
    /** 诊断路径。 */
    private final Path path;

    private DatabaseInstanceFileLock(Path path, FileChannel channel, FileLock lock) {
        this.path = path;
        this.channel = channel;
        this.lock = lock;
    }

    /**
     * 在正 timeout 内取得 `${baseDir}/mysql.instance.lock` 独占锁。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>验证路径与 timeout，创建实例根目录并打开固定 lock file channel。</li>
     *     <li>循环 tryLock；同 JVM overlap 与其它进程持有都解释为暂时 busy。</li>
     *     <li>每轮检查中断和剩余 deadline，以短 park 避免忙等且不无界阻塞。</li>
     *     <li>成功返回 ownership guard；失败关闭 channel，不遗留本进程锁资源。</li>
     * </ol>
     *
     * @param baseDir 实例根目录
     * @param timeout 获取锁的正总等待上限
     * @return 持有跨进程独占锁的 guard
     * @throws CatalogRecoveryBusyException timeout、中断或文件锁 IO 失败时抛出
     */
    public static DatabaseInstanceFileLock acquire(Path baseDir, Duration timeout) {
        // 1. 在创建 lock file 前拒绝非法调用。
        if (baseDir == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("database instance lock base directory/timeout is invalid");
        }
        Path path = baseDir.toAbsolutePath().normalize().resolve("mysql.instance.lock");
        FileChannel channel = null;
        try {
            Files.createDirectories(path.getParent());
            channel = FileChannel.open(
                    path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            long deadline = deadline(timeout);

            // 2. tryLock 不做 JVM 内无界阻塞；overlap 与 OS busy 使用同一受控重试。
            while (true) {
                FileLock lock = null;
                try {
                    lock = channel.tryLock();
                } catch (OverlappingFileLockException busy) {
                    // 同 JVM 已有 DatabaseEngine/recovery guard，按 busy 等待而不是泄漏 unchecked 异常。
                }
                if (lock != null) {
                    return new DatabaseInstanceFileLock(path, channel, lock);
                }

                // 3. 中断不清除标记；timeout 由 monotonic clock 判定。
                if (Thread.currentThread().isInterrupted()) {
                    throw new CatalogRecoveryBusyException("database instance lock wait interrupted: " + path);
                }
                long remaining = deadline == Long.MAX_VALUE
                        ? Long.MAX_VALUE : deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new CatalogRecoveryBusyException("database instance lock wait timed out: " + path);
                }
                LockSupport.parkNanos(Math.min(remaining, MAX_PARK_NANOS));
            }
        } catch (IOException failure) {
            CatalogRecoveryBusyException wrapped =
                    new CatalogRecoveryBusyException("acquire database instance lock failed: " + path, failure);
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    wrapped.addSuppressed(closeFailure);
                }
            }
            throw wrapped;
        } catch (RuntimeException failure) {
            // 4. 未返回 guard 时释放 channel；已成功返回后 ownership 已转移，不进入本分支。
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    /**
     * 先释放 OS lock，再关闭 channel；关闭失败保留第一个异常并附加 suppressed。
     */
    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            lock.close();
        } catch (IOException closeFailure) {
            failure = new CatalogRecoveryException("release database instance lock failed: " + path, closeFailure);
        }
        try {
            channel.close();
        } catch (IOException closeFailure) {
            if (failure == null) {
                failure = new CatalogRecoveryException(
                        "close database instance lock channel failed: " + path, closeFailure);
            } else {
                failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** 计算饱和 monotonic deadline。 */
    private static long deadline(Duration timeout) {
        try {
            long now = System.nanoTime();
            long target = now + timeout.toNanos();
            return target < 0 && now > 0 ? Long.MAX_VALUE : target;
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
