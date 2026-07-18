package cn.zhangyis.db.storage.recovery;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;
import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 追加式 JSONL recovery progress 文件输出。该文件只服务启动诊断：后续恢复不会读取它来跳过阶段，
 * 因而即使文件存在历史事件，也不会改变 redo/doublewrite/undo 的幂等恢复语义。
 */
public final class FileRecoveryProgressSink implements RecoveryProgressSink {

    /** progress 文件路径；通常位于 engine baseDir 下，独立于 redo/checkpoint 控制文件。 */
    private final Path path;
    /** 串行化同一 sink 的 append + force，避免多线程诊断/恢复测试交错写半行。 */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 创建文件 sink。构造时不打开文件，实际 append 时创建父目录和文件，避免构造 StorageEngine 产生 IO 副作用。
     *
     * @param path JSONL 文件路径，不能为 null。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public FileRecoveryProgressSink(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("recovery progress file path must not be null");
        }
        this.path = path;
    }

    /**
     * 追加并强制落盘单个 progress 事件。数据流为：事件值对象 -> 手写 JSONL 编码 -> FileChannel append ->
     * force(true)。这里不持有 page latch、redo IO lock 或事务锁；若 IO 失败，抛项目运行时异常，由
     * {@link CrashRecoveryService} fail closed。
     *
     * @param event 已生成的不可变 progress 事件。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws DatabaseRuntimeException 可恢复的数据库运行期协作失败时抛出；调用方应依据当前事务状态选择回滚、重试或关闭资源
     */
    @Override
    public void append(RecoveryProgressEvent event) {
        if (event == null) {
            throw new DatabaseValidationException("recovery progress event must not be null");
        }
        byte[] bytes = (toJsonLine(event) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        lock.lock();
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
        } catch (IOException e) {
            throw new DatabaseRuntimeException("append recovery progress event failed: " + path, e);
        } finally {
            lock.unlock();
        }
    }

    private static String toJsonLine(RecoveryProgressEvent event) {
        return "{"
                + "\"sequence\":" + event.sequence()
                + ",\"mode\":\"" + event.mode() + "\""
                + ",\"stageName\":\"" + event.stageName() + "\""
                + ",\"kind\":\"" + event.kind() + "\""
                + ",\"state\":\"" + event.state() + "\""
                + ",\"recoveredToLsn\":" + event.recoveredToLsn().value()
                + ",\"detail\":\"" + escapeJson(event.detail()) + "\""
                + "}";
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        return escaped.toString();
    }
}
