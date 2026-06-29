package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

import java.nio.file.Path;
import java.util.List;

/**
 * Redo 物理文件仓储角色：向 redo writer/flusher/recovery 暴露稳定的「追加批次 / fsync / 顺序扫描」能力，
 * 屏蔽底层是单文件还是文件环。
 *
 * <p>两种实现共用 {@link RedoBatchFrameCodec} 的帧格式，因此一种实现写出的 redo 可被另一种实现在 crash recovery 时读回：
 * <ul>
 *   <li>{@link SingleFileRedoLogRepository}：R1 单 append-only 文件，简单但无界增长，保留给现有测试与最小场景；</li>
 *   <li>{@link RotatingRedoLogRepository}：0.18 文件环，轮转 + checkpoint 回收，长跑下占用有界。</li>
 * </ul>
 *
 * <p>本接口只承载两种实现都具备的公共能力；文件环特有的回收边界推进等在其具体类上，避免接口被单一实现的细节污染。
 */
public interface RedoLogFileRepository extends AutoCloseable {

    /**
     * 追加一个完整 redo 批次。调用方负责按 LSN 顺序调用；实现只保证单批 bytes 原样落到底层文件。
     *
     * @param batch 待写入批次。
     */
    void append(RedoLogBatch batch);

    /** 对底层 redo 文件执行 fsync/force；成功后 writer 已写入的 LSN 才能发布为 flushedToDiskLsn。 */
    void force();

    /**
     * 按 LSN 顺序扫描出当前保留的全部完整批次；不完整尾部（torn tail）视为 crash 截断点，不进入返回列表。
     *
     * @return 按 LSN 顺序排列的完整批次。
     */
    List<RedoLogBatch> readBatches();

    @Override
    void close();

    /**
     * 打开或创建单 redo 文件仓储（R1 行为）。保留为静态工厂，使既有 {@code RedoLogFileRepository.open(path)} 调用点
     * 在接口化后仍可编译，并默认得到单文件实现。
     *
     * @param path redo 文件路径。
     * @return 单文件仓储。
     */
    static RedoLogFileRepository open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("redo log path must not be null");
        }
        return SingleFileRedoLogRepository.open(path);
    }

    /**
     * 打开或创建 redo 文件环仓储（0.18）。
     *
     * @param dir                  redo 目录。
     * @param fileCount            文件数（≥1）。
     * @param maxFrameBytesPerFile 单文件帧容量上限（不含文件头，&gt;0）。
     * @return 文件环仓储。
     */
    static RotatingRedoLogRepository openRing(Path dir, int fileCount, long maxFrameBytesPerFile) {
        return RotatingRedoLogRepository.open(dir, fileCount, maxFrameBytesPerFile);
    }
}
