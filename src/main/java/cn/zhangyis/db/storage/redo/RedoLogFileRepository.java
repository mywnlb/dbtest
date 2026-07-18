package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.Lsn;

import java.nio.file.Path;
import java.util.List;

/**
 * Redo 物理文件仓储角色：向 redo writer/flusher/recovery 暴露稳定的「追加批次 / fsync / 顺序扫描」能力，
 * 屏蔽底层是单文件还是文件环。
 *
 * <p>两种实现共用 {@link RedoLogBlockCodec} / {@link RedoLogBlockScanner}，内部都嵌套
 * {@link RedoBatchFrameCodec}；它们共享 block/batch 语义，但物理容器不同（ring 文件额外带 v2 header），
 * 不把单个 ring 文件伪装成 standalone 单文件读取：
 * <ul>
 *   <li>{@link SingleFileRedoLogRepository}：无容器 header 的 append-only LogBlock 文件，保留给 opt-out/测试；</li>
 *   <li>{@link RotatingRedoLogRepository}：v2 header 文件环，轮转 + checkpoint 回收，长跑下占用有界。</li>
 * </ul>
 *
 * <p>本接口只承载两种实现都具备的公共能力；文件环特有的回收边界推进等在其具体类上，避免接口被单一实现的细节污染。
 */
public interface RedoLogFileRepository extends AutoCloseable {

    /** 当前 repository 写入/扫描的 redo data 持久格式版本。 */
    default int formatVersion() {
        return RedoLogBlockCodec.FORMAT_VERSION;
    }

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

    /**
     * 返回一次恢复扫描及其 retained 逻辑边界。默认实现适用于从 LSN 0 开始的单文件和测试仓储；
     * 文件环覆盖此方法，以便在“仅剩 torn batch”时仍保留非零 header startLsn。
     *
     * @return {@code readRecoveryScan} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    default RedoRecoveryScan readRecoveryScan() {
        List<RedoLogBatch> batches = readBatches();
        if (batches.isEmpty()) {
            return new RedoRecoveryScan(List.of(), Lsn.of(0), Lsn.of(0));
        }
        return new RedoRecoveryScan(batches, batches.getFirst().range().start(),
                batches.getLast().range().end());
    }

    /**
     * 释放本方法拥有的Redo/WAL资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override
    void close();

    /**
     * 打开或创建 standalone LogBlock v1 单文件仓储。保留静态工厂，使既有
     * {@code RedoLogFileRepository.open(path)} 调用点继续得到单文件实现。
     *
     * @param path redo 文件路径。
     * @return 单文件仓储。
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    static RedoLogFileRepository open(Path path) {
        if (path == null) {
            throw new DatabaseValidationException("redo log path must not be null");
        }
        return SingleFileRedoLogRepository.open(path);
    }

    /** 只读打开已存在单 redo 文件；不创建或修复恢复输入。
     *
     * @param path 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @return {@code openReadOnly} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    static RedoLogFileRepository openReadOnly(Path path) {
        return SingleFileRedoLogRepository.openReadOnly(path);
    }

    /**
     * 打开或创建带 v2 header 的 redo 文件环仓储。
     *
     * @param dir                  redo 目录。
     * @param fileCount            文件数（≥1）。
     * @param fileBytes 单文件 LogBlock 区容量（不含文件头，至少 512B 且按 512B 对齐）。
     * @return 文件环仓储。
     */
    static RotatingRedoLogRepository openRing(Path dir, int fileCount, long fileBytes) {
        return RotatingRedoLogRepository.open(dir, fileCount, fileBytes);
    }

    /** 只读打开完整 existing redo ring；不创建缺失目录/文件或修复 torn tail。
     *
     * @param dir 受控目录内的规范化文件路径；不得为 {@code null}，也不得逃逸所属表空间或日志目录
     * @param fileCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
     * @param fileBytes 待读取、校验或写入的字节数据；不得为 {@code null}，调用期间由调用方保有所有权且不得越过格式边界
     * @return {@code openRingReadOnly} 构造或定位的 redo 日志对象；成功时不为 {@code null}，LSN、预算和批次边界满足 WAL 顺序
     */
    static RotatingRedoLogRepository openRingReadOnly(Path dir, int fileCount, long fileBytes) {
        return RotatingRedoLogRepository.openReadOnly(dir, fileCount, fileBytes);
    }
}
