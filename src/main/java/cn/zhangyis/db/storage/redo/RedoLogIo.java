package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.domain.Lsn;

/**
 * Redo 文件写入与 fsync 端口。`RedoLogManager` 通过该端口隔离“状态锁”和“阻塞 IO 锁”，
 * 测试也可注入阻塞 fsync 来验证 append 不被文件 force 长时间占住。
 */
interface RedoLogIo {

    /**
     * 写出一个 redo 批次到 OS/page cache。调用方保证按 LSN 顺序串行调用。
     *
     * @param batch 待写出的批次。
     * @return 写出后的 written LSN。
     */
    Lsn write(RedoLogBatch batch);

    /** 当前已经写入 OS/page cache、但未必 fsync 的最高 LSN。 */
    Lsn writtenToDiskLsn();

    /**
     * 对 redo 文件执行 fsync，并把 durable 边界推进到 target。
     *
     * @param target writer 已完整写出的目标 LSN。
     * @return fsync 后的 durable LSN。
     */
    Lsn flushTo(Lsn target);
}
