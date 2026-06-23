/**
 * Undo 日志物理存储基座（T1.3a 单页 + T1.3b 多页链）：undo page/log header 格式、
 * INSERT undo record 编解码、RollPointer 寻址、跨页 undo log segment（生长/遍历/重读）、
 * 以及端口隔离的页分配。
 *
 * <p>T1.3a 提供 {@link cn.zhangyis.db.storage.undo.UndoPage}、
 * {@link cn.zhangyis.db.storage.undo.UndoPageAccess} 与
 * {@link cn.zhangyis.db.storage.undo.UndoLog}，完成单页 append →
 * {@link cn.zhangyis.db.domain.RollPointer} → read。T1.3b 新增
 * {@link cn.zhangyis.db.storage.undo.UndoSegmentHandle}、
 * {@link cn.zhangyis.db.storage.undo.UndoSpaceAllocator}、
 * {@link cn.zhangyis.db.storage.undo.UndoLogSegment} 与
 * {@link cn.zhangyis.db.storage.undo.UndoLogSegmentAccess}：跨页生长先 preflight 再分配，
 * 通过 FIL prev/next 链接 chain 页，first 页 log header 维护 LAST_PAGE_NO、LOG_RECORD_COUNT
 * 和 LOG_LAST_UNDO_NO，并支持按 first 页持久化重开。
 *
 * <p>依赖方向：undo 模块只依赖 {@link cn.zhangyis.db.storage.undo.UndoSpaceAllocator} 端口，不 import
 * storage.api 的 DiskSpaceManager 或 SegmentRef；磁盘适配器 {@code DiskSpaceUndoAllocator} 位于 storage.api，
 * 反向实现该端口。undo 页写复用 D3/D4 物理 redo（PAGE_INIT/PAGE_BYTES），本片不新增 redo 类型。
 *
 * <p>非目标（后续片）：rollback、UndoContext、rollback segment header/slot、history list、MVCC 旧版本、
 * purge、恢复期 rollback、undo 回收/truncation、并发 append、多 rseg/多 undo 表空间，以及真实
 * DB_ROLL_PTR 写入；本片聚簇记录的 DB_ROLL_PTR 仍保持 NULL。
 */
package cn.zhangyis.db.storage.undo;
