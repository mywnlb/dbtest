/**
 * Undo 日志物理存储基座：普通 undo page/log header v3、INSERT/UPDATE/DELETE_MARK record 编解码、
 * RollPointer 寻址、跨页 undo segment（生长/物理遍历/按指针重读）、持久 logical head，
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
 * 通过 FIL prev/next 链接 chain 页，first 页 log header 维护 LAST_PAGE_NO、LOG_RECORD_COUNT、物理
 * LOG_LAST_UNDO_NO 和 {@link cn.zhangyis.db.storage.undo.UndoLogicalHead}。1.4b 起每张页携带格式版本，
 * first 页连续 15B logical head 是 recovery/purge 的权威反向链入口；v3 在每张普通页持久化独立 log kind，
 * 并在 first page 保存 rollback-segment history 的 prev/next；
 * v1/v2/未知版本页 fail-closed。
 *
 * <p>依赖方向：undo 模块只依赖 {@link cn.zhangyis.db.storage.undo.UndoSpaceAllocator} 端口，不 import
 * storage.api 的 DiskSpaceManager 或 SegmentRef；磁盘适配器 {@code DiskSpaceUndoAllocator} 位于 storage.api，
 * 反向实现该端口。undo 页写由 PAGE_INIT、metadata/payload logical delta 与尚未替代的 PAGE_BYTES 共同保护；
 * logical head 复用 UNDO_LOG_HEADER_FIELD；page3 history base 与 first-page links 分别使用稳定的
 * RSEG_HISTORY_BASE / UNDO_HISTORY_LINK_FIELD 分类。
 *
 * <p>{@code storage.undo} 只表达物理日志，不决定事务可见性或 SQL 语义；运行期/恢复期 rollback、MVCC 与 purge
 * 编排位于 {@code storage.trx/recovery}。v3 已覆盖独立 INSERT/UPDATE log、external payload、bounded cached
 * reuse 与单 rseg 持久 history；仍未覆盖并发 multi-writer、多 rseg/多 undo 表空间、持久 free-list 和
 * MySQL/InnoDB 二进制格式兼容。
 */
package cn.zhangyis.db.storage.undo;
