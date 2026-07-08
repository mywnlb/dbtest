package cn.zhangyis.db.storage.mtr;

/**
 * MTR 本地 redo 分类。它不是持久 redo type，也不会写入 redo 文件；当前持久格式仍只有
 * {@code PAGE_INIT}/{@code PAGE_BYTES}。分类只用于约束调用方在收集物理字节 redo 时说明语义来源，
 * 方便测试、诊断以及后续逐步迁移到更细的 MLOG 命令。
 */
public enum MtrRedoCategory {

    /** 新页初始化，由 {@link MiniTransaction#newPage} 固定产生，不受普通分类 scope 影响。 */
    PAGE_INIT,

    /** 未声明语义来源的普通页字节写；这是兼容既有调用方的默认分类。 */
    PAGE_BYTES_GENERIC,

    /** Record 模块页内 insert/update/delete-mark/purge/reorganize 等操作产生的字节写。 */
    RECORD_PAGE_BYTES,

    /** B+Tree split/merge/root shrink/sibling link 等结构维护产生的字节写。 */
    BTREE_STRUCTURE_BYTES,

    /** FSP space header、XDES、segment inode 等空间管理元数据产生的字节写。 */
    FSP_METADATA_BYTES,

    /** Undo 页头、undo record、undo FIL 链维护等 undo 子系统产生的字节写。 */
    UNDO_PAGE_BYTES
}
