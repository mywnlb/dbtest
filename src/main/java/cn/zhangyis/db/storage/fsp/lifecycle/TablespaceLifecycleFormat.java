package cn.zhangyis.db.storage.fsp.lifecycle;

/** page-0 生命周期头磁盘协议常量；仓储与 raw loader 共享，避免两条读取路径漂移。 */
public final class TablespaceLifecycleFormat {

    /** ASCII "UNDO"。 */
    public static final int MAGIC = 0x554E444F;

    /** 首版固定布局版本。 */
    public static final int VERSION = 1;

    private TablespaceLifecycleFormat() {
    }
}
