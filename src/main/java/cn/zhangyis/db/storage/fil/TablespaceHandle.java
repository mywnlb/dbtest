package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 运行时已打开表空间的逻辑句柄。当前只持有不可变 metadata 快照，用于把 Registry 返回的运行时视图
 * 与 loader 产出的权威 metadata 区分开；物理文件生命周期由 PageStore/DataFileHandle 负责，不挂在本对象上。
 *
 * @param tablespace 当前运行时 metadata 快照；状态变更会发布新快照，调用方不能把旧快照当作最新权威状态。
 */
public record TablespaceHandle(Tablespace tablespace) {

    public TablespaceHandle {
        if (tablespace == null) {
            throw new DatabaseValidationException("tablespace handle snapshot must not be null");
        }
    }
}
