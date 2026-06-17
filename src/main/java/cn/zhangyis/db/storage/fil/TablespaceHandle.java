package cn.zhangyis.db.storage.fil;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 运行时已打开表空间句柄。首版只持有 metadata 快照，后续会挂接生命周期锁和 DataFileHandle。
 *
 * @param tablespace 当前运行时 metadata 快照；后续文件句柄、生命周期锁会围绕该快照建立访问边界。
 */
public record TablespaceHandle(Tablespace tablespace) {

    public TablespaceHandle {
        if (tablespace == null) {
            throw new DatabaseValidationException("tablespace handle snapshot must not be null");
        }
    }
}
