package cn.zhangyis.db.storage.fsp.header;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 页 0 自增扩展的已校验快照。
 *
 * @param format 持久格式；当前新表固定为 1
 * @param highWater 已消耗最大值的 unsigned-long 原始位
 * @param active DD 声明该表含自增列时为 true
 */
public record AutoIncrementHeader(int format, long highWater, boolean active) {
    /** 当前持久格式。 */
    public static final int CURRENT_FORMAT = 1;

    public AutoIncrementHeader {
        if (format != CURRENT_FORMAT) {
            throw new DatabaseValidationException(
                    "unsupported auto-increment header format: " + format);
        }
    }
}
