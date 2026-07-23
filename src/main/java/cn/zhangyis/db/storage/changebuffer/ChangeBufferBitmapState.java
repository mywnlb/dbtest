package cn.zhangyis.db.storage.changebuffer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * 一个目标页在 IBUF bitmap 中的 4-bit 逻辑投影。
 *
 * @param freeSpaceClass 低两位的保守连续空闲等级，范围 0..3
 * @param buffered 是否存在至少一条尚未 consume 的 Change Buffer record
 * @param changeBufferInternal 目标页是否属于 Change Buffer 内部结构；为真时永远不得再次缓冲
 */
public record ChangeBufferBitmapState(int freeSpaceClass, boolean buffered,
                                      boolean changeBufferInternal) {

    public ChangeBufferBitmapState {
        if (freeSpaceClass < 0 || freeSpaceClass > 3) {
            throw new DatabaseValidationException("change buffer free-space class must be in [0,3]: "
                    + freeSpaceClass);
        }
    }
}
