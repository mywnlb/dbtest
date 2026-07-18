package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 目标 {@code SpaceId} 没有可定位表空间的异常。
 *
 * <p>该异常表示运行期 registry/loader 找不到元数据，或表空间已经进入
 * {@code DISCARDED}、不能再作为现存对象访问；它不等同于底层路径一定不存在。
 * 普通 IO 收到该异常后不得猜测文件路径继续读写，调用方可根据上层字典状态重新执行
 * discovery/open，DDL 清理路径则可把“已经不存在”作为自己的幂等条件处理。</p>
 */
public class TablespaceNotFoundException extends DatabaseRuntimeException {

    /**
     * 创建表空间不存在异常。
     *
     * @param message 应包含目标 SpaceId 及 registry/discard 状态的定位信息
     */
    public TablespaceNotFoundException(String message) {
        super(message);
    }

    /**
     * 创建保留根因的表空间不存在异常。
     *
     * @param message 应包含目标 SpaceId 及定位阶段的诊断信息
     * @param cause 元数据装载或物理路径解析失败的原始原因；包装时不得丢弃
     */
    public TablespaceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
