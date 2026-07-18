package cn.zhangyis.db.storage.fil.exception;

import cn.zhangyis.db.common.exception.DatabaseFatalException;

/**
 * 数据文件几何结构无法建立安全页定位的致命异常。
 *
 * <p>当前由打开路径在物理文件长度不能被配置 {@code PageSize} 整除时抛出。此时无法从
 * {@code fileLength / pageSize} 得到可信页数，也不能判断尾部是 torn extension、错误配置还是外部损坏，
 * 所以不会把 channel 注册为可用句柄。该异常属于实例级 fatal 配置/物理一致性问题，调用方不得截掉
 * 余数后继续普通 IO；必须由恢复、备份还原或人工诊断决定处置。</p>
 */
public class DataFileCorruptedException extends DatabaseFatalException {

    /**
     * 使用文件几何诊断信息创建损坏异常。
     *
     * @param message 应包含路径、实际字节长度和期望 page size
     */
    public DataFileCorruptedException(String message) {
        super(message);
    }

    /**
     * 使用文件几何诊断信息及原始校验原因创建损坏异常。
     *
     * @param message 应包含文件路径和无法建立页边界的原因
     * @param cause 底层长度读取或格式校验失败原因；不得丢弃
     */
    public DataFileCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
