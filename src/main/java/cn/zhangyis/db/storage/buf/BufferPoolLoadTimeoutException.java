package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * 等待某页载入（LOADING）完成超时或被中断。表达"有界等待"约束：命中正在载入的页时，等待者不会无限期阻塞，
 * 到达配置的 load 超时或被中断即抛出本异常，调用方可重试或上报，绝不悬挂（设计 §7.3 IO owner 完成/失败语义）。
 */
public final class BufferPoolLoadTimeoutException extends DatabaseRuntimeException {

    public BufferPoolLoadTimeoutException(String message) {
        super(message);
    }

    public BufferPoolLoadTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
