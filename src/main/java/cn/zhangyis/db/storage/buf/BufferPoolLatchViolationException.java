package cn.zhangyis.db.storage.buf;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Buffer Pool 内部锁边界被破坏时抛出的领域异常。
 *
 * <p>该异常表示实现代码在持有 Buffer Pool 内部锁时进入了物理 IO、载入 future 等待或脏页淘汰 flush
 * 等可能阻塞路径。它不是用户数据损坏，而是内核并发不变量违规；调用方通常只能让当前操作失败并暴露诊断上下文。
 */
public final class BufferPoolLatchViolationException extends DatabaseRuntimeException {

    public BufferPoolLatchViolationException(String message) {
        super(message);
    }

    public BufferPoolLatchViolationException(String message, Throwable cause) {
        super(message, cause);
    }
}
