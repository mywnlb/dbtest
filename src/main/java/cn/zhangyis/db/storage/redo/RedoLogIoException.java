package cn.zhangyis.db.storage.redo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * Redo 文件 IO 异常。表示 redo 文件打开、追加、force 或扫描时发生可诊断的运行时错误；
 * 调用方可关闭实例、重试启动恢复或向上报告，但不能把底层 IOException 丢失。
 */
public class RedoLogIoException extends DatabaseRuntimeException {

    /** 创建不包装底层异常、但带完整 redo IO 上下文的异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public RedoLogIoException(String message) {
        super(message);
    }

    /** 创建并保留底层 IO cause 的异常。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public RedoLogIoException(String message, Throwable cause) {
        super(message, cause);
    }
}
