package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * undo 日志物理格式损坏：undo record 解码越界/字段不符、recordAt offset 出 record area、RollPointer 与页不符、
 * 打开的页信封类型非 UNDO。属高风险数据一致性问题，不能静默跳过（设计 §10）。
 */
public class UndoLogFormatException extends DatabaseRuntimeException {
    /**
     * 创建 {@code UndoLogFormatException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     */
    public UndoLogFormatException(String message) { super(message); }
    /**
     * 创建 {@code UndoLogFormatException}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param message 包含领域上下文的诊断信息；不得为空白，也不能替代原始异常原因
     * @param cause 需要分类或包装的原始失败；不得为 {@code null}，包装时必须保留 cause 与 suppressed 异常图
     */
    public UndoLogFormatException(String message, Throwable cause) { super(message, cause); }
}
