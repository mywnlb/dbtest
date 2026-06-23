package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * undo 日志物理格式损坏：undo record 解码越界/字段不符、recordAt offset 出 record area、RollPointer 与页不符、
 * 打开的页信封类型非 UNDO。属高风险数据一致性问题，不能静默跳过（设计 §10）。
 */
public class UndoLogFormatException extends DatabaseRuntimeException {
    public UndoLogFormatException(String message) { super(message); }
    public UndoLogFormatException(String message, Throwable cause) { super(message, cause); }
}
