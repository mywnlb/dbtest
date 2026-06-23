package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseRuntimeException;

/**
 * undo record 放不下当前 undo 页（写页前判定，不留半改页）。调用方可分配新 undo 页重试（多页链 T1.3b 接入）。
 */
public class UndoPageOverflowException extends DatabaseRuntimeException {
    public UndoPageOverflowException(String message) { super(message); }
    public UndoPageOverflowException(String message, Throwable cause) { super(message, cause); }
}
