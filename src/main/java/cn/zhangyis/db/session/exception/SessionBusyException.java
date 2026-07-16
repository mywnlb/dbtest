package cn.zhangyis.db.session.exception;

/** 同一 Session 已有 execute/close，占用超过 statement timeout。 */
public final class SessionBusyException extends SessionStateException {
    public SessionBusyException(String message) { super(message); }
    public SessionBusyException(String message, Throwable cause) { super(message, cause); }
}
