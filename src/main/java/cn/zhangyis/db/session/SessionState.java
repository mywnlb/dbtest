package cn.zhangyis.db.session;

/** Session 生命周期；FAILED 只允许 close，CLOSING/CLOSED 拒绝新语句。 */
public enum SessionState { OPEN, EXECUTING, FAILED, CLOSING, CLOSED }
