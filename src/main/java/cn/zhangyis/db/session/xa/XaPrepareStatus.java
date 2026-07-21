package cn.zhangyis.db.session.xa;

/**
 * XA PREPARE 的会话级结果。
 */
public enum XaPrepareStatus {

    /** 写分支已持久进入 PREPARED，等待 phase two。 */
    PREPARED,
    /** 分支没有写入，已按只读优化普通提交且不进入 registry PREPARED。 */
    READ_ONLY
}
