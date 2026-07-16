package cn.zhangyis.db.dd.domain;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/**
 * MDL owner 身份；独立于 session 包，使 DD 内核不反向依赖 session 生命周期。低半区由显式 DD/DDL 调用方使用，
 * 高半区由 {@link #forSession(long)} 专门编码 Session owner，避免相同数字的外部 DDL owner 被误判为可重入 Session。
 */
public record MdlOwnerId(long value) {

    /** Session 专用命名空间起点；普通 {@link #of(long)} 永远不能构造该区间。 */
    private static final long SESSION_NAMESPACE_BASE = 1L << 62;

    public MdlOwnerId {
        if (value <= 0) {
            throw new DatabaseValidationException("metadata lock owner id must be positive: " + value);
        }
    }

    /** 创建普通 DD/DDL owner；高半区保留给 Session，调用方不能通过猜数字与 Session 锁合并。 */
    public static MdlOwnerId of(long value) {
        if (value >= SESSION_NAMESPACE_BASE) {
            throw new DatabaseValidationException("metadata lock owner id enters reserved session namespace: " + value);
        }
        return new MdlOwnerId(value);
    }

    /**
     * 把实例内 SessionId 映射到不可与普通 DDL owner 碰撞的高半区。SessionId 仍由 session 模块管理，DD 只接收数值。
     */
    public static MdlOwnerId forSession(long sessionId) {
        if (sessionId <= 0 || sessionId > Long.MAX_VALUE - SESSION_NAMESPACE_BASE) {
            throw new DatabaseValidationException("session id cannot be represented in MDL owner namespace: "
                    + sessionId);
        }
        return new MdlOwnerId(SESSION_NAMESPACE_BASE + sessionId);
    }

    /** DDL facade 用于拒绝冒充 Session 的显式 owner。 */
    public boolean sessionOwner() {
        return value >= SESSION_NAMESPACE_BASE;
    }
}
