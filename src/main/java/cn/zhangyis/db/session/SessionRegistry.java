package cn.zhangyis.db.session;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.session.exception.SessionStateException;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DatabaseEngine 拥有的 Session 目录。CHM 操作只注册/注销引用，不在 map 临界区执行 rollback 或用户回调；
 * close 先复制稳定快照，再在目录之外并发请求 Session cooperative close。
 */
public final class SessionRegistry {
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private final AtomicLong nextId = new AtomicLong(1);
    /**
     * 本对象拥有的 {@code sessions} 受控集合；元素生命周期与外层对象一致，仅由本类方法更新，对外暴露时必须返回副本或不可变视图。
     */
    private final ConcurrentHashMap<SessionId, SqlSession> sessions = new ConcurrentHashMap<>();

    /** 预留一个不复用的实例内身份；溢出时 fail-closed。
     *
     * @return {@code nextId} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public SessionId nextId() {
        long value = nextId.getAndIncrement();
        if (value <= 0) throw new SessionStateException("session id range exhausted");
        return SessionId.of(value);
    }

    /** 注册已完整构造的 Session；rollback/begin 失败的半对象不得进入目录。
     *
     * @param session SQL 解析、绑定或执行链路提供的语句、值或会话上下文；不得为 {@code null}，必须属于当前语句及会话的同一次执行
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     * @throws SessionStateException 当前生命周期、版本或所有权与请求不一致时抛出；调用方应重新读取权威状态后回滚或重试
     */
    public void register(SqlSession session) {
        if (session == null) throw new DatabaseValidationException("registered session must not be null");
        if (sessions.putIfAbsent(session.id(), session) != null) {
            throw new SessionStateException("duplicate session id " + session.id().value());
        }
    }

    /** Session close callback 使用的幂等注销；不调用 Session.close，避免递归。
     *
     * @param id 参与 {@code deregister} 的稳定领域标识 {@code SessionId}；不得为 {@code null}，并须由对应值对象构造校验产生
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public void deregister(SessionId id) {
        if (id == null) throw new DatabaseValidationException("deregistered session id must not be null");
        sessions.remove(id);
    }

    /** 按 id 排序的不可变 close 快照，便于确定性诊断；复制时不执行外部代码。 */
    public List<SqlSession> snapshot() {
        return sessions.values().stream().sorted(Comparator.comparingLong(session -> session.id().value())).toList();
    }

    public int size() { return sessions.size(); }
}
