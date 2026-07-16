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
    private final AtomicLong nextId = new AtomicLong(1);
    private final ConcurrentHashMap<SessionId, SqlSession> sessions = new ConcurrentHashMap<>();

    /** 预留一个不复用的实例内身份；溢出时 fail-closed。 */
    public SessionId nextId() {
        long value = nextId.getAndIncrement();
        if (value <= 0) throw new SessionStateException("session id range exhausted");
        return SessionId.of(value);
    }

    /** 注册已完整构造的 Session；rollback/begin 失败的半对象不得进入目录。 */
    public void register(SqlSession session) {
        if (session == null) throw new DatabaseValidationException("registered session must not be null");
        if (sessions.putIfAbsent(session.id(), session) != null) {
            throw new SessionStateException("duplicate session id " + session.id().value());
        }
    }

    /** Session close callback 使用的幂等注销；不调用 Session.close，避免递归。 */
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
