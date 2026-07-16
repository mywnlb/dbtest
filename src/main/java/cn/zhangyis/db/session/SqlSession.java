package cn.zhangyis.db.session;

import cn.zhangyis.db.sql.executor.SqlExecutionResult;

/** 进程内 SQL Session 公开 API。 */
public interface SqlSession extends AutoCloseable {
    SessionId id();
    SqlExecutionResult execute(String sql);
    SessionSnapshot snapshot();
    @Override void close();
}
