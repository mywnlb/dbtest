package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.DictionaryDdlService;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.executor.storage.SqlDdlGateway;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL DDL port 到数据字典协调器的组合根 adapter。每条语句使用独立 owner，结束后由 coordinator ticket
 * try-with-resources 释放，不与 Session transaction-duration MDL 形成可重入或自锁。
 */
public final class DefaultSqlDdlGateway implements SqlDdlGateway {

    private final DictionaryDdlService ddl;
    private final AtomicLong statementSequence = new AtomicLong();

    public DefaultSqlDdlGateway(DictionaryDdlService ddl) {
        if (ddl == null) {
            throw new DatabaseValidationException("SQL DDL gateway coordinator must not be null");
        }
        this.ddl = ddl;
    }

    @Override
    public void createSecondaryIndex(BoundCreateIndex statement, Duration timeout) {
        if (statement == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SQL CREATE INDEX requires statement/positive timeout");
        }
        ddl.createSecondaryIndex(
                MdlOwnerId.forDdlStatement(statementSequence.incrementAndGet()),
                statement.command(), timeout);
    }
}
