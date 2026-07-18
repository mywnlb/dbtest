package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.binder.exception.UnsupportedSqlShapeException;

import java.time.Duration;

/**
 * SQL Session 到 DD coordinator 的 DDL port。该接口不暴露 page、segment、MTR 或 DDL log。
 */
@FunctionalInterface
public interface SqlDdlGateway {

    /** 未注入 DDL composition root 的组件测试默认明确拒绝，不静默伪造成功。 */
    SqlDdlGateway UNSUPPORTED = (statement, timeout) -> {
        throw new UnsupportedSqlShapeException("SQL DDL gateway is not configured");
    };

    /**
     * 执行已绑定的 CREATE INDEX；实现必须使用独立 DDL MDL owner，不能复用 Session transaction owner。
     *
     * @param statement schema/name 已规范化、但未持有 DD lease 的命令
     * @param timeout statement deadline 剩余的正有界时间
     */
    void createSecondaryIndex(BoundCreateIndex statement, Duration timeout);
}
