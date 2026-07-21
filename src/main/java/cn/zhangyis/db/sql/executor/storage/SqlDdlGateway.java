package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.binder.bound.BoundDropIndex;
import cn.zhangyis.db.sql.binder.bound.BoundAlterTablespace;
import cn.zhangyis.db.sql.binder.bound.BoundAlterTable;
import cn.zhangyis.db.sql.binder.exception.UnsupportedSqlShapeException;

import java.time.Duration;

/**
 * SQL Session 到 DD coordinator 的 DDL port。该接口不暴露 page、segment、MTR 或 DDL log。
 */
public interface SqlDdlGateway {

    /** 未注入 DDL composition root 的组件测试默认明确拒绝，不静默伪造成功。 */
    SqlDdlGateway UNSUPPORTED = new SqlDdlGateway() {
        @Override
        public void createSecondaryIndex(BoundCreateIndex statement, Duration timeout) {
            throw new UnsupportedSqlShapeException("SQL DDL gateway is not configured");
        }

        @Override
        public void dropSecondaryIndex(BoundDropIndex statement, Duration timeout) {
            throw new UnsupportedSqlShapeException("SQL DDL gateway is not configured");
        }
    };

    /**
     * 执行已绑定的 CREATE INDEX；实现必须使用独立 DDL MDL owner，不能复用 Session transaction owner。
     *
     * @param statement schema/name 已规范化、但未持有 DD lease 的命令
     * @param timeout statement deadline 剩余的正有界时间
     */
    void createSecondaryIndex(BoundCreateIndex statement, Duration timeout);

    /**
     * 执行已绑定的 DROP INDEX；实现必须使用独立 DDL MDL owner，并由 DD coordinator 决定提交与物理回收。
     *
     * @param statement schema/table/index name 已规范化、但未持有 DD lease 的命令
     * @param timeout statement deadline 剩余的正有界时间
     */
    void dropSecondaryIndex(BoundDropIndex statement, Duration timeout);

    /**
     * 执行 DISCARD/IMPORT TABLESPACE。默认实现明确拒绝，使旧组件测试的轻量 gateway 无需伪造文件副作用。
     *
     * @param statement 已限定表名的生命周期动作
     * @param timeout statement 剩余正有界时间
     */
    default void alterTablespace(BoundAlterTablespace statement, Duration timeout) {
        throw new UnsupportedSqlShapeException("SQL ALTER TABLESPACE gateway is not configured");
    }

    /**
     * 执行按用户顺序 staged 的通用阻塞式 ALTER。
     *
     * @param statement 已完成纯 SQL 类型/default 校验的 DD command
     * @param timeout statement 剩余正有界时间
     */
    default void alterTable(BoundAlterTable statement, Duration timeout) {
        throw new UnsupportedSqlShapeException("SQL ALTER TABLE gateway is not configured");
    }
}
