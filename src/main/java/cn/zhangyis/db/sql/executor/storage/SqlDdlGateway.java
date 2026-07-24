package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.binder.bound.BoundCreateTable;
import cn.zhangyis.db.sql.binder.bound.BoundDropIndex;
import cn.zhangyis.db.sql.binder.bound.BoundAlterTablespace;
import cn.zhangyis.db.sql.binder.bound.BoundAlterTable;
import cn.zhangyis.db.sql.binder.bound.BoundCreateSchema;
import cn.zhangyis.db.sql.binder.bound.BoundDropSchema;
import cn.zhangyis.db.sql.binder.bound.BoundDropTables;
import cn.zhangyis.db.sql.binder.exception.UnsupportedSqlShapeException;
import cn.zhangyis.db.sql.executor.SqlWarning;

import java.time.Duration;
import java.util.List;

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
     * 执行已绑定的 CREATE TABLE。默认实现明确拒绝，使未注入生产组合根的组件测试不能伪造建表成功。
     *
     * @param statement 名称、列/default 与索引已规范化，但尚未分配 DD/物理 identity 的命令
     * @param timeout statement 剩余正有界时间；MDL、redo durable、SDI 与 catalog force 共用该预算
     */
    default void createTable(BoundCreateTable statement, Duration timeout) {
        throw new UnsupportedSqlShapeException(
                "SQL CREATE TABLE gateway is not configured");
    }

    /**
     * 执行携带 IF NOT EXISTS 语义的 CREATE TABLE 并返回 warning。
     */
    default List<SqlWarning> createTableWithWarnings(
            BoundCreateTable statement, Duration timeout) {
        createTable(statement, timeout);
        return List.of();
    }

    /**
     * 执行 CREATE SCHEMA/DATABASE。
     */
    default List<SqlWarning> createSchema(
            BoundCreateSchema statement, Duration timeout) {
        throw new UnsupportedSqlShapeException(
                "SQL CREATE SCHEMA gateway is not configured");
    }

    /**
     * 以一个 DDL 原子边界执行多目标 DROP TABLE。
     */
    default List<SqlWarning> dropTables(
            BoundDropTables statement, Duration timeout) {
        throw new UnsupportedSqlShapeException(
                "SQL DROP TABLE gateway is not configured");
    }

    /**
     * 执行 DROP SCHEMA/DATABASE 及其表集合。
     */
    default List<SqlWarning> dropSchema(
            BoundDropSchema statement, Duration timeout) {
        throw new UnsupportedSqlShapeException(
                "SQL DROP SCHEMA gateway is not configured");
    }

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
     * 执行通用 ALTER。DD coordinator负责选择instant metadata、单索引在线协议或有明确能力缺口的
     * blocking fallback；gateway不得在SQL层拆分action list。
     *
     * @param statement 已完成纯 SQL 类型/default 校验的 DD command
     * @param timeout statement 剩余正有界时间
     */
    default void alterTable(BoundAlterTable statement, Duration timeout) {
        throw new UnsupportedSqlShapeException("SQL ALTER TABLE gateway is not configured");
    }
}
