package cn.zhangyis.db.engine.adapter;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.ddl.DictionaryDdlService;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.sql.binder.bound.BoundCreateIndex;
import cn.zhangyis.db.sql.binder.bound.BoundCreateTable;
import cn.zhangyis.db.sql.binder.bound.BoundDropIndex;
import cn.zhangyis.db.sql.binder.bound.BoundAlterTablespace;
import cn.zhangyis.db.sql.binder.bound.BoundAlterTable;
import cn.zhangyis.db.sql.binder.bound.BoundCreateSchema;
import cn.zhangyis.db.sql.binder.bound.BoundDropSchema;
import cn.zhangyis.db.sql.binder.bound.BoundDropTables;
import cn.zhangyis.db.sql.executor.SqlWarning;
import cn.zhangyis.db.sql.executor.storage.SqlDdlGateway;
import cn.zhangyis.db.dd.exception.DictionaryObjectExistsException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL DDL port 到数据字典协调器的组合根 adapter。每条语句使用独立 owner，结束后由 coordinator ticket
 * try-with-resources 释放，不与 Session transaction-duration MDL 形成可重入或自锁。
 */
public final class DefaultSqlDdlGateway implements SqlDdlGateway {

    /**
     * 本对象持有的 {@code ddl} 模块协作者；由组合根注入或在受控启动阶段创建，生命周期覆盖本对象且不得绕过其稳定接口访问下层状态。
     */
    private final DictionaryDdlService ddl;
    /**
     * 跨线程发布的权威状态或计数；更新只允许通过本类定义的原子状态转换完成。
     */
    private final AtomicLong statementSequence = new AtomicLong();

    /**
     * 创建 {@code DefaultSqlDdlGateway}；先校验并保存构造参数，成功后对象处于可用初始状态，失败时不发布半初始化实例。
     *
     * @param ddl 由组合根提供的 {@code DictionaryDdlService} 协作者；不得为 {@code null}，其生命周期必须覆盖本次 {@code 构造} 调用
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    public DefaultSqlDdlGateway(DictionaryDdlService ddl) {
        if (ddl == null) {
            throw new DatabaseValidationException("SQL DDL gateway coordinator must not be null");
        }
        this.ddl = ddl;
    }

    /**
     * 将 SQL CREATE TABLE 命令交给现有原子 DD coordinator。独立 owner 的生命周期完全位于本次
     * coordinator 调用内，不复用刚被 implicit commit 终结的用户事务 owner。
     *
     * @param statement Binder 已完成纯输入校验且不持资源的建表命令
     * @param timeout statement 剩余正有界时间；超时不得由 adapter 自行删除物理文件或 marker
     * @throws DatabaseValidationException 参数缺失或 timeout 非正时抛出，且不进入 DD
     */
    @Override
    public void createTable(BoundCreateTable statement, Duration timeout) {
        if (statement == null || timeout == null
                || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "SQL CREATE TABLE requires statement/positive timeout");
        }
        ddl.createTable(
                MdlOwnerId.forDdlStatement(
                        statementSequence.incrementAndGet()),
                statement.command(), timeout);
    }

    /**
     * 在 coordinator 的 table X 重验后，仅把“表已存在”转换为 1050 warning。
     */
    @Override
    public List<SqlWarning> createTableWithWarnings(
            BoundCreateTable statement, Duration timeout) {
        try {
            createTable(statement, timeout);
            return List.of();
        } catch (DictionaryObjectExistsException exists) {
            if (!statement.ifNotExists()) {
                throw exists;
            }
            return List.of(new SqlWarning(
                    1050, "42S01",
                    "Table already exists: "
                            + statement.command().name().canonicalKey()));
        }
    }

    /**
     * 创建默认 charset/collation 为当前教学实例 1/1 的 schema；重复对象只在 IF 分支降为 warning。
     */
    @Override
    public List<SqlWarning> createSchema(
            BoundCreateSchema statement, Duration timeout) {
        require(statement, timeout, "CREATE SCHEMA");
        try {
            ddl.createSchema(
                    nextOwner(), statement.name(), 1, 1, timeout);
            return List.of();
        } catch (DictionaryObjectExistsException exists) {
            if (!statement.ifNotExists()) {
                throw exists;
            }
            return List.of(new SqlWarning(
                    1007, "HY000",
                    "Schema already exists: "
                            + statement.name().canonicalName()));
        }
    }

    /**
     * 把完整目标列表一次交给 DD batch coordinator，并将缺失目标映射为 1051 warning。
     */
    @Override
    public List<SqlWarning> dropTables(
            BoundDropTables statement, Duration timeout) {
        require(statement, timeout, "DROP TABLE");
        return ddl.dropTables(
                        nextOwner(), statement.tables(),
                        statement.ifExists(), timeout)
                .stream()
                .map(name -> new SqlWarning(
                        1051, "42S02",
                        "Unknown table: " + name.canonicalKey()))
                .toList();
    }

    /**
     * 执行 schema tombstone 与级联表删除；IF EXISTS 缺失时返回 1008 warning。
     */
    @Override
    public List<SqlWarning> dropSchema(
            BoundDropSchema statement, Duration timeout) {
        require(statement, timeout, "DROP SCHEMA");
        boolean dropped = ddl.dropSchema(
                nextOwner(), statement.name(),
                statement.ifExists(), timeout);
        return dropped ? List.of() : List.of(new SqlWarning(
                1008, "HY000",
                "Schema does not exist: "
                        + statement.name().canonicalName()));
    }

    /**
     * 根据调用参数构造 {@code createSecondaryIndex} 对应的SQL 与存储引擎适配层领域对象；构造前完成范围与组合校验，成功结果不为 {@code null}。
     *
     * @param statement 调用方请求的目标状态、阶段或模式；不得为 {@code null}，且必须是当前状态机允许的后继值
     * @param timeout 本次等待或操作的最大时长；不得为 {@code null} 且必须为正，超时不得留下未释放资源
     * @throws DatabaseValidationException 输入、配置或持久格式不满足本方法约束时抛出；调用方应修正输入，恢复流程中则应停止消费该证据
     */
    @Override
    public void createSecondaryIndex(BoundCreateIndex statement, Duration timeout) {
        if (statement == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SQL CREATE INDEX requires statement/positive timeout");
        }
        ddl.createSecondaryIndex(
                MdlOwnerId.forDdlStatement(statementSequence.incrementAndGet()),
                statement.command(), timeout);
    }

    /**
     * 将 DROP INDEX bound command 交给使用独立 owner 的 DD coordinator。
     *
     * @param statement 已完成逻辑名称规范化、尚未持有 DD/物理资源的命令
     * @param timeout statement 剩余正有界时间；MDL、history、pin、WAL 与 force 共用该预算
     * @throws DatabaseValidationException 参数缺失或 timeout 非正时抛出，且不进入 DD
     */
    @Override
    public void dropSecondaryIndex(BoundDropIndex statement, Duration timeout) {
        if (statement == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("SQL DROP INDEX requires statement/positive timeout");
        }
        ddl.dropSecondaryIndex(
                MdlOwnerId.forDdlStatement(statementSequence.incrementAndGet()),
                statement.command(), timeout);
    }

    /**
     * 把受控表空间生命周期动作交给 DD；路径和 page0 identity 均由实例组合根内的 coordinator
     * 计算，SQL 不能提交任意主机路径。
     *
     * @param statement 已补全 schema 的 DISCARD/IMPORT 意图
     * @param timeout 本条语句剩余正有界时间
     */
    @Override
    public void alterTablespace(BoundAlterTablespace statement, Duration timeout) {
        if (statement == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "SQL ALTER TABLESPACE requires statement/positive timeout");
        }
        MdlOwnerId owner = MdlOwnerId.forDdlStatement(statementSequence.incrementAndGet());
        switch (statement.action()) {
            case DISCARD -> ddl.discardTablespace(owner, statement.table(), timeout);
            case IMPORT -> ddl.importTablespace(owner, statement.table(), timeout);
        }
    }

    /**
     * 用独立 statement owner 执行一次通用 ALTER；一个 bound command 只调用一次 coordinator，
     * 不能把多个 action 拆成可被其它 DDL 插入的多次提交。
     *
     * @param statement 保序 ALTER command
     * @param timeout 本条语句剩余正有界时间
     */
    @Override
    public void alterTable(BoundAlterTable statement, Duration timeout) {
        if (statement == null || timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "SQL ALTER TABLE requires statement/positive timeout");
        }
        ddl.alterTable(
                MdlOwnerId.forDdlStatement(statementSequence.incrementAndGet()),
                statement.command(), timeout);
    }

    private MdlOwnerId nextOwner() {
        return MdlOwnerId.forDdlStatement(
                statementSequence.incrementAndGet());
    }

    private static void require(
            Object statement, Duration timeout, String operation) {
        if (statement == null || timeout == null
                || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException(
                    "SQL " + operation
                            + " requires statement/positive timeout");
        }
    }
}
