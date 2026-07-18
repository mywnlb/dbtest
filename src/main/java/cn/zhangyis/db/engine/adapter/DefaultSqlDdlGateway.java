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
}
