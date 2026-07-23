package cn.zhangyis.db.sql.optimizer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.binder.SqlBinder;
import cn.zhangyis.db.sql.binder.SqlBindingContext;
import cn.zhangyis.db.sql.binder.bound.BoundRelationalStatement;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.logical.BoundToLogicalConverter;
import cn.zhangyis.db.sql.optimizer.logical.LogicalPlan;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;
import cn.zhangyis.db.sql.parser.ast.StatementNode;

/**
 * M1 SQL 编译器，把三个可独立测试的阶段串成单向 pipeline；本类无共享可变状态，也不管理 metadata
 * scope 的发布和关闭。
 */
public final class DefaultSqlStatementCompiler implements SqlStatementCompiler {
    /** 只负责名称绑定和类型推导的语义 Binder。 */
    private final SqlBinder binder;
    /** 把扁平 semantic Bound 转换为关系树的无状态转换器。 */
    private final BoundToLogicalConverter logicalConverter;
    /** 独占访问路径和物理形状选择的优化器。 */
    private final QueryOptimizer optimizer;

    /**
     * 创建显式组合的 SQL compiler。
     *
     * @param binder 语义绑定器；不得发布 metadata scope
     * @param logicalConverter semantic Bound 到逻辑树的转换器
     * @param optimizer 逻辑树到物理计划的选择器
     * @throws DatabaseValidationException 任一协作者缺失时抛出
     */
    public DefaultSqlStatementCompiler(
            SqlBinder binder, BoundToLogicalConverter logicalConverter,
            QueryOptimizer optimizer) {
        if (binder == null || logicalConverter == null || optimizer == null) {
            throw new DatabaseValidationException(
                    "SQL compiler collaborators must not be null");
        }
        this.binder = binder;
        this.logicalConverter = logicalConverter;
        this.optimizer = optimizer;
    }

    /**
     * 顺序编译一条数据访问语句；任一阶段失败时不发布部分计划，metadata scope 仍由 Session 关闭。
     *
     * <p>数据流：</p>
     * <ol>
     *     <li>校验 AST 与 binding context，避免空输入触发 metadata 操作。</li>
     *     <li>Binder 在现有 scope 中解析名称和 typed value，只产生 semantic Bound IR。</li>
     *     <li>Converter 构造 scan/filter/project/values/table-modify 逻辑树，不选择访问路径。</li>
     *     <li>Optimizer 选择不可变物理计划并返回；本方法不 publish scope、不打开执行资源。</li>
     * </ol>
     *
     * @param statement Parser 产生的数据访问 AST
     * @param context 当前 schema、时区及未发布 statement metadata scope
     * @return 完整且可由 Executor 消费的物理计划
     * @throws DatabaseValidationException 输入缺失时抛出；metadata scope 未发生发布
     * @throws SqlBindingException 名称、类型或 metadata binding 失败时抛出；scope 仍归调用方
     * @throws SqlOptimizationException 逻辑树无法形成安全物理路径时抛出；scope 仍归调用方
     */
    @Override
    public PhysicalPlan compile(StatementNode statement, SqlBindingContext context) {
        // 1、纯输入错误必须早于 Binder 对 metadata scope 的访问。
        if (statement == null || context == null) {
            throw new DatabaseValidationException(
                    "compiler statement/context must not be null");
        }
        // 2、Bound IR 只表达 exact metadata 和 SQL 语义。
        BoundRelationalStatement bound = binder.bind(statement, context);
        // 3、关系树是规则和后续 memo/cost 演进的稳定中间边界。
        LogicalPlan logicalPlan = logicalConverter.convert(bound);
        // 4、成功结果只携带物理意图；scope 发布仍由调用 Session 原子控制。
        return optimizer.optimize(logicalPlan);
    }
}
