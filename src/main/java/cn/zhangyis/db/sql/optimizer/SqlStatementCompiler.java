package cn.zhangyis.db.sql.optimizer;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.sql.binder.SqlBindingContext;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;
import cn.zhangyis.db.sql.optimizer.exception.SqlOptimizationException;
import cn.zhangyis.db.sql.optimizer.physical.PhysicalPlan;
import cn.zhangyis.db.sql.parser.ast.StatementNode;

/**
 * 一条数据访问 SQL 从 AST 到 PhysicalPlan 的编译边界。Session 只在该边界成功返回后发布 metadata scope。
 */
public interface SqlStatementCompiler {

    /**
     * 完成 semantic binding、logical conversion 和 physical optimization。
     *
     * @param statement Parser 产生的数据访问 AST
     * @param context 当前 schema、时区及未发布 statement metadata scope
     * @return 可由 Executor 消费的不可变物理计划
     * @throws DatabaseValidationException AST 或 context 缺失时抛出
     * @throws SqlBindingException 名称、类型或 metadata binding 失败时抛出
     * @throws SqlOptimizationException 逻辑树无法安全转换为物理访问路径时抛出
     */
    PhysicalPlan compile(StatementNode statement, SqlBindingContext context);
}
