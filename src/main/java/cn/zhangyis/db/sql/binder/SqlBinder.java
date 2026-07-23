package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.sql.binder.bound.BoundRelationalStatement;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;
import cn.zhangyis.db.sql.parser.ast.StatementNode;

/**
 * Parser AST 到纯语义 Bound IR 的稳定边界。实现不得选择索引、构造物理 range 或发布 metadata scope。
 */
public interface SqlBinder {

    /**
     * 在调用方持有的 statement metadata scope 内完成名称解析与类型转换。
     *
     * @param statement Parser 产生的数据访问语句
     * @param context 当前 schema、时区与未发布 metadata scope
     * @return 不含访问路径的不可变语义 Bound IR
     * @throws SqlBindingException 名称、类型、SQL shape 或 metadata lease 无法绑定时抛出；
     *         context 中的 scope 仍归调用方 abort/close
     */
    BoundRelationalStatement bind(StatementNode statement, SqlBindingContext context);
}
