package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 开启 SQL transaction 的稳定请求；不出现 storage 枚举。
 *
 * @param isolationLevel 调用方当前事务及其一致性视图或保存点状态；不得为 {@code null}，事务必须由当前会话拥有且处于本操作允许的生命周期阶段
 * @param readOnly 资源的访问模式；写模式允许受控修改，读模式禁止产生 dirty、redo 或元数据发布副作用
 * @param autocommit 会话事务生命周期标志；必须反映权威事务状态，决定语句后提交、仅回滚或是否存在活跃事务
 */
public record SqlTransactionRequest(SqlIsolationLevel isolationLevel, boolean readOnly, boolean autocommit) {
    public SqlTransactionRequest {
        if (isolationLevel == null) throw new DatabaseValidationException("SQL isolation level must not be null");
    }
}
