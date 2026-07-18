package cn.zhangyis.db.session;

import cn.zhangyis.db.sql.executor.SqlExecutionResult;

/** 进程内 SQL Session 公开 API。 */
public interface SqlSession extends AutoCloseable {
    /**
     * 返回该会话在注册表和锁观测中的稳定身份；身份在会话关闭前后均不改变，调用方不取得会话生命周期所有权。
     *
     * @return {@code id} 定位或分配的稳定值对象；成功时不为 {@code null}，其身份、范围和特殊值已由构造校验保证
     */
    SessionId id();
    /**
     * 执行已校验的领域命令并协调下游接口；成功返回可观察结果，失败保留原始异常与事务边界。
     *
     * @param sql 传给 {@code execute} 的文本值；不得为 {@code null} 或空白，并保持调用方提供的字符顺序
     * @return {@code execute} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    SqlExecutionResult execute(String sql);
    /**
     * 采集 {@code snapshot} 对应的会话与事务边界稳定快照；返回对象与后续内部修改隔离，不转移内部可变状态的所有权。
     *
     * @return {@code snapshot} 的不可变领域结果或状态快照；包含已完成动作、剩余工作及失败边界，成功时不为 {@code null}
     */
    SessionSnapshot snapshot();
    /**
     * 释放本方法拥有的会话与事务边界资源；遵守既定释放顺序，重复或失败调用不得掩盖原始状态。
     */
    @Override void close();
}
