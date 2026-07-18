package cn.zhangyis.db.sql.executor.storage;

import cn.zhangyis.db.common.exception.DatabaseValidationException;

/** 已确认提交结果；transactionNumber=0 表示只读或未首写事务。
 *
 * @param transactionNumber 提交结果关联的事务序号；必须为当前实例分配的非负单调值，不能冒用其他事务身份
 * @param durable 资源是否处于删除、空闲、静默、持久化或终态；必须与权威状态机一致，不能由调用方猜测
 * @param releasedLockCount 调用方请求的长度、数量或容量；必须非负、满足格式上界且不能导致算术溢出
 */
public record SqlCommitOutcome(long transactionNumber, boolean durable, int releasedLockCount) {
    public SqlCommitOutcome {
        if (transactionNumber < 0 || releasedLockCount < 0) {
            throw new DatabaseValidationException("invalid SQL commit outcome counters");
        }
    }
}
