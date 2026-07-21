package cn.zhangyis.db.engine.xa;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.engine.recovery.DatabaseInstanceFileLock;
import cn.zhangyis.db.domain.XaId;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * XA 离线检查与最终裁决工具。每次调用只取得 instance lock 并打开 `mysql.xa`，
 * 不打开 catalog、StorageEngine、Session 或任何用户表空间。
 */
public final class XaRecoveryMaintenance {

    /**
     * 离线读取全部未完成 XA branch。
     *
     * @param baseDir 数据库实例根目录
     * @param lockTimeout 获取 instance lock 的正等待上限
     * @return 按 registry sequence 排序的未完成快照
     */
    public List<XaRegistryEntry> inspect(Path baseDir, Duration lockTimeout) {
        validate(baseDir, lockTimeout);
        try (DatabaseInstanceFileLock ignored = DatabaseInstanceFileLock.acquire(baseDir, lockTimeout);
             FileXaRegistry registry = FileXaRegistry.openOrCreate(baseDir)) {
            return registry.pendingEntries();
        }
    }

    /**
     * 离线为 PREPARED 写入不可逆决议；PREPARING 只允许回滚。该方法不执行 storage phase two，
     * 后续普通启动会消费决议并在成功后写 COMPLETED。
     *
     * @param baseDir 数据库实例根目录
     * @param lockTimeout 获取 instance lock 的正等待上限
     * @param xid 待裁决 branch
     * @param commit true 表示提交；false 表示回滚
     * @return durable decision entry
     */
    public XaRegistryEntry decide(Path baseDir, Duration lockTimeout, XaId xid, boolean commit) {
        validate(baseDir, lockTimeout);
        if (xid == null) {
            throw new DatabaseValidationException("offline XA decision xid must not be null");
        }
        try (DatabaseInstanceFileLock ignored = DatabaseInstanceFileLock.acquire(baseDir, lockTimeout);
             FileXaRegistry registry = FileXaRegistry.openOrCreate(baseDir)) {
            XaRegistryEntry current = registry.find(xid)
                    .orElseThrow(() -> new XaException("XA XID is unknown: " + xid));
            if (current.state() == XaRegistryState.PREPARING && commit) {
                throw new XaException("PREPARING XA branch can only be rolled back offline: " + xid);
            }
            if (current.state() != XaRegistryState.PREPARING
                    && current.state() != XaRegistryState.PREPARED
                    && current.state() != XaRegistryState.COMMIT_DECIDED
                    && current.state() != XaRegistryState.ROLLBACK_DECIDED) {
                throw new XaException("XA branch cannot be decided from state " + current.state());
            }
            return commit
                    ? registry.decideCommit(xid, current.transactionId())
                    : registry.decideRollback(xid, current.transactionId());
        }
    }

    private static void validate(Path baseDir, Duration lockTimeout) {
        if (baseDir == null || lockTimeout == null
                || lockTimeout.isZero() || lockTimeout.isNegative()) {
            throw new DatabaseValidationException(
                    "offline XA maintenance requires base directory and positive lock timeout");
        }
    }
}
