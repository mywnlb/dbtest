package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.domain.TableDefinition;
import cn.zhangyis.db.dd.exception.MetadataLockTimeoutException;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.dd.service.TableMetadataLease;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 单条语句的 metadata staging guard。openTable 复用事务 lease 或暂存新 lease；publish 转移 owner，未 publish 的
 * close 是 abort，反序关闭本语句新取得的资源，不触碰事务既有 lease。
 */
public final class StatementBindingScope implements AutoCloseable {
    private final TransactionMetadataScope transaction;
    private final long deadline;
    private final LinkedHashMap<String, StagedLease> staged = new LinkedHashMap<>();
    private State state = State.OPEN;

    StatementBindingScope(TransactionMetadataScope transaction, long deadline) {
        this.transaction = transaction;
        this.deadline = deadline;
    }

    /** 按 canonical table key 获取 exact TableDefinition；READ→WRITE 先取得新 lease，失败时保留旧 READ。 */
    public TableDefinition openTable(QualifiedTableName name, TableAccessIntent intent) {
        ensureOpen();
        if (name == null || intent == null) {
            throw new SqlBindingException("table name/access intent must not be null");
        }
        String key = name.canonicalKey();
        StagedLease local = staged.get(key);
        if (local != null && covers(local.intent(), intent)) return local.lease().table();

        TransactionMetadataScope.HeldLease existing = transaction.lookup(key);
        if (existing != null && covers(existing.intent(), intent)) return existing.lease().table();

        TableMetadataLease acquired = transaction.acquire(name, intent, remaining());
        StagedLease replacedLocal = staged.put(key, new StagedLease(key, intent, acquired));
        if (replacedLocal != null) replacedLocal.lease().close();
        return acquired.table();
    }

    /** 将 staged leases 一次转交 transaction；重复发布或发布后继续 bind 都是状态错误。 */
    public void publish() {
        ensureOpen();
        List<StagedLease> leases = List.copyOf(staged.values());
        try {
            transaction.publish(leases);
        } finally {
            // publish 要么事务接管，要么 transaction.publish 已关闭 orphan；本 scope 不再拥有这些 lease。
            staged.clear();
            state = State.PUBLISHED;
        }
    }

    /** bind 失败的 abort 路径；只关闭 staged lease，复用的事务 lease 不在此集合中。 */
    @Override
    public void close() {
        if (state != State.OPEN) return;
        state = State.ABORTED;
        RuntimeException failure = null;
        List<TableMetadataLease> leases = new ArrayList<>();
        for (StagedLease entry : staged.values()) leases.add(entry.lease());
        staged.clear();
        for (int i = leases.size() - 1; i >= 0; i--) {
            try { leases.get(i).close(); }
            catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
            }
        }
        if (failure != null) throw failure;
    }

    private Duration remaining() {
        long nanos = deadline == Long.MAX_VALUE ? Long.MAX_VALUE : deadline - System.nanoTime();
        if (nanos <= 0) throw new MetadataLockTimeoutException("statement metadata deadline expired");
        return Duration.ofNanos(nanos);
    }

    private void ensureOpen() {
        if (state != State.OPEN) throw new SqlBindingException("statement binding scope is no longer open: " + state);
    }

    private static boolean covers(TableAccessIntent held, TableAccessIntent requested) {
        return held == TableAccessIntent.WRITE || requested == TableAccessIntent.READ;
    }

    record StagedLease(String key, TableAccessIntent intent, TableMetadataLease lease) { }
    private enum State { OPEN, PUBLISHED, ABORTED }
}
