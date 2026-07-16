package cn.zhangyis.db.sql.binder;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.MdlOwnerId;
import cn.zhangyis.db.dd.domain.QualifiedTableName;
import cn.zhangyis.db.dd.service.DataDictionaryService;
import cn.zhangyis.db.dd.service.TableAccessIntent;
import cn.zhangyis.db.dd.service.TableMetadataLease;
import cn.zhangyis.db.sql.binder.exception.SqlBindingException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQL transaction 的 DD pin/MDL owner。所有可变 held lease 状态由显式 lock 保护；Session 虽串行执行语句，
 * close/engine shutdown 仍可能竞争，因此不能依赖线程约定隐藏释放边界。
 */
public final class TransactionMetadataScope implements AutoCloseable {
    private final DataDictionaryService dictionary;
    private final MdlOwnerId owner;
    private final ReentrantLock lock = new ReentrantLock();
    /** insertion order 同时是获取顺序；事务终结按其反序关闭。 */
    private final LinkedHashMap<String, HeldLease> held = new LinkedHashMap<>();
    private boolean closed;

    public TransactionMetadataScope(DataDictionaryService dictionary, MdlOwnerId owner) {
        if (dictionary == null || owner == null) {
            throw new DatabaseValidationException("transaction metadata dictionary/owner must not be null");
        }
        this.dictionary = dictionary;
        this.owner = owner;
    }

    /** 创建共享一个绝对 deadline 的 statement staging scope；等待 MDL 前本对象不持有内部 lock。 */
    public StatementBindingScope beginStatement(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new DatabaseValidationException("statement metadata timeout must be positive");
        }
        lock.lock();
        try {
            ensureOpen();
            return new StatementBindingScope(this, deadline(timeout));
        } finally {
            lock.unlock();
        }
    }

    HeldLease lookup(String key) {
        lock.lock();
        try {
            ensureOpen();
            return held.get(key);
        } finally {
            lock.unlock();
        }
    }

    TableMetadataLease acquire(QualifiedTableName name, TableAccessIntent intent, Duration remaining) {
        return dictionary.openTable(owner, name, intent, remaining);
    }

    /**
     * 接收 statement staged lease。新 lease 先进入事务权威 map，再关闭被替换的 READ lease；这样 close 失败也不会
     * 让 WRITE lease 失去 owner。若事务已关闭，则本方法反序关闭 staged lease 后抛错，保证 shutdown 竞争不泄漏。
     */
    void publish(List<StatementBindingScope.StagedLease> staged) {
        RuntimeException failure = null;
        lock.lock();
        try {
            if (closed) {
                failure = new SqlBindingException("transaction metadata scope is already closed");
            } else {
                List<TableMetadataLease> replaced = new ArrayList<>();
                for (StatementBindingScope.StagedLease entry : staged) {
                    HeldLease previous = held.put(entry.key(), new HeldLease(entry.intent(), entry.lease()));
                    if (previous != null && previous.lease() != entry.lease()) replaced.add(previous.lease());
                }
                failure = closeReverse(replaced, failure);
                returnOrThrow(failure);
                return;
            }
        } finally {
            lock.unlock();
        }
        List<TableMetadataLease> orphaned = staged.stream().map(StatementBindingScope.StagedLease::lease).toList();
        failure = closeReverse(orphaned, failure);
        throw failure;
    }

    /** storage commit/rollback 完成后调用；先冻结 closed，再反序释放 pin 和 table/schema MDL。 */
    @Override
    public void close() {
        List<TableMetadataLease> leases;
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            leases = held.values().stream().map(HeldLease::lease).toList();
            held.clear();
        } finally {
            lock.unlock();
        }
        RuntimeException failure = closeReverse(leases, null);
        returnOrThrow(failure);
    }

    private void ensureOpen() {
        if (closed) throw new SqlBindingException("transaction metadata scope is already closed");
    }

    private static RuntimeException closeReverse(List<TableMetadataLease> leases, RuntimeException failure) {
        for (int i = leases.size() - 1; i >= 0; i--) {
            try {
                leases.get(i).close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure; else failure.addSuppressed(closeFailure);
            }
        }
        return failure;
    }

    private static void returnOrThrow(RuntimeException failure) {
        if (failure != null) throw failure;
    }

    private static long deadline(Duration timeout) {
        long nanos;
        try { nanos = timeout.toNanos(); }
        catch (ArithmeticException error) { throw new DatabaseValidationException("metadata timeout is too large", error); }
        long now = System.nanoTime();
        long result = now + nanos;
        return result < 0 && now > 0 ? Long.MAX_VALUE : result;
    }

    record HeldLease(TableAccessIntent intent, TableMetadataLease lease) { }
}
